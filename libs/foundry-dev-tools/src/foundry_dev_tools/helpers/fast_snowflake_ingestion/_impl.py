"""Fast Snowflake to Foundry ingestion via Snowpark, for Foundry lightweight transforms."""

from __future__ import annotations

import json
import logging
import secrets
import shutil
import string
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

from foundry_dev_tools._optional.snowflake import snowflake_errors, snowpark_exceptions
from foundry_dev_tools.helpers.fast_snowflake_ingestion.utils import (
    filter_kwargs_for_function,
    get_output_foundry_path_components,
    prepare_job_info,
)

if TYPE_CHECKING:
    import snowflake.snowpark as snow
    from snowflake.connector.s3_storage_client import SnowflakeS3RestClient

    from transforms.api import Output

logger = logging.getLogger(__name__)

QUERY_TAG_MAX_LENGTH = 2000


def _get_bucket_accelerate_config_patch(self: SnowflakeS3RestClient, bucket_name: str) -> bool:  # noqa: ARG001
    return False


def _transfer_accelerate_config_patch(
    self: SnowflakeS3RestClient,
    use_accelerate_endpoint: bool | None = None,  # noqa: ARG001
) -> bool:
    self.endpoint = f"https://{self.s3location.bucket_name}.s3.{self.region_name}.amazonaws.com"
    return False


def disable_s3_transfer_acceleration() -> None:
    """Forces the regional S3 endpoint in the Snowflake connector, by monkeypatching ``SnowflakeS3RestClient``.

    Workaround for https://github.com/snowflakedb/snowflake-connector-python/issues/2317.

    Problem: unless the server sends ``useS3RegionalUrl``/``useRegionalUrl`` in the stage info,
    the connector uses the global bucket endpoint ``https://<bucket>.s3.amazonaws.com`` for internal stages.
    Connector versions before 4.0.0 additionally call ``https://<bucket>.s3.amazonaws.com/?accelerate``
    to check whether S3 transfer acceleration is enabled for the bucket (4.0.0 skips this for internal stages,
    but still uses the global endpoint). In networks where only the Snowflake account and the regional bucket
    endpoint ``https://<bucket>.s3.<region>.amazonaws.com`` are reachable (e.g. a Foundry network egress policy
    allowing only these hosts), stage file transfers (``GET``/``PUT``, e.g. ``session.file.get``) fail with::

        RequestExceedMaxRetryError: GET with url b'https://<bucket>.s3.amazonaws.com/?accelerate'
        failed for exceeding maximum retries

    The regional endpoint also keeps the transfer cheap. When the Snowflake account (and therefore its
    internal stage bucket) runs in the same AWS region as the Foundry instance, Foundry can reach the
    bucket privately: its Control Panel sets up an AWS S3 gateway endpoint for a "same region S3 bucket
    policy" network egress policy, which allows the regional bucket host. Traffic through an S3 gateway
    endpoint stays on the AWS network and AWS charges nothing for it, neither for the endpoint
    nor for the data transferred between AWS services and S3 within the same region. Gateway endpoints
    only work within their own region, so a stage bucket in a different region than Foundry does not
    get this benefit. References:

    - https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html
      ("There is no additional charge for using gateway endpoints.", "A gateway endpoint is available
      only in the Region where you created it.")
    - https://aws.amazon.com/s3/faqs/ ("There is no Data Transfer charge for data transferred between
      Amazon EC2 (or any AWS service) and Amazon S3 within the same Region")
    - https://palantir.com/docs/foundry/administration/configure-private-link-egress-aws/
      ("Use same region S3 bucket policies for private connectivity to S3.")

    The patch replaces two methods of ``SnowflakeS3RestClient``:

    - ``_get_bucket_accelerate_config`` always returns False, so the ``?accelerate`` call is never made.
    - ``transfer_accelerate_config`` always sets the regional endpoint and reports acceleration as disabled.

    Side effects: the patch is applied process-wide to every ``SnowflakeS3RestClient``,
    calling it more than once is harmless. As ``transfer_accelerate_config`` is replaced entirely,
    endpoints sent by the server (e.g. FIPS endpoints) and the connector's China region handling
    (``.amazonaws.com.cn``) are overridden as well. Disable it via ``enable_s3_monkeypatch=False``
    in :py:class:`FastSnowflakeIngestion` if that is a problem.

    Alternatively, setting the account parameter below made the server return the regional endpoint,
    so the patch is not needed (confirmed in the issue above, not documented by Snowflake for this purpose,
    the related private connectivity feature requires the Business Critical edition)::

        use role accountadmin;
        alter account set ENABLE_INTERNAL_STAGES_PRIVATELINK = true;
    """
    from snowflake.connector.s3_storage_client import SnowflakeS3RestClient

    SnowflakeS3RestClient._get_bucket_accelerate_config = _get_bucket_accelerate_config_patch  # noqa: SLF001
    SnowflakeS3RestClient.transfer_accelerate_config = _transfer_accelerate_config_patch


def _random_table_name() -> str:
    return "".join(secrets.choice(string.ascii_uppercase) for _ in range(8))


class FastSnowflakeIngestion:
    """Unloads a Snowpark DataFrame to a Snowflake stage and uploads the files to a Foundry output."""

    def __init__(
        self,
        snowpark_session: snow.Session,
        output: Output,
        provisioned_vcpus: int = 4,
        custom_tags: dict[str, Any] | None = None,
        # Shared database parameters
        use_shared_db: bool = False,
        db_name: str | None = None,
        schema_name: str | None = None,
        # Fallback control
        enable_automatic_stage_fallback: bool = True,
        fallback_db_name: str | None = None,
        fallback_schema_name: str | None = None,
        # Snowflake connector workaround
        enable_s3_monkeypatch: bool = True,
    ):
        """Initializes a FastSnowflakeIngestion instance with connection and staging info.

        Args:
            snowpark_session: Active Snowpark Session object.
            output: Foundry transform Output.
            provisioned_vcpus: Number of vCPUs to provision for parallel operations. Defaults to 4.
            custom_tags: any custom tags a user wants the session to store. As JSON-like kwargs.
            use_shared_db: Whether to use a shared database with custom stage. Defaults to False.
            db_name: The name of the Snowflake database to use. Required when use_shared_db=True.
            schema_name: The schema name within the database. Required when use_shared_db=True.
            enable_automatic_stage_fallback: Whether to attempt a fallback to the shared database
                ``fallback_db_name.fallback_schema_name`` when the primary stage setup fails.
                Only takes effect if both fallback names are provided. Defaults to True.
            fallback_db_name: The Snowflake database used for the fallback stage.
            fallback_schema_name: The schema within ``fallback_db_name`` used for the fallback stage.
            enable_s3_monkeypatch: Whether to call :py:func:`disable_s3_transfer_acceleration`
                before any stage operation. Defaults to True.

        Raises:
            ValueError: If use_shared_db=True but db_name or schema_name are not provided,
                or if only one of fallback_db_name and fallback_schema_name is provided.

        Examples:
            # Using session stage (non-shared database):
            >>> fsi = FastSnowflakeIngestion(
            ...     snowpark_session=session,
            ...     output=out,
            ... )

            # Using shared database with custom stage:
            >>> fsi = FastSnowflakeIngestion(
            ...     snowpark_session=session,
            ...     output=out,
            ...     db_name="SHARED_DB",
            ...     schema_name="EXPORT_SCHEMA",
            ...     use_shared_db=True,
            ... )

            # With a fallback stage:
            >>> fsi = FastSnowflakeIngestion(
            ...     snowpark_session=session,
            ...     output=out,
            ...     fallback_db_name="FALLBACK_DB",
            ...     fallback_schema_name="STAGING",
            ... )

            # Adding custom session tags:
            >>> fsi = FastSnowflakeIngestion(
            ...    snowpark_session=session,
            ...    output=out,
            ...    custom_tags = {
            ...        "custom_snowflake_tag_1": "my telemetry tag value 1",
            ...        "custom_snowflake_tag_2": {
            ...            "custom_nested_snowflake_tag_1": 123,
            ...            "custom_nested_snowflake_tag_2": ["A", "B", "C"],
            ...        }
            ...    }
            ... )
        """
        # Validate shared database parameters
        if use_shared_db and (not db_name or not schema_name):
            msg = "When use_shared_db=True, both `db_name` and `schema_name` must be provided."
            raise ValueError(msg)
        if bool(fallback_db_name) != bool(fallback_schema_name):
            msg = "Both `fallback_db_name` and `fallback_schema_name` must be provided, or neither."
            raise ValueError(msg)

        if enable_s3_monkeypatch:
            disable_s3_transfer_acceleration()

        # Core parameters
        self._snowpark_session = snowpark_session
        self._output = output
        self._table_name = _random_table_name()
        self._provisioned_vcpus = provisioned_vcpus
        self._temp_dir: str | None = None

        # Save telemetry job data & custom tags as session tags
        custom_tags = custom_tags or {}
        self.set_session_query_tag(**custom_tags)

        # Shared database parameters
        self._use_shared_db = use_shared_db
        self._db_name = db_name
        self._schema_name = schema_name

        # Fallback
        self._enable_automatic_stage_fallback = enable_automatic_stage_fallback
        self._fallback_db_name = fallback_db_name
        self._fallback_schema_name = fallback_schema_name
        self._is_using_fallback = False

        # Resolve a working stage (session stage / user shared DB -> fallback DB)
        self._resolve_stage()

    def _can_fallback(self) -> bool:
        return self._enable_automatic_stage_fallback and bool(self._fallback_db_name)

    def _resolve_stage(self) -> None:
        """Resolves a working Snowflake stage by trying strategies in order.

        Strategy order:
            1. If use_shared_db=True, try the user-provided DB/schema.
            2. If use_shared_db=False, try the session's default stage.
            3. If the above fails, enable_automatic_stage_fallback=True and the fallback
               DB/schema are provided, try the fallback shared database.

        Each strategy is attempted with a local try/except around the stage
        creation call. On snowflake.connector.errors.Error or SnowparkSQLException the next strategy is tried.

        Raises:
            snowflake.connector.errors.Error | SnowparkSQLException: If all attempted strategies fail.
        """
        if self._use_shared_db:
            # Strategy 1: user-provided shared DB
            try:
                self._setup_shared_database_stage(self._db_name, self._schema_name)
            except (snowflake_errors.Error, snowpark_exceptions.SnowparkSQLException) as e:
                logger.warning(
                    "Shared database stage setup failed for '%s.%s': %s", self._db_name, self._schema_name, e
                )
                if not self._can_fallback():
                    raise
            else:
                return
        else:
            # Strategy 2: session default stage
            try:
                self._setup_session_stage()
            except (snowflake_errors.Error, snowpark_exceptions.SnowparkSQLException) as e:
                logger.warning("Session stage setup failed: %s", e)
                if not self._can_fallback():
                    raise
            else:
                return

        # Strategy 3: fallback shared DB
        logger.info("Attempting fallback stage: %s.%s", self._fallback_db_name, self._fallback_schema_name)
        self._table_name = _random_table_name()
        self._is_using_fallback = True
        self._use_shared_db = True
        self._db_name = self._fallback_db_name
        self._schema_name = self._fallback_schema_name
        self._setup_shared_database_stage(self._fallback_db_name, self._fallback_schema_name)

    def _setup_shared_database_stage(self, db_name: str, schema_name: str) -> None:
        """Set up temporary stage configuration for shared database environments.

        Switches the session to the given database and schema, then creates a
        fully qualified temporary stage in the format:
            DB.SCHEMA.TEMP_EXPORT_<table_name>

        NOTE: Make sure the role has the privilege to create temporary stages in the schema.

        Args:
            db_name: The Snowflake database name.
            schema_name: The schema name within the database.

        Raises:
            snowflake.snowpark.exceptions.SnowparkSQLException: If the stage cannot be created
                (e.g. insufficient privileges).
        """
        self._snowpark_session.use_database(db_name)
        self._snowpark_session.use_schema(schema_name)
        self._fully_qualified_stage_name = f"{db_name}.{schema_name}.TEMP_EXPORT_{self._table_name}"
        self._snowpark_session.sql(f"CREATE OR REPLACE TEMPORARY STAGE {self._fully_qualified_stage_name};").collect()
        self._temporary_stage_name = self._fully_qualified_stage_name
        self._remote_file_path = f"@{self._fully_qualified_stage_name}/{self._table_name}"

    def _setup_session_stage(self) -> None:
        """Set up temporary stage configuration for non-shared database environments.

        Uses the session's default stage via get_session_stage(). No explicit
        database switching is performed.

        Raises:
            snowflake.connector.errors.Error: If the session stage cannot be obtained.
        """
        self._fully_qualified_stage_name = None
        self._temporary_stage_name = self._snowpark_session.get_session_stage()
        self._remote_file_path = f"{self._temporary_stage_name}/{self._table_name}"

    def save_snowpark_dataframe_to_foundry_output(
        self, df: snow.DataFrame, save_session_id: bool = True, **kwargs
    ) -> bool:
        """Orchestrates the complete process of saving a Snowpark DataFrame to a Foundry output.

        The local temporary directory is removed afterwards, also on failure.

        Args:
            df: The DataFrame to save.
            save_session_id: Whether to save current Snowpark.Session.session_id to the
                Output as a 0-byte file as part of the file name. Defaults to True.
            **kwargs: Additional arguments passed to underlying functions.

        Keyword Arguments:
            For write_df_into_location_parquet():
                header (bool): Whether to include a header row. Default: True.
                format_type_options (dict): Format-specific options for Parquet.
                    See: https://docs.snowflake.com/en/sql-reference/sql/copy-into-location#type-parquet
                explain_df (bool): Whether to print the DataFrame's execution plan. Default: False.
                copy_options (dict): Additional COPY options.
                    See: https://docs.snowflake.com/en/sql-reference/sql/copy-into-location#copy-options-copyoptions

            For upload_temp_folder_to_output_dataset():
                output (Output): Will be used from the class object.
                path_files (list): List of file paths. Default: None (uses all files).
                cpu_cores (int): Number of CPU cores to use for upload. Default: provisioned_vcpus.

            For commit_dataset_upload():
                column_descriptions (dict): Column descriptions to add to the final dataset.

        Example::

            >>> kwargs_example = {
            ...     "header": True,
            ...     "format_type_options": {
            ...         "COMPRESSION": "AUTO",
            ...         "SNAPPY_COMPRESSION": True,
            ...     },
            ...     "explain_df": False,
            ...     "copy_options": {
            ...         "OVERWRITE": True,
            ...         "SINGLE": False,
            ...         "MAX_FILE_SIZE": 200 * 1024 * 1024,
            ...         "INCLUDE_QUERY_ID": False,
            ...         "DETAILED_OUTPUT": True
            ...     },
            ...     "path_files": None,
            ...     "cpu_cores": 4,
            ...     "column_descriptions": {
            ...         "column1": "Description for column 1",
            ...         "column2": "Description for column 2",
            ...         "timestamp_col": "Timestamp when record was created"
            ...     }
            ... }
            >>> success = fsi.save_snowpark_dataframe_to_foundry_output(
            ...     df=my_df, **kwargs_example
            ... )

        Returns:
            bool: Always True, any failure raises an exception instead.

        Raises:
            snowflake.connector.errors.Error: If the write or download step fails.
            Exception: If any other step in the process fails.
        """
        # (Optional, enabled by default) Save the current Session ID within the name of an empty txt file on the Output
        if save_session_id:
            self.save_session_id_to_output()

        try:
            # 1. Write df to temp stage
            write_kwargs = filter_kwargs_for_function(self.write_df_into_location_parquet, kwargs)
            write_kwargs.update({"df": df, "remote_file_path": self._remote_file_path})
            self.write_df_into_location_parquet(**write_kwargs)

            # 2. Sync the files from Snowflake to Foundry's tempdir
            download_kwargs = filter_kwargs_for_function(self.download_snowflake_files_to_foundry_tmp_folder, kwargs)
            self.download_snowflake_files_to_foundry_tmp_folder(**download_kwargs)

            # 3. Extract output components using utils function
            dataset_rid, transaction_rid, branch_id, _ = get_output_foundry_path_components(self._output)

            # 4. Upload files
            upload_kwargs = filter_kwargs_for_function(self.upload_temp_folder_to_output_dataset, kwargs)
            upload_kwargs.update({"output": self._output})
            self.upload_temp_folder_to_output_dataset(**upload_kwargs)

            # 5. Commit transaction
            commit_kwargs = filter_kwargs_for_function(self.commit_dataset_upload, kwargs)
            commit_kwargs.update(
                {
                    "dataset_rid": dataset_rid,
                    "branch_id": branch_id,
                    "transaction_rid": transaction_rid,
                }
            )
            self.commit_dataset_upload(**commit_kwargs)

        except Exception:
            logger.exception("Error in save_snowpark_dataframe_to_foundry_output")
            raise
        finally:
            self.cleanup_temp_dir()

        return True

    def get_database_and_staging_info(self) -> dict[str, Any]:
        """Provides comprehensive metadata about the database type and staging configuration.

        Returns detailed information about whether a shared or non-shared database
        approach is being used, along with all relevant staging parameters.

        Returns:
            dict: A dictionary containing:
                - use_shared_db: Boolean indicating shared vs non-shared database mode
                - db_name: The database in use, None when the session stage is used
                - schema_name: The schema in use, None when the session stage is used
                - fully_qualified_stage_name: Full stage name (None for non-shared mode)
                - temporary_stage_name: Name of the temporary stage, dropped by Snowflake at session end
                - remote_file_path: Complete path for file operations
                - is_using_fallback: Whether the fallback stage is currently active
        """
        return {
            "use_shared_db": self._use_shared_db,
            "db_name": self._db_name,
            "schema_name": self._schema_name,
            "fully_qualified_stage_name": self._fully_qualified_stage_name,
            "temporary_stage_name": self._temporary_stage_name,
            "remote_file_path": self._remote_file_path,
            "is_using_fallback": self._is_using_fallback,
        }

    def get_snowpark_session(self) -> snow.Session:
        """Returns the Snowpark session associated with this ingestion instance."""
        return self._snowpark_session

    def write_df_into_location_parquet(
        self,
        df: snow.DataFrame,
        remote_file_path: str,
        header: bool = True,
        format_type_options: dict[str, str] | None = None,
        copy_options: dict[str, Any] | None = None,
        explain_df: bool = False,
    ) -> list[snow.Row]:
        """Writes a Snowpark DataFrame to a specified remote Snowflake stage location as Parquet.

        ``TIMESTAMP_TZ`` and ``TIMESTAMP_LTZ`` columns cannot be unloaded to Parquet, as Parquet has no type
        that stores a time zone (offset) per value. Cast them to ``TIMESTAMP_NTZ`` (drops the offset)
        or ``DATE`` (also drops the time of day) before calling this method.

        Args:
            df: The DataFrame to write.
            remote_file_path: The destination path in the Snowflake stage.
            header: Whether to include a header row. Defaults to True.
            format_type_options: Format-specific options for Parquet.
                Defaults to AUTO compression.
            copy_options: Additional COPY options. Defaults to standard
                options with 200MB max file size.
            explain_df: Whether to print the DataFrame's execution plan. Defaults to False.

        Returns:
            A list of Row objects containing unloading results.
        """
        if explain_df:
            df.explain()

        if copy_options is None:
            copy_options = {
                "OVERWRITE": True,
                "SINGLE": False,
                "MAX_FILE_SIZE": 200 * 1024 * 1024,  # 200 MB
                "INCLUDE_QUERY_ID": False,
                "DETAILED_OUTPUT": True,
            }

        if format_type_options is None:
            format_type_options = {"COMPRESSION": "AUTO"}

        return df.write.copy_into_location(
            remote_file_path,
            file_format_type="parquet",
            format_type_options=format_type_options,
            header=header,
            **copy_options,
        )

    def download_snowflake_files_to_foundry_tmp_folder(self) -> list[snow.GetResult]:
        """Downloads files from the Snowflake stage to a temporary local directory.

        Returns:
            List of results from the file download operation.
        """
        self._temp_dir = tempfile.mkdtemp()

        return self._snowpark_session.file.get(
            stage_location=self._remote_file_path, target_directory=self._temp_dir, parallel=self._provisioned_vcpus
        )

    def cleanup_temp_dir(self) -> None:
        """Removes the local temporary directory created by the download step, if any."""
        if self._temp_dir:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            self._temp_dir = None

    def get_files_from_temp_dir(self) -> list[str]:
        """Lists all files in the temporary directory created during download.

        Returns:
            List of filenames in the temp directory.

        Raises:
            RuntimeError: If the temp directory has not been created.
        """
        if not self._temp_dir:
            msg = (
                "No temporary directory has been created. "
                "Make sure to call 'download_snowflake_files_to_foundry_tmp_folder' first."
            )
            raise RuntimeError(msg)
        return [f.name for f in Path(self._temp_dir).iterdir() if f.is_file()]

    def get_path_files(self, files: list[str] | None = None) -> list[Path]:
        """Maps file names in the temp directory to their full paths.

        Args:
            files: Specific list of filenames to include.
                If not provided, uses all files in the temp directory.

        Returns:
            List of file pathlib.Path.
        """
        if not files:
            files = self.get_files_from_temp_dir()

        return [Path(self._temp_dir, f) for f in files]

    def upload_temp_folder_to_output_dataset(
        self, output: Output, path_files: list[Path] | None = None, cpu_cores: int | None = None
    ) -> None:
        """Iterates through a list of file paths and calls the `upload_file` function for each.

        Uploads in parallel with a ThreadPoolExecutor if there is more than 1 file and more than 1 CPU.

        Args:
            output: the transform output
            path_files: the files to upload, defaults to all files in the temp directory
            cpu_cores: amount of cores provisioned for the transform.
                The function will use N-1 or 1 CPUs depending which is greater.
        """
        if not cpu_cores:
            cpu_cores = self._provisioned_vcpus

        if not path_files:
            path_files = self.get_path_files()

        base_dir = Path(self._temp_dir) if self._temp_dir else None

        if len(path_files) <= 1 or cpu_cores <= 1:
            for file_path in path_files:
                self.upload_file(output=output, file_path=file_path, base_dir=base_dir)
        else:
            with ThreadPoolExecutor(max_workers=max(1, cpu_cores - 1)) as pool:
                for future in [pool.submit(self.upload_file, output, file_path, base_dir) for file_path in path_files]:
                    future.result()

    def save_session_id_to_output(self) -> None:
        """Saves current Snowpark.Session.session_id to the Output filesystem.

        The session id is part of the file name, the file is 0 bytes in size. Does not commit any transactions.
        """
        session_id = str(self._snowpark_session.session_id)
        file_name = f"_snowflake_session_metadata/{session_id}.txt"
        fs = self._output.filesystem()
        with fs.open(file_name, "w") as f:
            f.write("\n")

        logger.info("Saved Session ID %s to the provided Output under the name '%s'.", session_id, file_name)

    def set_session_query_tag(self, **custom_tags) -> None:
        """Sets the Snowpark session query tag to Foundry job telemetry data combined with custom tags.

        For an example of the job info, see
        :py:func:`foundry_dev_tools.helpers.fast_snowflake_ingestion.utils.prepare_job_info`.

        Snowflake limits ``QUERY_TAG`` to 2000 characters. A longer tag is truncated to that limit
        (it is then no longer valid JSON, but still usable for substring matching) and the full tag is logged.

        Args:
            **custom_tags: optional, additional custom tags.
        """
        job_info = prepare_job_info(out=self._output, **custom_tags)
        query_tag = json.dumps(job_info)
        if len(query_tag) > QUERY_TAG_MAX_LENGTH:
            logger.warning(
                "Query tag exceeds %d characters and is truncated. Full query tag: %s",
                QUERY_TAG_MAX_LENGTH,
                query_tag,
            )
            query_tag = query_tag[:QUERY_TAG_MAX_LENGTH]
        # the setter escapes the value, so quotes in tag values are safe
        self._snowpark_session.query_tag = query_tag

    @staticmethod
    def upload_file(output: Output, file_path: Path, base_dir: Path | None = None) -> None:
        """Uploads a single file using filesystem to the Transform's output.

        Args:
            output: the transform output
            file_path: full path to the file supposed to be uploaded
            base_dir: the directory the destination path is relative to.
                If None, the destination is the path below the first directory under the system temp directory.
        """
        if base_dir is not None:
            rel = file_path.relative_to(base_dir)
        else:
            tmp = Path(tempfile.gettempdir())
            rel = file_path.relative_to(tmp) if file_path.is_relative_to(tmp) else Path(file_path.name)
            # drop the first ephemeral dir under tmp
            if len(rel.parts) > 1:
                rel = Path(*rel.parts[1:])
        destination_path = rel.as_posix()

        # each thread should have its own filesystem which should not be created inside of the context manager
        fs = output.filesystem()

        with file_path.open("rb") as src, fs.open(destination_path, "wb") as dst:
            shutil.copyfileobj(src, dst)

    @staticmethod
    def commit_dataset_upload(
        dataset_rid: str, branch_id: str, transaction_rid: str, column_descriptions: dict[str, str] | None = None
    ) -> None:
        """Commits the dataset upload transaction to finalize the data ingestion.

        Only available inside a Foundry lightweight transform.

        Args:
            dataset_rid: The dataset RID.
            branch_id: The branch ID.
            transaction_rid: The transaction RID.
            column_descriptions: Column descriptions to add to the dataset. Defaults to empty dict.
        """
        import transforms_container_ops_python

        transforms_container_ops_python.io.commit_dataset_upload(
            dataset_rid,
            branch_id,
            transaction_rid,
            column_descriptions or {},
        )
