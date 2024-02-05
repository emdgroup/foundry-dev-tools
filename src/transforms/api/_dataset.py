"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # noqa: E501

import inspect
import logging
import warnings
from pathlib import Path
from typing import Any, Optional

import pyspark

import foundry_dev_tools.config
from foundry_dev_tools.cached_foundry_client import CachedFoundryClient
from foundry_dev_tools.foundry_api_client import (
    BranchNotFoundError,
    DatasetHasNoSchemaError,
    DatasetHasNoTransactionsError,
    SQLReturnType,
)
from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache
from foundry_dev_tools.utils.misc import is_dataset_a_view
from foundry_dev_tools.utils.repo import git_toplevel_dir

LOGGER = logging.getLogger(__name__)


def _as_list(list_or_single_item: "list[Any] | Any | None") -> "list[Any]":
    """Helper function turning single values or None into lists.

    Args:
        list_or_single_item (List[Any] | Any | None): item or list to return as a list

    Returns:
        list:
            either the single item as a list, or the list passed in list_or_single_item

    """
    if not list_or_single_item:
        return []

    return (
        list_or_single_item
        if isinstance(list_or_single_item, list)
        else [list_or_single_item]
    )


class Input:
    """Specification of a transform dataset input.

    Some API requests may be sent when the Input class is constructed. However, the actual download
    is only initiated when dataframe() or get_local_path_to_dataset() is called.

    """

    def __init__(
        self,
        alias: "str | None" = None,
        branch: "str | None" = None,
        description=None,
        stop_propagating=None,
        stop_requiring=None,
        checks=None,
    ):
        """Specification of a transform dataset input.

        Args:
            alias (str | None): Dataset rid or the absolute Compass path of the dataset.
                If not specified, parameter is unbound.
            branch (str | None): Branch name to resolve the input dataset to.
                If not specified, resolved at build-time.
            stop_propagating (Markings | None): not implemented in Foundry DevTools
            stop_requiring (OrgMarkings | None): not implemented in Foundry DevTools
            checks (List[Check], Check): not implemented in foundry-dev-tools
            description (str): not implemented in foundry-dev-tools

        """
        # extract caller filename to retrieve git information
        caller_filename = inspect.stack()[1].filename
        LOGGER.debug("Input instantiated from %s", caller_filename)
        self.config = foundry_dev_tools.config.Configuration.get_config()
        self._cached_client = CachedFoundryClient(self.config)
        self._cache = DiskPersistenceBackedSparkCache(**self.config)
        self._spark_df = None
        if branch is None:
            branch = _get_branch(Path(caller_filename))

        if self._is_online:
            (
                self._is_spark_df_retrievable,
                self._dataset_identity,
                self.branch,
            ) = self._online(alias, branch)
        else:
            (
                self._is_spark_df_retrievable,
                self._dataset_identity,
                self.branch,
            ) = self._offline(alias, branch)

    @property
    def _is_online(self) -> bool:
        return (
            "transforms_freeze_cache" not in self.config
            or self.config["transforms_freeze_cache"] is False
        )

    def _online(self, alias, branch) -> "tuple[bool, dict, str]":
        try:
            dataset_identity = self._cached_client.api.get_dataset_identity(
                alias, branch
            )
        except BranchNotFoundError:
            LOGGER.debug(
                "Dataset %s not found on branch %s, "
                "falling back to dataset from master.",
                alias,
                branch,
            )
            branch = "master"
            dataset_identity = self._cached_client.api.get_dataset_identity(
                alias, branch
            )
        if dataset_identity["last_transaction_rid"] is None:
            raise DatasetHasNoTransactionsError(alias)
        if self._dataset_has_schema(dataset_identity, branch):
            return (
                True,
                dataset_identity,
                branch,
            )
        LOGGER.debug(
            "Dataset rid: %s, path: %s on branch %s has no schema, "
            "falling back to file download. "
            "Only filesystem() is supported with this dataset.",
            dataset_identity["dataset_rid"],
            dataset_identity["dataset_path"],
            branch,
        )
        return False, dataset_identity, branch

    def _offline(self, alias: str, branch: str) -> "tuple[bool, dict, str]":
        dataset_identity = self._cache.get_dataset_identity_not_branch_aware(alias)
        if self._cache.dataset_has_schema(dataset_identity):
            return True, dataset_identity, branch
        return False, dataset_identity, branch

    def _dataset_has_schema(self, dataset_identity, branch):
        try:
            self._cached_client.api.get_dataset_schema(
                dataset_identity["dataset_rid"],
                dataset_identity["last_transaction_rid"],
                branch,
            )
            return True
        except DatasetHasNoSchemaError:
            return False

    def _retrieve_spark_df(self, dataset_identity, branch) -> pyspark.sql.DataFrame:
        if dataset_identity in list(self._cache.keys()):
            return self._retrieve_from_cache(dataset_identity, branch)
        return self._retrieve_from_foundry_and_cache(dataset_identity, branch)

    def _retrieve_from_cache(self, dataset_identity, branch):
        LOGGER.debug(
            "Returning data for %s on branch %s from cache", dataset_identity, branch
        )
        return self._cache[dataset_identity]

    def _read_spark_df_with_sql_query(
        self, dataset_path: str, branch="master"
    ) -> pyspark.sql.DataFrame:
        query = f"SELECT * FROM `{dataset_path}`"  # noqa: S608
        if (
            "transforms_sql_sample_select_random" in self.config
            and self.config["transforms_sql_sample_select_random"] is True
        ):
            query = query + " ORDER BY RAND()"
        if (
            "transforms_sql_sample_row_limit" in self.config
            and self.config["transforms_sql_sample_row_limit"] is not None
        ):
            query = query + f" LIMIT {self.config['transforms_sql_sample_row_limit']}"
        LOGGER.debug(
            "Executing Foundry/SparkSQL Query: %s \n on branch %s", query, branch
        )
        return self._cached_client.api.query_foundry_sql(
            query, branch=branch, return_type=SQLReturnType.SPARK
        )

    def _retrieve_from_foundry_and_cache(
        self, dataset_identity: dict, branch: str
    ) -> pyspark.sql.DataFrame:
        LOGGER.debug("Caching data for %s on branch %s", dataset_identity, branch)
        transaction = dataset_identity["last_transaction"]["transaction"]
        if is_dataset_a_view(transaction):
            foundry_stats = self._cached_client.api.foundry_stats(
                dataset_identity["dataset_rid"],
                dataset_identity["last_transaction"]["rid"],
            )
            size_in_bytes = int(foundry_stats["computedDatasetStats"]["sizeInBytes"])
        else:
            size_in_bytes = transaction["metadata"]["totalFileSize"]
        size_in_mega_bytes = size_in_bytes / 1024 / 1024
        size_in_mega_bytes_rounded = round(size_in_mega_bytes, ndigits=2)
        LOGGER.debug("Dataset has size of %s MegaBytes.", size_in_mega_bytes_rounded)
        if (
            "transforms_force_full_dataset_download" in self.config
            and self.config["transforms_force_full_dataset_download"] is True
        ) or (
            size_in_mega_bytes < self.config["transforms_sql_dataset_size_threshold"]
        ):
            spark_df = self._cached_client.load_dataset(
                dataset_identity["dataset_rid"], branch
            )
        else:
            dataset_name = dataset_identity["dataset_path"].split("/")[-1]
            warnings.warn(
                f"Retrieving subset ({self.config['transforms_sql_sample_row_limit']} rows) of dataset '{dataset_name}'"
                f" with rid '{dataset_identity['dataset_rid']}' "
                f"because dataset size {size_in_mega_bytes_rounded} megabytes >= "
                f"{self.config['transforms_sql_dataset_size_threshold']} megabytes "
                f"(as defined in config['transforms_sql_dataset_size_threshold'])."
            )
            spark_df = self._read_spark_df_with_sql_query(
                dataset_identity["dataset_path"], branch
            )
            self._cache[dataset_identity] = spark_df
        return spark_df

    def dataframe(self) -> Optional[pyspark.sql.DataFrame]:
        """Get the cached :external+spark:py:class:`~pyspark.sql.DataFrame` of this Input.

        Only available if the input has a schema. The Spark DataFrame will get loaded the first
        time this method is invoked.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`: The cached DataFrame of this Input

        """
        if self._is_spark_df_retrievable and self._spark_df is None:
            if self._is_online:
                self._spark_df = self._retrieve_spark_df(
                    self._dataset_identity, self.branch
                )
            else:
                self._spark_df = self._cache[self._dataset_identity]

        return self._spark_df

    def get_dataset_identity(self) -> dict:
        """Returns identity of this Input.

        Returns:
            dict:
                with the keys dataset_path, dataset_rid, last_transaction_rid

        """
        return self._dataset_identity

    def get_local_path_to_dataset(self) -> str:
        """Returns path to the dataset's files on disk.

        Calling this method for the first time may trigger downloading the dataset files.

        Returns:
            str:
                path to the dataset's files on disk

        """
        return (
            self._cached_client._fetch_dataset(self._dataset_identity, self.branch)
            if self._is_online
            else self._cache.get_path_to_local_dataset(self._dataset_identity)
        )


def _get_branch(caller_filename: Path) -> str:
    git_dir = git_toplevel_dir(caller_filename)
    if not git_dir:
        # fallback for VS Interactive Console
        # or Jupyter lab on Windows
        git_dir = Path.cwd()

    head_file = git_dir.joinpath(".git", "HEAD")
    if head_file.is_file():
        with head_file.open() as hf:
            ref = hf.read().strip()

        if ref.startswith("ref: refs/heads/"):
            return ref[16:]

        return "HEAD"  # immitate behaviour of `git rev-parse --abbrev-ref HEAD`

    warnings.warn("Could not detect git branch of project, falling back to 'master'.")
    return "master"


class Output:
    """Specification of a transform dataset output.

    Writing the Output back to Foundry is not implemented.

    """

    def __init__(
        self,
        alias: "str | None" = None,
        sever_permissions: "bool | None" = False,
        description: "str | None" = None,
        checks=None,
    ):
        """Specification of a transform output.

        Args:
            alias (str | None): Dataset rid or the absolute Compass path of the dataset.
                If not specified, parameter is unbound.
            sever_permissions (bool | None): not implemented in foundry-dev-tools
            description (str | None): not implemented in foundry-dev-tools
            checks (List[Check], Check): not implemented in foundry-dev-tools
        """
        self.alias = alias


class UnmarkingDef:
    """Base class for unmarking datasets configuration."""

    def __init__(self, marking_ids: "list[str] | str", on_branches: "list[str] | str"):
        """Default constructor.

        Args:
            marking_ids (List[str], str): List of marking identifiers or single marking identifier.
            on_branches (List[str], str): Branch on which to apply unmarking.
        """
        self.marking_ids = _as_list(marking_ids)
        self.branches = _as_list(on_branches)


class Markings(UnmarkingDef):
    """Specification of a marking that stops propagating from input.

    The actual marking removal is not implemented.
    """


class OrgMarkings(UnmarkingDef):
    """Specification of a marking that is no longer required on the output.

    The actual marking requirement check is not implemented.
    """
