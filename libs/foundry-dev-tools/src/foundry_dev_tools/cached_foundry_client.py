"""Cached Foundry Client, high level client to work with Foundry."""

from __future__ import annotations

import logging
import os
import pickle
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.errors.dataset import (
    BranchNotFoundError,
    DatasetHasNoSchemaError,
    DatasetNotFoundError,
)
from foundry_dev_tools.foundry_api_client import FoundryRestClient
from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache
from foundry_dev_tools.utils.converter.foundry_spark import (
    infer_dataset_format_from_foundry_schema,
)
from foundry_dev_tools.utils.misc import is_dataset_a_view

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyspark.sql

    from foundry_dev_tools.utils import api_types


class CachedFoundryClient:
    """A Foundry Client that offers a high level API to Foundry.

    Methods to save, load datasets are implemented.
    """

    def __init__(self, config: dict | None = None, ctx: FoundryContext | FoundryRestClient | None = None):
        """Initialize `CachedFoundryClient`.

        Possible to pass overwrite config and
        uses `Configuration` to read it from the
        ~/.foundry-dev-tools/config file.

        Args:
            config: config dict to overwrite values from config file
            ctx: foundrycontext to use, if supplied the `config` parameter will be ignored
        """
        if ctx:
            if isinstance(ctx, FoundryContext):
                self.api = FoundryRestClient(ctx=ctx)
                self.cache = DiskPersistenceBackedSparkCache(ctx=ctx)
            else:
                self.api = ctx
                self.cache = DiskPersistenceBackedSparkCache(ctx=ctx.ctx)
        else:
            config = config or {}
            self.api = FoundryRestClient(config)
            self.cache = DiskPersistenceBackedSparkCache(ctx=self.api.ctx)

    def load_dataset(self, dataset_path_or_rid: str, branch: str = "master") -> pyspark.sql.DataFrame:
        """Loads complete dataset from Foundry and stores in cache.

        Cache is invalidated once new transaction is present in Foundry.
        Last 2 transactions are kept in cache and older transactions are cleaned up.

        Args:
            dataset_path_or_rid (str): Path to dataset or the rid of the dataset
            branch (str): The branch of the dataset

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`

        """
        _, dataset_identity = self.fetch_dataset(dataset_path_or_rid, branch)
        return self.cache[dataset_identity]

    def fetch_dataset(self, dataset_path_or_rid: str, branch: str = "master") -> tuple[str, api_types.DatasetIdentity]:
        """Downloads complete dataset from Foundry and stores in cache.

        Returns local path to dataset

        Args:
            dataset_path_or_rid: Path to dataset or the rid of the dataset
            branch: The branch of the dataset

        Returns:
            `Tuple[str, dict]`:
                local path to the dataset, dataset_identity

        """
        dataset_identity = self._get_dataset_identity(dataset_path_or_rid, branch)

        return os.fspath(self._fetch_dataset(dataset_identity, branch=branch)), dataset_identity

    def _fetch_dataset(self, dataset_identity: api_types.DatasetIdentity, branch: str = "master") -> Path:
        last_transaction = dataset_identity["last_transaction"]

        if dataset_identity in list(self.cache.keys()):
            return self._return_local_path_of_cached_dataset(dataset_identity, branch)
        try:
            foundry_schema = self.api.get_dataset_schema(
                dataset_identity["dataset_rid"],
                dataset_identity["last_transaction"]["rid"],
                branch=branch,
            )
        except DatasetHasNoSchemaError:
            # Binary datasets or no schema
            foundry_schema = None
        if is_dataset_a_view(last_transaction["transaction"]):
            self.cache[dataset_identity] = self.api.query_foundry_sql(
                f'SELECT * FROM `{dataset_identity["dataset_rid"]}`',  # noqa: S608
                branch=branch,
                return_type="spark",
            )
            return self._return_local_path_of_cached_dataset(dataset_identity, branch)
        return self._download_dataset_and_return_local_path(dataset_identity, branch, foundry_schema)

    def _get_dataset_identity(
        self,
        dataset_path_or_rid: api_types.Rid,
        branch: api_types.DatasetBranch,
    ) -> api_types.DatasetIdentity:
        if self.api.ctx.config.transforms_freeze_cache is False:
            return self._get_dataset_identity_online(dataset_path_or_rid, branch)
        return self._get_dataset_identity_offline(dataset_path_or_rid)

    def _get_dataset_identity_online(
        self,
        dataset_path_or_rid: api_types.Rid,
        branch: api_types.DatasetBranch,
    ) -> api_types.DatasetIdentity:
        return self.api.get_dataset_identity(dataset_path_or_rid, branch)

    def _get_dataset_identity_offline(self, dataset_path_or_rid: api_types.Rid) -> api_types.DatasetIdentity:
        # Note: this is not branch aware, so it will return a dataset from the cache,
        # even though the branch might be different to that requested.
        return self.cache.get_dataset_identity_not_branch_aware(dataset_path_or_rid)

    def _return_local_path_of_cached_dataset(
        self,
        dataset_identity: api_types.DatasetIdentity,
        branch: api_types.DatasetBranch,
    ) -> Path:
        LOGGER.debug("Returning data for %s on branch %s from cache", dataset_identity, branch)
        return self.cache.get_path_to_local_dataset(dataset_identity)

    def _download_dataset_and_return_local_path(
        self,
        dataset_identity: api_types.DatasetIdentity,
        branch: api_types.DatasetBranch,
        foundry_schema: api_types.FoundrySchema | None,
    ) -> Path:
        LOGGER.debug("Caching data for %s on branch %s", dataset_identity, branch)
        self._download_dataset_to_cache_dir(dataset_identity, branch, foundry_schema)
        return self.cache.get_path_to_local_dataset(dataset_identity)

    def _download_dataset_to_cache_dir(
        self,
        dataset_identity: api_types.DatasetIdentity,
        branch: api_types.DatasetBranch,
        foundry_schema: api_types.FoundrySchema | None,
    ):
        list_of_files = self.api.list_dataset_files(
            dataset_identity["dataset_rid"],
            exclude_hidden_files=True,
            view=branch,
        )
        suffix = "." + infer_dataset_format_from_foundry_schema(foundry_schema, list_of_files)

        path = self.cache.get_cache_dir().joinpath(
            dataset_identity["dataset_rid"],
            dataset_identity["last_transaction"]["rid"] + suffix,
        )
        self.api.download_dataset_files(
            dataset_rid=dataset_identity["dataset_rid"],
            output_directory=path,
            files=list_of_files,
            view=branch,
        )
        self.cache.set_item_metadata(path, dataset_identity, foundry_schema)

    def save_dataset(
        self,
        df: pd.DataFrame | pyspark.sql.DataFrame | pl.DataFrame,
        dataset_path_or_rid: str,
        branch: str = "master",
        exists_ok: bool = False,
        mode: str = "SNAPSHOT",
    ) -> tuple[str, str]:
        """Saves a dataframe to Foundry. If the dataset in Foundry does not exist it is created.

        If the branch does not exist, it is created. If the dataset exists, an exception is thrown.
        If exists_ok=True is passed, the dataset is overwritten.
        Creates SNAPSHOT transactions by default.

        Args:
            df (:external+pandas:py:class:`pandas.DataFrame` | :external+polars:py:class:`polars.DataFrame` | :external+spark:py:class:`pyspark.sql.DataFrame`):
                A pyspark, pandas or polars DataFrame to upload
            dataset_path_or_rid (str): Path or Rid of the dataset in which the object should be stored.
            branch (str): Branch of the dataset in which the object should be stored
            exists_ok (bool): By default, this method creates a new dataset.
                Pass exists_ok=True to overwrite according to strategy from parameter 'mode'
            mode (str): Foundry Transaction type:
                SNAPSHOT (only new files are present after transaction),
                UPDATE (replace files with same filename, keep present files),
                APPEND (add files that are not present yet)

        Returns:
            :py:class:`Tuple`:
                tuple with (dataset_rid, transaction_rid)

        Raises:
            ValueError: when dataframe is None
            ValueError: when branch is None

        """  # noqa: E501
        if df is None:
            msg = "Please provide a spark or pandas dataframe object with parameter 'df'"
            raise ValueError(msg)
        if branch is None:
            msg = "Please provide a dataset branch with parameter 'branch'"
            raise ValueError(msg)

        with tempfile.TemporaryDirectory() as path:
            from foundry_dev_tools._optional.pandas import pd
            from foundry_dev_tools._optional.polars import pl

            if not pd.__fake__ and isinstance(df, pd.DataFrame):
                df.to_parquet(
                    os.sep.join([path + "/dataset.parquet"]),  # noqa: PTH118
                    engine="pyarrow",
                    compression="snappy",
                    flavor="spark",
                )
            elif not pl.__fake__ and isinstance(df, pl.DataFrame):
                df.write_parquet(
                    os.sep.join([path + "/dataset.parquet"]),  # noqa: PTH118
                    use_pyarrow=True,
                    compression="snappy",
                )
            else:
                df.write.format("parquet").option("compression", "snappy").save(path=path, mode="overwrite")

            filenames = list(filter(lambda file: not file.endswith(".crc"), os.listdir(path)))
            filepaths = [Path(path).joinpath(file) for file in filenames]
            # to be backwards compatible to most readers, that expect files
            # to be under spark/
            folder = round(time.time() * 1000) if mode == "APPEND" else "spark"
            dataset_paths_in_foundry = [f"{folder}/" + file for file in filenames]
            path_file_dict = dict(zip(dataset_paths_in_foundry, filepaths, strict=False))
            dataset_rid, transaction_id = self._save_objects(
                path_file_dict,
                dataset_path_or_rid,
                branch,
                exists_ok,
                mode,
            )

        foundry_schema = self.api.infer_dataset_schema(dataset_rid, branch)
        self.api.upload_dataset_schema(dataset_rid, transaction_id, foundry_schema, branch)
        return dataset_rid, transaction_id

    def _save_objects(
        self,
        path_file_dict: dict,
        dataset_path_or_rid: str,
        branch: str,
        exists_ok: bool = False,
        mode: str = "SNAPSHOT",
    ) -> tuple[str, str]:
        if path_file_dict is None or len(path_file_dict) == 0:
            msg = "Please provide at least one file like object in dict 'path_file_dict"
            raise ValueError(msg)
        if branch is None:
            msg = "Please provide a dataset branch with parameter 'branch'"
            raise ValueError(msg)

        try:
            identity = self.api.get_dataset_identity(dataset_path_or_rid)
            dataset_rid = identity["dataset_rid"]
            dataset_path = identity["dataset_path"]
            # Check if dataset and branch exists and not in trash
            self.api.get_dataset(dataset_rid)
            self.api.get_branch(dataset_rid, branch)
            if self.api.is_dataset_in_trash(dataset_path):
                msg = f"Dataset '{dataset_path}' is in trash."
                raise ValueError(msg)
        except DatasetNotFoundError:
            dataset_rid = self.api.create_dataset(dataset_path_or_rid)["rid"]
            self.api.create_branch(dataset_rid, branch)
            exists_ok = True
        except BranchNotFoundError:
            self.api.create_branch(dataset_rid, branch)
            exists_ok = True

        if not exists_ok:
            msg = (
                f"Dataset '{dataset_path_or_rid}' already exists. If you are sure to overwrite"
                " / modify the existing dataset, call this method with parameter exists_ok=True"
            )
            raise ValueError(
                msg,
            )

        transaction_id = self.api.open_transaction(dataset_rid, mode, branch)
        try:
            self.api.upload_dataset_files(dataset_rid, transaction_id, path_file_dict)
        except Exception as e:
            self.api.abort_transaction(dataset_rid, transaction_id)
            msg = (
                "There was an issue while uploading the dataset files."
                f" Transaction {transaction_id} on dataset {dataset_rid} has been aborted."
            )
            raise ValueError(
                msg,
            ) from e
        else:
            self.api.commit_transaction(dataset_rid, transaction_id)

        return dataset_rid, transaction_id

    def save_model(
        self,
        model_obj: object,
        dataset_path_or_rid: str,
        branch: str = "master",
        exists_ok: bool = False,
        mode: str = "SNAPSHOT",
    ) -> tuple[str, str]:
        """Saves a python object to a foundry dataset.

        The python object is pickled and uploaded to path model.pickle.
        The uploaded model can be loaded for performing predictions inside foundry
        pipelines.

        Args:
            model_obj (object): Any python object that can be pickled
            dataset_path_or_rid (str): Path or Rid of the dataset in which the object should be stored.
            branch (bool): Branch of the dataset in which the object should be stored
            exists_ok (bool): By default, this method creates a new dataset.
                Pass exists_ok=True to overwrite according to strategy from parameter 'mode'
            mode (str): Foundry Transaction type:
                SNAPSHOT (only new files are present after transaction),
                UPDATE (replace files with same filename, keep present files),
                APPEND (add files that are not present yet)

        Raises:
            ValueError: When model_obj or branch is None
        Returns:
            :py:class:`Tuple`:
                Tuple with (dataset_rid, transaction_rid)

        """
        if model_obj is None:
            msg = "Please provide a model object with parameter 'model_obj'"
            raise ValueError(msg)
        if branch is None:
            msg = "Please provide a dataset branch with parameter 'branch'"
            raise ValueError(msg)

        with tempfile.TemporaryDirectory() as path:
            model_path = Path(path).joinpath("model.pickle")
            with model_path.open(mode="wb") as file:
                pickle.dump(model_obj, file)
            return self._save_objects(
                {"model.pickle": os.fspath(model_path)},
                dataset_path_or_rid,
                branch,
                exists_ok,
                mode,
            )
