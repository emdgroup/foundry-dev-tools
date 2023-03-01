"""Cached Foundry Client, high level client to work with Foundry."""
import logging
import os
import pickle
import tempfile
import time
from typing import Tuple, Union

import pandas as pd

import foundry_dev_tools
from foundry_dev_tools.foundry_api_client import (
    BranchNotFoundError,
    DatasetHasNoSchemaError,
    DatasetNotFoundError,
)
from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache
from foundry_dev_tools.utils.converter.foundry_spark import (
    infer_dataset_format_from_foundry_schema,
)

from .foundry_api_client import FoundryRestClient

LOGGER = logging.getLogger(__name__)


class CachedFoundryClient:
    """A Foundry Client that offers a high level API to Foundry.

    Methods to save, load datasets are implemented.
    """

    def __init__(self, config: dict = None):
        """Initialize `CachedFoundryClient`.

        Possible to pass overwrite config and
        uses `Configuration` to read it from the
        ~/.foundry-dev-tools/config file.

        Args:
            config (dict): config dict to overwrite values from config file
        """
        self.overwrite_config = config
        self.config = foundry_dev_tools.Configuration.get_config(self.overwrite_config)
        self.cache = DiskPersistenceBackedSparkCache(**self.config)

    @property
    def api(self) -> FoundryRestClient:
        """We are creating a new instance of the client everytime it is used.

        Because it might be used in a request or session context in the AppService where
        every request/session has a token provided by a request header.

        Returns:
            :py:class:`~foundry_dev_tools.foundry_api_client.FoundryRestClient`

        """
        return FoundryRestClient(self.overwrite_config)

    def load_dataset(
        self, dataset_path_or_rid: str, branch: str = "master"
    ) -> "pyspark.sql.DataFrame":
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

    def fetch_dataset(
        self, dataset_path_or_rid: str, branch: str = "master"
    ) -> Tuple[str, dict]:
        """Downloads complete dataset from Foundry and stores in cache.

        Returns local path to dataset

        Args:
            dataset_path_or_rid (str): Path to dataset or the rid of the dataset
            branch (str): The branch of the dataset

        Returns:
            `Tuple[str, dict]`:
                local path to the dataset, dataset_identity

        """
        dataset_identity = self._get_dataset_identity(dataset_path_or_rid, branch)

        if dataset_identity in list(self.cache.keys()):
            return (
                self._return_local_path_of_cached_dataset(dataset_identity, branch),
                dataset_identity,
            )
        try:
            foundry_schema = self.api.get_dataset_schema(
                dataset_identity["dataset_rid"],
                dataset_identity["last_transaction_rid"],
                branch=branch,
            )
        except DatasetHasNoSchemaError:
            # Binary datasets or no schema
            foundry_schema = None
        return (
            self._download_dataset_and_return_local_path(
                dataset_identity, branch, foundry_schema
            ),
            dataset_identity,
        )

    def _get_dataset_identity(self, dataset_path_or_rid, branch):
        if (
            "transforms_freeze_cache" not in self.config
            or self.config["transforms_freeze_cache"] is False
        ):
            return self._get_dataset_identity_online(dataset_path_or_rid, branch)
        return self._get_dataset_identity_offline(dataset_path_or_rid)

    def _get_dataset_identity_online(self, dataset_path_or_rid, branch) -> dict:
        return self.api.get_dataset_identity(dataset_path_or_rid, branch)

    def _get_dataset_identity_offline(self, dataset_path_or_rid) -> dict:
        # Note: this is not branch aware, so it will return a dataset from the cache,
        # even though the branch might be different to that requested.
        return self.cache.get_dataset_identity_not_branch_aware(dataset_path_or_rid)

    def _return_local_path_of_cached_dataset(self, dataset_identity, branch) -> str:
        LOGGER.debug(
            "Returning data for %s on branch %s from cache", dataset_identity, branch
        )
        path = self.cache.get_path_to_local_dataset(dataset_identity)
        return path

    def _download_dataset_and_return_local_path(
        self, dataset_identity, branch, foundry_schema
    ) -> str:
        LOGGER.debug("Caching data for %s on branch %s", dataset_identity, branch)
        self._download_dataset_to_cache_dir(dataset_identity, branch, foundry_schema)
        path = self.cache.get_path_to_local_dataset(dataset_identity)
        return path

    def _download_dataset_to_cache_dir(self, dataset_identity, branch, foundry_schema):
        list_of_files = self.api.list_dataset_files(
            dataset_identity["dataset_rid"], exclude_hidden_files=True, view=branch
        )
        params = {
            "dataset_rid": dataset_identity["dataset_rid"],
            "files": list_of_files,
            "view": branch,
        }
        suffix = "." + infer_dataset_format_from_foundry_schema(
            foundry_schema, list_of_files
        )

        path = (
            os.sep.join(
                [
                    self.cache.get_cache_dir(),
                    dataset_identity["dataset_rid"],
                    dataset_identity["last_transaction_rid"],
                ]
            )
            + suffix
        )
        params["output_directory"] = path
        self.api.download_dataset_files(**params)
        self.cache.set_item_metadata(path, dataset_identity, foundry_schema)

    def save_dataset(
        self,
        df: Union[pd.DataFrame, "pyspark.sql.DataFrame"],
        dataset_path_or_rid: str,
        branch: str = "master",
        exists_ok: bool = False,
        mode: str = "SNAPSHOT",
    ) -> Tuple[str, str]:
        # pylint: disable=invalid-name,too-many-arguments
        """Saves a dataframe to Foundry. If the dataset in Foundry does not exist it is created.

        If the branch does not exist, it is created. If the dataset exists, an exception is thrown.
        If exists_ok=True is passed, the dataset is overwritten.
        Creates SNAPSHOT transactions by default.

        Args:
            df (:external+pandas:py:class:`pandas.DataFrame` | :external+spark:py:class:`pyspark.sql.DataFrame`): A
                pyspark  or pandas DataFrame to upload
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

        """
        if df is None:
            raise ValueError(
                "Please provide a spark or pandas dataframe "
                "object with parameter 'df'"
            )
        if branch is None:
            raise ValueError("Please provide a dataset branch with parameter 'branch'")

        with tempfile.TemporaryDirectory() as path:
            if isinstance(df, pd.DataFrame):
                df.to_parquet(
                    os.sep.join([path + "/dataset.parquet"]),
                    engine="pyarrow",
                    compression="snappy",
                    flavor="spark",
                )
            else:
                df.write.format("parquet").option("compression", "snappy").save(
                    path=path, mode="overwrite"
                )

            filenames = list(
                filter(lambda file: not file.endswith(".crc"), os.listdir(path))
            )
            filepaths = list(map(lambda file: os.sep.join([path, file]), filenames))
            if mode == "APPEND":
                folder = round(time.time() * 1000)
            else:
                # to be backwards compatible to most readers, that expect files
                # to be under spark/
                folder = "spark"
            dataset_paths_in_foundry = list(
                map(lambda file: f"{folder}/" + file, filenames)
            )
            path_file_dict = dict(zip(dataset_paths_in_foundry, filepaths))
            dataset_rid, transaction_id = self._save_objects(
                path_file_dict, dataset_path_or_rid, branch, exists_ok, mode
            )

        foundry_schema = self.api.infer_dataset_schema(dataset_rid, branch)
        self.api.upload_dataset_schema(
            dataset_rid, transaction_id, foundry_schema, branch
        )
        return dataset_rid, transaction_id

    def _save_objects(
        self,
        path_file_dict: dict,
        dataset_path_or_rid: str,
        branch: str,
        exists_ok: bool = False,
        mode: str = "SNAPSHOT",
    ) -> Tuple[str, str]:
        # pylint: disable=too-many-arguments
        if path_file_dict is None or len(path_file_dict) == 0:
            raise ValueError(
                "Please provide at least one file like object in dict 'path_file_dict"
            )
        if branch is None:
            raise ValueError("Please provide a dataset branch with parameter 'branch'")

        try:
            identity = self.api.get_dataset_identity(dataset_path_or_rid)
            dataset_rid = identity["dataset_rid"]
            dataset_path = identity["dataset_path"]
            # Check if dataset and branch exists and not in trash
            self.api.get_dataset(dataset_rid)
            self.api.get_branch(dataset_rid, branch)
            if self.api.is_dataset_in_trash(dataset_path):
                raise ValueError(f"Dataset '{dataset_path}' is in trash.")
        except DatasetNotFoundError:
            dataset_rid = self.api.create_dataset(dataset_path_or_rid)["rid"]
            self.api.create_branch(dataset_rid, branch)
            exists_ok = True
        except BranchNotFoundError:
            self.api.create_branch(dataset_rid, branch)
            exists_ok = True

        if not exists_ok:
            raise ValueError(
                f"Dataset '{dataset_path_or_rid}' already exists. "
                f"If you are sure to overwrite / modify the existing dataset, "
                f"call this method with"
                f" parameter exists_ok=True"
            )

        transaction_id = self.api.open_transaction(dataset_rid, mode, branch)
        success = False
        try:
            self.api.upload_dataset_files(dataset_rid, transaction_id, path_file_dict)
            success = True
        finally:
            if success:
                self.api.commit_transaction(dataset_rid, transaction_id)
            else:
                self.api.abort_transaction(dataset_rid, transaction_id)
                raise ValueError(
                    f"There was an issue while uploading the dataset files. "
                    f"Transaction {transaction_id} on dataset {dataset_rid} "
                    f"has been aborted."
                )

        return dataset_rid, transaction_id

    def save_model(
        self,
        model_obj: object,
        dataset_path_or_rid: str,
        branch: str = "master",
        exists_ok: bool = False,
        mode: str = "SNAPSHOT",
    ) -> Tuple[str, str]:
        # pylint: disable=too-many-arguments
        """Saves a python object to a foundry dataset.

        The python object is pickled and uploaded to path model.pickle.
        The uploaded model can be loaded for performing predictions inside foundry
        pipelines.

        Args:
            model_obj (object): Any python object than can be pickled
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
            raise ValueError("Please provide a model object with parameter 'model_obj'")
        if branch is None:
            raise ValueError("Please provide a dataset branch with parameter 'branch'")

        with tempfile.TemporaryDirectory() as path:
            model_path = os.sep.join([path, "model.pickle"])
            with open(model_path, "wb") as file:
                pickle.dump(model_obj, file)
            result = self._save_objects(
                {"model.pickle": model_path},
                dataset_path_or_rid,
                branch,
                exists_ok,
                mode,
            )

        return result
