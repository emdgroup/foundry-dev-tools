"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # pylint: disable=line-too-long

import inspect
import logging
import pathlib
import warnings
from os import getcwd, path
from subprocess import CalledProcessError
from typing import Any, List, Optional, Tuple, Union

import pyspark

import foundry_dev_tools
from foundry_dev_tools import CachedFoundryClient
from foundry_dev_tools.config import execute_as_subprocess
from foundry_dev_tools.foundry_api_client import (
    BranchNotFoundError,
    DatasetHasNoSchemaError,
)
from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache

LOGGER = logging.getLogger(__name__)


def _as_list(list_or_single_item: Union[None, List[Any], Any]) -> List[Any]:
    """Helper function turning single values or None into lists.

    Args:
        list_or_single_item (Union[None, List[Any], Any]): item or list to return as a list

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


class Input:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """Specification of a transform dataset input.

    The actual download is initialized when the Input class is constructed.
    It is currently not lazy loaded.

    """

    def __init__(
        self,
        alias: Optional[str] = None,
        branch: Optional[str] = None,
        description=None,
        stop_propagating=None,
        stop_requiring=None,
        checks=None,
    ):
        # pylint: disable=unused-argument,too-many-arguments
        """Specification of a transform dataset input.

        Args:
            alias (Optional[str]): Dataset rid or the absolute Compass path of the dataset.
                If not specified, parameter is unbound.
            branch (Optional[str]): Branch name to resolve the input dataset to.
                If not specified, resolved at build-time.
            stop_propagating (Optional[Markings]): not implemented in Foundry DevTools
            stop_requiring (Optional[OrgMarkings]): not implemented in Foundry DevTools
            checks (List[Check], Check): not implemented in foundry-dev-tools
            description (str): not implemented in foundry-dev-tools

        """
        # extract caller filename to retrieve git information
        caller_filename = inspect.stack()[1].__getattribute__("filename")
        LOGGER.debug("Input instantiated from %s", caller_filename)
        self.config = foundry_dev_tools.Configuration.get_config()
        self._cached_client = CachedFoundryClient(self.config)
        self._cache = DiskPersistenceBackedSparkCache(**self.config)
        if branch is None:
            branch = _get_branch(caller_filename)

        if (
            "transforms_freeze_cache" not in self.config
            or self.config["transforms_freeze_cache"] is False
        ):
            self._spark_df, self._dataset_identity, self.branch = self._online(
                alias, branch
            )
        else:
            self._spark_df, self._dataset_identity, self.branch = self._offline(
                alias, branch
            )

    def _online(
        self, alias, branch
    ) -> Tuple[Optional[pyspark.sql.DataFrame], dict, str]:
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
        if self._dataset_has_schema(dataset_identity, branch):
            return (
                self._retrieve_spark_df(dataset_identity, branch),
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
        self._cached_client.fetch_dataset(dataset_identity["dataset_rid"], branch)
        return None, dataset_identity, branch

    def _offline(
        self, alias: str, branch: str
    ) -> Tuple[Optional[pyspark.sql.DataFrame], dict, str]:
        dataset_identity = self._cache.get_dataset_identity_not_branch_aware(alias)
        if self._cache.dataset_has_schema(dataset_identity):
            return self._cache[dataset_identity], dataset_identity, branch
        return None, dataset_identity, branch

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
        query = f"SELECT * FROM `{dataset_path}`"
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
            query, branch=branch, return_type="spark"
        )

    def _retrieve_from_foundry_and_cache(
        self, dataset_identity: dict, branch: str
    ) -> pyspark.sql.DataFrame:
        LOGGER.debug("Caching data for %s on branch %s", dataset_identity, branch)
        stats = self._cached_client.api.get_dataset_stats(
            dataset_identity["dataset_rid"], dataset_identity["last_transaction_rid"]
        )
        size_in_mega_bytes = stats["sizeInBytes"] / 1024 / 1024
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
                f"because dataset size {size_in_mega_bytes_rounded} megabytes > "
                f"{self.config['transforms_sql_dataset_size_threshold']} megabytes "
                f"(as defined in config['transforms_sql_dataset_size_threshold'])."
            )
            spark_df = self._read_spark_df_with_sql_query(
                dataset_identity["dataset_path"], branch
            )
            self._cache[dataset_identity] = spark_df
        return spark_df

    def dataframe(self) -> pyspark.sql.DataFrame:
        """Get the cached :external+spark:py:class:`~pyspark.sql.DataFrame` of this Input.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`: The cached DataFrame of this Input

        """
        return self._spark_df

    def get_dataset_identity(self) -> dict:
        """Returns identity of this Input.

        Returns:
            dict:
                with the keys dataset_path, dataset_rid, last_transaction_rid

        """
        return self._dataset_identity


def _get_branch(caller_filename: str) -> str:
    git_dir = path.dirname(caller_filename)

    if git_dir == "" or not path.exists(git_dir):
        # fallback for VS Interactive Console
        # or Jupyter lab on Windows
        git_dir = getcwd()

    try:
        branch = execute_as_subprocess(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=pathlib.Path(git_dir)
        )
    except (FileNotFoundError, CalledProcessError):
        warnings.warn(
            "Could not detect git branch of project, falling back to 'master'."
        )
        branch = "master"
    return branch


class Output:  # pylint: disable=too-few-public-methods
    """Specification of a transform dataset output.

    Writing the Output back to Foundry is not implemented.

    """

    def __init__(
        self,
        alias: Optional[str] = None,
        sever_permissions: Optional[bool] = False,
        description: Optional[str] = None,
        checks=None,
    ):
        # pylint: disable=unused-argument,too-many-arguments
        """Specification of a transform output.

        Args:
            alias (Optional[str]): Dataset rid or the absolute Compass path of the dataset.
                If not specified, parameter is unbound.
            sever_permissions (Optional[bool]): not implemented in foundry-dev-tools
            description (Optional[str]): not implemented in foundry-dev-tools
            checks (List[Check], Check): not implemented in foundry-dev-tools
        """
        self.alias = alias


class UnmarkingDef:
    # pylint: disable=too-few-public-methods
    """Base class for unmarking datasets configuration."""

    def __init__(
        self, marking_ids: Union[List[str], str], on_branches: Union[List[str], str]
    ):
        # pylint: disable=unused-argument,too-many-arguments
        """Default constructor.

        Args:
            marking_ids (List[str], str): List of marking identifiers or single marking identifier.
            on_branches (List[str], str): Branch on which to apply unmarking.
        """
        self.marking_ids = _as_list(marking_ids)
        self.branches = _as_list(on_branches)


class Markings(UnmarkingDef):
    # pylint: disable=too-few-public-methods
    """Specification of a marking that stops propagating from input.

    The actual marking removal is not implemented.
    """


class OrgMarkings(UnmarkingDef):
    # pylint: disable=too-few-public-methods
    """Specification of a marking that is no longer required on the output.

    The actual marking requirement check is not implemented.
    """
