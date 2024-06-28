"""DataFrame cache implementations.

File based spark cache

"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import MutableMapping
from typing import TYPE_CHECKING

from foundry_dev_tools.errors.meta import FoundryDevToolsError
from foundry_dev_tools.utils import api_types
from foundry_dev_tools.utils.caches.metadata_store import DatasetMetadataStore
from foundry_dev_tools.utils.converter.foundry_spark import (
    foundry_schema_to_read_options,
    foundry_schema_to_spark_schema,
)
from foundry_dev_tools.utils.spark import get_spark_session

if TYPE_CHECKING:
    from pathlib import Path

    import pyspark

    from foundry_dev_tools.config.context import FoundryContext


ReadOptions = dict[str, str]


class DiskPersistenceBackedSparkCache(MutableMapping[api_types.DatasetIdentity, "pyspark.sql.DataFrame"]):
    """A cache that stores spark dataframes inside a directory."""

    _DEFAULT_FORMAT = "parquet"
    _STORE_LAST_N_TRANSACTIONS = 2

    def __init__(
        self,
        ctx: FoundryContext,
    ):
        self.ctx = ctx
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_store = DatasetMetadataStore(self.ctx)

    @property
    def _cache_dir(self) -> Path:
        return self.ctx.config.cache_dir

    def __setitem__(self, key: api_types.DatasetIdentity, value: pyspark.sql.dataframe.DataFrame) -> None:
        _validate_cache_key(key)
        path = self._get_storage_location(key, self._DEFAULT_FORMAT)
        value.write.format(self._DEFAULT_FORMAT).save(path=os.fspath(path / "spark"), mode="overwrite")
        self.set_item_metadata(path, key, value.schema.jsonValue())

    def set_item_metadata(
        self,
        path: Path,
        dataset_identity: api_types.DatasetIdentity,
        schema: api_types.FoundrySchema | None,
    ) -> None:
        """Writes schema and metadata.json entry.

        Use when files are added to cache without calling cache[entry] = df

        Args:
            path: direct path to transaction folder, e.g. /.../dss-rid/transaction1.parquet
            dataset_identity: dataset_identity with keys dataset_rid, last_transaction_rid, dataset_path
            schema: spark schema or foundrySchema or None

        """
        if schema is not None:
            with path.joinpath("_schema.json").open(mode="w", encoding="UTF-8") as file:
                json.dump(schema, file)
        self.metadata_store[dataset_identity["dataset_path"]] = dataset_identity
        # cleanup old transactions
        self._cleanup_old_transactions(dataset_identity)

    def __delitem__(self, key: api_types.DatasetIdentity) -> None:
        _validate_cache_key(key)
        try:
            shutil.rmtree(self.get_cache_dir().joinpath(key["dataset_rid"]))
            del self.metadata_store[key["dataset_path"]]
        except FileNotFoundError as exc:
            msg = f"{key}"
            raise KeyError(msg) from exc

    def __getitem__(self, key: api_types.DatasetIdentity) -> pyspark.sql.DataFrame:
        _validate_cache_key(key)
        try:
            inferred_format = _infer_dataset_format(self.get_cache_dir(), key)
            path = self._get_storage_location(key, inferred_format)
            return _read(path=path, dataset_format=inferred_format)
        except FileNotFoundError as exc:
            msg = f"{key}"
            raise KeyError(msg) from exc

    def __len__(self) -> int:
        return len(self.metadata_store)

    def __iter__(self):
        yield from self.metadata_store.values()

    def _get_storage_location(self, key: api_types.DatasetIdentity, file_format: str = "parquet") -> Path:
        return self.get_cache_dir().joinpath(
            key["dataset_rid"],
            key["last_transaction_rid"] + f".{file_format}",
        )

    def get_path_to_local_dataset(self, dataset_identity: api_types.DatasetIdentity) -> Path:
        """Returns local path to dataset.

        Args:
            dataset_identity (dict): with dataset_rid and last_transaction_rid

        Returns:
            :py:class:`~str`:
                path to dataset in cache

        """
        return get_dataset_path(self.get_cache_dir(), dataset_identity)

    def get_cache_dir(self) -> Path:
        """Returns cache directory.

        Returns:
            :py:class:`~str`:
                path to temporary cache directory

        """
        return self._cache_dir

    def get_dataset_identity_not_branch_aware(self, dataset_path_or_rid: str) -> api_types.DatasetIdentity:
        """If dataset is in cache, returns complete identity, otherwise KeyError.

        Args:
            dataset_path_or_rid (str): path to dataset in Foundry Compass or dataset_rid

        Returns:
            :py:class:`~dict`:
                dataset identity

        Raises:
            KeyError: if dataset with dataset_path_or_rid not in cache

        """
        search_key = "dataset_rid" if "ri.foundry.main.dataset" in dataset_path_or_rid else "dataset_path"
        all_cache_entries = self.metadata_store.values()
        maybe_dataset = list(
            filter(
                lambda dataset_identity: dataset_identity[search_key] == dataset_path_or_rid,  # type: ignore[literal-required]
                all_cache_entries,
            ),
        )
        if len(maybe_dataset) == 1:
            return maybe_dataset[0]

        msg = f"{dataset_path_or_rid}"
        raise KeyError(msg)

    def dataset_has_schema(self, dataset_identity: api_types.DatasetIdentity) -> bool:
        """Checks if dataset stored in cache folder has _schema.json attached.

        Args:
            dataset_identity: dataset identity

        Returns:
            :py:class:`~bool`:
                if dataset has schema return true

        """
        path = get_dataset_path(self.get_cache_dir(), dataset_identity)
        return path.joinpath("_schema.json").is_file()

    def _cleanup_old_transactions(self, dataset_identity: api_types.DatasetIdentity) -> None:
        dataset_root_folder = self.get_path_to_local_dataset(dataset_identity).parent
        all_transactions = [x.name for x in dataset_root_folder.iterdir() if x.is_dir()]
        # transaction rid's are sortable by time
        all_transactions_sorted = sorted(all_transactions, reverse=True)
        if all_transactions_sorted[0].rsplit(".", 1)[0] != dataset_identity["last_transaction_rid"]:
            msg = (
                f"Cache dir not in sync with db.\nPlease delete the cache dir ({self.get_cache_dir()})"
                " and restart the transform."
            )
            raise FoundryDevToolsError(
                msg,
            )

        transaction_to_delete = all_transactions_sorted[self._STORE_LAST_N_TRANSACTIONS :]
        for transaction in transaction_to_delete:
            directory_path = os.fspath(self.get_cache_dir() / dataset_identity["dataset_rid"] / transaction)
            shutil.rmtree(directory_path)


def _filter_unknown_files(paths: list[str]) -> list[str]:
    return list(
        filter(
            lambda item: not item.startswith(".DS_Store") or item.endswith(("parquet", "csv")),
            paths,
        ),
    )


def _validate_cache_key(key: api_types.DatasetIdentity) -> None:
    if "dataset_rid" not in key or "last_transaction_rid" not in key or "dataset_path" not in key:
        msg = (
            f"cache key: {key} is invalid. Mandatory dict keys are 'dataset_rid',"
            " 'last_transaction_rid' and 'dataset_path'"
        )
        raise FoundryDevToolsError(
            msg,
        )


def _infer_dataset_format(cache_dir: Path, dataset_identity: api_types.DatasetIdentity) -> str:
    path = cache_dir.joinpath(dataset_identity["dataset_rid"])
    last_transaction_rid = _filter_unknown_files(os.listdir(path))[0]
    return last_transaction_rid.split(".")[-1]


def get_dataset_path(cache_dir: Path, dataset_identity: api_types.DatasetIdentity) -> Path:
    """Returns the local directory of the dataset of the last transaction.

    Args:
        cache_dir (str): the base directory of the cache
        dataset_identity (dict): the identity of the dataset,
            containing dataset_rid and last_transaction_rid

    Returns:
        :py:class:`~str`:
            local path of the last transaction of the dataset

    """
    file_format = _infer_dataset_format(cache_dir, dataset_identity)
    return cache_dir.joinpath(
        dataset_identity["dataset_rid"],
        dataset_identity["last_transaction_rid"] + f".{file_format}",
    )


def _read(
    path: Path,
    dataset_format: str = "parquet",
    schema: api_types.FoundrySchema | None = None,
    read_options: ReadOptions | None = None,
) -> pyspark.sql.DataFrame:
    if dataset_format == "parquet":
        return _read_parquet(path)
    return _read_csv(path, schema=schema, read_options=read_options)


def _load_spark_schema(path: Path) -> tuple[pyspark.sql.types.StructType, dict]:
    with path.joinpath("_schema.json").open(encoding="UTF-8") as file:
        spark_or_foundry_schema = json.load(file)
    if "fieldSchemaList" not in spark_or_foundry_schema:
        legacy_read_options = {"header": "true"}
        from foundry_dev_tools._optional.pyspark import pyspark_sql_types

        return (
            pyspark_sql_types.StructType.fromJson(spark_or_foundry_schema),
            legacy_read_options,
        )
    return foundry_schema_to_spark_schema(spark_or_foundry_schema), foundry_schema_to_read_options(
        spark_or_foundry_schema,
    )


def _read_parquet(path: Path) -> pyspark.sql.DataFrame:
    return get_spark_session().read.format("parquet").load(os.fspath(path.joinpath("*")))


def _read_csv(
    path: Path,
    schema: api_types.FoundrySchema | None = None,  # TODO, type correct?
    read_options: ReadOptions | None = None,
) -> pyspark.sql.DataFrame:
    if not schema:
        schema, read_options = _load_spark_schema(path=path)
    reader = get_spark_session().read.format("csv")
    if read_options:
        for dict_key, value in read_options.items():
            reader = reader.option(dict_key, value)
    return reader.load(os.fspath(path.joinpath("*")), schema=schema)
