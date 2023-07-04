"""DataFrame cache implementations.

File based spark cache

"""
import json
import os
import shutil
import sys
from collections.abc import MutableMapping
from pathlib import Path

from foundry_dev_tools.utils.caches.metadata_store import DatasetMetadataStore
from foundry_dev_tools.utils.converter.foundry_spark import (
    foundry_schema_to_read_options,
    foundry_schema_to_spark_schema,
)
from foundry_dev_tools.utils.importer import import_optional_dependency
from foundry_dev_tools.utils.spark import get_spark_session

pyspark = import_optional_dependency("pyspark")


class DiskPersistenceBackedSparkCache(MutableMapping):
    """A cache that stores spark dataframes inside a directory."""

    _DEFAULT_FORMAT = "parquet"
    _STORE_LAST_N_TRANSACTIONS = 2

    def __init__(self, cache_dir: str, clear_cache: bool = False, **kwargs):
        self._cache_dir = cache_dir
        if clear_cache:
            shutil.rmtree(self._cache_dir, ignore_errors=True)
        Path(self._cache_dir).mkdir(parents=True, exist_ok=True)
        self.metadata_store = DatasetMetadataStore(self._cache_dir)

    def __setitem__(self, key: dict, value: "pyspark.sql.dataframe.DataFrame") -> None:
        _validate_cache_key(key)
        path = self._get_storage_location(key, self._DEFAULT_FORMAT)
        value.write.format(self._DEFAULT_FORMAT).save(path=path, mode="overwrite")
        self.set_item_metadata(path, key, value.schema.jsonValue())

    def set_item_metadata(self, path: str, dataset_identity: dict, schema: dict):
        """Writes schema and metadata.json entry.

        Use when files are added to cache without calling cache[entry] = df

        Args:
            path (str): direct path to transaction folder, e.g. /.../dss-rid/transaction1.parquet
            dataset_identity (dict): dataset_identity with keys dataset_rid, last_transaction_rid, dataset_path
            schema (dict): spark schema or foundrySchema or None

        """
        if schema is not None:
            with Path(path).joinpath("_schema.json").open(
                mode="w", encoding="UTF-8"
            ) as file:
                json.dump(schema, file)
        self.metadata_store[dataset_identity["dataset_path"]] = dataset_identity
        # cleanup old transactions
        self._cleanup_old_transactions(dataset_identity)

    def __delitem__(self, key: dict) -> None:
        _validate_cache_key(key)
        try:
            shutil.rmtree(os.sep.join([self.get_cache_dir(), key["dataset_rid"]]))
            del self.metadata_store[key["dataset_path"]]
        except FileNotFoundError as exc:
            raise KeyError(f"{key}") from exc

    def __getitem__(self, key: dict) -> "pyspark.sql.DataFrame":
        _validate_cache_key(key)
        try:
            inferred_format = _infer_dataset_format(self.get_cache_dir(), key)
            path = self._get_storage_location(key, inferred_format)
            return _read(path=path, dataset_format=inferred_format)
        except FileNotFoundError as exc:
            raise KeyError(f"{key}") from exc

    def _read_parquet(self, key: dict):
        path = self._get_storage_location(key, "parquet")
        return get_spark_session().read.format("parquet").load(os.sep.join([path, "*"]))

    def _read_csv(self, key: dict):
        path = self._get_storage_location(key, "csv")
        schema, read_options = self._load_spark_schema(path=path)
        reader = get_spark_session().read.format("csv")
        if read_options:
            for dict_key, value in read_options.items():
                reader = reader.option(dict_key, value)
        return reader.load(os.sep.join([path, "*"]), schema=schema)

    def _load_spark_schema(
        self, path: str
    ) -> "tuple[pyspark.sql.types.StructType, dict]":
        with Path(path).joinpath("_schema.json").open(encoding="UTF-8") as file:
            spark_or_foundry_schema = json.load(file)
        if "fieldSchemaList" not in spark_or_foundry_schema:
            legacy_read_options = {"header": "true"}
            return (
                pyspark.sql.types.StructType.fromJson(spark_or_foundry_schema),
                legacy_read_options,
            )
        return foundry_schema_to_spark_schema(
            spark_or_foundry_schema
        ), foundry_schema_to_read_options(spark_or_foundry_schema)

    def __len__(self) -> int:
        return len(self.metadata_store)

    def __iter__(self):
        yield from self.metadata_store.values()

    def _get_storage_location(self, key: dict, file_format="parquet"):
        return os.sep.join(
            [
                self.get_cache_dir(),
                key["dataset_rid"],
                key["last_transaction_rid"] + f".{file_format}",
            ]
        )

    def get_path_to_local_dataset(self, dataset_identity: dict) -> str:
        """Returns local path to dataset.

        Args:
            dataset_identity (dict): with dataset_rid and last_transaction_rid

        Returns:
            :py:class:`~str`:
                path to dataset in cache

        """
        return get_dataset_path(self.get_cache_dir(), dataset_identity)

    def get_cache_dir(self) -> str:
        """Returns cache directory.

        Returns:
            :py:class:`~str`:
                path to temporary cache directory

        """
        return self._cache_dir

    def get_dataset_identity_not_branch_aware(self, dataset_path_or_rid: str) -> dict:
        """If dataset is in cache, returns complete identity, otherwise KeyError.

        Args:
            dataset_path_or_rid (str): path to dataset in Foundry Compass or dataset_rid

        Returns:
            :py:class:`~dict`:
                dataset identity

        Raises:
            KeyError: if dataset with dataset_path_or_rid not in cache

        """
        search_key = (
            "dataset_rid"
            if "ri.foundry.main.dataset" in dataset_path_or_rid
            else "dataset_path"
        )
        all_cache_entries = self.metadata_store.values()
        maybe_dataset = list(
            filter(
                lambda dataset_identity: dataset_identity[search_key]
                == dataset_path_or_rid,
                all_cache_entries,
            )
        )
        if len(maybe_dataset) == 1:
            return maybe_dataset[0]

        raise KeyError(f"{dataset_path_or_rid}")

    def dataset_has_schema(self, dataset_identity: dict) -> bool:
        """Checks if dataset stored in cache folder has _schema.json attached.

        Args:
            dataset_identity (dict): dataset identity

        Returns:
            :py:class:`~bool`:
                if dataset has schema return true

        """
        path = get_dataset_path(self.get_cache_dir(), dataset_identity)
        if Path(path).joinpath("_schema.json").is_file():
            return True
        return False

    def _cleanup_old_transactions(self, dataset_identity: dict):
        dataset_root_folder = Path(
            self.get_path_to_local_dataset(dataset_identity)
        ).parent
        all_transactions = [x.name for x in dataset_root_folder.iterdir() if x.is_dir()]
        # transaction rid's are sortable by time
        all_transactions_sorted = sorted(all_transactions, reverse=True)
        if (
            all_transactions_sorted[0].rsplit(".", 1)[0]
            != dataset_identity["last_transaction_rid"]
        ):
            print(all_transactions_sorted[0], file=sys.stderr)
            print(dataset_identity["last_transaction_rid"], file=sys.stderr)
            raise RuntimeError(
                "Cache dir not in sync with db.\n"
                f"Please delete the cache dir ({self.get_cache_dir()})"
                "and restart the transform."
            )

        transaction_to_delete = all_transactions_sorted[
            self._STORE_LAST_N_TRANSACTIONS :
        ]
        for transaction in transaction_to_delete:
            directory_path = os.fspath(
                Path(self.get_cache_dir())
                / dataset_identity["dataset_rid"]
                / transaction
            )
            shutil.rmtree(directory_path)


def _filter_unknown_files(paths: list):
    return list(
        filter(
            lambda item: not item.startswith(".DS_Store")
            or item.endswith("parquet")
            or item.endswith("csv"),
            paths,
        )
    )


def _validate_cache_key(key: dict):
    if (
        "dataset_rid" not in key
        or "last_transaction_rid" not in key
        or "dataset_path" not in key
    ):
        raise ValueError(
            f"cache key: {key} is invalid. "
            f"Mandatory dict keys are 'dataset_rid', "
            f"'last_transaction_rid' and 'dataset_path'"
        )


def _infer_dataset_format(cache_dir: str, dataset_identity: dict):
    path = os.sep.join([cache_dir, dataset_identity["dataset_rid"]])
    last_transaction_rid = _filter_unknown_files(os.listdir(path))[0]
    return last_transaction_rid.split(".")[-1]


def get_dataset_path(cache_dir: str, dataset_identity: dict) -> str:
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
    return os.sep.join(
        [
            cache_dir,
            dataset_identity["dataset_rid"],
            dataset_identity["last_transaction_rid"] + f".{file_format}",
        ]
    )


def _read(
    path: str, dataset_format="parquet", schema=None, read_options=None
) -> "pyspark.sql.DataFrame":
    if dataset_format == "parquet":
        return _read_parquet(path)
    return _read_csv(path, schema=schema, read_options=read_options)


def _load_spark_schema(path: str) -> "tuple[pyspark.sql.types.StructType, dict]":
    with Path(path).joinpath("_schema.json").open(encoding="UTF-8") as file:
        spark_or_foundry_schema = json.load(file)
    if "fieldSchemaList" not in spark_or_foundry_schema:
        legacy_read_options = {"header": "true"}
        return (
            pyspark.sql.types.StructType.fromJson(spark_or_foundry_schema),
            legacy_read_options,
        )
    return foundry_schema_to_spark_schema(
        spark_or_foundry_schema
    ), foundry_schema_to_read_options(spark_or_foundry_schema)


def _read_parquet(path: str):
    return get_spark_session().read.format("parquet").load(os.sep.join([path, "*"]))


def _read_csv(path: str, schema=None, read_options=None):
    if not schema:
        schema, read_options = _load_spark_schema(path=path)
    reader = get_spark_session().read.format("csv")
    if read_options:
        for dict_key, value in read_options.items():
            reader = reader.option(dict_key, value)
    return reader.load(os.sep.join([path, "*"]), schema=schema)
