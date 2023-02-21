"""A Metadata store for cached datasets.

Primary use is resolution of dataset_path and dataset_Rid for
offline usage of Foundry DevTools
"""
# pylint: disable=invalid-name
import json
import os.path
from collections.abc import MutableMapping
from shutil import rmtree
from typing import Iterator


class DatasetMetadataStore(MutableMapping):
    """A Metadata store for cached datasets.

    Primary use is resolution of dataset_path and dataset_rid for
    offline usage of Foundry DevTools
    """

    def __init__(self, cache_dir: str):
        """Init meta data store.

        Args:
            cache_dir (str): cache directory path
        """
        self._cache_dir = cache_dir
        self._db_path = os.sep.join([self._cache_dir, "metadata.json"])
        if not os.path.isfile(self._db_path):
            # clear cache if metadata.json does not exist
            rmtree(self._cache_dir)
            os.makedirs(self._cache_dir)
            # create empty metadata.json
            with open(self._db_path, "w", encoding="UTF-8") as file:
                json.dump({}, file)

    def __setitem__(self, dataset_path: str, dataset_identity: dict) -> None:
        db = self._read_db()
        db[dataset_identity["dataset_path"]] = dataset_identity
        self._write_db(db)

    def __delitem__(self, dataset_path: str) -> None:
        db = self._read_db()
        if dataset_path in db:
            del db[dataset_path]
            self._write_db(db)
        else:
            raise KeyError(dataset_path)

    def __getitem__(self, dataset_path: str) -> dict:
        db = self._read_db()
        if dataset_path in db:
            return db[dataset_path]
        raise KeyError(dataset_path)

    def __len__(self) -> int:
        db = self._read_db()
        return len([db[key] for key in db.keys()])

    def __iter__(self) -> Iterator[dict]:
        db = self._read_db()
        yield from db.keys()

    def _read_db(self):
        with open(self._db_path, "r", encoding="UTF-8") as file:
            return json.load(file)

    def _write_db(self, db):
        with open(self._db_path, "w", encoding="UTF-8") as file:
            json.dump(db, file, indent=4)
            file.flush()
            os.fsync(file.fileno())
