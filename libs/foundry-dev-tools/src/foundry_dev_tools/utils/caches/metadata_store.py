"""A Metadata store for cached datasets.

Primary use is resolution of dataset_path and dataset_Rid for
offline usage of Foundry DevTools
"""

from __future__ import annotations

import json
import os
from collections.abc import MutableMapping
from shutil import rmtree
from typing import TYPE_CHECKING

from foundry_dev_tools.utils import api_types

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from foundry_dev_tools.config.context import FoundryContext


# TODO shelve? https://docs.python.org/3/library/shelve.html
class DatasetMetadataStore(MutableMapping[api_types.FoundryPath, api_types.DatasetIdentity]):
    """A Metadata store for cached datasets.

    Primary use is resolution of dataset_path and dataset_rid for
    offline usage of Foundry DevTools
    """

    def __init__(self, ctx: FoundryContext):
        """Init meta data store.

        Args:
            ctx: the foundry context, used for the cache_dir
        """
        self.ctx = ctx
        self._db_path = self._cache_dir / "metadata.json"
        if not self._db_path.exists():
            # clear cache if metadata.json does not exist
            rmtree(self._cache_dir)
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            # create empty metadata.json
            with self._db_path.open(mode="w", encoding="UTF-8") as file:
                json.dump({}, file)

    @property
    def _cache_dir(self) -> Path:
        return self.ctx.config.cache_dir

    def __setitem__(self, dataset_path: api_types.FoundryPath, dataset_identity: api_types.DatasetIdentity) -> None:
        db = self._read_db()
        db[dataset_identity["dataset_path"]] = dataset_identity
        self._write_db(db)

    def __delitem__(self, dataset_path: api_types.FoundryPath) -> None:
        db = self._read_db()
        if dataset_path in db:
            del db[dataset_path]
            self._write_db(db)
        else:
            raise KeyError(dataset_path)

    def __getitem__(self, dataset_path: api_types.FoundryPath) -> api_types.DatasetIdentity:
        db = self._read_db()
        if dataset_path in db:
            return db[dataset_path]
        raise KeyError(dataset_path)

    def __len__(self) -> int:
        db = self._read_db()
        return len(db.keys())

    def __iter__(self) -> Iterator[api_types.FoundryPath]:
        db = self._read_db()
        yield from db.keys()

    def _read_db(self) -> DatasetMetadataStore:
        with self._db_path.open(encoding="UTF-8") as file:
            return json.load(file)

    def _write_db(self, db: DatasetMetadataStore) -> None:
        with self._db_path.open(mode="w", encoding="UTF-8") as file:
            json.dump(db, file, indent=4)
            file.flush()
            os.fsync(file.fileno())
