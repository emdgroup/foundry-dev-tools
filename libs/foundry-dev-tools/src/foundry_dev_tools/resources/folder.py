"""Folder helper class."""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, ClassVar

from foundry_dev_tools.clients.compass import CompassClient
from foundry_dev_tools.resources import resource

if TYPE_CHECKING:
    from collections.abc import Iterator


class Folder(resource.Resource):
    """Helper class for Foundry Folders."""

    rid_start: ClassVar[str] = "ri.compass.main.folder"

    @wraps(CompassClient.get_child_objects_of_folder)
    def get_child_objects(self, *args, **kwargs) -> Iterator[dict]:
        """Wrapper around :py:meth:`foundry_dev_tools.clients.compass.CompassClient.get_child_objects_of_folder`."""
        yield from self._context.compass.get_child_objects_of_folder(self.rid, *args, **kwargs)

    def create_folder(self, name: str, marking_ids: set[str] | None = None) -> Folder:
        """Wrapper around :py:meth:`foundry_dev_tools.clients.compass.CompassClient.api_create_folder`."""
        # we already get the complete resource, but not with the needed default decorations
        res = self._context.compass.api_create_folder(name, self.rid, marking_ids).json()

        return Folder.from_rid(self._context, res["rid"])


resource.RID_CLASS_REGISTRY[Folder.rid_start] = Folder
