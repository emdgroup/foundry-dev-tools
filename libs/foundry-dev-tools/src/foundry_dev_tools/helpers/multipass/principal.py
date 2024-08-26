"""Group helper class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from foundry_dev_tools.helpers import multipass

if TYPE_CHECKING:
    import sys

    from foundry_dev_tools.config.context import FoundryContext
    from foundry_dev_tools.utils import api_types

    if sys.version_info < (3, 11):
        from typing_extensions import Self
    else:
        from typing import Self


class Principal(ABC):
    """Helper class for principals."""

    _context: FoundryContext

    id: api_types.PrincipalId
    attributes: dict[str, list[str]]

    def __init__(self, *args, **kwargs) -> None:
        """Not intended to be initialized directly. Use :py:meth:`Principal.from_id` instead."""
        self._from_json(*args, **kwargs)

    @abstractmethod
    def _from_json(self, *args, **kwargs) -> None:
        pass

    @classmethod
    @abstractmethod
    def _create_instance(cls, ctx: FoundryContext, json: dict) -> Self:
        pass

    @classmethod
    def from_id(
        cls,
        context: FoundryContext,
        principal_id: api_types.PrincipalId,
    ) -> Self:
        """Returns group from id.

        Args:
            context: the foundry context for the group
            principal_id: the id of the principal on foundry
        """
        json = context.multipass.api_get_principals({principal_id}).json()[0]

        # Users contain the 'username' attribute while groups only specify a 'name' attribute
        if "username" in json:
            inst = multipass.User._create_instance(context, json)  # noqa: SLF001
        else:
            inst = multipass.Group._create_instance(context, json)  # noqa: SLF001

        return inst
