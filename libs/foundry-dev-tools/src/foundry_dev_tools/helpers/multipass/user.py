"""Group helper class."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.helpers.multipass.principal import Principal

if TYPE_CHECKING:
    import sys

    from foundry_dev_tools import FoundryContext
    from foundry_dev_tools.utils import api_types

    if sys.version_info < (3, 11):
        from typing_extensions import Self
    else:
        from typing import Self


class User(Principal):
    """Helper class for users."""

    username: str

    def __init__(self, *args, **kwargs) -> None:
        """Not intended to be initialized directly. Use :py:meth:`User.me` instead."""
        super().__init__(*args, **kwargs)

    def _from_json(
        self,
        id: api_types.UserId,  # noqa: A002
        attributes: dict[str, list[str]],
        username: str,
    ) -> None:
        self.id = id
        self.attributes = attributes
        self.username = username

    @classmethod
    def _create_instance(
        cls,
        ctx: FoundryContext,
        json: dict,
    ) -> Self:
        instance = cls.__new__(cls)
        instance._context = ctx  # noqa: SLF001
        cls.__init__(instance, **json)
        return instance

    @classmethod
    def me(cls, context: FoundryContext) -> Self:
        """Returns the user.

        Args:
            context: the foundry context for the user
        """
        json = context.multipass.api_me().json()
        return cls._create_instance(context, json=json)
