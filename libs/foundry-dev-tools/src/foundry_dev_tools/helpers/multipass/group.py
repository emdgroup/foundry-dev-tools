"""Group helper class."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.helpers.multipass.principal import Principal

if TYPE_CHECKING:
    import sys
    from datetime import datetime

    from foundry_dev_tools import FoundryContext
    from foundry_dev_tools.utils import api_types

    if sys.version_info < (3, 11):
        from typing_extensions import Self
    else:
        from typing import Self


class Group(Principal):
    """Helper class for groups."""

    name: str

    def __init__(self, *args, **kwargs) -> None:
        """Not intended to be initialized directly. Use :py:meth:`Group.from_id` or :py:meth:`Group.create_group` instead."""  # noqa: E501
        super().__init__(*args, **kwargs)

    def _from_json(
        self,
        id: api_types.GroupId,  # noqa: A002
        attributes: dict[str, list[str]],
        name: str,
    ) -> None:
        self.id = id
        self.attributes = attributes
        self.name = name

    @classmethod
    def _create_instance(cls, ctx: FoundryContext, json: dict) -> Self:
        instance = cls.__new__(cls)
        instance._context = ctx  # noqa: SLF001
        cls.__init__(instance, **json)
        return instance

    @classmethod
    def from_id(
        cls,
        context: FoundryContext,
        group_id: api_types.GroupId,
    ) -> Self:
        """Returns group from id.

        Args:
            context: the foundry context for the group
            group_id: the id of the group on foundry
        """
        json = context.multipass.api_get_group(group_id).json()

        inst = cls._create_instance(context, json=json)
        inst.sync()

        return inst

    @classmethod
    def create(
        cls,
        context: FoundryContext,
        name: str,
        organization_rids: set[api_types.OrganizationRid],
        /,
        *,
        description: str | None = None,
    ) -> Self:
        """Create a new multipass group.

        Args:
            context: the foundry context for the group
            name: the name the group should receive on creation
            organization_rids: a set of organization identifiers the group will belong to
            description: an optional group description
        """
        json = context.multipass.api_create_group(name, organization_rids, description).json()

        return cls._create_instance(context, json=json)

    def update(self, group_description: str) -> Self:
        """Update the multipass group.

        Args:
            group_description: The updated description to apply to the group
        """
        self._context.multipass.api_update_group(self.id, group_description)
        return self.sync()

    def rename(self, new_group_name: str) -> dict[str, api_types.Group]:
        """Rename group.

        Args:
            new_group_name: the name the group will be renamed to

        Returns:
            dict[str, api_types.Group]:
                the dict contains an entry for the renamed group and an alias group
                which still holds to the old name to support compatibility for projects referring to the old name
        """
        json = self._context.multipass.api_rename_group(self.id, new_group_name).json()
        self.sync()

        return json

    def delete(self) -> Self:
        """Deletes multipass group."""
        self._context.multipass.api_delete_group(self.id)
        return self

    def update_managers(
        self,
        deleted_manager_managers: set[api_types.PrincipalId] | None = None,
        deleted_member_managers: set[api_types.PrincipalId] | None = None,
        new_manager_managers: set[api_types.PrincipalId] | None = None,
        new_member_managers: set[api_types.PrincipalId] | None = None,
    ) -> Self:
        """Update group managers by adding new member managers or manager managers or removing member managers or manager managers.

        Args:
            deleted_manager_managers: A set of principal identifiers for which to withdraw the role as manager manager
            deleted_member_managers: A set of principal identifiers for which to withdraw the role as member manager
            new_manager_managers: A set of principal identifiers that should be assigned the role as manager manager
            new_member_managers: A set of principal identifiers that should be assigned the role as member manager
        """  # noqa: E501
        self._context.multipass.api_update_group_managers(
            self.id, deleted_manager_managers, deleted_member_managers, new_manager_managers, new_member_managers
        )
        return self

    def get_manager_managers(
        self,
    ) -> list[api_types.User | api_types.Group]:
        """Returns the principals of the group who can add and remove members and who can grant the right to manage group permissions to others."""  # noqa: E501
        return self._context.multipass.api_get_group_manager_managers(self.id).json()

    def add_manager_managers(
        self,
        manager_managers: set[api_types.PrincipalId],
    ) -> Self:
        """Assign principals as manager managers to the group.

        Args:
            manager_managers: A set of principal identifiers that should be assigned the role as manager manager
        """
        self._context.multipass.add_group_manager_managers(self.id, manager_managers)
        return self

    def remove_manager_managers(
        self,
        manager_managers: set[api_types.PrincipalId],
    ) -> Self:
        """Withdraw the provided principals from the manager managers for the group.

        Args:
            manager_managers: A set of principal identifiers that should be withdrawn from the manager managers.
        """
        self._context.multipass.remove_group_manager_managers(self.id, manager_managers)
        return self

    def get_member_managers(
        self,
    ) -> list[api_types.User | api_types.Group]:
        """Returns the principals of the group who can add and remove members."""
        return self._context.multipass.api_get_group_member_managers(self.id).json()

    def add_member_manager(self, member_managers: set[api_types.PrincipalId]) -> Self:
        """Assign principals as member managers to the group.

        Args:
            member_managers: A set of principal identifiers that should be assigned the role as member manager
        """
        self._context.multipass.add_group_member_managers(self.id, member_managers)
        return self

    def remove_member_managers(
        self,
        member_managers: set[api_types.PrincipalId],
    ) -> Self:
        """Withdraw the provided principals from the member managers for the group.

        Args:
            member_managers: A set of principal identifiers that should be withdrawn from the member managers.
        """
        self._context.multipass.remove_group_member_managers(self.id, member_managers)
        return self

    def add_members(
        self,
        principal_ids: set[api_types.PrincipalId],
        expirations: dict[api_types.GroupId, dict[api_types.PrincipalId, datetime]] | None = None,
    ) -> Self:
        """Add principals to the group.

        Args:
            principal_ids: The identifiers of the principals to be added to the group
            expirations: Optional expiration settings that can be passed
                if principals should only have temporal access to the group
        """
        self._context.multipass.api_add_group_members({self.id}, principal_ids, expirations)
        return self

    def get_members(
        self,
    ) -> list[api_types.User | api_types.Group]:
        """Receive all immediate group members of the group."""
        return self._context.multipass.api_get_immediate_group_members(self.id).json()

    def remove_members(self, principal_ids: set[api_types.PrincipalId]) -> Self:
        """Remove members from the group.

        Args:
            principal_ids: A set of principal identifiers which should be removed from the member list of the group
        """
        self._context.multipass.api_remove_group_members(self.id, principal_ids)
        return self

    def update_expiration_settings(
        self,
        max_expiration: datetime | None = None,
        max_duration_in_seconds: int | None = None,
    ) -> Self:
        """Update group member expiration settings of the group.

        Args:
            max_expiration: The time in the future on which all new membership will be automatically expired
                and no new membership can be requested after this time
            max_duration_in_seconds: When adding a new membership, it can last no longer than
                the specified maximum duration. Expiration of existing memberships will be adjusted accordingly.
                Value passed must be greater equal
                :py:const:`~foundry_dev_tools.clients.multipass.MINIMUM_MAX_DURATION_IN_SECONDS` and defaults to
                :py:const:`~foundry_dev_tools.clients.multipass.DEFAULT_MAX_DURATION_IN_SECONDS`
                if it does not meet the condition
        """
        self._context.multipass.api_update_group_member_expiration_settings(
            self.id, max_expiration, max_duration_in_seconds
        ).json()
        return self

    def get_expiration_settings(
        self,
    ) -> api_types.GroupMemberExpirationSettings:
        """Returns group member expiration settings for the group."""
        return self._context.multipass.api_get_group_member_expiration_settings({self.id}).json()

    def reset_expiration_settings(self) -> Self:
        """Reset the group member expiration settings for the group."""
        self._context.multipass.reset_group_member_expiration_settings(self.id)
        return self

    def sync(self) -> Self:
        """Fetches the attributes again."""
        resource_json = self._context.multipass.api_get_group(self.id).json()
        self._from_json(**resource_json)

        return self
