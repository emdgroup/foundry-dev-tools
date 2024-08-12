"""Implementation of the multipass API."""

from __future__ import annotations

import warnings
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.utils import api_types
from foundry_dev_tools.utils.api_types import assert_in_literal

if TYPE_CHECKING:
    from collections.abc import Iterator

    import requests

DEFAULT_TOKEN_LIFETIME_IN_SECONDS = 604800
"""The default token lifetime in seconds which is equivalent to seven days (7*24*60*60)."""

MINIMUM_TOKEN_PAGE_SIZE = 0
DEFAULT_TOKEN_PAGE_SIZE = 100
MAXIMUM_TOKEN_PAGE_SIZE = 500

MINIMUM_MAX_DURATION_IN_SECONDS = 0
"""The minimum max duration for expiration settings which is equivalent to zero seconds."""
DEFAULT_MAX_DURATION_IN_SECONDS = 604800
"""The default max duration for expiration settings which is equivalent to seven days (7*24*60*60)."""


def _validate_timezone(dt: datetime, variable_name: str) -> datetime:
    """Checks whether a datetime object contains timezone information and if not so, assigns 'UTC' timezone.

    Args:
        dt: The datetime for which to check the presence of time zone information
        variable_name: The name of the variable to inform the user in case of a malformed datetime object

    Returns:
        datetime:
            the datetime object with the time zone information
    """
    if not dt.tzinfo:
        msg = (
            f"{dt.isoformat(timespec='seconds')} passed for parameter '{variable_name} is missing timezone information."
            f"Ensure timezone is provided for datetime object. Defaults to '{timezone.utc!s}' timezone!"
        )
        warnings.warn(msg)

        dt = dt.replace(tzinfo=timezone.utc)

    return dt


# PLACEHOLDER
class MultipassClient(APIClient):
    """To be implemented/transferred."""

    api_name = "multipass"

    def get_user_info(self) -> dict:
        """Returns the json dict from the :py:meth:`foundry_dev_tools.clients.multipass.MultipassClient.api_me` API.

        Returns:
            dict:

        .. code-block:: python

           {
               "id": "<multipass-id>",
               "username": "<username>",
               "attributes": {
                   "multipass:email:primary": ["<email>"],
                   "multipass:given-name": ["<given-name>"],
                   "multipass:organization": ["<your-org>"],
                   "multipass:organization-rid": ["ri.multipass..organization. ..."],
                   "multipass:family-name": ["<family-name>"],
                   "multipass:upn": ["<upn>"],
                   "multipass:realm": ["<your-company>"],
                   "multipass:realm-name": ["<your-org>"],
               },
           }

        """
        return self.api_me().json()

    def api_me(self, **kwargs) -> requests.Response:
        """Gets the user's info.

        Args:
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            "me",
            **kwargs,
        )

    def api_create_group(
        self,
        name: str,
        organization_rids: set[api_types.OrganizationRid],
        description: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Create a new multipass group.

        Args:
            name: The name the group should receive on creation
            organization_rids: A set of organization identifiers the group will belong to
            description: An optional group description
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is the newly created project itself
        """
        body = {"groupName": name, "organizationRids": list(organization_rids)}

        if description:
            body["attributes"] = {"multipass:description": [description]}

        return self.api_request("POST", "administration/groups", json=body, **kwargs)

    def api_get_group(self, group_id: api_types.GroupId, **kwargs) -> requests.Response:
        """Returns the multipass group information.

        Args:
            group_id: multipass principal id
            **kwargs: gets passed to :py:meth:`APIClient.api_request`


        .. code-block:: python

            {
                'id': '<id>',
                'name': '<groupname>',
                'attributes': {
                'multipass:realm': ['palantir-internal-realm'],
                'multipass:organization': ['<your-org>'],
                'multipass:organization-rid': ['ri.multipass..organization.<...>'],
                'multipass:realm-name': ['Palantir Internal']
            }

        """
        return self.api_request(
            "GET",
            f"groups/{group_id}",
            **kwargs,
        )

    def api_update_group(self, group_id: api_types.GroupId, group_description: str, **kwargs) -> requests.Response:
        """Update the specified multipass group.

        Args:
            group_id: The identifier of the group which to update
            group_description: The updated description to apply to the group
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is the updated group
        """
        body = {"multipass:description": [group_description]}

        return self.api_request("PUT", f"administration/groups/{group_id}", json=body, **kwargs)

    def api_rename_group(self, group_id: api_types.GroupId, new_group_name: str, **kwargs) -> requests.Response:
        """Rename a group.

        Args:
            group_id: identifier of the group for which to update its name
            new_group_name: the name the group will be renamed to
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which consists of an entry for the renamed or original group
                and a group which serves as alias group, keeping the old name and directing to the new group
        """
        body = {"groupName": new_group_name}

        return self.api_request(
            "POST",
            f"administration/groups/{group_id}/name",
            json=body,
            **kwargs,
        )

    def api_delete_group(self, group_id: api_types.GroupId, **kwargs) -> requests.Response:
        """Deletes multipass group.

        Args:
            group_id: the multipass principal id to delete
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "DELETE",
            f"administration/groups/{group_id}",
            **kwargs,
        )

    def api_get_group_manager_managers(
        self,
        group_id: api_types.GroupId,
        **kwargs,
    ) -> requests.Response:
        """Returns the principals of a group who can add and remove members and who can grant the right to manage group permissions to others.

        Args:
            group_id: The identifier of the group for which to retrieve the manager managers
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a list of principals being manager managers
        """  # noqa: E501
        return self.api_request(
            "GET",
            f"administration/groups/{group_id}/manager-managers",
            **kwargs,
        )

    def api_get_group_member_managers(
        self,
        group_id: api_types.GroupId,
        **kwargs,
    ) -> requests.Response:
        """Returns the principals of a group who can add and remove members.

        Args:
            group_id: The identifier of the group for which to retrieve the member managers
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a list of principals being manager managers
        """
        return self.api_request(
            "GET",
            f"administration/groups/{group_id}/member-managers",
            **kwargs,
        )

    def api_update_group_managers(
        self,
        group_id: api_types.GroupId,
        deleted_manager_managers: set[api_types.PrincipalId] | None = None,
        deleted_member_managers: set[api_types.PrincipalId] | None = None,
        new_manager_managers: set[api_types.PrincipalId] | None = None,
        new_member_managers: set[api_types.PrincipalId] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Update group managers by adding new member managers or manager managers or removing member managers or manager managers.

        Args:
            group_id: The identifier of the group for which to update the group managers
            deleted_manager_managers: A set of principal identifiers for which to withdraw the role as manager manager
            deleted_member_managers: A set of principal identifiers for which to withdraw the role as member manager
            new_manager_managers: A set of principal identifiers that should be assigned the role as manager manager
            new_member_managers: A set of principal identifiers that should be assigned the role as member manager
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """  # noqa: E501
        body = {}

        if deleted_manager_managers:
            body["deletedManagerManagers"] = list(deleted_manager_managers)
        if deleted_member_managers:
            body["deletedMemberManagers"] = list(deleted_member_managers)
        if new_manager_managers:
            body["newManagerManagers"] = list(new_manager_managers)
        if new_member_managers:
            body["newMemberManagers"] = list(new_member_managers)

        return self.api_request(
            "POST",
            f"administration/groups/{group_id}/managers",
            json=body,
            **kwargs,
        )

    def add_group_manager_managers(
        self,
        group_id: api_types.GroupId,
        manager_managers: set[api_types.PrincipalId] | None = None,
    ) -> requests.Response:
        """Assign principals as manager managers for the specified group.

        Args:
            group_id: The identifier of the group for which to add the manager managers
            manager_managers: A set of principal identifiers that should be assigned the role as manager manager
        """
        return self.api_update_group_managers(group_id, new_manager_managers=manager_managers)

    def add_group_member_managers(
        self,
        group_id: api_types.GroupId,
        member_managers: set[api_types.PrincipalId] | None = None,
    ) -> requests.Response:
        """Assign principals as member managers for the specified group.

        Args:
            group_id: The identifier of the group for which to add the member managers
            member_managers: A set of principal identifiers that should be assigned the role as member manager
        """
        return self.api_update_group_managers(group_id, new_member_managers=member_managers)

    def remove_group_manager_managers(
        self,
        group_id: api_types.GroupId,
        manager_managers: set[api_types.PrincipalId] | None = None,
    ) -> requests.Response:
        """Withdraw the provided principals from the manager managers for the specified group.

        Args:
            group_id: The identifier of the group for which to remove manager managers
            manager_managers: A set of principal identifiers that should be withdrawn from the manager managers.
        """
        return self.api_update_group_managers(group_id, deleted_manager_managers=manager_managers)

    def remove_group_member_managers(
        self,
        group_id: api_types.GroupId,
        member_managers: set[api_types.PrincipalId] | None = None,
    ) -> requests.Response:
        """Withdraw the provided principals from the member managers for the specified group.

        Args:
            group_id: The identifier of the group for which to remove member managers
            member_managers: A set of principal identifiers that should be withdrawn from the member managers.
        """
        return self.api_update_group_managers(group_id, deleted_member_managers=member_managers)

    def api_add_group_members(
        self,
        group_ids: set[api_types.GroupId],
        principal_ids: set[api_types.PrincipalId],
        expirations: dict[api_types.GroupId, dict[api_types.PrincipalId, datetime]] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Add principals to the specified group.

        Args:
            group_ids: A set of group identifiers principals should be added to
            principal_ids: The identifiers of the principals to be added to the groups
            expirations: Optional expiration settings that can be passed
                if principals should only have temporal access to groups
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        body = {
            "groupIds": list(group_ids) if group_ids else None,
            "principalIds": list(principal_ids) if principal_ids else None,
            "expirations": {},
        }

        for group_id, expiration_mapping in (expirations or {}).items():
            for principal_id, expiration in expiration_mapping.items():
                validated_expiration = _validate_timezone(expiration, f"expirations['{group_id}']['{principal_id}']")

                if validated_expiration <= datetime.now(validated_expiration.tzinfo):
                    msg = (
                        f"expiration value '{validated_expiration.isoformat(timespec='seconds')}' "
                        f"for 'expirations['{group_id}']['{principal_id}']' must not be in past!"
                    )
                    raise ValueError(msg)

                body["expirations"].setdefault(group_id, {})[principal_id] = validated_expiration.isoformat(
                    timespec="seconds"
                )

        return self.api_request(
            "POST",
            "administration/groups/bulk/members",
            json=body,
            **kwargs,
        )

    def api_get_immediate_group_members(
        self,
        group_id: api_types.GroupId,
        **kwargs,
    ) -> requests.Response:
        """Returns all immediate group members for a specific group.

        Args:
            group_id: The group identifiers for which to retrieve all immediate members
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a list of principals who are members of the specified group
        """
        return self.api_request(
            "GET",
            f"administration/groups/{group_id}/members",
            **kwargs,
        )

    def api_get_all_group_members(
        self,
        group_ids: set[api_types.GroupId],
        **kwargs,
    ) -> requests.Response:
        """Returns group members for the specified groups, both immediate and indirect members
        and will only traverse groups which the user has view membership permissions on.

        Request no more than 100 group identifiers!

        Args:
            group_ids: A set of group identifiers for which to retrieve all members
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a mapping between the group id and the associated principal ids
                who are members of the given group
        """  # noqa: D205
        body = {"groupIds": list(group_ids)}
        return self.api_request(
            "PUT",
            "groups/members/all",
            json=body,
            **kwargs,
        )

    def api_get_all_group_users(
        self,
        group_id: api_types.GroupId,
        **kwargs,
    ) -> requests.Response:
        """Get all members of a group, immediate and indirect and also traverse groups where the user has no view membership permissions on.

        Similar to :py:meth:`~MultipassClient.api_get_all_group_members` but broader context by capturing all members
        and even those which the user does not have membership permissions to view.

        Args:
            group_id: The group id for which to retrieve all users
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a list of user principals
        """  # noqa: E501
        return self.api_request(
            "GET",
            f"administration/groups/{group_id}/users",
            **kwargs,
        )

    def api_remove_group_members(
        self, group_id: api_types.GroupId, principal_ids: set[api_types.PrincipalId], **kwargs
    ) -> requests.Response:
        """Remove members from the specified group.

        Args:
            group_id: The group identifiers for which to remove principals from the list of members
            principal_ids: A set of principal identifiers which should be removed from the member list of the group
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "DELETE",
            f"administration/groups/{group_id}/members",
            json=list(principal_ids),
            **kwargs,
        )

    def api_get_group_member_expirations(
        self,
        group_ids: set[api_types.GroupId],
        **kwargs,
    ) -> requests.Response:
        """Get the expiration for the members of the specified group identifiers.

        Request no more than 100 group identifiers!

        Args:
            group_ids: The identifiers of the groups for which to retrieve the expiration of members
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a mapping between the group id and the associated principals
                along their expiration datetime
        """
        body = {"groupIds": list(group_ids)}

        return self.api_request(
            "PUT",
            "groups/expirations/members",
            json=body,
            **kwargs,
        )

    def api_get_group_member_expiration_settings(
        self,
        group_ids: set[api_types.GroupId],
        **kwargs,
    ) -> requests.Response:
        """Returns group member expiration settings for the specified groups that the user has view permission on.

        Args:
            group_ids: A set of group identifiers for which to gather the expiration settings information
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a mapping between the group identifiers
                and their respective expiration settings
        """
        body = {"groupIds": list(group_ids)}
        return self.api_request(
            "PUT",
            "groups/member-expiration-settings",
            json=body,
            **kwargs,
        )

    def api_update_group_member_expiration_settings(
        self,
        group_id: api_types.GroupId,
        max_expiration: datetime | None = None,
        max_duration_in_seconds: int | None = None,
        **kwargs,
    ) -> requests.Response:
        """Update group member expiration settings for the specified group.

        Args:
            group_id: The identifier of the group whose expiration settings will be updated
            max_expiration: The time in the future on which all new membership will be automatically expired
                and no new membership can be requested after this time
            max_duration_in_seconds: When adding a new membership, it can last no longer than
                the specified maximum duration. Expiration of existing memberships will be adjusted accordingly.
                Value passed must be greater equal :py:const:`MINIMUM_MAX_DURATION_IN_SECONDS` and defaults to
                :py:const:`DEFAULT_MAX_DURATION_IN_SECONDS` if it does not meet the condition
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which holds the updated expiration settings
        """
        if max_expiration:
            max_expiration = _validate_timezone(max_expiration, "max_expiration")
            now = datetime.now(max_expiration.tzinfo)

            if max_expiration <= now:
                new_expiration = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

                msg = (
                    f"'max_expiration' is {max_expiration.isoformat(timespec='seconds')} in past but must be in future!"
                    f" Defaulting to {new_expiration.isoformat(timespec='seconds')}."
                )
                warnings.warn(msg)

                max_expiration = new_expiration

        if max_duration_in_seconds and max_duration_in_seconds < MINIMUM_MAX_DURATION_IN_SECONDS:
            msg = (
                f"'max_duration_in_seconds' is {max_duration_in_seconds} "
                f"but must be greater equal {MINIMUM_MAX_DURATION_IN_SECONDS}. "
                f"Defaulting to {DEFAULT_MAX_DURATION_IN_SECONDS}."
            )
            warnings.warn(msg)

            max_duration_in_seconds = DEFAULT_MAX_DURATION_IN_SECONDS

        body = {
            "maxExpiration": max_expiration.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if max_expiration
            else None,
            "maxDurationInSeconds": max_duration_in_seconds,
        }

        return self.api_request(
            "PUT",
            f"groups/member-expiration-settings/groups/{group_id}",
            json=body,
            **kwargs,
        )

    def reset_group_member_expiration_settings(
        self,
        group_id: api_types.GroupId,
    ) -> bool:
        """Reset the group member expiration settings for the specified group.

        Args:
            group_id: The identifier of the group for which to restore the initial state of the expiration settings

        Returns:
            bool:
                indicator whether the group member expiration settings have successfully been reset
        """
        resp = self.api_update_group_member_expiration_settings(
            group_id, max_expiration=None, max_duration_in_seconds=None
        )

        return resp.json()

    def api_create_third_party_application(
        self,
        client_type: api_types.MultipassClientType,
        display_name: str,
        description: str | None,
        grant_types: list[api_types.MultipassGrantType],
        redirect_uris: list | None,
        logo_uri: str | None,
        organization_rid: str,
        allowed_organization_rids: list | None = None,
        resources: list[api_types.Rid] | None = None,
        operations: list[str] | None = None,
        marking_ids: list[str] | None = None,
        role_set_id: str | None = None,
        role_grants: dict[str, list[str]] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Creates Foundry Third Party application (TPA).

        https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/
        User must have 'Manage OAuth 2.0 clients' workflow permissions.

        Args:
            client_type: Server Application (CONFIDENTIAL) or
                Native or single-page application (PUBLIC)
            display_name: Display name of the TPA
            description: Long description of the TPA
            grant_types: Usually, ["AUTHORIZATION_CODE", "REFRESH_TOKEN"] (authorization code grant)
                or ["REFRESH_TOKEN", "CLIENT_CREDENTIALS"] (client credentials grant)
            redirect_uris: Redirect URLs of TPA, used in combination with AUTHORIZATION_CODE grant
            logo_uri: URI or embedded image 'data:image/png;base64,<...>'
            organization_rid: Parent Organization of this TPA
            allowed_organization_rids: Passing None or empty list means TPA is activated for all
                Foundry organizations
            resources: Resources allowed to access by the client, otherwise no resource restrictions
            operations: Operations the client can be granted, otherwise no operation restrictions
            marking_ids: Markings allowed to access by the client, otherwise no marking restrictions
            role_set_id: roles allowed for this client, defaults to `oauth2-client`
            role_grants: mapping between roles and principal ids dict[role id,list[principal id]]
            **kwargs: gets passed to :py:meth:`APIClient.api_request`


        See below for the structure

        .. code-block:: python

            {
                "clientId":"<...>",
                "clientSecret":"<...>",
                "clientType":"<CONFIDENTIAL/PUBLIC>",
                "organizationRid":"<...>",
                "displayName":"<...>",
                "description":null,
                "logoUri":null,
                "grantTypes":[<"AUTHORIZATION_CODE","REFRESH_TOKEN","CLIENT_CREDENTIALS">],
                "redirectUris":[],
                "allowedOrganizationRids":[]
            }

        """
        assert_in_literal(client_type, api_types.MultipassClientType, "client_type")

        for grant_type in grant_types:
            assert_in_literal(grant_type, api_types.MultipassGrantType, "grant_types")

        return self.api_request(
            "POST",
            "clients",
            json={
                "organizationRid": organization_rid,
                "clientType": client_type,
                "displayName": display_name,
                "description": description,
                "logoUri": logo_uri,
                "grantTypes": grant_types,
                "redirectUris": redirect_uris,
                "allowedOrganizationRids": allowed_organization_rids,
                "resources": resources,
                "operations": operations,
                "markingIds": marking_ids,
                "roleSetId": role_set_id,
                "roleGrants": role_grants,
            },
            **kwargs,
        )

    def api_delete_third_party_application(self, client_id: str, **kwargs) -> requests.Response:
        """Deletes a Third Party Application.

        Args:
            client_id : The unique identifier of the TPA.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "DELETE",
            f"clients/{client_id}",
            **kwargs,
        )

    def api_update_third_party_application(
        self,
        client_id: str,
        client_type: api_types.MultipassClientType,
        display_name: str,
        description: str | None,
        grant_types: list[api_types.MultipassGrantType],
        redirect_uris: list | None,
        logo_uri: str | None,
        organization_rid: str,
        allowed_organization_rids: list | None = None,
        resources: list[api_types.Rid] | None = None,
        operations: list[str] | None = None,
        marking_ids: list[str] | None = None,
        role_set_id: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Updates Foundry Third Party application (TPA).

        https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/
        User must have 'Manage OAuth 2.0 clients' workflow permissions.

        Args:
            client_id: The unique identifier of the TPA.
            client_type: Server Application (CONFIDENTIAL) or
                Native or single-page application (PUBLIC)
            display_name: Display name of the TPA
            description: Long description of the TPA
            grant_types: Usually, ["AUTHORIZATION_CODE", "REFRESH_TOKEN"] (authorization code grant)
                or ["REFRESH_TOKEN", "CLIENT_CREDENTIALS"] (client credentials grant)
            redirect_uris: Redirect URLs of TPA, used in combination with AUTHORIZATION_CODE grant
            logo_uri: URI or embedded image 'data:image/png;base64,<...>'
            organization_rid: Parent Organization of this TPA
            allowed_organization_rids: Passing None or empty list means TPA is activated for all
                Foundry organizations
            resources: Resources allowed to access by the client, otherwise no resource restrictions
            operations: Operations the client can be granted, otherwise no operation restrictions
            marking_ids: Markings allowed to access by the client, otherwise no marking restrictions
            role_set_id: roles allowed for this client, defaults to `oauth2-client`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Reponse in following structure:

        .. code-block:: python

            {
                "clientId":"<...>",
                "clientType":"<CONFIDENTIAL/PUBLIC>",
                "organizationRid":"<...>",
                "displayName":"<...>",
                "description":null,
                "logoUri":null,
                "grantTypes":[<"AUTHORIZATION_CODE","REFRESH_TOKEN","CLIENT_CREDENTIALS">],
                "redirectUris":[],
                "allowedOrganizationRids":[]
            }

        """
        assert_in_literal(client_type, api_types.MultipassClientType, "client_type")

        for grant_type in grant_types:
            assert_in_literal(grant_type, api_types.MultipassGrantType, "grant_types")

        return self.api_request(
            "PUT",
            f"clients/{client_id}",
            json={
                "organizationRid": organization_rid,
                "clientType": client_type,
                "displayName": display_name,
                "description": description,
                "logoUri": logo_uri,
                "grantTypes": grant_types,
                "redirectUris": redirect_uris,
                "allowedOrganizationRids": allowed_organization_rids,
                "resources": resources,
                "operations": operations,
                "markingIds": marking_ids,
                "roleSetId": role_set_id,
            },
            **kwargs,
        )

    def api_rotate_third_party_application_secret(
        self,
        client_id: str,
        **kwargs,
    ) -> requests.Response:
        """Rotates Foundry Third Party application (TPA) secret.

        Args:
            client_id: The unique identifier of the TPA.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        See below for the structure:

        .. code-block:: python

            {
                "clientId":"<...>",
                "clientSecret": "<...>",
                "clientType":"<CONFIDENTIAL/PUBLIC>",
                "organizationRid":"<...>",
                "displayName":"<...>",
                "description":null,
                "logoUri":null,
                "grantTypes":[<"AUTHORIZATION_CODE","REFRESH_TOKEN","CLIENT_CREDENTIALS">],
                "redirectUris":[],
                "allowedOrganizationRids":[]
            }

        """
        return self.api_request(
            "PUT",
            f"clients/{client_id}/rotateSecret",
            **kwargs,
        )

    def api_enable_third_party_application(
        self,
        client_id: str,
        operations: list | None = None,
        resources: list | None = None,
        marking_ids: list[str] | None = None,
        grant_types: list[api_types.MultipassGrantType] | None = None,
        require_consent: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Enables Foundry Third Party application (TPA).

        Args:
            client_id: The unique identifier of the TPA.
            operations: Scopes that this TPA is allowed to use (To be confirmed)
                if None or empty list is passed, all scopes will be activated.
            resources: Compass Project RID's that this TPA is allowed to access,
                if None or empty list is passed, unrestricted access will be given.
            marking_ids: Marking Ids that this TPA is allowed to access,
                if None or empty list is passed, unrestricted access will be given.
            grant_types: Grant types that this TPA is allowed to use to access resources,
                if None is passed, no grant type restrictions
                if an empty list is passed, no grant types are allowed for this TPA
            require_consent: Wether users need to provide consent for this application to act on their behalf,
                defaults to true
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Response with the following structure:

        .. code-block:: python

            {
                "client": {
                    "clientId": "<...>",
                    "organizationRid": "ri.multipass..organization.<...>",
                    "displayName": "<...>",
                    "description": None,
                    "logoUri": None,
                },
                "installation": {"resources": [], "operations": [], "markingIds": None},
            }

        """
        if grant_types is not None:
            for grant_type in grant_types:
                assert_in_literal(grant_type, api_types.MultipassGrantType, "grant_types")

        return self.api_request(
            "PUT",
            f"client-installations/{client_id}",
            json={
                "operations": operations,
                "resources": resources,
                "markingIds": marking_ids,
                "grantTypes": grant_types,
                "require_consent": require_consent,
            },
            **kwargs,
        )

    def api_create_token(
        self,
        name: str,
        description: str = "",
        seconds_to_live: int = DEFAULT_TOKEN_LIFETIME_IN_SECONDS,
        **kwargs,
    ) -> requests.Response:
        """Issue a new token generated by the user.

        Args:
            name: The name of the token
            description: A description to explain the purpose of the token
            seconds_to_live: The lifetime of the token until when it is valid
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json including the actual jwt bearer token along some token information

        Response with the following structure:

        .. code-block:: python

            {
                'access_token': 'ey<...>',
                'tokenInfo': {
                    'clientId': '<...>',
                    'description': '<description>',
                    'expires_in': <seconds-to-live>,
                    'grantType': '<...>',
                    'name': '<name>',
                    'scope': {
                        'type': '<...>'
                    },
                    'state': 'ENABLED',
                    'tokenId': '<...>',
                    'userId': '<...>'
                }
            }
        """
        body = {"name": name, "description": description, "secondsToLive": seconds_to_live}

        return self.api_request(
            "POST",
            "tokens",
            json=body,
            **kwargs,
        )

    def api_revoke_token(self, token_id: api_types.TokenId, **kwargs) -> requests.Response:
        """Issue a new token generated by the user.

        Args:
            token_id: The identifier of the token that should be revoked
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which holds a boolean value
                indicating whether the revocation was successful

        """
        body = {"tokenId": token_id}

        return self.api_request(
            "POST",
            "tokens/revoke",
            json=body,
            **kwargs,
        )

    def api_get_tokens(
        self,
        token_type: api_types.TokenType | None = None,
        limit: int = DEFAULT_TOKEN_PAGE_SIZE,
        page_token: int | None = None,
        **kwargs,
    ) -> requests.Response:
        """Issue a new token generated by the user.

        Args:
            token_type: Only tokens of this type will be returned
            limit: The maximum number of tokens to fetch per page
            page_token: start position for request.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which holds a list of tokens providing information about the token
                and a `nextPageToken` used for pagination

        Response with the following structure:

        .. code-block:: python

            {
                'values': [
                    {
                        'tokenId': '<...>',
                        'clientId': '<...>',
                        'grantType': '<...>',
                        'userId': '<...>'
                        'expires_in': <seconds-to-live>,
                        'scope': {
                            'type': '<...>'
                        },
                        'name': '<name>',
                        'description': '<description>',
                        'state': 'ENABLED'
                    },
                    ...
                ],
                'nextPageToken': <...>
            }
        """
        if limit < MINIMUM_TOKEN_PAGE_SIZE:
            warnings.warn(
                f"Parameter `limit` ({limit}) is less than the minimum ({MINIMUM_TOKEN_PAGE_SIZE}). "
                f"Defaulting to {MINIMUM_TOKEN_PAGE_SIZE}."
            )
            limit = MINIMUM_TOKEN_PAGE_SIZE
        elif limit > MAXIMUM_TOKEN_PAGE_SIZE:
            warnings.warn(
                f"Parameter `limit` ({limit}) is greater than the maximum ({MAXIMUM_TOKEN_PAGE_SIZE}). "
                f"Defaulting to {MAXIMUM_TOKEN_PAGE_SIZE}."
            )
            limit = MAXIMUM_TOKEN_PAGE_SIZE

        params = {"limit": limit, "start": page_token}

        if token_type:
            assert_in_literal(token_type, api_types.TokenType, "token_type")
            params["type"] = token_type

        return self.api_request(
            "GET",
            "tokens",
            params=params,
            **kwargs,
        )

    def get_tokens(
        self,
        token_type: api_types.TokenType | None = None,
        limit: int = DEFAULT_TOKEN_PAGE_SIZE,
    ) -> Iterator[dict]:
        """Issue a new token generated by the user (automatic pagination).

        Args:
            token_type: Only tokens of this type will be returned
            limit: The maximum number of tokens to fetch per page

        Returns:
            Iterator[dict]:
                An iterator over all the tokens
        """
        next_page_token = None

        while True:
            response_as_json = self.api_get_tokens(
                token_type=token_type, limit=limit, page_token=next_page_token
            ).json()

            yield from response_as_json["values"]

            if (next_page_token := response_as_json["nextPageToken"]) is None:
                break

    def api_get_ttl(self) -> requests.Response:
        """Returns the time-to-live of the current token, being passed along the request.

        Returns:
            requests.Response:
                the response contains the remaining lifetime of the token until it expires
        """
        return self.api_request("GET", "token/ttl")

    def api_get_all_organizations(
        self,
        **kwargs,
    ) -> requests.Response:
        """Returns a list of all organizations the user can view.

        Args:
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which is a list of organizations and their associated properties
        """
        return self.api_request(
            "GET",
            "organizations/all",
            **kwargs,
        )
