"""Implementation of the multipass API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils import api_types


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

    def api_get_group(self, group_id: str, **kwargs) -> requests.Response:
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

    def api_delete_group(self, group_id: str, **kwargs) -> requests.Response:
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
        return self.api_request(
            "POST",
            "clients",
            json={
                "organizationRid": organization_rid,
                "clientType": str(client_type.value),
                "displayName": display_name,
                "description": description,
                "logoUri": logo_uri,
                "grantTypes": [str(grant.value) for grant in grant_types],
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
        return self.api_request(
            "PUT",
            f"clients/{client_id}",
            json={
                "organizationRid": organization_rid,
                "clientType": str(client_type.value),
                "displayName": display_name,
                "description": description,
                "logoUri": logo_uri,
                "grantTypes": [str(grant.value) for grant in grant_types],
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
        return self.api_request(
            "PUT",
            f"client-installations/{client_id}",
            json={
                "operations": operations,
                "resources": resources,
                "markingIds": marking_ids,
                "grantTypes": [str(grant.value) for grant in grant_types] if grant_types else [],
                "require_consent": require_consent,
            },
            **kwargs,
        )
