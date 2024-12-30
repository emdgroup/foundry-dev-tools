"""Implementation of the foundry-stats API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils import api_types


class MagritteCoordinatorClient(APIClient):
    """The mothership that controls remote Magritte agents."""

    api_name = "magritte-coordinator"

    def api_get_oidc_metadata(self, **kwargs) -> requests.Response:
        """Returns the OIDC Metadata of the Foundry Stack.

        Args:
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        Returns:
            dict: {'issuer': 'https://pltroidcpublic<id>.s3.<region>.amazonaws.com/foundry'}
        """
        return self.api_request(
            "POST",
            "oidc/metadata",
            **kwargs,
        )

    def get_oidc_issuer(self) -> str:
        """Returns the OIDC issuer of the Foundry Stack."""
        return self.api_get_oidc_metadata().json()["issuer"]

    def api_add_source_v3(
        self, config: dict, description: dict, runtime_platform_request: dict, parent_rid: api_types.FolderRid, **kwargs
    ) -> requests.Response:
        return self.api_request(
            "POST",
            "source-store/source/v3",
            json={
                "config": config,
                "description": description,
                "runtimePlatformRequest": runtime_platform_request,
                "parentRid": parent_rid,
            },
            **kwargs,
        )

    def api_update_source_v2(
        self,
        source_rid: api_types.Rid,
        updated_source_config: dict | None = None,
        patch_cloud_runtime_platform: dict | None = None,
        **kwargs,
    ) -> requests.Response:
        """Updates a Source.

        Args:
            source_rid: The Rid of the Magritte Source
            updated_source_config: example {"type":"s3-direct","url":"s3://bucket1"}
            patch_cloud_runtime_platform: example
                {
                    "secretsToUpdate": {},
                    "removedSecrets": [],
                    "patchCloudIdentity": {},
                    "patchOidcRuntime": {},
                }
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response: dict with top level key updatedSource
            {
                "updatedSource": {
                    "sourceConfig": {
                        "type": "s3-direct",
                        "url": "s3://bucket"
                    },
                    "connections": {},
                    "additionalSecrets": {},
                    "mavenCoordinates": {
                        "userProvided": [],
                        "foundryProvided": []
                    }
                }
            }
        """
        payload = {}
        if updated_source_config:
            payload["patchSourceConfigRequest"] = {}
            payload["patchSourceConfigRequest"]["updatedSourceConfig"] = updated_source_config
        if patch_cloud_runtime_platform:
            payload["patchRuntimePlatformRequest"] = {}
            payload["patchRuntimePlatformRequest"]["patchCloudRuntimePlatform"] = patch_cloud_runtime_platform
            payload["patchRuntimePlatformRequest"]["type"] = "patchCloudRuntimePlatform"
        return self.api_request(
            "PUT",
            f"source-store/v2/sources/{source_rid}",
            json=payload,
            **kwargs,
        )
