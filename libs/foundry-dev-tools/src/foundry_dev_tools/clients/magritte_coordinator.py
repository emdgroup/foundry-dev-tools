"""Implementation of the foundry-stats API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.api_types import (
        CodeResourceType,
        FolderRid,
        MarkingId,
        NetworkEgressPolicyRid,
        Rid,
        SourceRid,
    )


class MagritteCoordinatorClient(APIClient):
    """The mothership that controls remote Magritte agents and cloud runtimes."""

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
        self, config: dict, description: dict, runtime_platform_request: dict, parent_rid: FolderRid, **kwargs
    ) -> requests.Response:
        """Low level method to add a new source to Data Connection.

        Use a high level method like :py:meth:`MagritteCoordinatorClient.create_s3_direct_source`
        for easier usage.

        Args:
            config: Source Config, individual format for each supported Source.
            description: Textual description field.
            runtime_platform_request: For Direct Connect { "cloud": {"networkEgresses": []}, "type": "cloud" }
            parent_rid: Rid of the parent Compass folder.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
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
        source_rid: Rid,
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

    def create_s3_direct_source(
        self,
        name: str,
        parent_rid: FolderRid,
        url: str,
        description: str | None = "",
        region: str | None = None,
        catalog: dict | None = None,
        sts_role_configuration: dict | None = None,
        network_egress_policies: set[Rid] | None = None,
        **source_kwargs,
    ) -> str:
        """Create a new Source of type s3-direct. Only supports Direct Connect sources.

        Args:
            name: Name of the Source in Compass
            parent_rid: Rid of the parent Compass folder.
            url: S3 Url, e.g. s3://bucket-name
            description: Textual description field.
            region: AWS Region of the bucket.
            catalog: Catalog Integration for Iceberg Tables.
            sts_role_configuration: sts role configuration for Assuming a role to connect to the bucket.
            network_egress_policies: Network Egress Policies required to connect to the bucket.
            source_kwargs: Any other arguments get added to the "source" key of the payload.
        """
        payload = {
            "source": {
                "type": "s3-direct",
                "url": url,
                **({"region": region} if region else {}),
                **({"catalog": catalog} if catalog else {}),
                **({"stsRoleConfiguration": sts_role_configuration} if sts_role_configuration else {}),
                **source_kwargs,
            }
        }
        network_egress_policies = network_egress_policies or set()

        response = self.api_add_source_v3(
            config=payload,
            description={"name": name, "description": description},
            runtime_platform_request={
                "cloud": {"networkEgresses": list(network_egress_policies)},
                "type": "cloud",
            },
            parent_rid=parent_rid,
        )
        return response.json()

    def get_source_config(self, source_rid: SourceRid) -> dict:
        """Returns the configuration of a Source."""
        return self.api_bulk_get_source_config(source_rids={source_rid}).json()[source_rid]

    def api_bulk_get_source_config(self, source_rids: set[SourceRid], **kwargs) -> requests.Response:
        """Returns the configuration of a multiple sources."""
        return self.api_request(
            "POST",
            "source-store/sources/config/bulk",
            json=list(source_rids),
            **kwargs,
        )

    def get_source_description(self, source_rid: SourceRid) -> dict:
        """Returns the description of a Source.

        Returns:
            dictionary with example payload:
            {'apiName': None, 'description': 'the desc.', 'name': 'source-name', 'type': 's3-direct'}
        """
        return self.api_bulk_get_source_description(source_rids={source_rid}).json()[source_rid]

    def api_bulk_get_source_description(self, source_rids: set[SourceRid], **kwargs) -> requests.Response:
        """Returns the description of multiple Sources."""
        return self.api_request(
            "POST",
            "source-store/sources/description/bulk",
            json=list(source_rids),
            **kwargs,
        )

    def api_get_runtime_platform(self, source_rid: SourceRid, **kwargs) -> requests.Response:
        """Get runtime platform information for a source.

        Returns:
            response: example format {
                "type": "cloud",
                "cloud": {
                    "credential": {
                        "credentialId": "ri.credential..credential.7ddaaa49-1337-1337-a0c0-8b621fa30364",
                        "secretNames": []
                    },
                    "networkEgressPolicies": {
                        "networkEgressPolicies": []
                    },
                    "cloudIdentity": null,
                    "oidcRuntime": null
                }
            }

        """
        return self.api_request(
            "GET",
            f"source-store/v2/source/{source_rid}/runtimePlatform",
            **kwargs,
        )

    def set_network_egress_policies(self, source_rid: SourceRid, network_egress_policies: set[NetworkEgressPolicyRid]):
        """Sets the network egress policies for a source, overwrites all pre-existing egress policies."""
        _ = self.api_update_source_v2(
            source_rid=source_rid,
            patch_cloud_runtime_platform={"updatedNetworkEgresses": list(network_egress_policies)},
        )

    def get_network_egress_policies(self, source_rid: SourceRid) -> list[NetworkEgressPolicyRid]:
        """Returns list of network egress policies for a source."""
        runtime_platform = self.api_get_runtime_platform(source_rid=source_rid)
        return runtime_platform.json()["cloud"]["networkEgressPolicies"]["networkEgressPolicies"]

    def add_network_egress_policy(self, source_rid: SourceRid, network_egress_policy_rid: NetworkEgressPolicyRid):
        """Adds a single egress policy to a source. Additive operation."""
        existing_egress_policies = self.get_network_egress_policies(source_rid=source_rid)
        existing_egress_policies.append(network_egress_policy_rid)
        self.set_network_egress_policies(source_rid=source_rid, network_egress_policies=set(existing_egress_policies))

    def delete_network_egress_policy(self, source_rid: SourceRid, network_egress_policy_rid: NetworkEgressPolicyRid):
        """Deletes a single egress policy from a source."""
        existing_egress_policies = self.get_network_egress_policies(source_rid=source_rid)
        existing_egress_policies.remove(network_egress_policy_rid)
        self.set_network_egress_policies(source_rid=source_rid, network_egress_policies=set(existing_egress_policies))

    def enable_s3_oidc_runtime(self, source_rid: SourceRid):
        """Enables OpenID Connect (OIDC) in (S3) Source."""
        _ = self.api_update_source_v2(
            source_rid=source_rid,
            patch_cloud_runtime_platform={"patchOidcRuntime": {"oidcRuntime": {"audience": "sts.amazonaws.com"}}},
        )

    def disable_s3_oidc_runtime(self, source_rid: SourceRid):
        """Disables OpenID Connect (OIDC) in (S3) Source."""
        _ = self.api_update_source_v2(
            source_rid=source_rid,
            patch_cloud_runtime_platform={"patchOidcRuntime": {"oidcRuntime": None}},
        )

    def api_modify_source_usage_restrictions(self, source_rid: SourceRid, payload: dict, **kwargs) -> requests.Response:
        """Update usage restrictions for a source.

        Args:
            source_rid: the source to modify usage restrictions for
            payload: the payload to modify the usage restrictions for, example:
                {"toRestrict": [], "toEnable": [{"type": "stemmaRepository", "stemmaRepository": {}}]}
            kwargs: any additional keyword arguments are passed to the requests library.

        """
        return self.api_request(
            "PUT",
            f"code-resource-source-import/sources/{source_rid}/usage-restrictions",
            json=payload,
            **kwargs,
        )

    def enable_code_imports(self, source_rid: SourceRid, to_enable: list[CodeResourceType]):
        """Enable code import on Magritte Source.

        Args:
            source_rid: Magritte Source Rid.
            to_enable: Pick from "stemmaRepository", "computeModule", "eddiePipeline"
        """
        base_payload = {"toRestrict": [], "toEnable": []}
        for entry in to_enable:
            base_payload["toEnable"].append({"type": entry, entry: {}})
        self.api_modify_source_usage_restrictions(source_rid=source_rid, payload=base_payload)

    def restrict_code_imports(self, source_rid: SourceRid, to_restrict: list[CodeResourceType]):
        """Restrict code import on Magritte Source.

        Args:
            source_rid: Magritte Source Rid.
            to_restrict: Pick from "stemmaRepository", "computeModule", "eddiePipeline"
        """
        base_payload = {"toRestrict": [], "toEnable": []}
        for entry in to_restrict:
            base_payload["toRestrict"].append({"type": entry, entry: {}})
        self.api_modify_source_usage_restrictions(source_rid=source_rid, payload=base_payload)

    def api_bulk_get_usage_restrictions_for_source(self, source_rids: list[SourceRid], **kwargs) -> requests.Response:
        """Get information about which types of code resources allow the import of the requested sources."""
        return self.api_request(
            "PUT",
            "code-resource-source-import/sources/usage-restrictions/bulk",
            json={"sourceRids": source_rids},
            **kwargs,
        )

    # Enable Export
    def api_get_export_state_for_source(self, source_rid: SourceRid, **kwargs) -> requests.Response:
        """Returns the current export config of a source.

        Returns:
            example format {
                "isEnabled": True,
                "exportableMarkings": ["marking_id"],
                "isEnabledWithoutMarkingsValidation": False
            }
        """
        return self.api_request(
            "GET",
            f"export-control/state/{source_rid}",
            **kwargs,
        )

    def api_update_export_state_for_source(
        self,
        source_rid: SourceRid,
        is_enabled: bool,
        is_enabled_without_markings_validation: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Updates Export config for a source.

        Args:
            source_rid: Magritte Source Rid.
            is_enabled: Whether the export is allowed or not.
            is_enabled_without_markings_validation: Whether export without marking validation is allowed or not.
            kwargs: any additional keyword arguments are passed to the requests library.
        """
        payload = {"isEnabled": is_enabled}
        if is_enabled_without_markings_validation is not None:
            payload["isEnabledWithoutMarkingsValidation"] = is_enabled_without_markings_validation
        return self.api_request(
            "POST",
            f"export-control/state/{source_rid}",
            json=payload,
            **kwargs,
        )

    def api_add_exportable_markings_for_source(
        self, source_rid: SourceRid, exportable_markings: list[MarkingId], **kwargs
    ) -> requests.Response:
        """Adds exportable markings for a source."""
        return self.api_request(
            "POST",
            f"export-control/markings/{source_rid}",
            json={"exportableMarkings": exportable_markings},
            **kwargs,
        )

    def api_remove_exportable_markings_for_source(
        self, source_rid: SourceRid, markings_to_remove: list[MarkingId], **kwargs
    ) -> requests.Response:
        """Remove exportable markings for a source."""
        return self.api_request(
            "DELETE",
            f"export-control/markings/{source_rid}",
            json={"markingsToRemove": markings_to_remove},
            **kwargs,
        )

    def create_snowflake_source(
        self,
        name: str,
        parent_rid: FolderRid,
        account_identifier: str,
        description: str | None = "",
        role: str | None = None,
        db: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        network_egress_policies: set[Rid] | None = None,
    ) -> str:
        """Create a new Source of type snowflake. Only supports Direct Connect sources.

        Args:
            name: Name of the Source in Compass
            parent_rid: Rid of the parent Compass folder.
            account_identifier: https://docs.snowflake.com/en/user-guide/admin-account-identifier.html
            description: Textual description field.
            role: The default access control role to use in the Snowflake session initiated by the driver
            db: The default database to use once connected
            schema: The default schema to use once connected
            warehouse: The virtual warehouse to use once connected
            network_egress_policies: Network Egress Policies required to connect to the Snowflake Account.
                Front-door plus internal stage bucket is usually required.

        """
        payload = {
            "source": {
                "type": "snowflake",
                "config": {
                    "connection": {
                        "accountIdentifier": account_identifier,
                        **({"role": role} if role else {}),
                        **({"db": db} if db else {}),
                        **({"schema": schema} if schema else {}),
                        **({"warehouse": warehouse} if warehouse else {}),
                    },
                    "auth": {
                        "basic": {"username": "", "password": "{{SNOWFLAKE_BASIC_AUTH_PASSWORD}}"},
                        "type": "basic",
                    },
                },
            }
        }
        network_egress_policies = network_egress_policies or set()

        response = self.api_add_source_v3(
            config=payload,
            description={"name": name, "description": description},
            runtime_platform_request={
                "cloud": {"networkEgresses": list(network_egress_policies)},
                "type": "cloud",
            },
            parent_rid=parent_rid,
        )
        return response.json()

    def enable_snowflake_external_oauth(self, source_rid: SourceRid):
        """Enables external OAuth (OIDC) in (Snowflake) Source."""
        config = self.get_source_config(source_rid=source_rid)["source"]

        account_identifier = config["config"]["connection"]["accountIdentifier"]
        config["config"]["auth"] = {"externalOauth": {}, "type": "externalOauth"}
        _ = self.api_update_source_v2(
            source_rid=source_rid,
            updated_source_config=config,
            patch_cloud_runtime_platform={
                "patchOidcRuntime": {
                    "oidcRuntime": {"audience": f"https://{account_identifier}.snowflakecomputing.com"}
                }
            },
        )

    def disable_snowflake_external_oauth(self, source_rid: SourceRid):
        """Disables external OAuth (OIDC) in (Snowflake) Source."""
        config = self.get_source_config(source_rid=source_rid)["source"]
        config["config"]["auth"] = {
            "basic": {"username": "", "password": "{{SNOWFLAKE_BASIC_AUTH_PASSWORD}}"},
            "type": "basic",
        }
        _ = self.api_update_source_v2(
            source_rid=source_rid,
            updated_source_config=config,
            patch_cloud_runtime_platform={"patchOidcRuntime": {"oidcRuntime": None}},
        )
