"""Implementation of the tables API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.api_types import FolderRid, Rid


class TablesClient(APIClient):
    """TablesClient class that implements the 'tables' api."""

    api_name = "tables"

    def api_create_table(
        self, name: str, parent_rid: FolderRid, upstream_config: dict, skip_validation: bool = False, **kwargs
    ) -> requests.Response:
        return self.api_request(
            "POST",
            api_path="tables",
            json={"name": name, "parentRid": parent_rid, "upstream": upstream_config},
            params={"skipValidation": skip_validation},
            **kwargs,
        )

    def create_glue_table(
        self,
        name: str,
        parent_rid: FolderRid,
        connection_rid: Rid,
        glue_table: str,
        glue_database: str,
        skip_validation: bool = False,
    ) -> dict:
        upstream_config = {
            "type": "glue",
            "glue": {"connectionRid": connection_rid, "relation": {"table": glue_table, "database": glue_database}},
        }
        return self.api_create_table(
            name=name, parent_rid=parent_rid, upstream_config=upstream_config, skip_validation=skip_validation
        ).json()
