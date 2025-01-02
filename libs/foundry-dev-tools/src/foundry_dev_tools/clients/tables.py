"""Implementation of the tables API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.api_types import FolderRid, SourceRid


class TablesClient(APIClient):
    """TablesClient class that implements the 'tables' api."""

    api_name = "tables"

    def api_create_table(
        self, name: str, parent_rid: FolderRid, upstream_config: dict, skip_validation: bool = False, **kwargs
    ) -> requests.Response:
        """Low level method to create a virtual table.

        Use a high level method like :py:meth:`TablesClient.create_glue_table`
        for easier usage.

        Args:
            name: The name of the table. This will be the name of the compass resource.
            parent_rid: The parent Rid of the compass resource.
            upstream_config: Individual table config.
            skip_validation: If true, skips the validation of the table. Default is False.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
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
        connection_rid: SourceRid,
        glue_table: str,
        glue_database: str,
        skip_validation: bool = False,
    ) -> dict:
        """Creates a Virtual Table backed by the Glue Catalog.

        Args:
            name: The name of the table. This will be the name of the compass resource.
            parent_rid: The parent Rid of the compass resource.
            connection_rid: The rid of the compass Source.
            glue_table: The name of the table in glue.
            glue_database: The name of the database in glue.
            skip_validation: If true, skips the validation of the table. Default is False.

        """
        upstream_config = {
            "type": "glue",
            "glue": {"connectionRid": connection_rid, "relation": {"table": glue_table, "database": glue_database}},
        }
        return self.api_create_table(
            name=name, parent_rid=parent_rid, upstream_config=upstream_config, skip_validation=skip_validation
        ).json()
