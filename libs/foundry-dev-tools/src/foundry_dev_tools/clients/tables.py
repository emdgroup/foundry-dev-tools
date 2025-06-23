"""Implementation of the tables API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.api_types import Branch, FolderRid, SourceRid, TableRid


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
        table: str,
        database: str,
        skip_validation: bool = False,
    ) -> dict:
        """Creates a Virtual Table backed by the Glue Catalog.

        Args:
            name: The name of the table. This will be the name of the compass resource.
            parent_rid: The parent Rid of the compass resource.
            connection_rid: The rid of the compass Source.
            table: The name of the table in glue.
            database: The name of the database in glue.
            skip_validation: If true, skips the validation of the table. Default is False.

        """
        upstream_config = {
            "type": "glue",
            "glue": {"connectionRid": connection_rid, "relation": {"table": table, "database": database}},
        }
        return self.api_create_table(
            name=name, parent_rid=parent_rid, upstream_config=upstream_config, skip_validation=skip_validation
        ).json()

    def create_snowflake_table(
        self,
        name: str,
        parent_rid: FolderRid,
        connection_rid: SourceRid,
        database: str,
        schema: str,
        table: str,
        skip_validation: bool = False,
    ) -> dict:
        """Creates a Virtual Table on a Snowflake Source.

        Args:
            name: The name of the table. This will be the name of the compass resource.
            parent_rid: The parent Rid of the compass resource.
            connection_rid: The rid of the compass Source.
            table: The name of the table in snowflake.
            database: The name of the database Snowflake.
            schema: The name of the schema in Snowflake.
            skip_validation: If true, skips the validation of the table. Default is False.

        """
        upstream_config = {
            "type": "snowflake",
            "snowflake": {
                "connectionRid": connection_rid,
                "relation": {"table": table, "database": database, "schema": schema},
            },
        }
        return self.api_create_table(
            name=name, parent_rid=parent_rid, upstream_config=upstream_config, skip_validation=skip_validation
        ).json()

    def api_list_tables(
        self, connection_rid: SourceRid, limit: int | None = 1000, page_token: str | None = None, **kwargs
    ) -> requests.Response:
        """List Virtual Tables for a source."""
        return self.api_request(
            "GET",
            api_path="tables",
            params={
                "connectionRid": connection_rid,
                **({"limit": limit} if limit else {}),
                **({"pageToken": page_token} if page_token else {}),
            },
            **kwargs,
        )

    def list_tables(self, connection_rid: SourceRid) -> list[dict]:
        """Returns all tables for a connection/source by handling pagination.

        Args:
            connection_rid: The rid of the compass Source/Connection.

        Returns:
            a list of dicts with the following format:
                {
                    'rid': 'ri.tables.main.table.<...>',
                    'upstream': {
                        'type': '<type>',
                        '<type>': {
                                'connectionRid': 'ri.magritte..source.<...>',
                                'relation': {'database': 'test', 'schema': 'test', 'table': 'test2'}
                            }
                    }
                }
        """
        tables_response = self.api_list_tables(connection_rid=connection_rid, limit=1000)
        response_json = tables_response.json()
        tables = response_json["tables"]
        while next_page_token := response_json.get("nextPageToken"):
            response_json = self.api_list_tables(connection_rid=connection_rid, page_token=next_page_token).json()
            tables.extend(response_json["tables"])
        return tables

    def api_get_table_schema(self, table_rid: TableRid, branches: list[Branch], **kwargs) -> requests.Response:
        """Retrieves schema for a Foundry table object per given list of branches.

        Args:
            table_rid: The RID of the table to get schema for.
            branches: List of branch names to get schema for.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response: The API response containing the table schema.

        Example::
            >>> response = tables_client.api_get_table_schema("ri.foundry.main.dataset.abc123", ["master"])
            >>> schema = response.json()
            >>> schema
            {
                "fieldSchemaList": [
                    {
                        "type": "DECIMAL",
                        "name": "column_A",
                        "nullable": false,
                        "precision": 38,
                        "scale": 0
                    },
                    {
                        "type": "STRING",
                        "name": "column_B",
                        "nullable": false
                    }
                ],
                "primaryKey": null,
                "customMetadata": {"format": "snowflake"}
            }
        """
        return self.api_request(
            "POST",
            api_path=f"tables/{table_rid}/schema",
            json={"branches": branches},
            **kwargs,
        )
