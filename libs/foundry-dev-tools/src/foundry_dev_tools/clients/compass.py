"""Implementation of the compass API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import requests

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.compass import FolderNotFoundError, ResourceNotFoundError
from foundry_dev_tools.errors.handling import ErrorHandlingConfig, raise_foundry_api_error
from foundry_dev_tools.utils import api_types

if TYPE_CHECKING:
    from collections.abc import Iterator

GET_PATHS_BATCH_SIZE = 100


@overload
def get_decoration(
    decoration: api_types.ResourceDecorationSetAll | None,
    conv: Literal[False],
) -> api_types.ResourceDecorationSet | None: ...


@overload
def get_decoration(
    decoration: api_types.ResourceDecorationSetAll | None,
    conv: Literal[True],
) -> list[api_types.ResourceDecoration] | None: ...


@overload
def get_decoration(
    decoration: api_types.ResourceDecorationSetAll | None,
    conv: Literal[True] = ...,
) -> list[api_types.ResourceDecoration] | None: ...


def get_decoration(
    decoration: api_types.ResourceDecorationSetAll | None,
    conv: bool = True,
) -> list[api_types.ResourceDecoration] | api_types.ResourceDecorationSet | None:
    """Parses the decoration argument used by the compass client methods."""
    if decoration == "all":
        return list(api_types.ALL_RESOURCE_DECORATIONS) if conv else api_types.ALL_RESOURCE_DECORATIONS
    return None if decoration is None else (list(decoration) if conv else decoration)


class CompassClient(APIClient):
    """CompassClient class that implements methods from the 'compass' API."""

    api_name = "compass"

    def api_get_resource(
        self,
        rid: api_types.Rid,
        decoration: api_types.ResourceDecorationSetAll | None = None,
        permissive_folders: bool | None = None,
        additional_operations: set[str] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Gets the resource of the resource identifier.

        Args:
            rid: the identifier of the resource
            decoration: extra decoration entries in the response
            permissive_folders: if true read permissions are not needed if not in hidden folder
            additional_operations: include extra operations in result if user has the operation
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        params = {"decoration": get_decoration(decoration)}
        if permissive_folders is not None:
            params["permissiveFolders"] = permissive_folders  # type: ignore[assignment]
        if additional_operations is not None:
            params["additionalOperations"] = additional_operations  # type: ignore[assignment]
        return self.api_request(
            "GET",
            f"resources/{rid}",
            params=params,
            error_handling=ErrorHandlingConfig(api_error_mapping={204: ResourceNotFoundError}),
            **kwargs,
        )

    def api_get_resource_by_path(
        self,
        path: api_types.FoundryPath,
        decoration: api_types.ResourceDecorationSetAll | None = None,
        permissive_folders: bool | None = None,
        additional_operations: set[str] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Gets the resource of the path.

        Args:
            path: the path of the resource
            decoration: extra decoration entries in the response
            permissive_folders: if true read permissions are not needed if not in hidden folder
            additional_operations: include extra operations in result if user has the operation
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        params = {"path": path, "decoration": get_decoration(decoration)}
        if permissive_folders is not None:
            params["permissiveFolders"] = permissive_folders  # type: ignore[assignment]
        if additional_operations is not None:
            params["additionalOperations"] = additional_operations  # type: ignore[assignment]
        return self.api_request(
            "GET",
            "resources",
            params=params,
            error_handling=ErrorHandlingConfig(api_error_mapping={204: ResourceNotFoundError}, path=path),
            **kwargs,
        )

    def api_add_to_trash(
        self,
        rids: set[api_types.Rid],
        user_bearer_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Add resource to trash.

        Args:
            rids: set of resource identifiers
            user_bearer_token: bearer token, needed when dealing with service project resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        response = self.api_request(
            "POST",
            "batch/trash/add",
            headers={"User-Bearer-Token": f"Bearer {user_bearer_token}"} if user_bearer_token else None,
            json=list(rids),
            **kwargs,
        )
        if response.status_code != requests.codes.no_content:
            raise_foundry_api_error(
                response,
                ErrorHandlingConfig(info="Issue while moving resource(s) to trash.", rids=rids),
            )
        return response

    def api_restore(
        self,
        rids: set[api_types.Rid],
        user_bearer_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Restore resource from trash.

        Args:
            rids: set of resource identifiers
            user_bearer_token: bearer token, needed when dealing with service project resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        response = self.api_request(
            "POST",
            "batch/trash/restore",
            headers={"User-Bearer-Token": f"Bearer {user_bearer_token}"} if user_bearer_token else None,
            json=list(rids),
            **kwargs,
        )
        if response.status_code != requests.codes.no_content:
            raise_foundry_api_error(response, ErrorHandlingConfig(info="Issue while restoring resource(s) from trash."))
        return response

    def api_delete_permanently(
        self,
        rids: set[api_types.Rid],
        delete_options: set[Literal["DO_NOT_REQUIRE_TRASHED", "DO_NOT_TRACK_PERMANENTLY_DELETED_RIDS"]] | None = None,
        user_bearer_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Permanently deletes a Resource.

        Args:
            rids: set of resource identifiers
            delete_options: delete options, self explanatory
            user_bearer_token: bearer token, needed when dealing with service project resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "POST",
            "trash/delete",
            headers={"User-Bearer-Token": f"Bearer {user_bearer_token}"} if user_bearer_token else None,
            params={"deleteOptions": list(delete_options) if delete_options else None},
            json=list(rids),
            **kwargs,
        )

    def api_create_folder(
        self,
        name: str,
        parent_id: api_types.Rid,
        marking_ids: set[str] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Creates an empty folder in compass.

        Args:
            name: name of the new folder
            parent_id: rid of the parent folder,
                e.g. ri.compass.main.folder.aca0cce9-2419-4978-bb18-d4bc6e50bd7e
            marking_ids: marking IDs
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                which contains a json:
                    with keys rid and name and other properties.

        """
        json: dict[str, str | set] = {"name": name, "parentId": parent_id}
        if marking_ids:
            json["markingIds"] = marking_ids
        return self.api_request(
            "POST",
            "folders",
            json=json,
            **kwargs,
        )

    def api_get_path(self, rid: api_types.Rid, **kwargs) -> requests.Response:
        """Returns the compass path for the rid."""
        kwargs.setdefault("error_handling", ErrorHandlingConfig(rid=rid))
        return self.api_request(
            "GET",
            f"resources/{rid}/path-json",
            **kwargs,
        )

    def api_get_paths(self, resource_ids: list[api_types.Rid], **kwargs) -> requests.Response:
        """Get paths for RIDs.

        Args:
            resource_ids: The identifiers of resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "POST",
            "batch/paths",
            json=resource_ids,
            **kwargs,
        )

    def get_paths(
        self,
        resource_ids: list[api_types.Rid],
    ) -> dict[api_types.Rid, api_types.FoundryPath]:
        """Returns a dict which maps RIDs to Paths.

        Args:
            resource_ids: The identifiers of resources

        Returns:
            dict:
                mapping between rid and path
        """
        list_len = len(resource_ids)
        if list_len < GET_PATHS_BATCH_SIZE:
            batches = [resource_ids]
        else:
            batches = [resource_ids[i : i + GET_PATHS_BATCH_SIZE] for i in range(0, list_len, GET_PATHS_BATCH_SIZE)]
        result: dict[api_types.Rid, api_types.FoundryPath] = {}
        for batch in batches:
            result = {**result, **self.api_get_paths(batch).json()}
        return result

    def api_get_children(
        self,
        rid: api_types.Rid,
        filter: set[str] | None = None,  # noqa: A002
        decoration: api_types.ResourceDecorationSetAll | None = None,
        limit: int | None = None,
        sort: str | None = None,
        page_token: str | None = None,
        permissive_folders: bool | None = None,
        include_operations: bool = False,
        additional_operations: str | None = None,
    ) -> requests.Response:
        """Returns children in a given compass folder.

        Args:
            rid: resource identifier
            filter: filter out resources, syntax "service.instance.type"
            decoration: extra information for the decorated resource
            limit: maximum items in a page
            sort: A space-delimited specifier of the form "[a][ b]"
                 [a] can be "name", "lastModified", or "rid"
                 [b] is optional and can be "asc" or "desc"
                 (e.g. "rid asc", "name", "lastModified desc")
            page_token: start position for request
            permissive_folders: Allows folder access if any sub-resource is accessible, ignoring folder permissions
                Default is false, non-hidden folders only
            include_operations: Controls inclusion of Compass GK operations
                Is set to false in Foundry DevTools to improve performance. Set to `None` for Foundry instance default
            additional_operations: Adds specific user-permitted operations to response. Requires include_perations=True

        """
        return self.api_request(
            "GET",
            f"folders/{rid}/children",
            params={
                "filter": filter,
                "decoration": get_decoration(decoration),
                "limit": limit,
                "sort": sort,
                "pageToken": page_token,
                "permissiveFolders": permissive_folders,
                "includeOperations": include_operations,
                "additionalOperations": additional_operations,
            },
            error_handling=ErrorHandlingConfig({"Compass:NotFound": FolderNotFoundError}),
        )

    def get_child_objects_of_folder(
        self,
        rid: api_types.Rid,
        filter: set[str] | None = None,  # noqa: A002
        decoration: api_types.ResourceDecorationSetAll | None = None,
        limit: int | None = None,
        sort: str | None = None,
        permissive_folders: bool | None = None,
        include_operations: bool = False,
        additional_operations: str | None = None,
    ) -> Iterator[dict]:
        """Returns children in a given compass folder (automatic pagination).

        Args:
            rid: resource identifier
            filter: filter out resources, syntax "service.instance.type"
            decoration: extra information for the decorated resource
            limit: maximum items in a page
            sort: A space-delimited specifier of the form "[a][ b]"
                 [a] can be "name", "lastModified", or "rid"
                 [b] is optional and can be "asc" or "desc"
                 (e.g. "rid asc", "name", "lastModified desc")
            permissive_folders: Allows folder access if any sub-resource is accessible, ignoring folder permissions
                Default is false, non-hidden folders only
            include_operations: Controls inclusion of Compass GK operations
                Is set to false in Foundry DevTools to improve performance. Set to `None` for Foundry instance default
            additional_operations: Adds specific user-permitted operations to response. Requires include_perations=True

        """
        page_token = None
        while True:
            response_as_json = self.api_get_children(
                rid=rid,
                filter=filter,
                decoration=decoration,
                limit=limit,
                sort=sort,
                page_token=page_token,
                permissive_folders=permissive_folders,
                include_operations=include_operations,
                additional_operations=additional_operations,
            ).json()
            yield from response_as_json["values"]
            if (page_token := response_as_json["nextPageToken"]) is None:
                break

    def api_get_resources(
        self,
        rids: set[api_types.Rid],
        decoration: api_types.ResourceDecorationSetAll | None = None,
        include_operations: bool = False,
        additional_operations: set[str] | None = None,
    ) -> requests.Response:
        """Returns the resources for the RIDs.

        Args:
            rids: the resource identifiers
            decoration: extra information to add to the result
            include_operations: Controls inclusion of Compass GK operations.
                Is set to false in Foundry DevTools to improve performance. Set to `None` for Foundry instance default.
            additional_operations: Adds specific user-permitted operations to response. Requires include_perations=True.
        """
        return self.api_request(
            "POST",
            "batch/resources",
            params={
                "decoration": get_decoration(decoration),
                "includeOperations": include_operations,
                "additionalOperations": list(additional_operations) if additional_operations is not None else None,
            },
            json=list(rids),
        )
