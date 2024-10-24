"""Implementation of the compass API."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, overload

import requests

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.compass import (
    FolderNotFoundError,
    ResourceNotFoundError,
)
from foundry_dev_tools.errors.handling import ErrorHandlingConfig, raise_foundry_api_error
from foundry_dev_tools.utils import api_types
from foundry_dev_tools.utils.api_types import assert_in_literal

if TYPE_CHECKING:
    from collections.abc import Iterator

GET_PATHS_BATCH_SIZE = 100
GET_PROJECTS_BATCH_SIZE = 1

DEFAULT_PROJECTS_PAGE_SIZE = 100
MINIMUM_PROJECTS_PAGE_SIZE = 1
MAXIMUM_PROJECTS_PAGE_SIZE = 500
MINIMUM_PROJECTS_SEARCH_OFFSET = 0
MAXIMUM_PROJECTS_SEARCH_OFFSET = 500

DEFAULT_IMPORTS_PAGE_SIZE = 100
MINIMUM_IMPORTS_PAGE_SIZE = 0
MAXIMUM_IMPORTS_PAGE_SIZE = 100


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
            params["additionalOperations"] = list(additional_operations)  # type: ignore[assignment]
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
            params["additionalOperations"] = list(additional_operations)  # type: ignore[assignment]
        return self.api_request(
            "GET",
            "resources",
            params=params,
            error_handling=ErrorHandlingConfig(api_error_mapping={204: ResourceNotFoundError}, path=path),
            **kwargs,
        )

    def api_check_name(
        self,
        parent_folder_rid: api_types.Rid,
        name: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Check if parent folder contains a child resource with the given name.

        Args:
            parent_folder_rid: the resource identifier of the parent folder that is checked
            name: the name to be checked
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                the response contains a json which is a bool,
                indicating whether the resource name already exists or not
        """
        body = {}

        if name is not None:
            body["name"] = name

        return self.api_request(
            "POST",
            f"resources/{parent_folder_rid}/checkName",
            json=body,
            error_handling=ErrorHandlingConfig({"Compass:NotFound": FolderNotFoundError}),
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
        json: dict[str, str | list] = {"name": name, "parentId": parent_id}
        if marking_ids:
            json["markingIds"] = list(marking_ids)
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

    def api_get_paths(self, rids: list[api_types.Rid], **kwargs) -> requests.Response:
        """Get paths for RIDs.

        Args:
            rids: The identifiers of resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "POST",
            "batch/paths",
            json=rids,
            **kwargs,
        )

    def get_paths(
        self,
        rids: list[api_types.Rid],
    ) -> dict[api_types.Rid, api_types.FoundryPath]:
        """Returns a dict which maps RIDs to Paths.

        Args:
            rids: The identifiers of resources

        Returns:
            dict:
                mapping between rid and path
        """
        list_len = len(rids)
        if list_len < GET_PATHS_BATCH_SIZE:
            batches = [rids]
        else:
            batches = [rids[i : i + GET_PATHS_BATCH_SIZE] for i in range(0, list_len, GET_PATHS_BATCH_SIZE)]
        result: dict[api_types.Rid, api_types.FoundryPath] = {}
        for batch in batches:
            result = {**result, **self.api_get_paths(batch).json()}
        return result

    def get_path(
        self,
        rid: api_types.Rid,
    ) -> api_types.FoundryPath | None:
        """Returns the Path of the resource or `None` if the resource does not exist.

        Args:
            rid: The identifier of the resource for which to retrieve the path

        Returns:
            path of the resource
        """
        response = self.api_get_path(rid)

        if response.status_code == 200:
            return response.json()

        return None

    def api_get_parent(self, rid: api_types.Rid, **kwargs) -> requests.Response:
        """Returns the parent of a given resource.

        Args:
            rid: The rid of the resource for which to obtain the parent
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                which contains a json that either holds the parent resource or None if no parent exists
        """
        return self.api_request(
            "GET",
            f"resources/{rid}/parent",
            **kwargs,
        )

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
                "filter": list(filter) if filter else None,
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

    def api_move_children(
        self,
        folder_rid: api_types.FolderRid,
        children: set[api_types.Rid],
        roles_map: dict[api_types.RoleId, list[api_types.RoleId]] | None = None,
        options: set[api_types.MoveResourcesOption] | None = None,
        expected_parents: dict[api_types.Rid, api_types.Rid] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Moves a list of resources to the given folder.

        Args:
            folder_rid: The resource identifier of the destination compass folder
            children: A set of resource identifiers for which the resources should be moved to the destination folder
            roles_map: Mapping that allows updating role grants on move,
                e.g. when moving resources between projects with different role sets.
                The mapping must contain an entry for any non-deleted roles in any role set
                that exists on the source but not on the destination, unless specifying `REMOVE_ROLE_GRANTS` in options.
                This cannot be used to map from role sets that are present at both the source and the destination
            options: Set of options to give control about the way resources should be moved
            expected_parents: A map from moved resource to expected current parent.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Raises:
            ResourcesNotFoundError: if destination folder does not exist
            ForbiddenOperationOnHiddenResource: if destination or any resources to move are hidden resources
            ForbiddenOperationOnServiceProjectResources: if destination or any of the resources to move
                are service projects or service project resources
            DuplicateNameError: if a resource with the given name already exists
            CircularDependencyError: if  the destination is the resource itself or one of its children
            UsersNamespaceOperationForbiddenError: if trying to move projects in or out of the Users namespace
            CannotMoveResourcesUnderHiddenResourceError: if the destination is a hidden resource
            UnexpectedParentError: if a parent is specified that does not match the resource's current parent

        """
        body = {"children": list(children)}

        if options:
            for option in options:
                assert_in_literal(option, api_types.MoveResourcesOption, "options")

            body["options"] = list(options)

        if roles_map:
            body["rolesMap"] = roles_map
        if expected_parents:
            body["expectedParents"] = expected_parents

        return self.api_request("POST", f"folders/{folder_rid}/children", json=body, **kwargs)

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
                "additionalOperations": list(additional_operations) if additional_operations else None,
            },
            json=list(rids),
        )

    def api_process_marking(
        self,
        rid: api_types.Rid,
        marking_id: api_types.MarkingId,
        path_operation_type: api_types.PatchOperation,
        user_bearer_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Process marking to add or remove from resource.

        Args:
            rid: resource identifier of the resource for which a marking is adjusted
            marking_id: The id of the marking to be used
            path_operation_type: path operation type, see :py:class:`api_types.PatchOperation`
            user_bearer_token: bearer token, needed when dealing with service project resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        assert_in_literal(path_operation_type, api_types.PatchOperation, "path_operation_type")

        body = {"markingPatches": [{"markingId": marking_id, "patchOperation": path_operation_type}]}

        return self.api_request(
            "POST",
            f"markings/{rid}",
            headers={"User-Bearer-Token": f"Bearer {user_bearer_token}"} if user_bearer_token else None,
            json=body,
            **kwargs,
        )

    def add_marking(
        self,
        rid: api_types.Rid,
        marking_id: api_types.MarkingId,
        user_bearer_token: str | None = None,
    ) -> requests.Response:
        """Add marking to resource.

        Args:
            rid: resource identifier of the resource for which a marking will be added
            marking_id: The id of the marking to be added
            user_bearer_token: bearer token, needed when dealing with service project resources
        """
        return self.api_process_marking(rid, marking_id, "ADD", user_bearer_token)

    def remove_marking(
        self,
        rid: api_types.Rid,
        marking_id: api_types.MarkingId,
        user_bearer_token: str | None = None,
    ) -> requests.Response:
        """Remove marking from resource.

        Args:
            rid: resource identifier of the resource for which a marking will be removed
            marking_id: The id of the marking to be removed
            user_bearer_token: bearer token, needed when dealing with service project resources
        """
        return self.api_process_marking(rid, marking_id, "REMOVE", user_bearer_token)

    def api_add_imports(
        self,
        project_rid: api_types.ProjectRid,
        rids: set[api_types.Rid],
        user_bearer_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Add reference to a project via import.

        Args:
            project_rid: resource identifier of the project
            rids: set of resource identifiers of the resources being imported
            user_bearer_token: bearer token, needed when dealing with service project resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        body = {"requests": [{"resourceRid": rid} for rid in rids]}

        return self.api_request(
            "POST",
            f"projects/imports/{project_rid}/import",
            headers={"User-Bearer-Token": f"Bearer {user_bearer_token}"} if user_bearer_token else None,
            json=body,
            **kwargs,
        )

    def api_remove_imports(
        self,
        project_rid: api_types.ProjectRid,
        rids: set[api_types.Rid],
        user_bearer_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Remove imported reference from a project.

        Args:
            project_rid: resource identifier of the project
            rids: set of resource identifiers of the resources that will be removed from import
            user_bearer_token: bearer token, needed when dealing with service project resources
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        body = {"resourceRids": list(rids)}

        return self.api_request(
            "DELETE",
            f"projects/imports/{project_rid}/import",
            headers={"User-Bearer-Token": f"Bearer {user_bearer_token}"} if user_bearer_token else None,
            json=body,
            **kwargs,
        )

    def api_get_imports(
        self,
        project_rid: api_types.ProjectRid,
        import_filter: api_types.ImportType | None = None,
        page_size: int = DEFAULT_IMPORTS_PAGE_SIZE,
        page_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns a list of imported resources along some dangling imports for the specified project.

        Args:
            project_rid: The resource identifier of the project for which to retrieve the imports
            import_filter: Filter for specific imports only
            page_size: The number of elements to fetch in one request. It is restricted to 100 at most
                and defaults to this value in case it should exceed
            page_token: start position for request
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                Besides a `nextPageToken` which marks the start position for the next request but may be None
                the response also contains a list of imports and dangling imports
        """
        if page_size < MINIMUM_IMPORTS_PAGE_SIZE:
            warnings.warn(
                f"Parameter `page_size` ({page_size}) is less than "
                f"the minimum page size ({MINIMUM_IMPORTS_PAGE_SIZE}). "
                f"Defaulting to {MINIMUM_IMPORTS_PAGE_SIZE}."
            )
            page_size = MINIMUM_IMPORTS_PAGE_SIZE
        elif page_size > MAXIMUM_IMPORTS_PAGE_SIZE:
            warnings.warn(
                f"Parameter `page_size` ({page_size}) is greater than "
                f"the maximum page size ({MAXIMUM_IMPORTS_PAGE_SIZE}). "
                f"Defaulting to {MAXIMUM_IMPORTS_PAGE_SIZE}."
            )
            page_size = MAXIMUM_IMPORTS_PAGE_SIZE

        body = {"pageSize": page_size}
        if import_filter:
            assert_in_literal(import_filter, api_types.ImportType, "import_filter")
            body["importFilter"] = import_filter
        if page_token:
            body["pageToken"] = page_token

        return self.api_request(
            "POST",
            f"projects/imports/{project_rid}",
            json=body,
            **kwargs,
        )

    def get_imports(
        self,
        project_rid: api_types.ProjectRid,
        import_filter: api_types.ImportType | None = None,
        page_size: int = DEFAULT_IMPORTS_PAGE_SIZE,
    ) -> Iterator[dict]:
        """Returns a list of imported resources for the specified project (automatic pagination).

        Args:
            project_rid: The resource identifier of the project for which to retrieve the imports
            import_filter: Filter for specific imports only
            page_size: The number of elements to fetch in one request. It is restricted to 100 at most
                and defaults to this value in case it should exceed

        Returns:
            Iterator[dict]:
                Returns an interator holding the imported resources.
        """
        page_token = None

        while True:
            response_as_json = self.api_get_imports(
                project_rid=project_rid,
                import_filter=import_filter,
                page_size=page_size,
                page_token=page_token,
            ).json()
            yield from response_as_json["values"]

            page_token = response_as_json["nextPageToken"]
            if page_token is None:
                break

    def get_dangling_imports(
        self,
        project_rid: api_types.ProjectRid,
        import_filter: api_types.ImportType | None = None,
        page_size: int = DEFAULT_IMPORTS_PAGE_SIZE,
    ) -> Iterator[dict]:
        """Returns a list of dangling imports for the specified project (automatic pagination).

        Args:
            project_rid: The resource identifier of the project for which to retrieve the imports
            import_filter: Filter for specific imports only
            page_size: The number of elements to fetch in one request. It is restricted to 100 at most
                and defaults to this value in case it should exceed

        Returns:
            Iterator[dict]:
                Returns an interator holding the imported resources.
        """
        page_token = None

        while True:
            response_as_json = self.api_get_imports(
                project_rid=project_rid,
                import_filter=import_filter,
                page_size=page_size,
                page_token=page_token,
            ).json()
            yield from response_as_json["danglingImports"]

            page_token = response_as_json["nextPageToken"]
            if page_token is None:
                break

    def api_set_name(
        self,
        rid: api_types.Rid,
        name: str,
        **kwargs,
    ) -> requests.Response:
        """Remove imported reference from a project.

        Args:
            rid: resource identifier of the resource whose name will be changed
            name: The resource name that should be set
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        body = {"name": name}

        return self.api_request(
            "POST",
            f"resources/{rid}/name",
            json=body,
            **kwargs,
        )

    def api_resources_exist(
        self,
        rids: set[api_types.Rid],
        **kwargs,
    ) -> requests.Response:
        """Check if resources exist.

        Args:
            rids: set of resource identifiers to check whether they exist
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            dict:
                with key-value pairs, where the key is the rid of the checked resource
                and the value indicates whether the resource exists or not.
        """
        return self.api_request(
            "POST",
            "batch/resources/exist",
            json=list(rids),
            **kwargs,
        )

    def resources_exist(self, rids: set[api_types.Rid]) -> dict[api_types.Rid, bool]:
        """Check if resources exist.

        Args:
            rids: set of resource identifiers to check whether they exist

        Returns:
            dict:
                mapping between rid and bool as indicator for resource existence
        """
        return self.api_resources_exist(rids).json()

    def resource_exists(
        self,
        rid: api_types.Rid,
    ) -> bool:
        """Check if resource exists.

        Args:
            rid: resource identifier of resource to check whether it exists

        Returns:
            bool:
                true if resource exists, false otherwise
        """
        result = self.resources_exist({rid})

        return result.get(rid, False)

    def api_get_projects_by_rids(self, rids: list[api_types.ProjectRid], **kwargs) -> requests.Response:
        """Fetch projects by their resource identifiers.

        Args:
            rids: list of project resource identifiers that shall be fetched
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                which contains a json dict:
                    with project information about every project
        """
        return self.api_request(
            "PUT",
            "hierarchy/v2/batch/projects",
            json=rids,
            **kwargs,
        )

    def get_projects_by_rids(
        self,
        rids: list[api_types.ProjectRid],
    ) -> dict[api_types.ProjectRid, dict[str, Any]]:
        """Returns a dict which maps rids to projects.

        Args:
            rids: list of project resource identifiers that shall be fetched

        Returns:
            dict:
                mapping between rid and project
        """
        list_len = len(rids)
        if list_len < GET_PATHS_BATCH_SIZE:
            batches = [rids]
        else:
            batches = [rids[i : i + GET_PATHS_BATCH_SIZE] for i in range(0, list_len, GET_PROJECTS_BATCH_SIZE)]

        result: dict[api_types.FolderRid, dict[str, Any]] = {}
        for batch in batches:
            result.update(self.api_get_projects_by_rids(batch).json())

        return result

    def get_project_by_rid(
        self,
        rid: api_types.ProjectRid,
    ) -> dict[str, Any] | None:
        """Returns a single project.

        Args:
            rid: resource identifier of a project to be fetched

        Returns:
            dict:
                mapping of project attribute keys and their respective values
        """
        result = self.api_get_projects_by_rids([rid]).json()

        return result.get(rid)

    def api_resolve_path(self, path: api_types.FoundryPath, **kwargs) -> requests.Response:
        """Fetch all resources that are part of the path string.

        Args:
            path: path: the path of the resource
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                the response contains a json which is a list of resources representing the path components
        """
        return self.api_request(
            "GET",
            "paths",
            params={"path": path},
            **kwargs,
        )

    def api_search_projects(
        self,
        query: str | None = None,
        decorations: api_types.ResourceDecorationSetAll | None = None,
        organizations: set[api_types.Rid] | None = None,
        tags: set[api_types.Rid] | None = None,
        roles: set[api_types.RoleId] | None = None,
        include_home_projects: bool | None = None,
        direct_role_grant_principal_ids: dict[str, list[api_types.RoleId]] | None = None,
        sort: api_types.SortSpec | None = None,
        page_size: int = DEFAULT_PROJECTS_PAGE_SIZE,
        page_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns a list of projects satisfying the search criteria.

        Args:
            query: search term for the project
            decorations: extra information for the decorated resource
            organizations: queries only for organizations with these marking identifiers
            tags: only includes projects with these tags
            roles: filters for projects where the user has one of the roles
            include_home_projects: whether to consider home projects of the user
            direct_role_grant_principal_ids:  only return projects for which the role identifiers
                for given principal identifiers have been granted
            sort: see :py:meth:`foundry_dev_tools.utils.api_types.Sort`
            page_size: the maximum number of projects to return. Must be in the range 0 < N <= 500
            page_token: start position for request. Must be in the range 0 <= N <= 500
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                the response contains a json which is a dict with a list of projects
                and an optional nextPageToken in case all the projects could not be fetched all at once

        Raises:
            ValueError: When `page_token` is in inappropriate format. Should be a number in the range 0 <= N <= 500.

        """
        if decorations is not None:
            decorations = get_decoration(decorations)

        if page_size < MINIMUM_PROJECTS_PAGE_SIZE:
            warnings.warn(
                f"Parameter `page_size` ({page_size}) is less than "
                f"the minimum page size ({MINIMUM_PROJECTS_PAGE_SIZE}). "
                f"Defaulting to {MINIMUM_PROJECTS_PAGE_SIZE}."
            )
            page_size = MINIMUM_PROJECTS_PAGE_SIZE
        elif page_size > MAXIMUM_PROJECTS_PAGE_SIZE:
            warnings.warn(
                f"Parameter `page_size` ({page_size}) is greater than "
                f"the maximum page size ({MAXIMUM_PROJECTS_PAGE_SIZE}). "
                f"Defaulting to {MAXIMUM_PROJECTS_SEARCH_OFFSET}."
            )
            page_size = MAXIMUM_PROJECTS_PAGE_SIZE

        if page_token is not None:
            if page_token.isdecimal() is False:
                msg = (
                    f"Parameter `page_token` ({page_token}) is expected to contain a number "
                    f"as the starting offset for the request. "
                    f"The search offset must be within the range from "
                    f"{MINIMUM_PROJECTS_SEARCH_OFFSET} to {MAXIMUM_PROJECTS_SEARCH_OFFSET}."
                )
                raise ValueError(msg)

            page_token_int = int(page_token)
            if page_token_int < MINIMUM_PROJECTS_SEARCH_OFFSET:
                msg = (
                    f"Parameter `page_token` ({page_token_int}) is less than "
                    f"the minimum search offset ({MINIMUM_PROJECTS_SEARCH_OFFSET})"
                )
                raise ValueError(msg)
            if page_token_int > MAXIMUM_PROJECTS_SEARCH_OFFSET:
                msg = (
                    f"Parameter `page_token` ({page_token_int}) is greater than "
                    f"the maximum search offset ({MAXIMUM_PROJECTS_SEARCH_OFFSET})"
                )
                raise ValueError(msg)

        body = {
            "query": query,
            "decorations": decorations,
            "organizations": list(organizations) if organizations else None,
            "tags": list(tags) if tags else None,
            "roles": list(roles) if roles else None,
            "includeHomeProjects": include_home_projects,
            "directRoleGrantPrincipalIds": direct_role_grant_principal_ids,
            "sort": sort,
            "pageSize": page_size,
            "pageToken": page_token,
        }

        return self.api_request(
            "POST",
            "search/projects",
            json=body,
            **kwargs,
        )

    def search_projects(
        self,
        query: str | None = None,
        decorations: api_types.ResourceDecorationSetAll | None = None,
        organizations: set[api_types.Rid] | None = None,
        tags: set[api_types.Rid] | None = None,
        roles: set[api_types.RoleId] | None = None,
        include_home_projects: bool | None = None,
        direct_role_grant_principal_ids: dict[str, list[api_types.RoleId]] | None = None,
        sort: api_types.SortSpec | None = None,
        page_size: int = DEFAULT_PROJECTS_PAGE_SIZE,
    ) -> Iterator[dict]:
        """Returns a list of projects satisfying the search criteria (automatic pagination).

        Args:
            query: search term for the project
            decorations: extra information for the decorated resource
            organizations: queries only for organizations with these marking identifiers
            tags: only includes projects with these tags
            roles: filters for projects where the user has one of the roles
            include_home_projects: whether to consider home projects of the user
            direct_role_grant_principal_ids:  only return projects for which the role identifiers
                for given principal identifiers have been granted
            sort: see :py:meth:`foundry_dev_tools.utils.api_types.Sort`
            page_size: the maximum number of projects to return. Must be in the range 0 < N <= 500

        Returns:
            Iterator[dict]:
                which contains the project data as a dict
        """
        page_token = None
        while True:
            response_as_json = self.api_search_projects(
                query=query,
                decorations=decorations,
                organizations=organizations,
                tags=tags,
                roles=roles,
                include_home_projects=include_home_projects,
                direct_role_grant_principal_ids=direct_role_grant_principal_ids,
                sort=sort,
                page_size=page_size,
                page_token=page_token,
            ).json()
            yield from response_as_json["values"]

            page_token = response_as_json["nextPageToken"]
            if page_token is None or (int(page_token) > MAXIMUM_PROJECTS_PAGE_SIZE):
                break

    def api_get_resource_roles(
        self,
        rids: set[api_types.Rid],
        **kwargs,
    ) -> requests.Response:
        """Retrieve the role grants for a set of resources.

        Args:
            rids: set of resource identifiers, for the resources for which the role grants should be returned
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                which consists of a json returning a mapping between resource identifier and the associated grants
        """
        body = {"rids": list(rids)}

        return self.api_request("POST", "roles", json=body, **kwargs)

    def get_resource_roles(
        self,
        rids: set[api_types.Rid],
    ) -> dict[api_types.Rid, api_types.ResourceGrantsResult]:
        """Returns a mapping between resource identifier and resource grants result.

        Args:
            rids: set of resource identifiers, for the resources for which the role grants should be returned
        """
        return self.api_get_resource_roles(rids).json()

    def api_update_project_roles(
        self,
        project_rid: api_types.Rid,
        role_grant_patches: set[api_types.RoleGrantPatch] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Updates the role grants for a project resource.

        Args:
            project_rid: resource identifier, for the resource for which roles will be updated
            role_grant_patches: list of role grants that should be patched.
                grant_patches have the following structure
                [{
                    "patchOperation": "ADD" | "REMOVE",
                    "roleGrant": {
                        "role": roleset_id,
                        "principal": {
                            "id": multipass_id,
                            "type": "USER" | "GROUP" | "EVERYONE"
                        }
                    }
                }]
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        body = {}

        if role_grant_patches is not None:
            body["roleGrantPatches"] = list(role_grant_patches)

        return self.api_request(
            "POST",
            f"hierarchy/projects/{project_rid}",
            json=body,
            **kwargs,
        )

    def api_update_resource_roles(
        self,
        rid: api_types.Rid,
        grant_patches: set[api_types.RoleGrantPatch] | None = None,
        disable_inherited_permissions_for_principals: set[api_types.UserGroupPrincipalPatch] | None = None,
        disable_inherited_permissions: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Updates the role grants for a resource (non-project).

        This endpoint cannot be used for projects or service project resources.
        To update roles on a project, use api_update_project_roles().

        Args:
            rid: resource identifier, for the resource for which roles will be updated
            grant_patches: list of role grants that should be patched.
                grant_patches have the following structure
                [{
                    "patchOperation": "ADD" | "REMOVE",
                    "roleGrant": {
                        "role": roleset_id,
                        "principal": {
                            "id": multipass_id,
                            "type": "USER" | "GROUP" | "EVERYONE"
                        }
                    }
                }]
            disable_inherited_permissions_for_principals: patch role grants for the provided inherited permissions
            disable_inherited_permissions: disable inherited permissions
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        body = {}

        if grant_patches is not None:
            body["grantPatches"] = list(grant_patches)
        if disable_inherited_permissions_for_principals is not None:
            body["disableInheritedPermissionsForPrincipals"] = list(disable_inherited_permissions_for_principals)
        if disable_inherited_permissions is not None:
            body["disableInheritedPermissions"] = disable_inherited_permissions

        return self.api_request(
            "POST",
            f"roles/v2/{rid}",
            json=body,
            **kwargs,
        )

    def api_get_home_folder(
        self,
        decoration: api_types.ResourceDecorationSetAll | None = None,
        additional_operations: set[str] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns the resource representing the user's home project or if it does not exist tries to create the folder.

        Args:
            decoration: extra decoration entries in the response
            additional_operations: include extra operations in result if user has the operation
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                the response contains a json which is a dict representing the decorated resource.
                It might be `None` if the user cannot access its home folder or if creation is not possible.

        """
        params = {"decoration": get_decoration(decoration)}
        if additional_operations is not None:
            params["additionalOperations"] = list(additional_operations)  # type: ignore[assignment]

        return self.api_request(
            "GET",
            "folders/home/v2",
            params=params,
            **kwargs,
        )

    def api_get_decorated_organization_and_project_information(
        self,
        rids: set[api_types.Rid],
        decoration: api_types.ResourceDecorationSetAll | None = None,
        additional_operations: set[str] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns the associated namespace and project information for a list of rids.

        At most 500 resources should be requested at once.

        Args:
            rids: The rids for which to fetch namespace and project information
            decoration: extra decoration entries in the response
            additional_operations: include extra operations in result if user has the operation
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                the response contains a json which maps a resource identifier to namespace and project information
        """
        params = {"decoration": get_decoration(decoration)}
        if additional_operations:
            params["additionalOperations"] = list(additional_operations)  # type: ignore[assignment]

        return self.api_request(
            "POST",
            "batch/hierarchy/projects/decorated",
            json=list(rids),
            params=params,
            **kwargs,
        )

    def api_get_all_namespace_rids(
        self,
        include_internal_namespaces: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """For a list of rids return the associated namespace and project information.

        Args:
            include_internal_namespaces: When set to true, namespaces created by the Compass system
                will be included in the response. Default is `False`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                the response contains a list of all namespace rids
        """
        params = {}
        if include_internal_namespaces is not None:
            params["includeInternalNamespaces"] = include_internal_namespaces

        return self.api_request(
            "GET",
            "hierarchy/v2/all-namespace-rids",
            params=params,
            **kwargs,
        )
