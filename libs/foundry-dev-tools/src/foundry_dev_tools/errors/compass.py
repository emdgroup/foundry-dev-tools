"""Compass specific errors."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.errors.meta import FoundryAPIError, FoundryDevToolsError

if TYPE_CHECKING:
    from foundry_dev_tools.resources.resource import Resource
    from foundry_dev_tools.utils import api_types


class ResourceNotFoundError(FoundryAPIError):
    """Returned when a Resource was not found."""

    message = "Resource not found."


class FolderNotFoundError(FoundryAPIError):
    """Exception is thrown when compass folder does not exist."""

    message = "Compass folder not found."


class MarkingNotFoundError(FoundryAPIError):
    """Returned when a Resource was not found."""

    message = "Marking not found."


class WrongResourceTypeError(FoundryDevToolsError):
    """Thrown when wrong resource type is requested while creating a :py:class:`Resource`."""

    def __init__(self, rid: api_types.Rid, path: api_types.FoundryPath, resource_type: type[Resource]):
        self.rid = rid
        self.path = path
        super().__init__(
            f"You wanted to create a Resource ({resource_type=}) "
            f"which needs to have a rid that starts with {resource_type.rid_start}, but you've supplied {path=} {rid=}",
        )


class NotProjectError(FoundryAPIError):
    """Exception is thrown when project rid is not a project."""

    message = "The project rid is not a project."


class ForbiddenOperationOnServiceProjectResourceError(FoundryAPIError):
    """Thrown when project is a service project resource and the token has insufficient rights."""

    message = (
        "The given project is a service project resource "
        "and the provided bearer token does not have compass:write-service-project operation."
    )


class InsufficientPermissionsError(FoundryAPIError):
    """Exception is thrown when permissions are insufficient."""

    message = "The user has insufficient permissions to and requires operations."


class AutosaveResourceOperationForbiddenError(FoundryAPIError):
    """Exception is thrown when resource is an autosave resource."""

    message = "The resource is an autosave resource."


class ForbiddenOperationOnHiddenResourceError(FoundryAPIError):
    """Exception is thrown when resource is hidden."""

    message = "The resource is a hidden resource."


class InvalidMarkingError(FoundryAPIError):
    """Exception is thrown when the requested marking is a Mutipass organizazion marking."""

    message = (
        "Marking is a multipass organization marking "
        "which can only be applied to projects, tag categories and collections."
    )
