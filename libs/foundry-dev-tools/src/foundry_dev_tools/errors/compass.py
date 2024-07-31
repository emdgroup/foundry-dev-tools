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


class TooManyResourcesRequestedError(FoundryAPIError):
    """Exception is thrown when too many resource rids have been requested in one batch."""

    message = "Too many resource rids have been requested in one batch."


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


class PathNotFoundError(FoundryAPIError):
    """Exception is thrown when a component of the path does not exist."""

    message = "Some component of the path does not exist and hence could not be found."


class UsersNamespaceOperationForbiddenError(FoundryAPIError):
    """Exception is thrown when trying to update a resource within the Users namespace."""

    message = "It is not possible to update resources within the own Users namespace."


class GatekeeperInsufficientPermissionsError(FoundryAPIError):
    """Exception is thrown when the user does not have permissions to create the project."""

    message = "The user does not have the permissions to create the project."


class DuplicateNameError(FoundryAPIError):
    """Exception is thrown when there exists already a project with the same name."""

    message = "A project already exists holding the same name."


class IllegalNameError(FoundryAPIError):
    """Exception is thrown when the project name does not conform to the naming conventions."""

    message = "The project name is illegal. Ensure that it does not contain any illegal characters."


class UnrecognizedAccessLevelError(FoundryAPIError):
    """Exception is thrown when the access level is unrecognized."""

    message = "Permissions on project are invalid regarding the access level."


class UnrecognizedPatchOperationError(FoundryAPIError):
    """Exception is thrown when the patch operation is unrecognized."""

    message = "Permissions on project are invalid regarding the patch operation."


class UnrecognizedPrincipalError(FoundryAPIError):
    """Exception is thrown when the principal is unrecognized."""

    message = "Permissions on project are invalid regarding the principal."


class OrganizationNotFoundError(FoundryAPIError):
    """Exception is thrown when the requested organization marking does not exist."""

    message = "The requested organization marking does not exist."


class InvalidOrganizationMarkingHierarchyError(FoundryAPIError):
    """Exception is thrown when the requested marking would cause an invalid hierarchy state.

    Potential reasons can be that the requested organization marking(s) does not exist on the parent
    or would result in an unmarked project under a marked Compass namespace.
    """

    message = "The requested marking would cause an invalid hierarchy state."


class MissingOrganizationMarkingError(FoundryAPIError):
    """Exception is thrown when the requested organization marking(s) is missing on the parent."""

    message = "Exception is thrown when the requested organization marking(s) is missing on the parent."


class InvalidMavenProductIdError(FoundryAPIError):
    """Exception is thrown when the provided maven product id is invalid."""

    message = "The provided maven product id is invalid."


class MavenProductIdAlreadySetError(FoundryAPIError):
    """Exception is thrown when the maven product id is either empty or already set."""

    message = (
        "The provided maven product id is either empty or "
        "already set and does not correspond with the provided value."
    )


class MavenProductIdConflictError(FoundryAPIError):
    """Exception is thrown when there is already another project associated with this maven group."""

    message = "Another project is already associated with this maven group."


class InvalidMavenGroupPrefixError(FoundryAPIError):
    """Exception is thrown when the portfolio's maven group is not the maven group of the product id."""

    message = "Portfolio's maven group is not the maven group of the product id."


class ResourceNotTrashedError(FoundryAPIError):
    """Exception is thrown when trying to delete a resource which is not trashed."""

    message = "Resource needs to be trashed before deleting it."


class CircularDependencyError(FoundryAPIError):
    """Exception is thrown when trying to move to a destination which is one of its children."""

    message = "Cannot move resources to destination which is one of its children."


class CannotMoveResourcesUnderHiddenResourceError(FoundryAPIError):
    """Exception is thrown when the destination is a hidden resource."""

    message = "The destination is a hidden resource."


class UnexpectedParentError(FoundryAPIError):
    """Exception is thrown when a parent is specified that does not match the resource's current parent."""

    message = "A parent is specified that does not match the resource's current parent."
