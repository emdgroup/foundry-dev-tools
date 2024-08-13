"""Error handling configuration for FoundryAPIErrors."""

from __future__ import annotations

import logging
from typing import Literal

import requests

from foundry_dev_tools.errors.compass import (
    AutosaveResourceOperationForbiddenError,
    CannotMoveResourcesUnderHiddenResourceError,
    CircularDependencyError,
    DuplicateNameError,
    ForbiddenOperationOnHiddenResourceError,
    ForbiddenOperationOnServiceProjectResourceError,
    GatekeeperInsufficientPermissionsError,
    IllegalNameError,
    InsufficientPermissionsError,
    InvalidMarkingError,
    InvalidMavenGroupPrefixError,
    InvalidMavenProductIdError,
    InvalidOrganizationMarkingHierarchyError,
    MarkingNotFoundError,
    MavenProductIdAlreadySetError,
    MavenProductIdConflictError,
    MissingOrganizationMarkingError,
    NotProjectError,
    OrganizationNotFoundError,
    PathNotFoundError,
    ResourceNotFoundError,
    ResourceNotTrashedError,
    TooManyResourcesRequestedError,
    UnexpectedParentError,
    UnrecognizedAccessLevelError,
    UnrecognizedPatchOperationError,
    UnrecognizedPrincipalError,
    UsersNamespaceOperationForbiddenError,
)
from foundry_dev_tools.errors.dataset import (
    BranchesAlreadyExistError,
    BranchNotFoundError,
    DatasetAlreadyExistsError,
    DatasetHasNoSchemaError,
    DatasetHasOpenTransactionError,
    DatasetNotFoundError,
)
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.errors.multipass import DuplicateGroupNameError
from foundry_dev_tools.errors.sql import (
    FoundrySqlQueryFailedError,
)
from foundry_dev_tools.utils.misc import decamelize

LOGGER = logging.getLogger(__name__)

DEFAULT_ERROR_MAPPING: dict[str | None, type[FoundryAPIError]] = {
    None: FoundryAPIError,
    "DataProxy:SchemaNotFound": DatasetHasNoSchemaError,
    "DataProxy:FallbackBranchesNotSpecifiedInQuery": BranchNotFoundError,
    "DataProxy:BadSqlQuery": FoundrySqlQueryFailedError,
    "DataProxy:DatasetNotFound": DatasetNotFoundError,
    "Catalog:DuplicateDatasetName": DatasetAlreadyExistsError,
    "Catalog:DatasetsNotFound": DatasetNotFoundError,
    "Catalog:BranchesAlreadyExist": BranchesAlreadyExistError,
    "Catalog:BranchesNotFound": BranchNotFoundError,
    "Catalog:InvalidArgument": DatasetNotFoundError,
    "Catalog:SimultaneousOpenTransactionsNotAllowed": DatasetHasOpenTransactionError,
    "Compass:AutosaveResourceOperationForbidden": AutosaveResourceOperationForbiddenError,
    "Compass:CannotMoveResourcesUnderHiddenResource": CannotMoveResourcesUnderHiddenResourceError,
    "Compass:CircularDependency": CircularDependencyError,
    "Compass:DuplicateName": DuplicateNameError,
    "Compass:ForbiddenOperationOnHiddenResource": ForbiddenOperationOnHiddenResourceError,
    "Compass:ForbiddenOperationOnServiceProjectResource": ForbiddenOperationOnServiceProjectResourceError,
    "Compass:GatekeeperInsufficientPermissions": GatekeeperInsufficientPermissionsError,
    "Compass:IllegalName": IllegalNameError,
    "Compass:InsufficientPermissions": InsufficientPermissionsError,
    "Compass:InvalidMarking": InvalidMarkingError,
    "Compass:InvalidMavenGroupPrefix": InvalidMavenGroupPrefixError,
    "Compass:InvalidMavenProductId": InvalidMavenProductIdError,
    "Compass:InvalidOrganizationMarkingHierarchy": InvalidOrganizationMarkingHierarchyError,
    "Compass:MarkingNotFound": MarkingNotFoundError,
    "Compass:MavenProductIdAlreadySet": MavenProductIdAlreadySetError,
    "Compass:MavenProductIdConflict": MavenProductIdConflictError,
    "Compass:MissingOrganizationMarking": MissingOrganizationMarkingError,
    "Compass:NotFound": ResourceNotFoundError,
    "Compass:NotProject": NotProjectError,
    "Compass:OrganizationNotFound": OrganizationNotFoundError,
    "Compass:PathNotFound": PathNotFoundError,
    "Compass:ResourceNotFound": ResourceNotFoundError,
    "Compass:ResourceNotTrashed": ResourceNotTrashedError,
    "Compass:TooManyResourcesRequested": TooManyResourcesRequestedError,
    "Compass:UnexpectedParent": UnexpectedParentError,
    "Compass:UnrecognizedAccessLevel": UnrecognizedAccessLevelError,
    "Compass:UnrecognizedPatchOperation": UnrecognizedPatchOperationError,
    "Compass:UnrecognizedPrincipal": UnrecognizedPrincipalError,
    "Compass:UsersNamespaceOperationForbidden": UsersNamespaceOperationForbiddenError,
    "FoundrySqlServer:InvalidDatasetNoSchema": DatasetHasNoSchemaError,
    "FoundrySqlServer:InvalidDatasetCannotAccess": BranchNotFoundError,
    "FoundrySqlServer:InvalidDatasetPathNotFound": DatasetNotFoundError,
    "Multipass:DuplicateGroupName": DuplicateGroupNameError,
}
"""This mapping maps the Error names coming from the API to the Foundry DevTools classes."""


class ErrorHandlingConfig:
    """Configuration for foundry error handling."""

    def __init__(
        self,
        api_error_mapping: dict[str | int, type[FoundryAPIError]] | type[FoundryAPIError] | None = None,
        info: str | None = None,
        **kwargs,
    ):
        """Configuration for foundry error handling.

        Args:
            api_error_mapping: Either a dictionary which maps either status codes
                or error names to python Exception classes,
                or just a python exception class to use it for every HTTP Error.
                Status codes that are not above 400, which do not raise an HTTP Error, will automatically be raised too
            info: additionial information about the error, passed to the constructor of the Exception
            kwargs: will be passed to the constructor of the Exception
        """
        self.api_error_mapping = api_error_mapping
        self.kwargs = kwargs
        self.info = info

    def _get_error_name(self, response: requests.Response) -> str | None:
        try:
            error_response = response.json()
        except:  # noqa: S110,E722
            pass
        else:
            if (
                (ec := error_response.get("errorCode"))
                and (en := error_response.get("errorName"))
                and (ei := error_response.get("errorInstanceId"))
            ):
                self.kwargs["error_code"] = ec
                self.kwargs["error_name"] = en
                self.kwargs["error_instance_id"] = ei
                if parameters := error_response.get("parameters"):
                    for k in parameters:
                        self.kwargs[decamelize(k)] = parameters[k]

                return en
        return None

    def get_exception_class(self, response: requests.Response) -> type[FoundryAPIError] | None:  # noqa: PLR0911
        """Returns the python exception class for the response."""
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            if self.api_error_mapping is not None:
                if isinstance(self.api_error_mapping, dict):
                    if status_exception := self.api_error_mapping.get(response.status_code):
                        return status_exception

                    if (en := self._get_error_name(response)) and (exc := (self.api_error_mapping.get(en))):
                        return exc
                else:
                    return self.api_error_mapping
            if (en := self._get_error_name(response)) and (exc := DEFAULT_ERROR_MAPPING.get(en)):
                return exc
            return FoundryAPIError
        else:
            if isinstance(self.api_error_mapping, dict) and (exc := self.api_error_mapping.get(response.status_code)):
                return exc
        return None

    def get_exception(self, response: requests.Response) -> FoundryAPIError | None:
        """Returns exception determined by :py:meth:`ErrorHandlingConfig.get_exception_class` filled out with the response and kwargs."""  # noqa: E501
        if exc := self.get_exception_class(response):
            return exc(response=response, info=self.info, **self.kwargs)
        return None


def raise_foundry_api_error(
    response: requests.Response,
    error_handling: ErrorHandlingConfig | Literal[False] | None = None,
):
    """Raise a foundry API error through the ErrorHandlingConfig.

    Convenience function around ErrorHandlingConfig.get_exception.
    """
    if error_handling is not False and (exc := (error_handling or ErrorHandlingConfig()).get_exception(response)):
        raise exc
