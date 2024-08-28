"""Multipass specific errors."""

from foundry_dev_tools.errors.meta import FoundryAPIError


class DuplicateGroupNameError(FoundryAPIError):
    """Exception is thrown when the group name already exists."""

    message = "The group name already exists!"
