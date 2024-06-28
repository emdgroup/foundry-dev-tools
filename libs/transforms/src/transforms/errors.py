"""Transforms specific FoundryDevToolsError classes."""

from foundry_dev_tools.errors.meta import FoundryDevToolsError


class FoundryTransformsError(FoundryDevToolsError):
    """Errors related to transforms."""


class UninitializedInputError(FoundryTransformsError):
    """Raised when the Input has not been initialized with a FoundryContext."""

    message = (
        "This Input has not been initialized, this is normally done by the transforms library automatically."
        "If you've created the Input manually and wanted to use it outside of the local transforms implementation "
        "you'll need to call the init_input method on it with a FoundryContext."
    )
