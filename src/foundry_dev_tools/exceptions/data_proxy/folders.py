"""
General module for dataset related exceptions.
"""
from typing import Optional

import requests

from ..generic import FoundryAPIError  # pylint: disable=relative-beyond-top-level


class FolderNotFoundError(FoundryAPIError):
    """Exception is thrown when compass folder does not exist."""

    def __init__(self, folder_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             folder_rid (str): folder_rid which can't be found
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Compass folder {folder_rid} not found; "
            f"If you are sure your folder_rid is correct, "
            f"and you have access, check if your jwt token "
            f"is still valid!\n" + (response.text if response is not None else "")
        )
        self.folder_rid = folder_rid
        self.response = response
