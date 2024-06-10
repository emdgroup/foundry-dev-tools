"""Implementation of the build2 API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.compass import ResourceNotFoundError
from foundry_dev_tools.errors.handling import ErrorHandlingConfig

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils import api_types


class Build2Client(APIClient):
    """Build2Client class implements the 'build2' API."""

    api_name = "build2"

    def api_get_build_report(self, build_rid: api_types.Rid, **kwargs) -> requests.Response:
        """Returns the build report.

        Args:
            build_rid: the build RID
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "GET",
            "info/builds2/" + build_rid,
            error_handling=ErrorHandlingConfig({204: ResourceNotFoundError}),
            **kwargs,
        )

    def api_get_job_report(self, job_rid: api_types.Rid, **kwargs) -> requests.Response:
        """Returns the report for a job.

        Args:
            job_rid: the job RID
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "GET",
            "info/jobs3/" + job_rid,
            error_handling=ErrorHandlingConfig({204: ResourceNotFoundError}),
            **kwargs,
        )
