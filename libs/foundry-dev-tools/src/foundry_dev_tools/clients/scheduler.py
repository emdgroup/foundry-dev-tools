"""Implementation of the scheduler API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.compass import ResourceNotFoundError
from foundry_dev_tools.errors.handling import ErrorHandlingConfig

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils import api_types


class SchedulerClient(APIClient):
    """SchedulerClient class that implements methods from the 'scheduler' API."""

    api_name = "scheduler"

    def get_schedules_by_run_build(
        self, rid: api_types.Rid, branch: api_types.DatasetBranch = "master", **kwargs
    ) -> requests.Response:
        """Gets the schedules of the resource dataset.

        Args:
            rid: the identifier of the datset
            branch: the branch name of the dataset
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        params = {"datasetRids": [rid], "branch": branch}

        return self.api_request(
            "POST",
            "scheduler/get-schedules-by-run-build-dataset",
            json=params,
            error_handling=ErrorHandlingConfig(api_error_mapping={204: ResourceNotFoundError}),
            **kwargs,
        )

    def get_schedule(self, rid: api_types.Rid, **kwargs) -> requests.Response:
        """Gets the schedule details of a given schedule.

        Args:
            rid: the identifier of the scheduler
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            f"scheduler/schedules/{rid}",
            error_handling=ErrorHandlingConfig(api_error_mapping={204: ResourceNotFoundError}),
            **kwargs,
        )
