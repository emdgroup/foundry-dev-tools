"""Implementation of the data-health API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.api_types import (
        CheckRid,
        DatasetRid,
    )


class DataHealthClient(APIClient):
    """DataHealthClient class that implements methods from the 'foundry-data-health' API."""

    api_name = "data-health"

    def api_get_checks_for_dataset(
        self,
        dataset_rid: DatasetRid,
        branch: str = "master",
        **kwargs,
    ) -> requests.Response:
        """Gets all the Check RIDs available for the given Dataset RID and branch name.

        Args:
            dataset_rid: The RID of the dataset
            branch: The name of the branch
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            api_path=f"checks/v2/{dataset_rid}/{branch}",
            **kwargs,
        )

    def api_get_latest_checks_history(
        self, check_rids: list[CheckRid] | CheckRid, limit: int | None = None, **kwargs
    ) -> requests.Response:
        """Returns CheckRun metadata for the last `limit` number of check runs for the given list of Check RIDs.

        Args:
            check_rids: a list of CheckRIDs or a single string value.
            limit: number of last check runs to go for for each Check RID. Defaults to 5.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        check_rids = check_rids if isinstance(check_rids, list) else [check_rids]
        limit = limit if limit else 5
        return self.api_request(
            "POST",
            api_path=f"checks/reports/v2/latest?limit={limit}",
            json=check_rids,
            **kwargs,
        )
