"""Implementation of the foundry-stats API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils import api_types


class FoundryStatsClient(APIClient):
    """To be implemented/transferred."""

    api_name = "foundry-stats"

    def api_foundry_stats(
        self,
        dataset_rid: api_types.DatasetRid,
        end_transaction_rid: api_types.TransactionRid,
        branch: api_types.Ref = "master",
        **kwargs,
    ) -> requests.Response:
        """Returns row counts and size of the dataset/view.

        Args:
            dataset_rid: The dataset RID.
            end_transaction_rid: The specific transaction RID,
                which will be used to return the statistics.
            branch: The branch to query
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                which has a json dict:
                    With the following structure:
                    {
                    datasetRid: str,
                    branch: str,
                    endTransactionRid: str,
                    schemaId: str,
                    computedDatasetStats: {
                    rowCount: str | None,
                    sizeInBytes: str,
                    columnStats: { "...": {nullCount: str | None, uniqueCount: str | None, avgLength: str | None, maxLength: str | None,} },
                    },
                    }
        """  # noqa: E501
        return self.api_request(
            "POST",
            "computed-stats-v2/get-v2",
            json={
                "datasetRid": dataset_rid,
                "branch": branch,
                "endTransactionRid": end_transaction_rid,
            },
            **kwargs,
        )
