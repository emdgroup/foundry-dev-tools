"""The DataProxy API client."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests


class FoundrySearchClient(APIClient):
    """DataProxyClient class that implements methods from the 'foundry-data-proxy' API."""

    api_name = "foundry-search"

    def api_linter_search_recommendations(
        self, payload: dict, page_token: str | None = None, **kwargs
    ) -> requests.Response:
        """Raw API call to get linter recommendations."""
        if page_token:
            payload["pageToken"] = page_token
        return self.api_request(
            "POST",
            "linter/v1/recommendations/search",
            json=payload,
            **kwargs,
        )

    def get_all_linter_recommendations(self) -> list[dict]:
        """Get all linter recommendations, pagination is handled here."""
        page_token = None
        results = []
        while True:
            response = self.api_linter_search_recommendations(
                payload={
                    "sort": {"order": "DESC", "category": "ESTIMATED_BATCH_COMPUTE_SAVINGS"},
                },
                page_token=page_token,
            )
            json_content = response.json()
            results += json_content["hits"]

            if json_content["nextPageToken"] is None:
                break
            page_token = json_content["nextPageToken"]

            time.sleep(0.5)
        return results
