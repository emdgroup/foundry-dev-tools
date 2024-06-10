"""Implementation of the metadata API."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote_plus

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.dataset import DatasetHasNoSchemaError
from foundry_dev_tools.errors.handling import ErrorHandlingConfig

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils import api_types


# PLACEHOLDER
class MetadataClient(APIClient):
    """MetadataClient class that implements methods from the 'foundry-metadata' API."""

    api_name = "foundry-metadata"

    def api_upload_dataset_schema(
        self,
        dataset_rid: api_types.DatasetRid,
        transaction_rid: api_types.TransactionRid,
        schema: dict,
        branch: api_types.Ref,
        **kwargs,
    ) -> requests.Response:
        """Uploads the foundry dataset schema for a dataset, transaction, branch combination.

        Args:
            dataset_rid: The rid of the dataset
            transaction_rid: The rid of the transaction
            schema: The foundry schema
            branch: The branch
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "POST",
            f"schemas/datasets/{dataset_rid}/branches/{quote_plus(branch)}",
            params={"endTransactionRid": transaction_rid},
            json=schema,
            **kwargs,
        )

    def api_get_dataset_schema(
        self,
        dataset_rid: api_types.DatasetRid,
        branch: api_types.Ref,
        transaction_rid: api_types.TransactionRid | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns the foundry dataset schema for a dataset, transaction, branch combination.

        Args:
            dataset_rid: The rid of the dataset
            transaction_rid: The rid of the transaction
            branch: The branch
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "GET",
            f"schemas/datasets/{dataset_rid}/branches/{quote_plus(branch)}",
            params={"endTransactionRid": transaction_rid},
            error_handling=ErrorHandlingConfig(
                {204: DatasetHasNoSchemaError},
                dataset_rid=dataset_rid,
                transaction_rid=transaction_rid,
                branch=branch,
            ),
            **kwargs,
        )
