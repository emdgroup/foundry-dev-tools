"""Implementation of the schema_inference API."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING
from urllib.parse import quote_plus

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.api_types import DatasetRid, Ref


# PLACEHOLDER
class SchemaInferenceClient(APIClient):
    """SchemaInferenceClient class that implements methods from the 'foundry-schema-inference' API."""

    api_name = "foundry-schema-inference"

    def infer_dataset_schema(self, dataset_rid: DatasetRid, branch: Ref = "master") -> dict:
        """Calls the foundry-schema-inference service to infer the dataset schema.

        Returns dict with foundry schema, if status == SUCCESS

        Args:
            dataset_rid: The dataset rid
            branch: The branch

        Returns:
            dict:
                with dataset schema, that can be used to call upload_dataset_schema

        Raises:
            ValueError: if foundry schema inference failed
        """
        response = self.api_infer_dataset_schema(dataset_rid, branch).json()
        if response["status"] == "SUCCESS":
            return response["data"]["foundrySchema"]
        if response["status"] == "WARN":
            warnings.warn(
                "Foundry Schema inference completed with status "
                f"'{response['status']}' "
                f"and message '{response['message']}'.",
                UserWarning,
            )
            return response["data"]["foundrySchema"]
        msg = (
            f'Foundry Schema inference failed with status \'{response["status"]}\' and message'
            f' \'{response["message"]}\'.'
        )
        raise ValueError(
            msg,
        )

    def api_infer_dataset_schema(self, dataset_rid: DatasetRid, branch: Ref, **kwargs) -> requests.Response:
        """Infer the dataset schema.

        Args:
            dataset_rid: the dataset rid to infer the schema
            branch: branch of the dataset
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "POST",
            f"datasets/{dataset_rid}/branches/{quote_plus(branch)}/schema",
            json={},
            **kwargs,
        )
