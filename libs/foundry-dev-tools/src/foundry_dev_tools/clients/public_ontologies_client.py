"""Ontologies API.

https://www.palantir.com/docs/foundry/api/ontology-resources/ontology/ontology-basics/
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import PublicAPIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.public_api_types import Ontology, OntologyRid


class OntologiesClient(PublicAPIClient):
    """The Ontologies API."""

    api_name = "ontologies"

    def api_list_ontologies(self) -> requests.Response:
        """Lists the Ontologies visible to the current user."""
        return self.api_request("GET")

    def list(self) -> list[Ontology] | None:
        """Lists the Ontologies visible to the current user.

        Returns:
            list[Ontology] | None
        """
        return self.api_list_ontologies().json().get("data")

    def api_get_ontology(self, ontology_rid: OntologyRid) -> requests.Response:
        """Gets a specific ontology with the given Ontology RID."""
        return self.api_request("GET", ontology_rid)

    def get(self, ontology_rid: OntologyRid) -> Ontology:
        """Gets a specific ontology with the given Ontology RID.

        Returns:
            Ontology
        """
        return self.api_get_ontology(ontology_rid).json()
