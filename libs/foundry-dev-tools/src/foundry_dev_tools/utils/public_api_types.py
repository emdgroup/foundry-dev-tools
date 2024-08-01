"""API Types for the publicly documented Foundry API."""

from typing import TypedDict

from foundry_dev_tools.utils import api_types

OntologyRid = api_types.Rid


class Ontology(TypedDict):
    """Ontology Object as returned by the API."""

    apiname: str
    displayName: str
    description: str
    rid: api_types.Rid
