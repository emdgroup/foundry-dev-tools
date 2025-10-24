"""Core entry point for the public Foundry SDK bindings."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from foundry_dev_tools.clients.public_ontologies_client import OntologiesClient
    from foundry_dev_tools.config.context import FoundryContext
    from foundry_dev_tools.public_sdk.datasets import PublicDatasetsClient


class PublicSDK:
    """Namespace that exposes clients built on Foundry's public API."""

    __slots__ = ("_context", "__dict__")

    def __init__(self, context: FoundryContext) -> None:
        self._context = context

    @cached_property
    def datasets(self) -> PublicDatasetsClient:
        """Dataset operations backed by the public v2 API."""
        from foundry_dev_tools.public_sdk.datasets import PublicDatasetsClient

        return PublicDatasetsClient(self._context)

    @cached_property
    def ontologies(self) -> OntologiesClient:
        """Public Ontologies API client."""
        from foundry_dev_tools.clients.public_ontologies_client import OntologiesClient

        return OntologiesClient(self._context)
