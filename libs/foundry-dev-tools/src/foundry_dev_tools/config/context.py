"""Contains the FoundryContext class, a state object for API clients."""

from __future__ import annotations

from functools import cached_property, partial
from typing import TYPE_CHECKING

import requests

from foundry_dev_tools.__about__ import __version__
from foundry_dev_tools.clients import (
    build2,
    catalog,
    compass,
    context_client,
    data_proxy,
    foundry_sql_server,
    foundry_stats,
    jemma,
    magritte_coordinator,
    metadata,
    multipass,
    public_ontologies_client,
    s3_client,
    scheduler,
    schema_inference,
    tables,
)
from foundry_dev_tools.config.config import Config, get_config_dict, parse_credentials_config, parse_general_config
from foundry_dev_tools.helpers.multipass import Group, User
from foundry_dev_tools.resources.dataset import Dataset
from foundry_dev_tools.resources.resource import Resource
from foundry_dev_tools.utils.config import entry_point_fdt_api_client

if TYPE_CHECKING:
    from foundry_dev_tools.cached_foundry_client import CachedFoundryClient
    from foundry_dev_tools.config.config_types import Host, Token
    from foundry_dev_tools.config.token_provider import TokenProvider
    from foundry_dev_tools.foundry_api_client import FoundryRestClient
    from foundry_dev_tools.utils import api_types


class FoundryContext:
    """FoundryContext holds config and token provider for API clients."""

    config: Config
    token_provider: TokenProvider
    client: requests.Session

    def __init__(
        self,
        config: Config | None = None,
        token_provider: TokenProvider | None = None,
        profile: str | None = None,
    ) -> None:
        if config is None or token_provider is None:
            config_dict = get_config_dict(profile)
            self.config = config or parse_general_config(config_dict)
            self.token_provider = token_provider or parse_credentials_config(config_dict)
        else:
            self.token_provider = token_provider
            self.config = config

        if not self.config.requests_session:
            self.client = context_client.ContextHTTPClient(
                debug=self.config.debug, requests_ca_bundle=self.config.requests_ca_bundle
            )
        else:
            self.client = self.config.requests_session

        self.token_provider.set_requests_session(self.client)
        self.client.auth = lambda r: self.token_provider.requests_auth_handler(r)
        self.client.headers["User-Agent"] = requests.utils.default_user_agent(
            f"foundry-dev-tools/{__version__}/python-requests"
        )

        if self.config.rich_traceback:
            from rich.traceback import install

            install()

    @property
    def host(self) -> Host:
        """Returns the hostname from the token provider."""
        return self.token_provider.host

    @property
    def token(self) -> Token:
        """Returns the token from the token provider."""
        return self.token_provider.token

    @cached_property
    def catalog(self) -> catalog.CatalogClient:
        """Returns :py:class:`foundry_dev_tools.clients.catalog.CatalogClient`."""
        return catalog.CatalogClient(self)

    @cached_property
    def compass(self) -> compass.CompassClient:
        """Returns :py:class:`foundry_dev_tools.clients.compass.CompassClient`."""
        return compass.CompassClient(self)

    @cached_property
    def jemma(self) -> jemma.JemmaClient:
        """Returns :py:class:`foundry_dev_tools.clients.jemma.JemmaClient`."""
        return jemma.JemmaClient(self)

    @cached_property
    def tables(self) -> tables.TablesClient:
        """Returns :py:class:`foundry_dev_tools.clients.tables.TablesClient`."""
        return tables.TablesClient(self)

    @cached_property
    def magritte_coordinator(self) -> magritte_coordinator.MagritteCoordinatorClient:
        """Returns :py:class:`foundry_dev_tools.clients.magritte_coordinator.MagritteCoordinatorClient`."""
        return magritte_coordinator.MagritteCoordinatorClient(self)

    @cached_property
    def scheduler(self) -> scheduler.SchedulerClient:
        """Returns :py:class:`foundry_dev_tools.clients.scheduler.SchedulerClient`."""
        return scheduler.SchedulerClient(self)

    @cached_property
    def metadata(self) -> metadata.MetadataClient:
        """Returns :py:class:`foundry_dev_tools.clients.metadata.MetadataClient`."""
        return metadata.MetadataClient(self)

    @cached_property
    def data_proxy(self) -> data_proxy.DataProxyClient:
        """Returns :py:class:`foundry_dev_tools.clients.data_proxy.DataProxyClient`."""
        return data_proxy.DataProxyClient(self)

    @cached_property
    def schema_inference(self) -> schema_inference.SchemaInferenceClient:
        """Returns :py:class:`foundry_dev_tools.clients.schema_inference.SchemaInferenceClient`."""
        return schema_inference.SchemaInferenceClient(self)

    @cached_property
    def multipass(self) -> multipass.MultipassClient:
        """Returns :py:class:`foundry_dev_tools.clients.multipass.MultipassClient`."""
        return multipass.MultipassClient(self)

    @cached_property
    def foundry_sql_server(self) -> foundry_sql_server.FoundrySqlServerClient:
        """Returns :py:class:`foundry_dev_tools.clients.foundry_sql_server.FoundrySqlServerClient`."""
        return foundry_sql_server.FoundrySqlServerClient(self)

    @cached_property
    def build2(self) -> build2.Build2Client:
        """Returns :py:class:`foundry_dev_tools.clients.build2.Build2Client`."""
        return build2.Build2Client(self)

    @cached_property
    def foundry_stats(self) -> foundry_stats.FoundryStatsClient:
        """Returns :py:class:`foundry_dev_tools.clients.foundry_stats.FoundryStatsClient`."""
        return foundry_stats.FoundryStatsClient(self)

    @cached_property
    def s3(self) -> s3_client.S3Client:
        """Returns :py:class:`foundry_dev_tools.clients.s3_client.S3Client`."""
        return s3_client.S3Client(self)

    @cached_property
    def ontologies(self) -> public_ontologies_client.OntologiesClient:
        """Returns :py:class:`foundry_dev_tools.clients.public_ontologies.OntologiesClient`."""
        return public_ontologies_client.OntologiesClient(self)

    @cached_property
    def cached_foundry_client(self) -> CachedFoundryClient:
        """Returns :py:class:`foundry_dev_tools.cached_foundry_client.CachedFoundryClient`.

        Will be deprecated in favor of the newer v2 clients.
        """
        from foundry_dev_tools.cached_foundry_client import CachedFoundryClient

        return CachedFoundryClient(ctx=self)

    @cached_property
    def foundry_rest_client(self) -> FoundryRestClient:
        """Returns :py:class:`foundry_dev_tools.foundry_api_client.FoundryRestClient`.

        Will be deprecated in favor of the newer v2 clients.
        """
        from foundry_dev_tools.foundry_api_client import FoundryRestClient

        return FoundryRestClient(ctx=self)

    def get_dataset(
        self,
        rid: api_types.Rid,
        /,
        *,
        branch: api_types.Ref = "master",
        create_branch_if_not_exists: bool = True,
        parent_ref: api_types.TransactionRid | None = None,
        parent_branch_id: api_types.DatasetBranch | None = None,
        **kwargs,
    ) -> Dataset:
        """Returns dataset at path.

        Args:
            rid: the rid of the dataset
            branch: the branch of the dataset
            create_branch_if_not_exists: create branch if branch does not exist
            parent_ref: optionally the transaction off which the branch will be based
                (only used if branch needs to be created)
            parent_branch_id: optionally a parent branch name, otherwise a root branch
                (only used if branch needs to be created)
            **kwargs: passed to :py:meth:`foundry_dev_tools.resources.resource.Resource.from_rid`
        """
        return Dataset.from_rid(
            self,
            rid,
            branch=branch,
            create_branch_if_not_exists=create_branch_if_not_exists,
            parent_ref=parent_ref,
            parent_branch_id=parent_branch_id,
            **kwargs,
        )

    def get_dataset_by_path(
        self,
        path: api_types.FoundryPath,
        /,
        *,
        branch: api_types.Ref = "master",
        create_if_not_exist: bool = False,
        create_branch_if_not_exists: bool = True,
        parent_ref: api_types.TransactionRid | None = None,
        parent_branch_id: api_types.DatasetBranch | None = None,
        **kwargs,
    ) -> Dataset:
        """Returns dataset at path.

        Args:
            path: the path where the dataset is located on foundry
            branch: the branch of the dataset
            create_if_not_exist: if the dataset does not exist, create it
            create_branch_if_not_exists: create branch if branch does not exist,
                branch always will be created if resource does not exist
            parent_ref: optionally the transaction off which the branch will be based
                (only used if branch needs to be created)
            parent_branch_id: optionally a parent branch name, otherwise a root branch
                (only used if branch needs to be created)
            **kwargs: passed to :py:meth:`foundry_dev_tools.resources.resource.Resource.from_path`
        """
        return Dataset.from_path(
            self,
            path,
            branch=branch,
            create_if_not_exist=create_if_not_exist,
            create_branch_if_not_exists=create_branch_if_not_exists,
            parent_ref=parent_ref,
            parent_branch_id=parent_branch_id,
            **kwargs,
        )

    def get_resource(
        self,
        rid: api_types.Rid,
        /,
        *,
        decoration: api_types.ResourceDecorationSetAll | None = None,
    ) -> Resource:
        """Returns resource from rid.

        Args:
            rid: the rid of the resource on foundry
            decoration: extra decoration for this resource
            **_kwargs: not used in base class
        """
        return Resource.from_rid(self, rid, decoration=decoration)

    def get_resource_by_path(
        self,
        path: api_types.FoundryPath,
        /,
        *,
        decoration: api_types.ResourceDecorationSetAll | None = None,
    ) -> Resource:
        """Returns resource from path.

        Args:
            path: the path where the resource is located on foundry
            decoration: extra decoration for this resource
            **_kwargs: not used in base class
        """
        return Resource.from_path(self, path, decoration=decoration)

    def get_user_info(self) -> User:
        """Returns the user's info."""
        return User.me(self)

    def create_group(
        self,
        name: str,
        organization_rids: set[api_types.OrganizationRid],
        /,
        *,
        description: str | None = None,
    ) -> Group:
        """Create new multipass group.

        Args:
            name: the name the group should receive on creation
            organization_rids: a set of organization identifiers the group will belong to
            description: an optional group description
        """
        return Group.create(self, name, organization_rids, description=description)

    def __repr__(self) -> str:
        return (
            "<"
            + self.__class__.__name__
            + "(config="
            + self.config.__str__()
            + ", token_provider="
            + self.token_provider.__str__()
            + ")>"
        )


for k, v in entry_point_fdt_api_client().items():
    cp = cached_property(partial(lambda cls, self: cls(self), v))
    cp.__set_name__(FoundryContext, k)
    setattr(FoundryContext, k, cp)
