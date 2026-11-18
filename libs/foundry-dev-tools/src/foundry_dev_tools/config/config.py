"""Classes and logic for the Configuration."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.config.palantir_mcp import (
    discover_palantir_mcp_credential,
    print_auto_pickup_notice,
)
from foundry_dev_tools.config.token_provider import (
    TOKEN_PROVIDER_MAPPING,
    AppServiceTokenProvider,
    TokenProvider,
)
from foundry_dev_tools.errors.config import (
    FoundryConfigError,
    MissingCredentialsConfigError,
    MissingFoundryHostError,
    TokenProviderConfigError,
)
from foundry_dev_tools.utils.config import (
    cfg_files,
    check_init,
    get_environment_variable_config,
    merge_dicts,
    path_from_path_or_str,
    user_cache,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from os import PathLike

    import requests

# compatibility for python version < 3.11
if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib


class Config:
    """Class for Configuration options."""

    def __init__(
        self,
        requests_ca_bundle: PathLike[str] | str | None = None,
        transforms_sql_sample_row_limit: int = 5000,
        transforms_sql_dataset_size_threshold: int = 500,
        transforms_sql_sample_select_random: bool = False,
        transforms_force_full_dataset_download: bool = False,
        cache_dir: PathLike[str] | str | None = None,
        transforms_freeze_cache: bool = False,
        transforms_output_folder: PathLike[str] | str | None = None,
        rich_traceback: bool = False,
        debug: bool = False,
        requests_session: requests.Session | None = None,
    ) -> None:
        """Initialize the configuration.

        Args:
            requests_ca_bundle: a path to a CA bundle if :py:mod:`requests` needs custom certificates to work
                e.g. in a corporate network
            transforms_sql_dataset_size_threshold: only download the complete dataset if it doesn't exceed this
                threshold otherwise only return a smaller number of rows, set by `transforms_sql_sample_row_limit`
            transforms_sql_sample_row_limit: Number of sql rows to return when the dataset
                is above the `transforms_sql_dataset_size_threshold`
            transforms_sql_sample_select_random: set to true if the sample rows should be random
                (query will take more time)
            transforms_force_full_dataset_download: if true, ignores the `transforms_sql_dataset_size_threshold`
                and downloads the full dataset
            cache_dir: path to the cache dir for downloaded datasets,
                default is :py:attr:`~foundry_dev_tools.utils.config.user_cache`
            transforms_freeze_cache: if this setting is enabled, transforms will work offline
                if the datasets are already in cache
            transforms_output_folder: When @transform in combination with TransformOutput.filesystem() is used,
                files are written to this folder.
            rich_traceback: enables a prettier traceback provided by the module `rich` See: https://rich.readthedocs.io/en/stable/traceback.html
            debug: enables debug logging
            requests_session (requests.Session): Overwrite the default used requests.Session.

        """
        self.requests_ca_bundle = os.fspath(requests_ca_bundle) if requests_ca_bundle else None
        self.cache_dir = path_from_path_or_str(cache_dir) if cache_dir is not None else user_cache()
        self.transforms_output_folder = (
            path_from_path_or_str(transforms_output_folder) if transforms_output_folder is not None else None
        )
        self.transforms_sql_sample_row_limit = int(transforms_sql_sample_row_limit)
        self.transforms_sql_dataset_size_threshold = int(transforms_sql_dataset_size_threshold)
        self.transforms_sql_sample_select_random = bool(transforms_sql_sample_select_random)
        self.transforms_force_full_dataset_download = bool(transforms_force_full_dataset_download)
        self.transforms_freeze_cache = bool(transforms_freeze_cache)
        self.rich_traceback = bool(rich_traceback)
        self.debug = bool(debug)
        self.requests_session = requests_session

    def __repr__(self) -> str:
        return "<" + self.__class__.__name__ + "(" + self.__dict__.__str__() + ")>"


def _load_config_file(config_file: Path) -> dict | None:
    try:
        with config_file.open("rb") as config_file_fd:
            return tomllib.load(config_file_fd)
    except OSError:
        return None


def _load_config_files(config_files: Iterable[Path]) -> dict:
    """Merges the given config files.

    The last file wins.
    """
    config = {}
    for cfg_file in config_files:
        if cfg_file.exists():
            c = _load_config_file(cfg_file) or {}
            config = merge_dicts(config, c)
    # When no files were found, check for old config
    if len(config) == 0:
        p = Path("~/.foundry-dev-tools/config").expanduser()
        if p.exists() and not p.is_dir():
            msg = "Please use the `fdt config migrate` command to migrate the v1 config to the v2 config format."
            raise FoundryConfigError(msg)
    return config


def _pure_config_dict(env: bool = True) -> dict:
    config = _load_config_files(cfg_files())
    if env:
        config = merge_dicts(config, get_environment_variable_config())
    return config


def _find_token_provider(credentials: dict) -> str | None:
    # go backwards, to use the last defined token provider in the config
    for k in reversed(credentials):
        if k in TOKEN_PROVIDER_MAPPING:
            return k
    return None


PALANTIR_MCP_PROVIDER = "palantir_mcp"

MISSING_TP_ERROR = TokenProviderConfigError(
    "To authenticate with Foundry you need a TokenProvider. The token provider can be configured either via the"
    " configuration file or the token_provider FoundryContext parameter."
)


def _auto_palantir_mcp_config(discovery_host: Host | None = None) -> dict | None:
    """Return a credentials config synthesized from the Palantir MCP cache, if available."""
    if "APP_SERVICE_TS" in os.environ:
        # App Service supplies its own token; do not override that environment.
        return None
    if discovery_host is None:
        auto_credential = discover_palantir_mcp_credential()
    else:
        # A known host makes discovery optional: no matching cache entry just means
        # that the caller's own credentials are used.
        try:
            auto_credential = discover_palantir_mcp_credential(host=discovery_host)
        except TokenProviderConfigError:
            return None
    if auto_credential is None:
        return None
    print_auto_pickup_notice(auto_credential)
    return {
        "credentials": {
            "domain": auto_credential.host.domain,
            "scheme": auto_credential.host.scheme,
            PALANTIR_MCP_PROVIDER: {},
        },
    }


def _resolve_auto_credentials(
    default_credentials: dict,
    domain: str | None,
    scheme: str | None,
    token_provider_name: str | None,
    discovery_host: Host | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Fill missing host/provider values from the Palantir MCP token cache."""
    if (domain and token_provider_name) or "APP_SERVICE_TS" in os.environ:
        return domain, scheme, token_provider_name

    # Only the Palantir MCP provider may be inferred. A provider that was selected
    # explicitly keeps its existing configuration validation, so explicit
    # credentials are never sent to a host discovered from the MCP cache.
    if token_provider_name not in (None, PALANTIR_MCP_PROVIDER):
        return domain, scheme, token_provider_name

    # Use the origin the provider will use, including the configured scheme.
    host = Host(domain, scheme) if domain else discovery_host
    # When a host is known, discovery is optional: a missing cache entry must not
    # block the regular missing-provider error.
    optional_discovery = token_provider_name is None and host is not None
    try:
        auto_credential = discover_palantir_mcp_credential(host=host)
    except TokenProviderConfigError:
        if not optional_discovery:
            raise
        auto_credential = None
    if auto_credential is None:
        return domain, scheme, token_provider_name

    if not domain:
        domain = auto_credential.host.domain
    if not scheme:
        scheme = auto_credential.host.scheme
    if not token_provider_name:
        token_provider_name = PALANTIR_MCP_PROVIDER
        default_credentials.setdefault(PALANTIR_MCP_PROVIDER, {})
        print_auto_pickup_notice(auto_credential)
    return domain, scheme, token_provider_name


def _resolve_profile_config(config: dict, profile: str | None) -> tuple[dict, dict]:
    """Return the profile's credentials and config sections."""
    if profile is None:
        profile = config.get("profile")
    if profile in ("config", "credentials"):
        msg = f"Profile name can't be {profile}"
        raise AttributeError(msg)
    if profile and (profile_config := config.get(profile)) and isinstance(profile_config, dict):
        return profile_config.get("credentials", {}), profile_config.get("config", {})
    # use empty profile sections if no profile is specified
    return {}, {}


def get_config_dict(
    profile: str | None = None,
    env: bool = True,
    auto_discover: bool = True,
    discovery_host: Host | None = None,
) -> dict | None:
    """Loads config from the config files and environment variables.

    Profiles make configs like this possible:

    .. code-block:: toml

        [config]
        example_option = 1

        [integration.config]
        example_option = 2

    Where 'integration.config' will be loaded instead of 'config'
    if the profile is set to integration.

    Args:
        profile: The profile to use, if None the default profile is used
        env: Whether to load the environment variables
        auto_discover: Whether to fall back to the Palantir MCP token cache when the
            configuration does not define credentials. Set to False when credentials
            are passed explicitly.
        discovery_host: Host to use when the configuration does not define one, for
            example an explicit URL supplied by a v1 caller.
    """
    config = _pure_config_dict(env=env)
    if not config:
        return _auto_palantir_mcp_config(discovery_host) if auto_discover else None
    profile_credentials, profile_config = _resolve_profile_config(config, profile)

    default_credentials = config.get("credentials", {})
    domain = profile_credentials.pop("domain", default_credentials.pop("domain", None))
    scheme = profile_credentials.pop("scheme", default_credentials.pop("scheme", None))

    # decide which will be the token provider used
    token_provider_name = _find_token_provider(profile_credentials) or _find_token_provider(default_credentials)

    # If the Foundry DevTools configuration does not fully define credentials,
    # fall back to the token Palantir MCP already cached. Explicit credentials
    # always take precedence.
    if auto_discover:
        domain, scheme, token_provider_name = _resolve_auto_credentials(
            default_credentials,
            domain,
            scheme,
            token_provider_name,
            discovery_host,
        )

    # a domain must always be provided
    if not domain:
        raise MissingFoundryHostError

    if not token_provider_name:
        if "APP_SERVICE_TS" in os.environ:
            token_provider_name = "app_service"  # noqa: S105
        else:
            raise MISSING_TP_ERROR
    # merge the profile config with the non-prefixed config
    return_config = {}
    merged_config = merge_dicts(config.get("config", {}), profile_config.get("config", {}))
    if merged_config:
        return_config["config"] = merged_config
    return_config["credentials"] = {
        "domain": domain,
        token_provider_name: merge_dicts(
            default_credentials.get(token_provider_name), profile_credentials.get(token_provider_name)
        ),
    }

    if scheme is not None:
        return_config["credentials"]["scheme"] = scheme
    return return_config


def parse_credentials_config(config_dict: dict | None) -> TokenProvider:
    """Parses the credentials config dictionary and returns a TokenProvider object."""
    # check if there is a credentials config present
    if config_dict is not None and (credentials_config := config_dict.get("credentials")):
        # a domain must always be provided
        if "domain" not in credentials_config:
            raise MissingFoundryHostError

        # create a host object with the domain and the optional scheme setting
        host = Host(credentials_config.pop("domain"), credentials_config.pop("scheme", None))
        # get the token provider config setting, if it does not exist use an empty dict
        try:
            tp_name, tp_config = credentials_config.popitem()
            # make it possible to do jwt = "eyJ" instead of jwt = {jwt="eyJ"}
            if tp_config is None or len(tp_config) == 0:
                tp_config = {}
            elif not isinstance(tp_config, dict):
                # palantir_mcp has no required option, allow `palantir_mcp = ""`
                tp_config = {} if tp_name == PALANTIR_MCP_PROVIDER else {tp_name: tp_config}
        except KeyError:
            tp_name, tp_config = None, None

        if tp_name:
            if mapped_class := TOKEN_PROVIDER_MAPPING.get(tp_name):
                # check the config kwargs and pass the valid kwargs to the mapped class
                return mapped_class(**check_init(mapped_class, "credentials", {"host": host, **tp_config}))

            # if the token_provider name was set but not present in the mapping
            msg = f"The token provider implementation {tp_name} does not exist."
            raise TokenProviderConfigError(msg)

        # use flask/dash/streamlit provider when used in the app service
        if "APP_SERVICE_TS" in os.environ:
            return AppServiceTokenProvider(host=host)
        raise MISSING_TP_ERROR
    raise MissingCredentialsConfigError


def parse_general_config(config_dict: dict | None = None) -> Config:
    """Parses the config dictionary and returns a Config object."""
    if config_dict is not None and (general_config := config_dict.get("config")):
        return Config(**check_init(Config, "config", general_config))
    return Config()
