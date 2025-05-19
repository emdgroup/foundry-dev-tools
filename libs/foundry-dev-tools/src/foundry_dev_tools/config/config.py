"""Classes and logic for the Configuration."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from foundry_dev_tools.config.config_types import Host
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


MISSING_TP_ERROR = TokenProviderConfigError(
    "To authenticate with Foundry you need a TokenProvider. The token provider can be configured either via the"
    " configuration file or the token_provider FoundryContext parameter."
)


def get_config_dict(profile: str | None = None, env: bool = True) -> dict | None:
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
    """
    config = _pure_config_dict(env=env)
    if not config:
        return None
    if profile is None:
        profile = config.get("profile")
    if profile in ("config", "credentials"):
        msg = f"Profile name can't be {profile}"
        raise AttributeError(msg)
    if profile and (profile_config := config.get(profile)) and isinstance(profile_config, dict):
        profile_credentials = profile_config.get("credentials", {})
        profile_config = profile_config.get("config", {})
    else:
        # use an empty profile config if no profile is specified
        # this aims to reduce duplicate code
        # but is going to do useless operations
        profile_credentials = {}
        profile_config = {}

    default_credentials = config.get("credentials", {})
    domain = profile_credentials.pop("domain", default_credentials.pop("domain", None))
    # a domain must always be provided
    if not domain:
        raise MissingFoundryHostError

    scheme = profile_credentials.pop("scheme", default_credentials.pop("scheme", None))

    # decide which will be the token provider used
    token_provider_name = _find_token_provider(profile_credentials) or _find_token_provider(default_credentials)
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
                tp_config = {tp_name: tp_config}
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
