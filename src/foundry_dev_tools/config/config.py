"""Classes and logic for the Configuration."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Iterable

from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.config.token_provider import (
    TOKEN_PROVIDER_MAPPING,
    JWTTokenProvider,
    TokenProvider,
)
from foundry_dev_tools.errors.config import (
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
    from os import PathLike
    from pathlib import Path

# compatibility for python version < 3.11
if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib


class Config:
    """Class for Configuration options."""

    def __init__(
        self,
        requests_ca_bundle: PathLike[str] | None = None,
        transforms_sql_sample_row_limit: int = 5000,
        transforms_sql_dataset_size_threshold: int = 500,
        transforms_sql_sample_select_random: bool = False,
        transforms_force_full_dataset_download: bool = False,
        cache_dir: PathLike[str] | None = None,
        transforms_freeze_cache: bool = False,
        transforms_output_folder: PathLike[str] | None = None,
        rich_traceback: bool = False,
        debug: bool = False,
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
            debug: enbales debug logging

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

    def __repr__(self) -> str:
        return "<" + self.__class__.__name__ + "(" + self.__dict__.__str__() + ")>"


def _load_config_file(config_file: Path) -> dict | None:
    if not config_file.is_file():
        return None
    with config_file.open("rb") as config_file_fd:
        return tomllib.load(config_file_fd)


def _load_config_files(config_files: Iterable[Path]) -> dict:
    """Merges the given config files.

    The last file wins.
    """
    config = {}
    for cfg_file in config_files:
        if cfg_file.exists():
            c = _load_config_file(cfg_file) or {}
            config = merge_dicts(config, c)
    return config


def get_config_dict(profile: str | None = None) -> dict | None:
    """Loads config from the config files and environment variables.

    Profiles make configs like this possible:

    .. code-block:: toml

        [config]
        example_option = 1

        [integration.config]
        example_option = 2

    Where 'integration.config' will be loaded instead of 'config'
    if the profile is set to integration.
    """
    config = _load_config_files(cfg_files())
    config = merge_dicts(config, get_environment_variable_config())
    if not config:
        return None
    if profile is None:
        profile = config.get("profile")
    if profile in ("config", "credentials"):
        msg = f"Profile name can't be {profile}"
        raise AttributeError(msg)
    if profile is not None and (profile_config := config.get(profile)) and isinstance(profile_config, dict):
        # merge the profile config with the non-prefixed config
        config = {
            "config": merge_dicts(config.get("config", {}), profile_config.get("config", {})),
            "credentials": merge_dicts(config.get("credentials", {}), profile_config.get("credentials", {})),
        }
    return config


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
        token_provider = credentials_config.pop("token_provider", {})
        tp_name, tp_config = token_provider.get("name"), token_provider.get("config", {})

        if tp_name:
            if mapped_class := TOKEN_PROVIDER_MAPPING.get(tp_name):
                # check the config kwargs and pass the valid kwargs to the mapped class
                return mapped_class(**check_init(mapped_class, "credentials", {"host": host, **tp_config}))

            # if the token_provider name was set but not present in the mapping
            msg = f"The token provider implementation {tp_name} does not exist."
            raise TokenProviderConfigError(msg)

        # the jwt token provider needs a config
        if tp_config:
            ci = check_init(JWTTokenProvider, "credentials", {"host": host, **tp_config})
            return JWTTokenProvider(**ci)
        msg = (
            "To authenticate with Foundry you need a TokenProvider. The token provider can be configured either via the"
            " configuration file or the token_provider FoundryContext parameter."
        )
        raise TokenProviderConfigError(msg)
    raise MissingCredentialsConfigError


def parse_general_config(config_dict: dict | None = None) -> Config:
    """Parses the config dictionary and returns a Config object."""
    if config_dict is not None and (general_config := config_dict.get("config")):
        return Config(**check_init(Config, "config", general_config))
    return Config()
