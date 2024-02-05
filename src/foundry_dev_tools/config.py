"""Global Config Module for Foundry DevTools.

Can be used to set configuration options

Usage:
    from foundry_dev_tools import Configuration
    Configuration.get_config()
    Configuration.set('transforms_sql_sample_row_limit', 500)
    Configuration.get('transforms_sql_sample_row_limit')

"""
import inspect
import logging
import os
import warnings
from collections import UserDict
from configparser import ConfigParser
from pathlib import Path
from typing import TYPE_CHECKING, Any

from foundry_dev_tools.utils.repo import find_project_config_file
from foundry_dev_tools.utils.token_provider import TOKEN_PROVIDERS

if TYPE_CHECKING:
    import pathlib

warnings.filterwarnings(
    "default", category=DeprecationWarning, module="foundry_dev_tools"
)

LOGGER = logging.getLogger(__name__)

TYPES = {
    "foundry_url": str,
    "jwt": str,
    "client_id": str,
    "client_secret": str,
    "grant_type": str,
    "requests_ca_bundle": os.path.expanduser,
    "transforms_sql_sample_row_limit": int,
    "transforms_sql_dataset_size_threshold": int,
    "transforms_sql_sample_select_random": bool,
    "transforms_force_full_dataset_download": bool,
    "cache_dir": os.path.expanduser,
    "transforms_freeze_cache": bool,
    "transforms_output_folder": os.path.expanduser,
    "enable_runtime_token_providers": bool,
    "scopes": str,
}


def type_convert(config: dict) -> dict:
    """Type conversion for the config.

    Each config key has a defined type in :py:data:`TYPES`.
    If the config key exists, it gets casted to that type.

    Args:
        config (dict): a dict with config entries

    Returns:
        dict:
            a dict with the same values except
            removed None values
            casted the types for defined keys in `TYPES`
    """
    return_config = {}
    for key, value in config.items():
        return_config[key] = (
            TYPES[key](value) if value is not None and key in TYPES else None
        )
    return return_config


def initial_config(
    project_dir: "Path|None" = None,
) -> "tuple[dict, pathlib.Path, pathlib.Path|None]":
    """Parses the config file and applies defaults.

    The order of config values and how they are applied are:
    defaults < global config file < project specific config < env variables
    < overwrites by :py:class:`~foundry_dev_tools.config.Config`

    Returns:
        a tuple (dict, pathlib.Path, pathlib.Path|None):
            the first dict is the config as parsed
            with defaults applied and environment variables applied
            the second is a Path to the ~/.foundry-dev-tools directory
            the third one is the Path to the project config file
    """
    foundry_dev_tools_directory = Path.home() / ".foundry-dev-tools"

    if not foundry_dev_tools_directory.exists():
        __foundry_local_directory = (
            Path.home() / ".foundry-local"
        )  # for backwards compatibility
        if __foundry_local_directory.exists():
            # provide the old folder as a fallback, but give a deprecation warning
            warnings.warn(
                "\nFoundrylocal has been renamed to Foundry DevTools.\n"
                f"Move the old config folder {__foundry_local_directory.absolute()} to "
                f"{foundry_dev_tools_directory.absolute()}\n"
                "The fallback to the old config folder will be removed in the future!",
                category=DeprecationWarning,
            )
            foundry_dev_tools_directory = __foundry_local_directory

    foundry_dev_tools_config_file = foundry_dev_tools_directory / "config"

    config = {
        "jwt": None,
        "client_id": None,
        "client_secret": None,
        "grant_type": "authorization_code",
        "scopes": None,
        "foundry_url": None,
        "cache_dir": foundry_dev_tools_directory / "cache",
        "transforms_output_folder": None,
        "transforms_sql_sample_select_random": False,
        "transforms_force_full_dataset_download": False,
        "enable_runtime_token_providers": True,
        "transforms_freeze_cache": False,
        "transforms_sql_sample_row_limit": 5000,
        "transforms_sql_dataset_size_threshold": 500,
    }

    return_config: "dict[str, Any]" = {}
    if foundry_dev_tools_config_file.exists():
        config_parser = ConfigParser()
        with foundry_dev_tools_config_file.open(encoding="UTF-8") as file:
            config_parser.read_file(file)
            if "default" in config_parser:
                return_config.update(config_parser["default"])

    if not project_dir:
        caller_filename = inspect.stack()[-1].filename
        project_config_file = find_project_config_file(Path(caller_filename).parent)
    else:
        project_config_file = find_project_config_file(project_dir)
    if project_config_file:
        project_config_parser = ConfigParser()
        with project_config_file.open(encoding="UTF-8") as file:
            project_config_parser.read_file(file)
        if "default" in project_config_parser:
            LOGGER.debug(
                "Using project based configuration file %s "
                "on top of global configuration.",
                project_config_file,
            )
            for key, value in project_config_parser.items("default"):
                return_config[key] = value

    for key in config:
        if f"FOUNDRY_DEV_TOOLS_{key.upper()}" in os.environ:
            return_config[key] = os.getenv(f"FOUNDRY_DEV_TOOLS_{key.upper()}")
        elif f"FOUNDRY_LOCAL_{key.upper()}" in os.environ:
            warnings.warn(
                "Foundrylocal has been renamed to Foundry DevTools.\n"
                "Rename your environment variables accordingly:\n"
                f"FOUNDRY_LOCAL_{key.upper()} to FOUNDRY_DEV_TOOLS_{key.upper()}\n"
                "The fallback to the old environment variables will be removed in the future!\n",
                category=DeprecationWarning,
            )
            return_config[key] = os.getenv(f"FOUNDRY_LOCAL_{key.upper()}")

    config.update(return_config)
    return_config = type_convert(config)
    return return_config, foundry_dev_tools_directory, project_config_file


class Config(UserDict):
    """Config dict for the global static configs.

    Inherits from the static INITIAL_CONFIG.
    It can be used just like any other dict in python.
    The difference is, that it automatically merges itself with
    the initial config, parsed from config files and env variables.
    If you change a value in this dict, it will override the values
    in the initial config, but the initial config will stay untouched.

    If you set a value to `None` it will not return `None`,
    but behave like the key doesn't exist.

    """

    def __init__(self, *args, **kwargs):
        """The constructor is the same as for a dict.

        The only difference is, that the config keys
        get casted to their type.
        """
        super().__init__(*args, **kwargs)

        # convert the dict to their types
        self.data = type_convert(self.data)

    def _combined(self) -> dict:
        ___data = dict(INITIAL_CONFIG)
        ___data.update(self.data)
        __data = dict(___data)
        # if keys are none, they are "deleted"
        for key, value in ___data.items():
            if value is None:
                del __data[key]

        return __data

    def __setitem__(self, key, value):
        # convert value to its type
        if key in TYPES and value is not None:
            value = TYPES[key](value)
        super().__setitem__(key, value)

    def __contains__(self, item):
        return self._combined().__contains__(item)

    def __missing__(self, key):
        return self._combined()[key]

    def __repr__(self):
        return self._combined().__repr__()

    def __iter__(self):
        return iter(self._combined())

    def items(self):
        """ItemsView of config dictionary."""
        return self._combined().items()

    def values(self):
        """ValuesView of config dictionary."""
        return self._combined().values()

    def __len__(self):
        return len(self._combined())

    def __getitem__(self, key):
        if key in self.data and self.data[key] is not None:
            return self.data[key]
        return self.__missing__(key)

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.data[key] = None

    def set(self, key, value):
        """Deprecated: Stores value in config.

        Args:
            key (str): config key
            value (any): config value
        """
        warnings.warn(
            f"Configuration.set(key, value) is deprecated. "
            f"Please use regular dict methods, e.g. Configuration['{key}'] = '...value...'",
            category=DeprecationWarning,
        )
        self[key] = value

    def delete(self, key: str):
        """Deprecated: Deletes configuration entry.

        Args:
            key (str): config key
        """
        warnings.warn(
            f"Configuration.delete('{key}') is deprecated. "
            f"Please use regular dict methods, e.g. del Configuration['{key}']",
            category=DeprecationWarning,
        )
        del self[key]

    def _get_token_from_token_provider(self):
        """Tries to get a token from a token provider."""
        for tp in TOKEN_PROVIDERS:
            if token := tp().get_token():
                return token
        return None

    def get_config(self, overwrite_config: "dict | None" = None):
        """Returns the Foundry DevTools config.

        Merges the overwrite config from :py:class:`Config` with `overwrite_config` and returns a dict.
        If needed, jwt is fetched from a token provider.

        This method gets used by :py:class:`~foundry_dev_tools.foundry_api_client.FoundryRestClient`,
        :py:class:`~foundry_dev_tools.cached_foundry_client.CachedFoundryClient`
        and :py:class:`~foundry_dev_tools.foundry_api_client.FoundrySqlClient`,
        otherwise their internal config would change, if you changed the global configuration.


        Args:
            overwrite_config (dict | None): This is an overwrite config only for the returned dict
                              It will get applied above the overwrite config in `self`.

        Returns:
            dict:
        """
        cnf = dict(self)
        if overwrite_config:
            cnf.update(overwrite_config)

        if "jwt" not in cnf:  # noqa: SIM102
            if (
                (token := self._get_token_from_token_provider())
                and "enable_runtime_token_providers" in cnf
                and cnf["enable_runtime_token_providers"]
            ):
                cnf["jwt"] = token

        if "foundry_url" not in cnf:
            raise ValueError(
                "Please add your foundry url to the  config. (e.g. foundry_url=https://foundry.example.com)"
            )

        if ("jwt" not in cnf) and ("client_id" not in cnf):
            raise ValueError(
                "Please provide at least one of: \n"
                "foundry token (config key: 'jwt') or "
                "Foundry Third Party Application client_id (config key: 'client_id') \n"
                "in configuration."
            )

        return cnf


(
    INITIAL_CONFIG,
    FOUNDRY_DEV_TOOLS_DIRECTORY,
    FOUNDRY_DEV_TOOLS_PROJECT_CONFIG_FILE,
) = initial_config()
Configuration = Config()
__all__ = ["INITIAL_CONFIG", "FOUNDRY_DEV_TOOLS_DIRECTORY", "Configuration"]
