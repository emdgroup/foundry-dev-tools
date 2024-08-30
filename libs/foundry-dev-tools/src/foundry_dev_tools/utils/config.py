"""Utils functions for the configuration.

The config directories/files.
The environment config.
Function to merge dictionaries.
And the function that checks if the kwargs for a __init__ are correct,
currently used for the :py:class:`foundry_dev_tools.config.config.Config`
and the token providers :py:mod:`foundry_dev_tools.config.token_provider`.
"""

from __future__ import annotations

import inspect
import os
import warnings
from functools import cache
from importlib.metadata import entry_points
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any

import platformdirs

from foundry_dev_tools.errors.config import FoundryConfigError
from foundry_dev_tools.utils.repo import git_toplevel_dir

if TYPE_CHECKING:
    from foundry_dev_tools.clients.api_client import APIClient
    from foundry_dev_tools.config.token_provider import TokenProvider

ENVIRONMENT_VARIABLE_PREFIX = "FDT_"
CFG_FILE_NAME = "config.toml"
PROJECT_CFG_FILE_NAME = ".foundry_dev_tools.toml"


@cache
def _platformdirs() -> platformdirs.PlatformDirsABC:
    return platformdirs.PlatformDirs("foundry-dev-tools")


@cache
def cfg_files(use_project_config: bool = True) -> dict[Path, None]:
    """Returns all the possible configuration file paths (cached).

    Returns a dict with the paths as keys.
    Sets are not ordered in python but dicts are since 3.8.

    The first one is the system-wide config.
    """
    ret = {site_cfg_file(): None, **user_cfg_files()}
    if use_project_config and (project_cfg := find_project_config_file()):
        ret[project_cfg] = None
    return ret


@cache
def user_cfg_files() -> dict[Path, None]:
    """Returns all possible user configuration files.

    Returns a dict with the paths as keys.
    Sets are not ordered in python but dicts are since 3.8.
    """
    return {
        Path.home().joinpath(".foundry-dev-tools", CFG_FILE_NAME): None,
        Path.home().joinpath(".config", "foundry-dev-tools", CFG_FILE_NAME): None,
        _platformdirs().user_config_path.joinpath(CFG_FILE_NAME): None,
    }


@cache
def site_cfg_file() -> Path:
    """Returns the site_config_path from :py:mod:`platformdirs`."""
    return _platformdirs().site_config_path.joinpath(CFG_FILE_NAME)


@cache
def user_cache() -> Path:
    """Returns the cached directory for the user (cached)."""
    return _platformdirs().user_cache_path


def merge_dicts(a: dict, b: dict) -> dict:
    """Merges two nested dicts."""
    if isinstance(a, dict) and isinstance(b, dict):
        for k in b:
            if k in a:
                a[k] = merge_dicts(a[k], b[k])
            else:
                a[k] = b[k]
        return a
    return b if b is not None else a


def path_from_path_or_str(path: Path | PathLike[str] | str) -> Path:
    """Returns the same variable if instance of Path otherwise create a Path."""
    if isinstance(path, Path):
        return path
    return Path(path)


def find_project_config_file(
    project_directory: Path | None = None, use_git: bool = False, check_caller_file: bool = True
) -> Path | None:
    """Get the project config file in a git repo.

    Args:
        project_directory: the path to a (sub)directory of a git repo,
            otherwise checks caller filename directory and working directory
        use_git: passed to :py:meth:git_toplevel_dir
        check_caller_file: if the directory of the current executed python script should be checked

    Returns:
        Path | None: Path to the project config or None if no file was found
    """
    if project_directory is None:
        git_directory = None
        if check_caller_file:
            git_directory = git_toplevel_dir(Path(inspect.stack()[-1].filename).parent, use_git=use_git)
        if git_directory is None:
            git_directory = git_toplevel_dir(Path.cwd(), use_git=use_git)
    else:
        git_directory = git_toplevel_dir(project_directory, use_git=use_git)
    if not git_directory:
        cwd_config = Path.cwd().joinpath(PROJECT_CFG_FILE_NAME)
        # return config in current directory only if it exists
        if cwd_config.exists():
            return cwd_config
        return None

    return git_directory.joinpath(PROJECT_CFG_FILE_NAME)


def get_environment_variable_config() -> dict:
    """Returns the `FDT_*` environment variables as a config dict."""
    env_dict = {}
    for name, value in os.environ.items():
        if name.startswith(ENVIRONMENT_VARIABLE_PREFIX):
            parts = name[len(ENVIRONMENT_VARIABLE_PREFIX) :].lower().split("__")
            if len(parts) <= 1:
                if parts[0] == "profile":  # profile is a special case
                    if len(value) == 0:
                        value = None  # allow erasing via env variable  # noqa: PLW2901
                else:
                    warnings.warn(f"{name} is not a valid Foundry DevTools configuration environment variable.")
                    continue
            cfg_parts = parts[:-1]
            o = env_dict
            for cfg_part in cfg_parts:
                if cfg_part not in o or not isinstance(o[cfg_part], dict):
                    o[cfg_part] = {}
                o = o[cfg_part]
            o[parts[-1]] = _try_convert_to_bool(value)
    from foundry_dev_tools.utils.compat import get_v1_environment_variables  # otherwise circular import

    v1_env_dict = get_v1_environment_variables()
    return merge_dicts(v1_env_dict, env_dict)


def _try_convert_to_bool(value: Any) -> Any:  # noqa: ANN401
    if isinstance(value, str) and value.lower() == "false":
        return False
    elif isinstance(value, str) and value.lower() == "true":  # noqa: RET505
        return True
    return value


@cache
def entry_point_fdt_token_provider() -> dict[str, type[TokenProvider]]:
    """Returns the token provider implementations registered via entry points."""
    return {ep.name: ep.load() for ep in entry_points(group="fdt_token_provider")}


@cache
def entry_point_fdt_api_client() -> dict[str, type[APIClient]]:
    """Returns the API clients registered via entry points."""
    return {ep.name: ep.load() for ep in entry_points(group="fdt_api_client")}


def check_init(
    init_class: type,
    config_path: str,
    kwargs: dict[str, Any],
) -> dict:
    """Checks if the supplied kwargs from the config dict are valid for the class to be instantiated.

    If a kwargs is of the wrong type it will try to be cast to the correct type.
    If this fails a :py:class:`FoundryConfigError` will be raised.
    If it succeeds it will still show a warning, that the value has been cast.

    Args:
        init_class: The class that will be instantiated with :py:attr:`kwargs`
        config_path: For the warnings/errors to show which config setting is invalid
            config.path + "." + invalid_kwarg_name
        kwargs: the kwargs to check

    Returns:
        valid_kwargs: dict
        the kwargs that (were cast to) work for the instantiated class

    """
    valid_kwargs = {}
    sig = inspect.signature(init_class)
    parms = sig.parameters
    for name, parameter in parms.items():
        conf_name = f"{config_path}.{name}"
        if name in kwargs and parameter.annotation is not parameter.empty and not isinstance(parameter.annotation, str):
            if not isinstance(kwargs[name], parameter.annotation):
                try:
                    valid_kwargs[name] = parameter.annotation(kwargs[name])
                    warnings.warn(
                        f"{conf_name} was type {type(kwargs[name])!s} but has been cast to"
                        f" {parameter.annotation!s}, this was needed to instantiate {init_class!s}.",
                    )
                    del kwargs[name]
                    continue
                except Exception as e:
                    msg = (
                        f"To initialize {init_class!s}, the config option {conf_name} needs to be of type"
                        f" {parameter.annotation!s}, but it is type {type(kwargs[name])!s}"
                    )
                    raise FoundryConfigError(
                        msg,
                    ) from e
            valid_kwargs[name] = kwargs[name]
            del kwargs[name]
        elif name in kwargs:
            valid_kwargs[name] = kwargs[name]
            del kwargs[name]
        elif (
            parameter.kind is not parameter.VAR_KEYWORD
            and parameter.kind is not parameter.VAR_POSITIONAL
            and parameter.default is parameter.empty
            and parameter.default is parameter.empty
        ):
            msg = f"{conf_name} is missing to create {init_class!s}."
            raise FoundryConfigError(msg)
    for name in kwargs:
        conf_name = f"{config_path}.{name}"
        warnings.warn(f"{conf_name} is not a valid config option for {init_class!s}")
    return valid_kwargs
