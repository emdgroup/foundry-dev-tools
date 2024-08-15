"""This module contains utility functions for compatibility between the v1 and v2 config."""

from __future__ import annotations

import os
import warnings
from urllib.parse import urlparse

from foundry_dev_tools.config.config import TokenProvider, get_config_dict, parse_credentials_config
from foundry_dev_tools.config.config_types import DEFAULT_SCHEME


def v1_to_v2_config_dict(config: dict, env: bool = True, get_config: bool = True) -> dict:  # noqa: C901
    """Converts the `config` argument to the new v2 config.

    Args:
        config: the v1 dictionary to convert
        env: whether to include the environment variables in the conversion
        get_config: wether to get the config from files and merge it
    """
    _config = (get_config_dict(env=env) if get_config else None) or {}
    jwt, client_id, client_secret, foundry_url, grant_type, scopes = (
        config.pop("jwt", False),
        config.pop("client_id", False),
        config.pop("client_secret", False),
        config.pop("foundry_url", None),
        config.pop("grant_type", False),
        config.pop("scopes", False),
    )
    domain = None
    scheme = None
    if jwt or foundry_url or client_id is not False:
        _config.setdefault("credentials", {})

    if foundry_url:
        parsed_foundry_url = urlparse(foundry_url)
        if not parsed_foundry_url.scheme:
            msg = f"{foundry_url} is not a valid URL <scheme>://<domain>"
            raise AttributeError(msg)
        domain = parsed_foundry_url.netloc
        scheme = parsed_foundry_url.scheme
        _config["credentials"]["domain"] = domain
        if scheme:
            _config["credentials"]["scheme"] = scheme

    if "credentials" in _config and (scheme := _config["credentials"].get("scheme")) and scheme == DEFAULT_SCHEME:
        del _config["credentials"]["scheme"]

    if jwt:
        _config["credentials"]["jwt"] = jwt
    elif client_id is not False:
        _config["credentials"].setdefault("oauth", {})
        _config["credentials"]["oauth"]["client_id"] = client_id

    if "oauth" in _config.get("credentials", {}):
        if client_secret is not False:
            _config["credentials"]["oauth"]["client_secret"] = client_secret
        if grant_type is not False:
            _config["credentials"]["oauth"]["grant_type"] = grant_type
        if scopes is not False:
            _config["credentials"]["oauth"]["scopes"] = scopes
    return _config


def v1_to_v2_config(config: dict) -> tuple[TokenProvider, dict]:
    """Converts a version 1 configuration to a version 2 configuration.

    Args:
        config (dict): The version 1 configuration to be converted.

    Returns:
        tuple[TokenProvider, dict]: A tuple containing the TokenProvider and the converted configuration.

    Raises:
        AttributeError: If the TokenProvider cannot be created due to missing jwt or client_id(/client_secret).
    """
    _config = v1_to_v2_config_dict(config)
    tp = parse_credentials_config(_config)

    if c := _config.get("config"):
        c.update(config)
        _config["config"] = c
    else:
        _config["config"] = config
    return tp, _config


def get_v1_environment_variables() -> dict:
    """Parses the v1 environment variables and converts them to a v2 dictionary.

    Returns:
        dict: A dictionary containing the processed v1 environment variables.
    """
    v1_env_config = {}
    for k, v in os.environ.items():
        if k.startswith("FOUNDRY_DEV_TOOLS_"):
            v1_env_config[k.removeprefix("FOUNDRY_DEV_TOOLS_").lower()] = v
    if len(v1_env_config) > 0:
        warnings.warn(
            "The v1 environment variables are deprecated, please use the v2 environment variables instead",
            DeprecationWarning,
        )
    return v1_to_v2_config_dict(v1_env_config, env=False, get_config=False)
