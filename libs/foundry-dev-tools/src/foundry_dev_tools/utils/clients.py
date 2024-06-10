"""Util functions for the API clients."""

from __future__ import annotations

from functools import cache


@cache
def build_api_url(url: str, api_name: str, api_path: str) -> str:
    """Cached function for building the api URLs."""
    return url + "/" + api_name + "/api/" + api_path
