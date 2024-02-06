"""A git credential helper.

Can be used by adding:
[credential "https://your-stack.palantirfoundry.com"]
  helper =
  helper = foundry

to your git config.
"""

from __future__ import annotations

import sys
from urllib.parse import urlparse

from foundry_dev_tools.config.config import (
    Host,
    get_config_dict,
    parse_credentials_config,
)


def get_host(git_config: dict[str, str]) -> Host:
    """Gets the :py:class:`Host` from either an URL or a domain/protocol combo."""
    if (host := git_config.get("host")) and (protocol := git_config.get("protocol")):
        return Host(domain=host, scheme=protocol)
    if url := git_config.get("url"):
        parsed = urlparse(url)
        if host := parsed.netloc:
            return Host(domain=host, scheme=protocol) if (protocol := parsed.scheme) else Host(domain=host)
    sys.exit(1)


def helper():
    """Git credential helper."""
    if len(sys.argv) > 1:
        # get the tokenprovider
        conf = get_config_dict()
        cred = parse_credentials_config(conf)

        # dict for the key value pairs we get from git(1)
        git_config = {}
        while True:
            try:
                if line := input():
                    # if it's not a valid line, just ignore it
                    if len(spl := line.split("=", maxsplit=1)) > 1:
                        git_config[spl[0]] = spl[1]
                else:
                    # if empty, break and process what we got
                    break
            except EOFError:
                break
        # we only implement the get 'operation'
        # the others are erase or store, which are not really applicable
        # for foundry devtools
        if sys.argv[1] == "get":
            host = get_host(git_config)
            if cred.host == host:
                # foundry accepts every username
                print("username=fdt")  # noqa: T201
                print(f"password={cred.token}")  # noqa: T201

                sys.exit(1)
    else:
        sys.exit(1)
