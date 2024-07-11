"""A git credential helper.

This credential helper does authenticate you to the Foundry Git server with the token from the Foundry DevTools config.
This allows you to use e.g. OAuth and removes the need to clone repositories with the token in the URL.
If you still include the token in the clone URL, git will prefer these over using this credential helper and will store your credentials in the `.git` directory of the repository.

Run the command `git-credential-foundry` in the command line and it will output the configuration you need to add to your git config. (You'll need a valid Foundry DevTools credentials configuration.)

Example output:

.. code-block:: zsh

 To use this helper, add the following to your git config:

 [credential "https://example.com"]
   helper =
   helper = "/path/to/bin/git-credential-foundry"

 Or run the following commands:
 git config --global credential."https://example.com".helper ""
 git config --global --add credential."https://example.com".helper "/path/to/bin/git-credential-foundry"

But below is the generic way to use it (replace the URL with your Foundry stack URL), which assumes that git-credential-foundry is in your PATH.

**Generic Instructions**:

Can be used by adding the following to your git config:

.. code-block:: ini

 [credential "https://your-stack.palantirfoundry.com"]
       helper =
       helper = foundry

(Git automatically adds git-credential- in front of the helper name, that's why we only add "foundry" here)

Or executing the commands:

.. code-block:: zsh

 git config --global credential."https://your-stack.palantirfoundry.com".helper ""
 git config --global --add credential."https://your-stack.palantirfoundry.com".helper "foundry"

"""  # noqa: E501

from __future__ import annotations

import sys
from urllib.parse import urlparse

from foundry_dev_tools.config.config import (
    Host,
    get_config_dict,
    parse_credentials_config,
)


def _get_host(git_config: dict[str, str]) -> Host:
    """Gets the :py:class:`Host` from either an URL or a domain/protocol combo."""
    if (host := git_config.get("host")) and (protocol := git_config.get("protocol")):
        return Host(domain=host, scheme=protocol)
    if url := git_config.get("url"):
        parsed = urlparse(url)
        if host := parsed.netloc:
            return Host(domain=host, scheme=protocol) if (protocol := parsed.scheme) else Host(domain=host)
    sys.exit(1)


def _helper():
    """Git credential helper."""
    if len(sys.argv) > 1 and sys.argv[1] != "-h" and sys.argv[1] != "--help":
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
            host = _get_host(git_config)
            if cred.host == host:
                # foundry accepts every username
                print("username=fdt")  # noqa: T201
                print(f"password={cred.token}")  # noqa: T201

                sys.exit(1)
    else:
        from rich.console import Console
        from rich.markdown import Markdown

        c = Console(markup=True)

        c.print("To use this helper, add the following to your git config:")
        conf = get_config_dict()
        cred = parse_credentials_config(conf)

        c.print(
            Markdown(
                f"""
```ini
[credential "{cred.host.url}"]
  helper =
  helper = "{sys.argv[0]}"
```

Or run the following commands:
```zsh
git config --global credential."{cred.host.url}".helper ""
git config --global --add credential."{cred.host.url}".helper "{sys.argv[0]}"
```
              """
            )
        )
        sys.exit(0)
