import configparser
import difflib
import io
import json
import platform
import sys
from pathlib import Path

import click
import requests
from rich import print as rprint
from rich.markdown import Markdown

from foundry_dev_tools import Configuration, FoundryRestClient
from foundry_dev_tools.foundry_api_client import _get_auth_token


@click.group("s3")
def s3_cli():
    """CLI for the s3 compatible foundry datasets API."""
    pass


@click.command("init")
def s3_cli_init():
    """Create a aws profile in the aws config, which uses foundry devtools for authentication."""
    aws_config_file = Path("~/.aws/config").expanduser()
    cp = configparser.ConfigParser()
    if not aws_config_file.exists():
        aws_config_file.parent.mkdir(parents=True, exist_ok=True)
        previous_config = ""
    else:
        previous_config = aws_config_file.read_text()
        cp.read_string(previous_config)
        if "profile foundry" in cp.sections():
            if not click.confirm(
                "This will remove your current aws profile called 'foundry' and replace it with our credential helper.\n"
                "Do you want to continue?"
            ):
                sys.exit(1)
            cp.remove_section("profile foundry")
    credential_process = (
        'cmd /C "fdt s3 auth 2>CON"'
        if platform.system() == "Windows"
        else "sh -c 'fdt s3 auth 2>/dev/tty'"
    )
    cp.read_dict(
        {
            "profile foundry": {
                "credential_process": credential_process,
                "region": "foundry",
                "endpoint_url": f"{Configuration['foundry_url']}/io/s3",
            }
        }
    )
    buf = io.StringIO()
    cp.write(buf)
    buf.seek(0)
    new_config = buf.read()
    with aws_config_file.open("w+") as aws_config_fd:
        aws_config_fd.write(new_config)

    diff = "\n".join(
        difflib.unified_diff(
            previous_config.splitlines(),
            new_config.splitlines(),
            fromfile="old config",
            tofile="new config",
        )
    )
    if diff:
        rprint(
            Markdown(
                f"""## Changes to your config:
```diff
{diff}```"""
            )
        )


@click.command("auth")
def s3_cli_auth():
    """Used by the aws cli for authentication."""
    # print everything that happens when initalizing i.e. oath input() prompt
    # to stderr, which gets redirected to the console through the hacks in the profile config
    orig = sys.stdout
    sys.stdout = sys.stderr
    fc = FoundryRestClient()
    sys.stdout = orig
    fc._headers()
    t = requests.post(
        f"{fc._api_base}/io/s3",
        params={
            "Action": "AssumeRoleWithWebIdentity",
            "WebIdentityToken": _get_auth_token(fc._config),
        },
    ).text

    print(
        json.dumps(
            {
                "Version": 1,
                "AccessKeyId": t[
                    t.find("<AccessKeyId>")
                    + len("<AccessKeyId>") : t.rfind("</AccessKeyId>")
                ],
                "SecretAccessKey": t[
                    t.find("<SecretAccessKey>")
                    + len("<SecretAccessKey>") : t.rfind("</SecretAccessKey>")
                ],
                "SessionToken": t[
                    t.find("<SessionToken>")
                    + len("<SessionToken>") : t.rfind("</SessionToken>")
                ],
                "Expiration": t[
                    t.find("<Expiration>")
                    + len("<Expiration>") : t.rfind("</Expiration>")
                ],
            }
        )
    )


s3_cli.add_command(s3_cli_init)
s3_cli.add_command(s3_cli_auth)
