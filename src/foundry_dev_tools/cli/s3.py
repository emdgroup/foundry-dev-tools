"""Provides authentication for the aws cli to the foundry s3 compatible API."""

from __future__ import annotations

import configparser
import datetime
import difflib
import io
import os
import platform
import shutil
import sys
from pathlib import Path

import click
from rich import print as rprint
from rich.markdown import Markdown

from foundry_dev_tools import FoundryContext


@click.group("s3")
def s3_cli():
    """CLI for the s3 compatible foundry datasets API."""


@click.command("init")
def s3_cli_init():
    """Create a aws profile in the aws config, which uses foundry devtools for authentication.

    Can be later used via `aws s3 --profile foundry ls s3://....`.
    """
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
                "This will remove your current aws profile "
                "called 'foundry' and replace it with our credential helper.\n"
                "Do you want to continue?",
            ):
                sys.exit(1)
            cp.remove_section("profile foundry")
    credential_process = (
        f'cmd /C "{sys.executable} -m foundry_dev_tools.cli.main s3 auth 2>CON"'
        if platform.system() == "Windows"
        else f"sh -c '{sys.executable} -m foundry_dev_tools.cli.main s3 auth 2>/dev/tty'"
    )
    ctx = FoundryContext()
    cp.read_dict(
        {
            "profile foundry": {
                "credential_process": credential_process,
                "region": "foundry",
                "endpoint_url": f"{ctx.host.url}/io/s3",
            },
        },
    )
    buf = io.StringIO()
    cp.write(buf)
    buf.seek(0)
    new_config = buf.read()
    diff = "\n".join(
        difflib.unified_diff(
            previous_config.splitlines(),
            new_config.splitlines(),
            fromfile="old config",
            tofile="new config",
        ),
    )
    if diff:
        rprint(
            Markdown(
                f"""## Changes to your config:
```diff
{diff}```""",
            ),
        )
        if previous_config:
            backup_path = aws_config_file.with_name(
                "config.bak." + datetime.datetime.now().strftime("%Y%m%d%H%M%S"),  # noqa: DTZ005
            )

            shutil.copy(
                aws_config_file,
                backup_path,
            )
            rprint(
                f"Created backup of previous config -> {os.fspath(backup_path.absolute())}",
            )

    with aws_config_file.open("w+") as aws_config_fd:
        aws_config_fd.write(new_config)


@click.command("auth")
def s3_cli_auth():
    """Used by the aws cli for authentication."""
    # print everything that happens when initalizing i.e. oath input() prompt
    # to stderr, which gets redirected to the console through the workarounds in the profile config
    # https://github.com/aws/aws-sdk/issues/358
    orig = sys.stdout
    sys.stdout = sys.stderr
    ctx = FoundryContext()
    creds = ctx.s3.get_credentials()
    sys.stdout = orig
    click.echo(
        "{"
        f"\"Version\": 1,\"AccessKeyId\":\"{creds['access_key']}\",\"SecretAccessKey\":\"{creds['secret_key']}\""
        f",\"SessionToken\":\"{creds['token']}\",\"Expiration\":\"{creds['expiry_time']}\""
        "}",
    )


s3_cli.add_command(s3_cli_init)
s3_cli.add_command(s3_cli_auth)
