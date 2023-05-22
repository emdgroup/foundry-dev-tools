"""Build command and its utility functions."""
import asyncio
import codecs
import json
import logging
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List

import click
import inquirer
import websockets
from click import UsageError
from rich import print as rprint
from rich.logging import RichHandler
from rich.markup import escape

from foundry_dev_tools import Configuration, FoundryRestClient
from foundry_dev_tools.config import _traverse_to_git_project_top_level_dir


def create_log_record(log_message: str) -> logging.LogRecord:
    """Parses the log message from the spark logs.

    If the log message is a json object, we try to convert it into
    a logrecord which should be relatively similar to the original
    logrecord that was emitted in pyspark.

    Args:
        log_message (str): the log message from spark websocket

    Returns:
        logging.LogRecord
    """
    if log_message.startswith("{") and log_message.endswith("}"):
        log_data = json.loads(log_message)
        if {
            "level",
            "origin",
            "message",
            "time",
            "unsafeParams",
        }.issubset(set(log_data.keys())):
            log_level = getattr(logging, log_data["level"], logging.ERROR)
            if "stacktrace" in log_data:
                stack_info = codecs.decode(
                    log_data["stacktrace"].format(
                        exception_message=log_data["unsafeParams"]["exception_message"]
                    ),
                    "unicode-escape",
                )
            else:
                stack_info = None

            log_record = logging.LogRecord(
                name=log_data["origin"],
                level=log_level,
                pathname=log_data["origin"],
                lineno=0,
                msg=f"[bold]{escape(log_data['message'])}[/bold]",
                args=tuple(
                    value
                    for key, value in log_data["unsafeParams"].items()
                    if key.startswith("param_")
                ),
                exc_info=None,
                func=None,
                sinfo=stack_info,
            )
            if sys.version_info.major == 3 and sys.version_info.minor < 11:
                # https://stackoverflow.com/a/75499881/3652805
                log_record.created = datetime.strptime(
                    log_data["time"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ).timestamp()
            else:
                log_record.created = datetime.fromisoformat(
                    log_data["time"]
                ).timestamp()
            return log_record
    return logging.LogRecord(
        name="",
        level=logging.INFO,
        pathname="spark",
        lineno=0,
        msg=escape(log_message),
        args=(),
        exc_info=None,
        func=None,
        sinfo=None,
    )


def is_transform_file(transform_file: Path) -> bool:
    """Check if file is a transform file.

    Conditions are that it must be a file (obviously)
    the name must end in ".py"
    and the file must contain either @transform|@transform_df
    """
    if not transform_file.is_file():
        return False

    if not transform_file.name.endswith(".py"):
        return False

    with transform_file.open("r") as tf:
        if TRANSFORM_REGEX.search(tf.read()):
            return True

    return False


async def tail_job_log(job_id: str, jwt: str):
    """Tails the job log.

    This method uses
    """
    connection_attempts = 0
    uri = f"wss://{Configuration['foundry_url']}/spark-reporter/ws/logs/driver/{job_id}"
    log = logging.getLogger("fdt_build")
    log.setLevel(logging.DEBUG)
    rh = RichHandler(logging.DEBUG, markup=True)
    rh.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))
    log.addHandler(rh)
    while connection_attempts < 30:
        try:
            async with websockets.connect(
                uri, subprotocols=[f"Bearer-{jwt}"]
            ) as websocket:
                log_message: str
                while log_message := await websocket.recv():
                    try:
                        log.handle(create_log_record(log_message))
                    except Exception as e:
                        print(
                            "fdt build >>> This shouldn't happen, "
                            f"but while parsing the log message this error occured: {e}\n"
                            "fdt build >>> Will output the log message in plain:"
                        )
                        print(log_message)
        except websockets.ConnectionClosed as e:
            if e.code == 1000:
                connection_attempts = 99999  # should break while loop
                rprint("Spark Job Completed.")
            elif e.code == 1011 and "connection with spark module failed" in e.reason:
                connection_attempts = 99999  # should break while loop
                rprint("Spark Job Completed Already, too late to tail logs.")
            elif e.code == 1008 or (
                e.code == 1011
                and e.reason == "Couldn't find Spark module id on job metadata"
            ):
                rprint("Waiting for Spark Driver Logs.")
            connection_attempts += 1
            time.sleep(2)


def get_transform(transforms: "list[str] | None") -> List[str]:
    """Get transform files.

    Either checks if the supplied files are transform files
    or gets the transform files edited in the last commit.

    Returns:
        list[str]: paths to transform files

    Raises:
        UsageError: if a supplied file is not a transform or
            if there are no transform files in the last commit.
    """
    if transforms:
        for t in transforms:
            if not is_transform_file(Path(t)):
                raise UsageError(f"{t} is not a transforms file or does not exist.")
        return transforms

    diff_files = (
        subprocess.check_output(
            [
                "git",
                "diff",
                "--name-only",
                "HEAD~1",
            ]
        )
        .decode("ascii")
        .splitlines(False)
    )
    t_files = []

    for f in diff_files:
        if is_transform_file(Path(f)):
            t_files.append(f)
    if len(t_files) == 0:
        raise UsageError("No transform files in the last commit.")
    return t_files


def get_repo() -> "tuple[str,str,str]":
    """Get the repository RID from the current working directory.

    Returns:
        tuple[str,str,str]: repo_rid,git_ref,git_revision_hash
    """
    if git_dir := _traverse_to_git_project_top_level_dir(Path.cwd()):
        gradle_props = git_dir.joinpath("gradle.properties")
        if gradle_props.is_file():
            try:
                with gradle_props.open("r") as gpf:
                    for line in gpf.readlines():
                        if line.startswith("transformsRepoRid"):
                            return (
                                line.split("=")[1].strip(),
                                get_git_ref(git_dir),
                                get_git_revision_hash(git_dir),
                            )
            except:  # noqa
                pass
            raise UsageError(
                "Can't get repository RID from the gradle.properties file. Malformed file?"
            )
        raise UsageError(
            "There is no gradle.properties file at the top of the git repository, can't get repository RID."
        )
    raise UsageError(
        "If you don't provide a repository RID you need to be in a repository directory"
        " to detect what you want to build."
    )


TRANSFORM_REGEX = re.compile("@transform|@transform_df")


def get_git_ref(git_dir: "Path | None") -> str:
    """Get the branch ref in the supplied git directory.

    Args:
        git_dir (Path | None): a Path to a git directory or None
            if None it will use the current working directory
    """
    return (
        subprocess.check_output(
            [
                "git",
                "symbolic-ref",
                "HEAD",
            ],
            cwd=git_dir,
        )
        .decode("ascii")
        .strip()
    )


def get_git_revision_hash(git_dir: "Path | None") -> str:
    """Get the git revision hash.

    Args:
        git_dir (Path | None): a Path to a git directory or None
            if None it will use the current working directory
    """
    return (
        subprocess.check_output(
            [
                "git",
                "rev-parse",
                "HEAD",
            ],
            cwd=git_dir,
        )
        .decode("ascii")
        .strip()
    )


def find_rid(all_jobs: dict, name: str) -> str:
    """TODO."""
    return [job["rid"] for job in all_jobs if job["name"] == name][0]


def tail_logs(logs: List[str], last_retrieved_state: "int | None") -> int:
    """TODO."""
    if last_retrieved_state is None:
        # If it's the first retrieval, display all logs
        rprint(
            "-----------------------------------------------------------------------------------------------------------"
        )
        rprint("\n".join(logs))
        last_retrieved_state = len(logs)
    else:
        # Append only new lines to the existing logs
        new_logs = logs[last_retrieved_state:]
        if new_logs:
            rprint("\n".join(new_logs))
            last_retrieved_state += len(new_logs)
    return last_retrieved_state


@click.command("build")
@click.option(
    "-t",
    "--transforms",
    help="The transforms python file path e.g. transforms-python/src/myproject/datasets/transform1.py\n"
    "Can be supplied multiple times\n"
    "If not provided you can choose (one of) the transform(s) edited in the last commit.",  # TODO
    multiple=True,
)
def build_cli(transforms):
    """Command to start a build and tail it logs.

    This command can be run with `fdt build`
    """
    client = FoundryRestClient()
    repo, ref_name, commit_hash = get_repo()
    transform_files = get_transform(transforms)

    if not transforms:  # user didn't supply files directly
        transform_files = inquirer.prompt(
            [
                inquirer.Checkbox(
                    "transform_files",
                    message="Select the transforms you want to run.",
                    choices=transform_files,
                )
            ]
        )["transform_files"]
    if not transform_files:
        raise UsageError("No transform files provided.")
    last_retrieved_state = None
    last_retrieved_state_build = None
    last_retrieved_state_build_rid = None
    finished = False
    while not finished:
        response_json = client.start_checks_and_build_for_commit(
            repository_id=repo,
            ref_name=ref_name,
            commit_hash=commit_hash,
            file_paths=transform_files,
        )

        checks_rid = find_rid(response_json["allJobs"], name="Checks")
        build_rid = find_rid(response_json["allJobs"], name="Build initialization")
        maybe_checks_logs = response_json["allJobLogs"][checks_rid]["logsByStep"]
        if (
            len(maybe_checks_logs) > 0
            and "logs" in maybe_checks_logs[0]
            and maybe_checks_logs[0]["logs"] is not None
        ):
            logs = maybe_checks_logs[0]["logs"].splitlines(False)
            last_retrieved_state = tail_logs(logs, last_retrieved_state)
        maybe_build_logs = response_json["allJobLogs"][build_rid]["logsByStep"]
        if len(maybe_build_logs) > 0 and "logs" in maybe_build_logs[0]:
            logs = maybe_build_logs[0]["logs"].splitlines(False)
            last_retrieved_state_build = tail_logs(logs, last_retrieved_state_build)
            maybe_build_id = (
                response_json.get("allJobStatusReports", {})
                .get(build_rid, {})
                .get("jobCustomMetadata", {})
                .get("startedBuildIds", [None])[0]
            )
        else:
            maybe_build_id = None
        if maybe_build_id:
            last_retrieved_state_build_rid = tail_logs(
                [
                    f"Open {Configuration['foundry_url']}/workspace/data-integration/job-tracker/builds/"
                    f"{maybe_build_id} to track Build."
                ],
                last_retrieved_state_build_rid,
            )
            build = client.get_build(maybe_build_id)
            job_rid = build["jobRids"][0]

            asyncio.run(
                tail_job_log(
                    job_id=job_rid.replace("ri.foundry.main.job.", ""),
                    jwt=client._headers()["Authorization"].replace("Bearer ", ""),
                )
            )

            finished = True
        time.sleep(5)
