"""Build command and its utility functions."""
import ast
import codecs
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from numbers import Number
from pathlib import Path
from typing import List
from urllib.parse import quote_plus, urlparse

import click
import inquirer
from click import UsageError
from rich import print as rprint
from rich.logging import RichHandler
from rich.markup import escape
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect
from websockets.typing import Subprotocol

from foundry_dev_tools import Configuration, FoundryRestClient
from foundry_dev_tools.utils.misc import TailHelper, print_horizontal_line
from foundry_dev_tools.utils.repo import get_repo

log = logging.getLogger("fdt_build")
log.setLevel(logging.DEBUG)
rh = RichHandler(logging.DEBUG, markup=True)
rh.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))
log.addHandler(rh)


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
                    value if isinstance(value, Number) else escape(str(value))
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


TRANSFORM_DECORATORS = [
    "lightweight",
    "transform",
    "transform_df",
    "transform_pandas",
    "transform_polars",
]


def is_transform_file(transform_file: Path) -> bool:
    """Check if file is a transform file.

    Conditions are that it must be a file (obviously)
    the name must end in ".py"
    and the file must contain a transform decorator.
    """
    if not transform_file.is_file():
        return False

    if not transform_file.name.endswith(".py"):
        return False

    with transform_file.open("r") as tf:
        parse = ast.parse(tf.read())
        for node in ast.walk(parse):
            if isinstance(node, ast.FunctionDef) and any(
                decorator.func.id in TRANSFORM_DECORATORS
                for decorator in node.decorator_list
                if hasattr(decorator, "func")
            ):
                return True

    return False


def tail_job_log(job_id: str, jwt: str):
    """Tails the job log.

    This method uses
    """
    MAX_ATTEMPTS = 30
    connection_attempts = 0
    uri = f"wss://{urlparse(Configuration['foundry_url']).hostname}/spark-reporter/ws/logs/driver/{job_id}"
    while connection_attempts < MAX_ATTEMPTS:
        try:
            with connect(uri, subprotocols=[Subprotocol(f"Bearer-{jwt}")]) as websocket:
                while log_message := websocket.recv():
                    try:
                        if isinstance(log_message, bytes):
                            log.handle(create_log_record(log_message.decode("UTF-8")))
                        else:
                            log.handle(create_log_record(log_message))
                    except Exception as e:
                        log.error(
                            "fdt build >>> This shouldn't happen, "
                            f"but while parsing the log message this error occured: {e}\n"
                        )
                        log.exception("The traceback:")
                        log.info(
                            "fdt build >>> Will output the log message in plain:\n"
                        )
                        log.info(escape(str(log_message)))
        except ConnectionClosed as cce:
            if cce.code == 1000:
                rprint("Spark Job Completed.")
                break
            if cce.code == 1011 and "connection with spark module failed" in cce.reason:
                rprint("Spark Job Completed Already, too late to tail logs.")
                break

            connection_attempts += 1
            rprint(
                f"Waiting for Spark Driver Logs. Attempt {connection_attempts}/{MAX_ATTEMPTS}"
            )
            time.sleep(2)


def get_transform_files(git_dir: Path) -> List[str]:
    """Get transform files.

    Gets the transform files edited in the last commit.

    Args:
        git_dir (Path): path to git directory

    Returns:
        list[str]: paths to transform files

    Raises:
        UsageError: if there are no transform files in the last commit.
    """
    diff_files = (
        subprocess.check_output(
            ["git", "log", "-1", "--name-only", "--pretty="], cwd=git_dir
        )
        .decode("ascii")
        .splitlines(False)
    )
    t_files = []

    for f in diff_files:
        if is_transform_file(git_dir.joinpath(f)):
            t_files.append(f)
    if len(t_files) == 0:
        raise UsageError("No transform files in the last commit.")
    return t_files


def _find_rid(all_jobs: dict, name: str) -> str:
    return next(job["rid"] for job in all_jobs if job["name"] == name)


def _get_logs(all_job_logs: dict, rid: str) -> "list[str] | None":
    logs_by_step = all_job_logs[rid]["logsByStep"]
    if len(logs_by_step) > 0 and (logs := logs_by_step[0].get("logs")):
        return logs.splitlines(False)
    return None


def _build_url_message(build_id: str):
    return (
        "Link to the foundry build: "
        f"{Configuration['foundry_url']}/workspace/data-integration/job-tracker/builds/{build_id}"
    )


def _get_started_build(req: dict, build_rid: str) -> "str | None":
    return (
        req.get("allJobStatusReports", {})
        .get(build_rid, {})
        .get("jobCustomMetadata", {})
        .get("startedBuildIds", [None])[0]
    )


@click.command("build")
@click.option(
    "-t",
    "--transform",
    help="The transform python file path e.g. transforms-python/src/myproject/datasets/transform1.py\n"
    "If not provided you can choose (one of) the transform(s) edited in the last commit.",
    # TODO multiple=True,
)
def build_cli(transform):  # noqa: PLR0915
    """Command to start a build and tail the logs.

    This command can be run with `fdt build`

    Args:
        transform (str): the transform file to execute
    """
    client = FoundryRestClient()
    repo, ref_name, commit_hash, git_dir = get_repo()
    if transform:
        if is_transform_file(Path.cwd().joinpath(transform)):
            transform_file = transform
        else:
            raise UsageError(f"{transform} is not a transform file.")
    else:  # user didn't supply files directly, get the files via inquirer from the last commits
        transform_file = inquirer.prompt(
            [
                inquirer.List(
                    "transform_file",
                    message="Select the transform you want to run.",
                    choices=get_transform_files(git_dir),
                )
            ]
        )["transform_file"]
    if not transform_file:
        raise UsageError("No transform file provided.")

    def _req():
        return client.start_checks_and_build(
            repository_id=repo,
            ref_name=ref_name,
            commit_hash=commit_hash,
            file_paths=[transform_file],
        )

    def _finish(response: dict, exit_code: int, retries: int = 0):
        if response["buildStatus"] == "RUNNING" and retries < 30:
            time.sleep(2)
            retries += 1
            return _finish(_req(), exit_code, retries)
        build_rid = _find_rid(response["allJobs"], name="Build initialization")
        branch = quote_plus(ref_name[11:])
        if build_id := _get_started_build(response, build_rid):
            print_horizontal_line(print_handler=rprint)
            rprint(f"Build status: {response['buildStatus']}")
            rprint()
            rprint(_build_url_message(build_id))
            rprint()
            job_report = client.get_build(build_id)
            jobRids = job_report["jobRids"]
            if len(jobRids) > 0:
                rprint("[bold]The resulting dataset(s):[/bold]")
                for job_rid in jobRids:
                    job_report = client.get_job_report(job_rid)
                    for rid, _ in job_report["jobResults"].items():
                        rprint(
                            f"{Configuration['foundry_url']}/workspace/data-integration/dataset/preview/"
                            + rid
                            + "/"
                            + branch
                            if branch
                            else rid
                        )

        raise SystemExit(exit_code)

    first_req = _req()
    checks_rid = _find_rid(first_req["allJobs"], name="Checks")
    build_rid = _find_rid(first_req["allJobs"], name="Build initialization")

    def escape_rprint(s: str):
        return rprint(escape(s))

    checks_tailer = TailHelper(escape_rprint)
    build_tailer = TailHelper(escape_rprint)
    if first_req["buildStatus"] in ("SUCCEEDED", "FAILED"):
        rprint(
            f"The checks and build are already finished and {first_req['buildStatus'].lower()}."
        )
        if (
            inquirer.prompt(
                [
                    inquirer.List(
                        "print_logs",
                        "Print the logs anyways?",
                        choices=["Yes", "No"],
                        default="No",
                    )
                ]
            )["print_logs"]
            == "Yes"
        ):
            checks_tailer.tail(_get_logs(first_req["allJobLogs"], checks_rid))
            build_tailer.tail(_get_logs(first_req["allJobLogs"], build_rid))
        _finish(first_req, 0)
    while True:
        response_json = _req()
        all_job_logs = response_json["allJobLogs"]
        checks_tailer.tail(_get_logs(all_job_logs, checks_rid))
        build_tailer.tail(_get_logs(all_job_logs, build_rid))
        if build_id := (
            response_json.get("allJobStatusReports", {})
            .get(build_rid, {})
            .get("jobCustomMetadata", {})
            .get("startedBuildIds", [None])[0]
        ):
            print_horizontal_line(print_handler=rprint)
            rprint(_build_url_message(build_id))
            print_horizontal_line(print_handler=rprint)
            tail_job_log(
                job_id=client.get_build(build_id)["jobRids"][0].replace(
                    "ri.foundry.main.job.", ""
                ),
                jwt=client._headers()["Authorization"].replace("Bearer ", ""),
            )
            _finish(response_json, 0)
            break
        if any(
            job_stat_rep.get("jobStatus", {}) == "FAILED"
            for job_stat_rep in response_json.get("allJobStatusReports", {}).values()
        ):
            _finish(response_json, 1)
        time.sleep(2)
