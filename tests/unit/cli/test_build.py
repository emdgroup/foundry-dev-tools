from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import websockets.exceptions
import websockets.frames
from click import UsageError

from foundry_dev_tools.cli.build import _build_cli, _build_url_message
from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import TracebackType

    import py.path

CHECK_JOB_RID = "ri.jemma.main.job.2d82040d-050b-41ed-9133-838329e3eff2"
BUILD_JOB_RID = "ri.jemma.main.job.7d57542d-98b0-4c13-b064-e9aab3d22e81"
BUILD_LOG_RESPONSE = {
    "build": {
        "rid": "ri.jemma.main.build.fc422ef5-dc95-4594-860c-c0da0911e33e",
        "createdAt": "2000-01-01T00:00:00.000000000Z",
        "completedAt": None,
    },
    "allJobs": [
        {
            "rid": BUILD_JOB_RID,
            "name": "Build initialization",
            "type": "foundry-run-build",
            "userId": "248dffd1-4216-40ae-a195-c1cd7e3ae778",
            "scope": {"type": "universal"},
            "parameters": {
                "buildParameters": {},
                "rids": [],
                "fallbackBranches": [],
                "filePaths": ["transforms-python/src/myproject/datasets/examples.py"],
            },
            "createdAt": "2000-01-01T00:00:00.000000000Z",
            "submittedAt": None,
            "startedAt": None,
            "completedAt": None,
        },
        {
            "rid": CHECK_JOB_RID,
            "name": "Checks",
            "type": "exec",
            "userId": None,
            "scope": None,
            "parameters": {
                "repositoryTarget": {
                    "repositoryRid": "ri.stemma.main.repository.a0b5defa-82d9-4959-bdef-28e02e00cd48",
                    "refName": "refs/heads/dev/branch",
                    "commitHash": "52bdea68c6538acc79eb03bc33292314f97551f4",
                },
            },
            "createdAt": "2000-01-01T00:00:00.000000000Z",
            "submittedAt": "2000-01-01T00:00:00.000000000Z",
            "startedAt": "2000-01-01T00:00:00.000000000Z",
            "completedAt": None,
        },
    ],
    "buildStatus": "RUNNING",
    "allJobLinks": {
        CHECK_JOB_RID: {
            "dependencyJobRids": [],
            "dependentJobRids": [BUILD_JOB_RID],
        },
        BUILD_JOB_RID: {
            "dependencyJobRids": [CHECK_JOB_RID],
            "dependentJobRids": [],
        },
    },
    "allJobLogs": {
        CHECK_JOB_RID: {"logsByStep": []},
        BUILD_JOB_RID: {"logsByStep": []},
    },
    "allJobStatusReports": {
        CHECK_JOB_RID: {
            "jobStatus": "RUNNING",
            "jobCustomMetadata": {},
            "stepStatusReports": [{"name": "foundry-publish", "stepStatus": "RUNNING"}],
        },
        BUILD_JOB_RID: {
            "jobStatus": "WAITING_FOR_DEPENDENCIES",
            "jobCustomMetadata": {},
            "stepStatusReports": [],
        },
    },
    "supersededBy": None,
    "retriggeredBy": None,
}

CHECK_JOB_CUSTOM_METADATA = {
    "filePathToDatasetRids": {
        "transforms-python/src/myproject/datasets/examples.py": [
            "ri.foundry.main.dataset.81d943dd-8b84-46ba-b720-5e227de8bb6a",
        ],
    },
    "publishedJobSpecTypes": ["BUILD2"],
    "desiredUnmarkings": {"unmarkings": []},
    "jobCustomMetadata": {
        "filePathToDatasetRids": {
            "transforms-python/src/myproject/datasets/examples.py": [
                "ri.foundry.main.dataset.81d943dd-8b84-46ba-b720-5e227de8bb6a",
            ],
        },
        "publishedJobSpecTypes": ["BUILD2"],
        "desiredUnmarkings": {"unmarkings": []},
    },
    "buildProfile": {"foundry-publish": "{}"},
    "platformError": False,
    "tags": [],
    "templates": {
        "parent-version": "9.876.5",
        "parent": "transforms",
        "children": "python,sql",
        "python": "1.234.5",
        "sql": "1.234.5",
    },
    "cpuModel": "Dummy CPU 4GHz",
}

CHECK_LOGS = [
    "[/ESCAPE ME]",
    "Checks...",
    "Running 1234",
    "",
    "Downloading something",
    "Unpacking something",
    "",
    "Something Something",
    "Checking...",
    "Checks passed",
]


BUILD_INIT_LOGS = "Starting Builds..."

STARTED_BUILD_RID = "ri.foundry.main.build.0e7ca16b-49f1-4b2d-953e-21b18bc7c560"

SPARK_LOGS = [
    "Hello!",
    "[/ESCAPE ME]",
    json.dumps(
        {
            "level": "ERROR",
            "origin": "origin",
            "message": "%s happened! %s",
            "time": "2000-01-01T00:00:00.000000Z",
            "stacktrace": "Stacktrace:\nSomething something\nError {exception_message} here:\nline 20",
            "unsafeParams": {
                "exception_message": "Some Error",
                "param_0": "Something",
                "param_1": "[/ESCAPE ME TOO!]",
            },
        },
    ),
    json.dumps(
        {
            "level": "INFO",
            "origin": "origin",
            "message": "Output generated: %s",
            "time": "2000-01-01T00:00:00.000000Z",
            "unsafeParams": {
                "param_0": {"key": {"value": [1], "markup": "[/ESCAPE ME]"}},
            },
        },
    ),
    json.dumps(
        {
            "level": "INFO",
            "origin": "origin",
            "message": "Integers: %d %i,"
            " Octal: %o,"
            " Hex: %x %X,"
            " Floats: %e %E %f %F %g %G,"
            " Char: %c,"
            " Representations: %r %s %a,"
            " Literal %%",
            "time": "2000-01-01T00:00:00.000000Z",
            "unsafeParams": {
                "param_0": 42,
                "param_1": 42,
                "param_2": 42,
                "param_3": 42,
                "param_4": 42,
                "param_5": 42.42,
                "param_6": 42.42,
                "param_7": 42.42,
                "param_8": 42.42,
                "param_9": 42.42,
                "param_10": 42.42,
                "param_11": "Z",
                "param_12": "String",
                "param_13": "String",
                "param_14": "String",
            },
        },
    ),
]
EXPECTED_SPARK_LOG_RECORDS = [
    logging.LogRecord(
        name="",
        level=logging.INFO,
        pathname="spark",
        lineno=0,
        msg="Hello!",
        args=(),
        exc_info=None,
        func=None,
        sinfo=None,
    ),
    logging.LogRecord(
        name="",
        level=logging.INFO,
        pathname="spark",
        lineno=0,
        msg="\\[/ESCAPE ME]",
        args=(),
        exc_info=None,
        func=None,
        sinfo=None,
    ),
    logging.LogRecord(
        name="origin",
        level=logging.ERROR,
        pathname="origin",
        lineno=0,
        msg="[bold]%s happened! %s[/bold]",
        args=("Something", "\\[/ESCAPE ME TOO!]"),
        exc_info=None,
        func=None,
        sinfo="Stacktrace:\nSomething something\nError Some Error here:\nline 20",
    ),
    logging.LogRecord(
        name="origin",
        level=logging.INFO,
        pathname="origin",
        lineno=0,
        msg="[bold]Output generated: %s[/bold]",
        args=("{'key': {'value': [1], 'markup': '\\[/ESCAPE ME]'}}",),
        exc_info=None,
        func=None,
        sinfo=None,
    ),
    logging.LogRecord(
        name="origin",
        level=logging.INFO,
        pathname="origin",
        lineno=0,
        msg="[bold]Integers: %d %i,"
        " Octal: %o,"
        " Hex: %x %X,"
        " Floats: %e %E %f %F %g %G,"
        " Char: %c,"
        " Representations: %r %s %a,"
        " Literal %%[/bold]",
        args=(
            42,
            42,
            42,
            42,
            42,
            42.42,
            42.42,
            42.42,
            42.42,
            42.42,
            42.42,
            "Z",
            "String",
            "String",
            "String",
        ),
        exc_info=None,
        func=None,
        sinfo=None,
    ),
]


def simulate_build_log():
    blog = BUILD_LOG_RESPONSE.copy()
    # initial request
    yield blog
    # checks logs
    blog["allJobLogs"][CHECK_JOB_RID]["logsByStep"] = [
        {
            "logs": os.linesep.join(CHECK_LOGS),
            "isTruncated": False,
        },
    ]
    yield blog
    # checks finish
    blog["allJobStatusReports"][CHECK_JOB_RID]["jobCustomMetadata"] = CHECK_JOB_CUSTOM_METADATA
    blog["allJobStatusReports"][CHECK_JOB_RID]["jobStatus"] = "SUCCEEDED"
    blog["allJobStatusReports"][CHECK_JOB_RID]["stepStatusReports"][0]["stepStatus"] = "SUCCEEDED"
    blog["allJobs"][1]["completedAt"] = "2000-01-01T00:10:00.000000000Z"
    yield blog
    # build init is very fast, instantly finishes
    blog["allJobs"][1]["submittedAt"] = "2000-01-01T00:10:00.000000000Z"
    blog["allJobs"][1]["createdAt"] = "2000-01-01T00:10:00.000000000Z"
    blog["allJobs"][1]["completedAt"] = "2000-01-01T00:10:10.000000000Z"
    blog["allJobStatusReports"][BUILD_JOB_RID]["jobCustomMetadata"] = {"startedBuildIds": [STARTED_BUILD_RID]}
    blog["allJobStatusReports"][BUILD_JOB_RID]["jobStatus"] = "SUCCEEDED"
    blog["allJobStatusReports"][BUILD_JOB_RID]["stepStatusReports"] = [
        {"name": "Start dataset builds", "stepStatus": "SUCCEEDED"},
    ]
    blog["allJobLogs"][BUILD_JOB_RID]["logsByStep"] = [
        {
            "logs": BUILD_INIT_LOGS,
            "isTruncated": False,
        },
    ]
    blog["buildStatus"] = "SUCCEEDED"
    # everything finished
    yield blog


@dataclass
class WebSocketMock:
    responses: Sequence[str]
    rcvd_frame: websockets.frames.Close | None = None
    sent_frame: websockets.frames.Close | None = None
    counter: int = -1

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ):
        pass

    def recv(self):
        self.counter += 1
        if self.counter >= len(self.responses):
            raise websockets.exceptions.ConnectionClosed(self.rcvd_frame, self.sent_frame)
        return self.responses[self.counter]


@mock.patch(
    "foundry_dev_tools.cli.build.get_repo",
    return_value=(
        "ri.stemma.main.repository.a0b5defa-82d9-4959-bdef-28e02e00cd48",
        "refs/heads/dev/branch",
        "52bdea68c6538acc79eb03bc33292314f97551f4",
        Path.cwd(),
    ),
)
@mock.patch(
    "foundry_dev_tools.clients.jemma.JemmaClient.start_checks_and_builds",
    side_effect=simulate_build_log(),
)
@mock.patch(
    "foundry_dev_tools.cli.build.connect",
    return_value=WebSocketMock(
        SPARK_LOGS,
        websockets.frames.Close(1000, "close"),
    ),
)
@mock.patch("foundry_dev_tools.cli.build._is_transform_file", return_value=True)
@mock.patch(
    "foundry_dev_tools.utils.misc.print_horizontal_line",
)
@mock.patch(
    "foundry_dev_tools.cli.build.print_horizontal_line",
)
def test_build(
    print_horizontal_line_mock,
    print_horizontal_line_mock_2,
    is_transform_file_mock,
    build_connect_mock,
    checks_and_builds_mock,
    get_repo_mock,
    capsys,
    caplog,
    test_context_mock,
):
    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(TEST_HOST.url, "build2", "info/jobs3")) + "/.*"),
        json={"jobResults": {"ri.foundry.main.dataset.81d943dd-8b84-46ba-b720-5e227de8bb6a": {}}},
    )
    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(TEST_HOST.url, "build2", "info/builds2")) + "/.*"),
        json={
            "jobRids": ["ri.foundry.main.job.254cf2ee-493b-4772-b1ac-6805d7c7904a"],
            # there is more, but not interesting for us
            # "buildRid": "ri.foundry.main.build.0e7ca16b-49f1-4b2d-953e-21b18bc7c560",  noqa: ERA001
            # "buildGroupRid": "ri.foundry.main.buildgroup.0e7ca16b-49f1-4b2d-953e-21b18bc7c560",  noqa: ERA001
            # "ignoreBuildPolicy": False, noqa: ERA001
        },
    )
    # reset current out/err
    capsys.readouterr()
    # run build cli
    with pytest.raises(SystemExit) as sys_exit:
        _build_cli("transforms-python/src/myproject/datasets/examples.py", test_context_mock)
    assert sys_exit.value.code == 0
    build_out_err = capsys.readouterr()
    assert build_out_err.out
    output = build_out_err.out.replace("\n", "")
    logs_wo_line = "".join(CHECK_LOGS)
    assert output.startswith(logs_wo_line)
    output = output[len(logs_wo_line) :]
    assert output.startswith(BUILD_INIT_LOGS)
    output = output[len(BUILD_INIT_LOGS) :]
    build_url_message = _build_url_message(TEST_HOST, STARTED_BUILD_RID)
    assert output.startswith(build_url_message)
    # after that we log in the websocket handler
    log_records = caplog.get_records("call")
    assert len(log_records) == len(SPARK_LOGS)
    for i in range(len(EXPECTED_SPARK_LOG_RECORDS)):
        assert EXPECTED_SPARK_LOG_RECORDS[i].name == log_records[i].name
        assert EXPECTED_SPARK_LOG_RECORDS[i].levelno == log_records[i].levelno
        assert EXPECTED_SPARK_LOG_RECORDS[i].filename == log_records[i].filename
        assert EXPECTED_SPARK_LOG_RECORDS[i].lineno == log_records[i].lineno

        assert EXPECTED_SPARK_LOG_RECORDS[i].msg == log_records[i].msg
        assert EXPECTED_SPARK_LOG_RECORDS[i].args == log_records[i].args
        assert EXPECTED_SPARK_LOG_RECORDS[i].exc_info == log_records[i].exc_info
        assert EXPECTED_SPARK_LOG_RECORDS[i].stack_info == log_records[i].stack_info

    compl = "Spark Job Completed."
    assert (
        output[output.index(compl) + len(compl) :]
        == "Build status: SUCCEEDEDLink to the foundry build: " + TEST_HOST.url
        + "/workspace/data-integration/job-tr"
        "acker/builds/ri.foundry.main.build.0e7ca16b-49f1-4b2d-953e-21b18bc7c560The resulting dataset(s):"
        + TEST_HOST.url
        + "/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.81d943dd-8b84-46ba-b720-5e227de8bb6a/dev"
        "%2Fbranch"
    )


def test_get_transform_files(tmpdir: py.path.LocalPath, git_env: dict):
    with tmpdir.as_cwd():
        from foundry_dev_tools.cli.build import (
            TRANSFORM_DECORATORS,
            _get_transform_files,
            _is_transform_file,
        )

        subprocess.check_call(["git", "init"], env=git_env)
        t = Path("transforms-python", "examples")
        t.mkdir(parents=True)
        tfiles = []
        for decorator in TRANSFORM_DECORATORS:
            transform_file = t.joinpath(f"{decorator}.py")
            with transform_file.open("w+") as tfile:
                tfile.write(
                    f"from transforms.api import {decorator},Output\n\n@transform_df()\ndef test_transform():\n   "
                    " pass",
                )
            tfiles.append(transform_file.as_posix())  # git returns with forward slash
        subprocess.check_call(["git", "add", "-A"], env=git_env)
        subprocess.check_call(["git", "commit", "-m", "transform commit"], env=git_env)

        for f in tfiles:
            assert _is_transform_file(Path(f))

        assert not _is_transform_file(Path("does not exist"))
        assert _get_transform_files(Path.cwd()) == tfiles
        with tmpdir.join("get_transform.txt").open("w+") as gttxt:
            gttxt.write("something something")
        subprocess.check_call(["git", "add", "-A"], env=git_env)
        subprocess.check_call(["git", "commit", "-m", "no transform in last commit"], env=git_env)
        with pytest.raises(UsageError, match="No transform files in the last commit."):
            _get_transform_files(Path.cwd())
