import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import websockets.exceptions
import websockets.frames
from click.testing import CliRunner

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Sequence

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
                    "refName": "refs/heads/master",
                    "commitHash": "52bdea68c6538acc79eb03bc33292314f97551f4",
                }
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
            "ri.foundry.main.dataset.81d943dd-8b84-46ba-b720-5e227de8bb6a"
        ]
    },
    "publishedJobSpecTypes": ["BUILD2"],
    "desiredUnmarkings": {"unmarkings": []},
    "errors": {
        "errorCode": "INTERNAL",
        "errorName": "Jemma:UnknownFailure",
        "errorInstanceId": None,
        "safe-args": {},
        "unsafe-args": {},
        "retryPolicy": None,
    },
    "jobCustomMetadata": {
        "filePathToDatasetRids": {
            "transforms-python/src/myproject/datasets/examples.py": [
                "ri.foundry.main.dataset.81d943dd-8b84-46ba-b720-5e227de8bb6a"
            ]
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


def simulate_build_log():
    blog = BUILD_LOG_RESPONSE.copy()
    # initial request
    yield blog
    # checks logs
    blog["allJobLogs"][CHECK_JOB_RID]["logsByStep"] = [
        {
            "logs": "\n".join(
                [
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
            ),
            "isTruncated": False,
        }
    ]
    yield blog
    # checks finish
    blog["allJobStatusReports"][CHECK_JOB_RID][
        "jobCustomMetadata"
    ] = CHECK_JOB_CUSTOM_METADATA
    blog["allJobStatusReports"][CHECK_JOB_RID]["jobStatus"] = "SUCCEEDED"
    blog["allJobStatusReports"][CHECK_JOB_RID]["stepStatusReports"][0][
        "stepStatus"
    ] = "SUCCEEDED"
    blog["allJobs"][1]["completedAt"] = "2000-01-01T00:10:00.000000000Z"
    yield blog
    # build init is very fast, instantly finishes
    blog["allJobs"][1]["submittedAt"] = "2000-01-01T00:10:00.000000000Z"
    blog["allJobs"][1]["sratedAt"] = "2000-01-01T00:10:00.000000000Z"
    blog["allJobs"][1]["completedAt"] = "2000-01-01T00:10:10.000000000Z"
    blog["allJobStatusReports"][BUILD_JOB_RID]["jobCustomMetadata"] = {
        "startedBuildIds": [
            "ri.foundry.main.build.0e7ca16b-49f1-4b2d-953e-21b18bc7c560"
        ]
    }
    blog["allJobStatusReports"][BUILD_JOB_RID]["jobStatus"] = "SUCCEEDED"
    blog["allJobStatusReports"][BUILD_JOB_RID]["stepStatusReports"] = [
        {"name": "Start dataset builds", "stepStatus": "RUNNING"}
    ]
    blog["allJobLogs"][BUILD_JOB_RID]["logsByStep"] = [
        {
            "logs": "Starting Builds...",
            "isTruncated": False,
        }
    ]
    # everything finished
    yield blog


@dataclass
class WebSocketMock:
    responses: "Sequence[str]"
    rcvd_frame: "websockets.frames.Close | None" = None
    sent_frame: "websockets.frames.Close | None" = None
    counter: int = -1

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: "BaseException | None",
        exc_value: "BaseException | None",
        traceback: "TracebackType | None",
    ):
        pass

    def recv(self):
        self.counter += 1
        if self.counter >= len(self.responses):
            raise websockets.exceptions.ConnectionClosed(
                self.rcvd_frame, self.sent_frame
            )
        return self.responses[self.counter]


@pytest.mark.no_patch_conf()
@mock.patch(
    "foundry_dev_tools.cli.build.get_repo",
    return_value=(
        "ri.stemma.main.repository.a0b5defa-82d9-4959-bdef-28e02e00cd48",
        "refs/heads/master",
        "52bdea68c6538acc79eb03bc33292314f97551f4",
    ),
)
@mock.patch(
    "foundry_dev_tools.cli.build.get_transform",
    return_value=["transforms-python/src/myproject/datasets/examples.py"],
)
@mock.patch(
    "foundry_dev_tools.cli.build.FoundryRestClient.get_build",
    return_value={
        "jobRids": ["ri.foundry.main.job.254cf2ee-493b-4772-b1ac-6805d7c7904a"],
        # there is more, but not interesting for us
        # "buildRid": "ri.foundry.main.build.0e7ca16b-49f1-4b2d-953e-21b18bc7c560",  noqa: ERA001
        # "buildGroupRid": "ri.foundry.main.buildgroup.0e7ca16b-49f1-4b2d-953e-21b18bc7c560",  noqa: ERA001
        # "ignoreBuildPolicy": False, noqa: ERA001
    },
)
@mock.patch(
    "foundry_dev_tools.cli.build.FoundryRestClient.start_checks_and_build_for_commit",
    side_effect=simulate_build_log(),
)
@mock.patch(
    "foundry_dev_tools.cli.build.connect",
    return_value=WebSocketMock(
        ["Hi i'm spark!", "Let's build a dataset", "{}"],
        websockets.frames.Close(1000, "close"),
    ),
)
@mock.patch("foundry_dev_tools.utils.misc.print_horizontal_line")
def test_build(a, b, c, d, e, f):
    from foundry_dev_tools.cli.build import build_cli

    runner = CliRunner()
    result = runner.invoke(
        build_cli,
        [
            "-t",
            "transforms-python/src/myproject/datasets/examples.py",
        ],  # to skip the prompt
        catch_exceptions=False,
    )
    print(result.stdout_bytes.decode("UTF-8"))
    print(result.stderr_bytes.decode("UTF-8"), file=sys.stderr)
    assert result.exit_code == 0
