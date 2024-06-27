from foundry_dev_tools.cli.git import _git_clone_cli, _is_repo_id, _parse_repo
from unittest.mock import patch, MagicMock, _Call
from rich.console import Console
from pytest_mock.plugin import MockerFixture
from functools import partial
from foundry_dev_tools.utils.clients import build_api_url
from subprocess import CalledProcessError
import pytest

REPO_RID = "ri.stemma.main.repository.dbbd1d38-6ec8-4458-8c1b-5e70425cc4f1"


def test_is_repo_id():
    assert _is_repo_id(REPO_RID)
    assert not _is_repo_id("https://your-stack.palantirfoundry.com")
    assert not _is_repo_id("ri.stemma.main.repository.asdf")
    assert not _is_repo_id("ri.stemma.main.repository.fffffff-ffff-ffff-ffff-ffffffffffff")


def _get_repo_url(scheme, host, repo_rid):
    return f"{scheme}://{host}/workspace/data-integration/code/repos/{repo_rid}/contents/refs%2Fheads%2Fmaster"


def test_parse_repo(test_context_mock, mocker: MockerFixture):
    c = Console()
    print_spy = mocker.spy(c, "print")
    parse_repo = partial(_parse_repo, c, test_context_mock)
    x = parse_repo(
        _get_repo_url("https", test_context_mock.host.domain, REPO_RID),
    )
    assert x == REPO_RID
    print_spy.assert_not_called()
    print_spy.reset_mock()
    x = parse_repo(_get_repo_url("git", "wrong-foundry-host.test", REPO_RID))
    assert x is None
    print_spy.assert_called_once()
    assert (
        print_spy.call_args.args
        == _Call(
            (
                (
                    f"The domain of the repo does not match the domain in your Foundry DevTools configuration ({test_context_mock.host.domain}).",
                ),
                {},
            )
        ).args
    )
    print_spy.reset_mock()

    x = parse_repo(_get_repo_url("git+https", test_context_mock.host.domain, "not-a-repo-rid"))
    assert x is None
    print_spy.assert_called_once()
    assert print_spy.call_args.args == _Call((("Could not find a valid repository ID in the repository URL",), {})).args
    print_spy.reset_mock()

    x = parse_repo(_get_repo_url("invalid-scheme", test_context_mock.host.domain, ""))
    assert x is None
    print_spy.assert_called_once()
    assert (
        print_spy.call_args.args
        == _Call(
            (("The repo argument should be either the repository ID or an  URL which contains the repository ID",), {})
        ).args
    )
    print_spy.reset_mock()

    x = parse_repo(REPO_RID)
    assert x == REPO_RID
    print_spy.assert_not_called()


@patch("foundry_dev_tools.cli.git.subprocess.check_output")
@patch("foundry_dev_tools.cli.git.os.execvp")
def test_git_clone_cli(mock_execvp: MagicMock, mock_check_output: MagicMock, mocker, test_context_mock):
    repo_name = "test-repo"
    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(test_context_mock.host.url, "compass", f"resources/{REPO_RID}"),
        json={
            "rid": REPO_RID,
            "name": repo_name,
            "created": {"time": "2021-06-09T11:08:13.670514359Z", "userId": "3c8fbda5-686e-4fcb-ad52-d95e4281d99f"},
            "modified": {"time": "2023-09-17T17:52:46.115596098Z", "userId": "a6d873b6-c97d-4a4e-b77c-a6b65703e651"},
            "lastModified": 1694973548441.0,
            "description": None,
            "operations": [
                "compass:view-long-description",
                "compass:view-project-imports",
                "compass:read-resource",
                "compass:open-resource-links",
                "gatekeeper:view-resource",
                "compass:import-resource-from",
                "compass:linked-items:view",
                "compass:import-resource",
                "compass:view-contact-information",
                "compass:discover",
                "compass:view",
            ],
            "urlVariables": {"compass:isProject": "false"},
            "favorite": None,
            "branches": None,
            "defaultBranch": None,
            "defaultBranchWithMarkings": None,
            "branchesCount": None,
            "hasBranches": None,
            "hasMultipleBranches": None,
            "backedObjectTypes": None,
            "path": None,
            "longDescription": None,
            "directlyTrashed": False,
            "inTrash": None,
            "isAutosave": False,
            "isHidden": False,
            "deprecation": None,
            "collections": None,
            "namedCollections": None,
            "tags": None,
            "namedTags": None,
            "alias": None,
            "collaborators": None,
            "namedAncestors": None,
            "markings": None,
            "projectAccessMarkings": None,
            "linkedItems": None,
            "contactInformation": None,
            "classification": None,
            "disableInheritedPermissions": None,
            "propagatePermissions": None,
            "resourceLevelRoleGrantsAllowed": None,
        },
    )
    mock_check_output.return_value = ""
    mock_check_output.side_effect = CalledProcessError(1, "bla")
    mock_execvp.return_value = ""

    c = Console()
    print_spy = mocker.spy(c, "print")
    _git_clone_cli(test_context_mock, c, REPO_RID, ".")
    assert (
        print_spy.call_args.args
        == _Call(
            (("git-credential-foundry not set up, please run the git-credential-foundry command to setup",), {})
        ).args
    )
    mock_execvp.assert_not_called()
    print_spy.reset_mock()
    mock_check_output.side_effect = FileNotFoundError
    _git_clone_cli(test_context_mock, c, REPO_RID, ".")
    assert print_spy.call_args.args == _Call((("git not found, please install git",), {})).args
    mock_execvp.assert_not_called()
    print_spy.reset_mock()

    mock_check_output.side_effect = None
    _git_clone_cli(test_context_mock, c, REPO_RID, ".")
    mock_execvp.assert_called_once()
    assert (
        mock_execvp.call_args.args
        == _Call(
            (
                (
                    "git",
                    [
                        "git",
                        "clone",
                        f"{test_context_mock.host.url}/stemma/git/{REPO_RID}/{repo_name}",
                        ".",
                    ],
                ),
                {},
            )
        ).args
    )
    print_spy.reset_mock()
    mock_execvp.reset_mock()
    _git_clone_cli(test_context_mock, c, REPO_RID, None)
    mock_execvp.assert_called_once()
    assert (
        mock_execvp.call_args.args
        == _Call(
            (
                (
                    "git",
                    [
                        "git",
                        "clone",
                        f"{test_context_mock.host.url}/stemma/git/{REPO_RID}/{repo_name}",
                    ],
                ),
                {},
            )
        ).args
    )
    print_spy.reset_mock()
