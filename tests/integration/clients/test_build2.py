import pytest
import requests

from foundry_dev_tools.errors.meta import FoundryAPIError
from tests.integration.conftest import TEST_SINGLETON

# TODO: Extend test cases and establish proper setup for build2 testing to run it independent of the Foundry instance
#       (bootstrapping repository, upload file with transform code and commit, create input dataset..)
DATASET_RID = "ri.foundry.main.dataset.b5219b51-c9b1-4929-bd65-899d16d0ec82"


def test_submit_build():
    try:
        _test_submit_build()
    except FoundryAPIError as err:
        if err.response.status_code == requests.codes.forbidden:
            msg = (
                "To test integration for build2 builds, you need to be member of "
                "ls-use-case-foundry-devtools-dev-workspace-viewer group (2ba614c8-65bb-4d1f-afa1-323610b755ec)!"
            )
            pytest.skip(msg)
        else:
            raise


def _test_submit_build():
    # Run dataset build
    build_information = TEST_SINGLETON.ctx.build2.submit_dataset_build(
        dataset_rid=DATASET_RID,
        force_build=True,
    )

    assert "buildRid" in build_information
    assert "buildGroupRid" in build_information
    assert build_information["isNewSubmission"] is None
