from unittest.mock import patch

import pytest

from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST


@pytest.mark.parametrize(
    ("job_spec_selection", "expected_type"),
    [
        ({"datasetRids": ["dataset-rid"], "isRequired": True}, "datasets"),
        ({"jobSpecRids": ["jobspec-rid"], "isRequired": True}, "jobSpecs"),
        ({"datasetRids": ["dataset-rid"], "datasetRidsToIgnore": ["dataset-rid-to-ignore"]}, "upstream"),
        (
            {
                "upstreamDatasetRids": ["upstream-dataset-rid"],
                "downstreamDatasetRids": ["downstream-dataset-rid"],
                "datasetRidsToIgnore": ["dataset-rid-to-ignore"],
            },
            "connecting",
        ),
    ],
)
@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_conversion_of_job_spec_selections_for_submit_build(
    api_request, job_spec_selection, expected_type, test_context_mock
):
    test_context_mock.mock_adapter.register_uri("POST", build_api_url(TEST_HOST.url, "build2", "manager/submitBuild"))

    test_context_mock.build2.api_submit_build([job_spec_selection])

    request_body = api_request.call_args.kwargs["json"]

    assert len(request_body["jobSpecSelections"]) == 1

    converted_job_spec_selection = request_body["jobSpecSelections"][0]
    assert converted_job_spec_selection["type"] == expected_type
    assert converted_job_spec_selection[expected_type] == job_spec_selection
