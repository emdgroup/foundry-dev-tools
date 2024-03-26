import json

import pytest
import requests

from foundry_dev_tools.errors.handling import raise_foundry_api_error
from foundry_dev_tools.errors.meta import FoundryAPIError


def test_raise_on_foundry_api_error():
    resp = requests.Response()
    resp.status_code = 403  # access denied
    resp._content = json.dumps(
        {
            "errorCode": "PERMISSION_DENIED",
            "errorName": "test:PermissionDenied",
            "errorInstanceId": "34ea0cb3-6d65-49ea-bec4-c6633890fd1e",
            "parameters": {"test": "param"},
        },
    ).encode()
    resp.request = requests.Request(
        method="GET",
        url="http://test_raise_on_foundry_api_error",
        headers={"content-type": "application/json"},
    ).prepare()

    with pytest.raises(FoundryAPIError) as exc_info:
        raise_foundry_api_error(resp)

    assert exc_info.type is FoundryAPIError
    assert exc_info.value.response == resp
    assert exc_info.value.kwargs == {
        "error_code": "PERMISSION_DENIED",
        "error_name": "test:PermissionDenied",
        "error_instance_id": "34ea0cb3-6d65-49ea-bec4-c6633890fd1e",
        "test": "param",
    }
