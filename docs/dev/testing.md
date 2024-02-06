# Writing tests

When contributing code to Foundry DevTools it is best practice to create tests for logic you've changed/added.

:::{tip}
Before writing tests have a look at the currently existing fixtures in `/tests/unit/conftest.py` and mocks in `/tests/unit/mocks`, which you probably will need.
:::

## Structure

The `tests` directory contains two subfolders `unit` and `integration`.
Each of the subfolders should work on their own, as they will be run seperately.

The subfolders of these directories are structered like the library itself.
For example the tests for `src/foundry_dev_tools/clients/api_client.py` are in `tests/unit/clients/test_api_client.py`.

The test functions start with `test_` and should contain the function/class name they test.


## Testing an api client

Examples how an API client could be tested.
Let's say this is our Client:

:::{code-block}
:caption: src/foundry_dev_tools/clients/example.py
from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests


class ExampleClient(APIClient):
    api_name = "example-api"

    def api_ping(self, ping: bool, **kwargs) -> requests.Response:
        """Ping Pong API.

        Args:
            ping: if ping is true then it will send ping, otherwise it will send pong
            **kwargs: gets passed to the context client

        Returns:
            requests.Response:
              the api response json object looks like this:
              {"ping":int,"pong":int}
              both are counters that increment when either a ping or a pong was received.
        """
        return self.api_request("GET", "ping", params={"msg": "ping" if ping else "pong"}, **kwargs)
:::

Now we want to test if it sends the correct messages. 
We will use the [`test_context_mock`](#tests.unit.conftest.test_context_mock) fixture.

:::{code-block}
:caption: tests/unit/clients/test_example.py
from foundry_dev_tools.utils.clients import build_api_url


def test_api_ping(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(test_context_mock.host.url, "example-api", "ping"),
        response_list=[{"json": {"ping": 1, "pong": 0}}, {"json": {"ping": 1, "pong": 1}}],
    )

    # make a ping api request with the message "ping"
    resp = test_context_mock.example.api_ping(True)

    # check if it sent a ping
    assert resp.request.query == "msg=ping"
    assert resp.json() == {"ping": 1, "pong": 0}

    # make a ping api request with the message "pong"
    resp = test_context_mock.example.api_ping(False)

    # check if it sent a pong
    assert resp.request.query == "msg=pong"
    assert resp.json() == {"ping": 1, "pong": 1}
:::


:::{seealso}
- [requests_mock library](https://requests-mock.readthedocs.io)
- [unittest mock](https://docs.python.org/3/library/unittest.mock.html)
:::
