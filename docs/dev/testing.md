# Writing tests

When contributing code to Foundry DevTools it is best practice to create tests for logic you've changed/added.

:::{tip}
Before writing tests have a look at the currently existing fixtures in `/tests/unit/conftest.py` and mocks in `/tests/unit/mocks`, which you probably will need.  
Furthermore `/tests/integration/utils.py` provides variables such as `INTEGRATION_TEST_COMPASS_ROOT_PATH`, `INTEGRATION_TEST_COMPASS_ROOT_RID` and some more, which can be helpful when writing integration tests.
:::

## Structure

The `tests` directory contains two subfolders `unit` and `integration`.
Each of the subfolders should work on their own, as they will be run seperately.

The subfolders of these directories are structered like the library itself.
For example the tests for `src/foundry_dev_tools/clients/api_client.py` are in `tests/unit/clients/test_api_client.py` for unit tests and `tests/integration/clients/test_api_client.py` for integration tests.

Test functions should begin with `test_`.  For unit tests, they should include the name of the function or class being tested.
In the case of integration tests, they should have an appropriate name that reflects the process or workflow being tested.


## Testing an api client

Examples on how an API client could be tested.
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
        """Sends a signal.

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

    def api_ping_history(self, **kwargs) -> requests.Response:
        """History of ping and pong counters.

        Args:
            **kwargs: gets passed to the context client

        Returns:
            requests.Response:
                the api response json ohject looks like this:
                [{"ping":int,"pong":int},{"ping":int,"pong":int},<...>]
                It is the history tracking the ping and pong counters after each increment by `api_ping` call.
        """
        return self.api_request("GET", "ping/history", **kwargs)
:::

### Unit testing

Now we want to test if it sends the correct messages. 
We will use the [`test_context_mock`](#tests.unit.conftest.test_context_mock) fixture providing us an instance of [FoundryMockContext](#tests.unit.mocks.FoundryMockContext) and a mock adapter to mock API requests and function calls.

:::{code-block}
:caption: tests/unit/clients/test_example.py
from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST

def test_api_ping(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(TEST_HOST.url, "example-api", "ping"),
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

### Integration testing

Having tested the logic of the `api_ping` call it is time to test some more complex behavior which also involves interaction with Foundry. 
We can consider a test scenario, such as sending initial ping signals and then retrieving the history to compare against the sent signals.
To send requests we can utilize the [`TEST_SINGLETON`](#tests.integration.conftest.TEST_SINGLETON) variable providing us an actual instance of the [FoundryContext](/dev/architecture/foundry_context_implementation.md).

:::{code-block}
:caption: tests/integration/clients/test_example.py
from tests.integration.conftest import TEST_SINGLETON

def test_api_ping_history():
    # send a ping signal
    resp = TEST_SINGLETON.ctx.example.api_ping(True)

    assert resp.status_code == 200
    assert resp.json() == {"ping": 1, "pong": 0}

    # send a pong signal
    resp = TEST_SINGLETON.ctx.example.api_ping(False)

    assert resp.status_code == 200
    assert resp.json() == {"ping": 1, "pong": 1}

    # send another pong signal
    resp = TEST_SINGLETON.ctx.example.api_ping(False)

    assert resp.status_code == 200
    assert resp.json() == {"ping": 1, "pong": 2}

    # validate the history
    resp = TEST_SINGLETON.ctx.example.api_ping_history()

    assert resp.status_code == 200
    
    history = resp.json()

    assert len(history) == 3
    assert history[0] == {"ping": 1, "pong": 0}
    assert history[1] == {"ping": 1, "pong": 1}
    assert history[1] == {"ping": 1, "pong": 2}
:::

:::{warning}
Integration tests which make use of the [`TEST_SINGLETON`](#tests.integration.conftest.TEST_SINGLETON) variable do not rely on mocks, so every API request will be sent to Foundry and actually be processed.
Thus, be cautious and pay attention to what you do to not break anything by accident!
:::
