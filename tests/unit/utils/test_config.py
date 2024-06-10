import os

import pytest
from pytest_mock import MockerFixture

from foundry_dev_tools.errors.config import FoundryConfigError
from foundry_dev_tools.utils.config import check_init, get_environment_variable_config


def test_check_init():
    class X:
        def __init__(self, a: int, b: bool, c: str = "", *args, **kwargs):
            pass

    check_init(X, "mock", {"a": 1, "b": True, "c": "test"})
    with pytest.warns(
        UserWarning,
        match=f"mock.a was type {str!s} but has been cast to {int!s}, this was needed to instantiate {X!s}.",
    ):
        check_init(X, "mock", {"a": "1", "b": True})
    with pytest.warns(UserWarning, match=f"mock.d is not a valid config option for {X!s}"):
        check_init(X, "mock", {"a": 1, "b": False, "d": 1})
    with pytest.raises(FoundryConfigError, match=f"mock.b is missing to create {X!s}"):
        check_init(X, "mock", {"a": 1})

    with pytest.raises(
        FoundryConfigError,
        match=f"To initialize {X!s}, the config option mock.a needs to be of type {int!s}, but it is type {dict!s}",
    ):
        check_init(X, "mock", {"a": {}, "b": True})


def test_get_environment_variable_config(mocker: MockerFixture):
    # Set up mock environment variables
    env = {
        "FDT_CREDENTIALS__DOMAIN": "domain",
        "FDT_CREDENTIALS__TOKEN_PROVIDER__NAME": "jwt",
        "FDT_CREDENTIALS__TOKEN_PROVIDER__CONFIG__JWT": "jwt_value",
        "FDT_TEST": "invalid",
        "OTHER_VARIABLE": "othervalue",
    }
    v1_env = {
        "FOUNDRY_DEV_TOOLS_JWT": "jwt_value_v1",
        "FOUNDRY_DEV_TOOLS_FOUNDRY_URL": "https://domain_v1",
    }
    v1_oauth_env = {
        "FOUNDRY_DEV_TOOLS_CLIENT_ID": "client_id",
        "FOUNDRY_DEV_TOOLS_FOUNDRY_URL": "https://domain_v1",
    }
    v1_oauth_env.update(env)
    mocker.patch.dict(
        os.environ,
        env,
    )

    with pytest.warns(
        UserWarning,
        match="FDT_TEST is not a valid Foundry DevTools configuration environment variable.",
    ):
        result = get_environment_variable_config()

    # Check the result
    assert result == {
        "credentials": {"domain": "domain", "token_provider": {"name": "jwt", "config": {"jwt": "jwt_value"}}},
    }

    # Check that non-FDT_ variables are ignored
    assert "othervalue" not in str(result)

    mocker.patch.dict(os.environ, v1_env)
    with (
        pytest.warns(
            UserWarning,
            match="FDT_TEST is not a valid Foundry DevTools configuration environment variable.",
        ),
        pytest.warns(
            DeprecationWarning,
            match="The v1 environment variables are deprecated, please use the v2 environment variables instead",
        ),
    ):
        result = get_environment_variable_config()

    # Check the result
    assert result == {
        "credentials": {
            "domain": "domain_v1",
            "scheme": "https",
            "token_provider": {"name": "jwt", "config": {"jwt": "jwt_value_v1"}},
        },
    }
    mocker.patch.dict(os.environ, v1_oauth_env, clear=True)
    with (
        pytest.warns(
            UserWarning,
            match="FDT_TEST is not a valid Foundry DevTools configuration environment variable.",
        ),
        pytest.warns(
            DeprecationWarning,
            match="The v1 environment variables are deprecated, please use the v2 environment variables instead",
        ),
    ):
        result = get_environment_variable_config()

    # Check the result
    assert result == {
        "credentials": {
            "domain": "domain_v1",
            "scheme": "https",
            "token_provider": {
                "name": "oauth",
                "config": {
                    "jwt": "jwt_value",  # TODO remove jwt when merging config dicts
                    "client_id": "client_id",
                },
            },
        },
    }
