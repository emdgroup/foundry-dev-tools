import pytest

from foundry_dev_tools.errors.config import FoundryConfigError
from foundry_dev_tools.utils.config import check_init


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
