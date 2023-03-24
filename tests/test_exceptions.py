import inspect

from foundry_dev_tools import exceptions


def test_imports():
    if "exceptions" in globals() or "exceptions" in dir():
        print("MyClass was imported.")
    else:
        print("MyClass was not imported.")
