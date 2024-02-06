import sys


def remove_transforms_modules():
    # remove transform modules from sys.modules so mocking works
    transform_modules = [module for module in sys.modules if module.startswith("transforms.") or module == "transforms"]
    for tmod in transform_modules:
        del sys.modules[tmod]
