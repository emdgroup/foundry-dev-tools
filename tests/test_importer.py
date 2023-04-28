import pytest

from foundry_dev_tools.utils.importer import import_optional_dependency


def test_import_optional_at_runtime():
    # idea is from here: https://stackoverflow.com/a/63288902/3652805
    match = "Missing .*notapackage.* pip .* conda .* notapackage"
    module = import_optional_dependency("notapackage")
    with pytest.raises(ValueError, match=match):
        module.any_function_should_raise()
