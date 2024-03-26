import sys
from unittest.mock import patch

import pytest

orig_import = __import__


def no_optional(n, *args, **kwargs):
    if n in ("pyspark", "pyarrow", "pandas"):
        raise ImportError
    return orig_import(n, *args, **kwargs)


def remove_optional_imports():
    # remove modules form sysmodules so they'll get reimported
    sys.modules.pop("foundry_dev_tools._optional", None)
    sys.modules.pop("foundry_dev_tools._optional.pandas", None)
    sys.modules.pop("foundry_dev_tools._optional.pyarrow", None)
    sys.modules.pop("foundry_dev_tools._optional.pyspark", None)


def test_optional_pandas():
    remove_optional_imports()
    with patch("builtins.__import__", side_effect=no_optional):
        from foundry_dev_tools._optional.pandas import pd
        from foundry_dev_tools._optional.pyarrow import pa
        from foundry_dev_tools._optional.pyspark import pyspark

        # test when dependencies are not installed
        with pytest.raises(ImportError):
            _ = pd.DataFrame
        with pytest.raises(ImportError):
            _ = pyspark.sql.DataFrame
        with pytest.raises(ImportError):
            _ = pa.Table

    remove_optional_imports()
    from foundry_dev_tools._optional.pandas import pd
    from foundry_dev_tools._optional.pyarrow import pa
    from foundry_dev_tools._optional.pyspark import pyspark

    # should throw no errors when dependencies are installed
    _ = pd.DataFrame
    _ = pyspark.sql.DataFrame
    _ = pa.Table
