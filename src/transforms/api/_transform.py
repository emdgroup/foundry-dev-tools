"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # noqa: E501

import collections
import inspect
import os
import re
from collections.abc import Callable
from functools import cached_property
from os import PathLike
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import fs
import pyspark

import foundry_dev_tools.config
from foundry_dev_tools.utils.spark import get_spark_session
from transforms.api._dataset import Input, Output

DECORATOR_TYPE = Literal[
    "spark", "pandas", "lightweight", "lightweight-pandas", "lightweight-polars"
]


class Transform:
    """A Python Transform.

    Wraps a function and holds the Input and Output context.
    When the function is called, the Input and Output parameters
    are automatically passed.
    """

    def __init__(
        self,
        compute_func: Callable,
        outputs: "dict[str, Output] | None" = None,
        inputs: "dict[str, Input] | None" = None,
        decorator: DECORATOR_TYPE = "spark",
    ):
        """Initialize the `Transform`.

        Args:
            compute_func (Callable): The compute function to wrap.
            inputs (Dict[str, Input]): A dictionary mapping input names to :class:`Input` specs.
            outputs (Dict[str, Output]): A dictionary mapping output names to :class:`Output` specs.
            decorator (str): decorator
        """
        # Wrap the compute function and store its properties
        self._compute_func = compute_func

        self.inputs = inputs or {}
        self.outputs = outputs or {}

        self._bound_transform = True
        self._type = decorator

        for name, tinput in self.inputs.items():
            if not isinstance(tinput, Input):
                raise ValueError(
                    f"Input '{name}' to transform {self} is not "
                    f"a transforms.api.Input"
                )
        for _, toutput in self.outputs.items():
            if not isinstance(toutput, Output):
                raise ValueError(
                    f"Output '{compute_func.__name__}' of transform {self} is not "
                    f"a transforms.api.Output"
                )

        self._use_context = "ctx" in inspect.getfullargspec(compute_func).args

    def __call__(self, *args, **kwargs):
        """Passthrough call to the underlying compute function."""
        try:
            return self._compute_func(*args, **kwargs)
        except Exception as exc:
            setattr(exc, "__transform_compute_error", True)
            raise

    def compute(self):
        """Execute the wrapped transform function."""
        handlers: Dict[DECORATOR_TYPE, Callable[[], Any]] = {
            "spark": self._compute_spark,
            "pandas": self._compute_pandas,
            "transform": self._compute_transform,
            "lightweight": self._compute_lightweight,
            "lightweight-pandas": self._compute_lightweight_pandas,
            "lightweight-polars": self._compute_lightweight_polars,
        }

        return handlers[self._type]()

    def _compute_spark(
        self,
    ) -> "pyspark.sql.DataFrame":
        """Execute the wrapped transform function.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                the result of the transforms function

        """
        kwargs = {name: i.dataframe() for name, i in self.inputs.items()}
        if self._use_context:
            kwargs["ctx"] = TransformContext()

        output_df = self(**kwargs)

        if not isinstance(output_df, pyspark.sql.DataFrame):
            raise ValueError(
                f"Expected {self} to return a pyspark.sql.DataFrame, instead got {output_df}"
            )

        return output_df

    def _compute_pandas(self):
        """Execute the wrapped transform function.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                the result of the transforms function

        """
        kwargs = {name: i.dataframe().toPandas() for name, i in self.inputs.items()}
        if self._use_context:
            kwargs["ctx"] = TransformContext()

        output_df = self(**kwargs)
        import pandas as pd

        if not isinstance(output_df, pd.DataFrame):
            raise ValueError(
                f"Expected {self} to return a pandas.DataFrame, instead got {output_df}"
            )

        return output_df

    def _compute_transform(self):
        # prepare Transform
        inputs = {
            argument_name: TransformInput(i) for argument_name, i in self.inputs.items()
        }
        outputs = {
            argument_name: TransformOutput(o, argument_name)
            for argument_name, o in self.outputs.items()
        }
        kwargs = {**inputs, **outputs}

        if self._use_context:
            kwargs["ctx"] = TransformContext()

        self(**kwargs)

        return {name: i.dataframe() for name, i in outputs.items()}

    def _compute_lightweight(self):
        if self._use_context:
            raise ValueError(
                "Lightweight transforms do not support the context argument."
            )

        inputs = {
            argument_name: LightweightTransformInput(i)
            for argument_name, i in self.inputs.items()
        }
        outputs = {
            argument_name: LightweightTransformOutput(o, argument_name)
            for argument_name, o in self.outputs.items()
        }
        kwargs = {**inputs, **outputs}

        self(**kwargs)

        return {name: i.df for name, i in outputs.items()}

    def _compute_lightweight_pandas(self):
        if self._use_context:
            raise ValueError(
                "Lightweight transforms do not support the context argument."
            )

        inputs = {
            argument_name: LightweightTransformInput(i).pandas()
            for argument_name, i in self.inputs.items()
        }
        return self(**inputs)

    def _compute_lightweight_polars(self):
        if self._use_context:
            raise ValueError(
                "Lightweight transforms do not support the context argument."
            )

        inputs = {
            argument_name: LightweightTransformInput(i).polars()
            for argument_name, i in self.inputs.items()
        }
        return self(**inputs)


class TransformContext:
    """The TransformContext is passed to the transform function if ctx is the first argument."""

    def __init__(self):
        pass

    @property
    def spark_session(self) -> pyspark.sql.SparkSession:
        """Return the spark session.

        Returns:
            :external+spark:py:class:`~pyspark.sql.SparkSession`

        """
        return get_spark_session()

    @property
    def is_incremental(self) -> bool:
        """Not implemented.

        Returns:
            bool:
                false, as it is not implemented

        """
        import warnings

        warnings.warn(
            "is_incremental functionality not implemented in Foundry DevTools"
        )
        return False


class TransformInput:
    """TransformInput class, passed when using @transform decorator."""

    def __init__(self, input_arg: Input):
        self._input_arg = input_arg
        self._dataset_identity = input_arg.get_dataset_identity()
        self.branch = input_arg.branch
        self.rid = self._dataset_identity["dataset_rid"]
        self.path = self._dataset_identity["dataset_path"]

    def dataframe(self):
        """Returns the dataframe of this input.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                The dataframe for the dataset.
        """
        return self._input_arg.dataframe()

    def pandas(self):
        """Returns the pandas dataframe of this transform input.

        Returns:
            :external+pandas:py:class:`~pandas.DataFrame`:
                the pandas dataframe of this transform input.

        """
        return self.dataframe().toPandas()

    def filesystem(self):
        """Returns a read-only filesystem object, that has read and write functions.

        Returns:
            FileSystem:
                A `FileSystem` object for reading from `Foundry`.
        """
        return FileSystem(base_path=self._input_arg.get_local_path_to_dataset())


class LightweightTransformInput(TransformInput):
    """LightweightTransformInput class, passed when using @lightweight decorator."""

    def __init__(self, input_arg: Input):
        super().__init__(input_arg)
        self.branch = input_arg.branch
        self.rid = input_arg.get_dataset_identity()["dataset_rid"]
        self._local_path = input_arg.get_local_path_to_dataset()
        del self.path  # we change the field to a property in this class

    @cached_property
    def _parquet_files(self) -> List[Path]:
        return list(Path(self._local_path).glob("**/*.parquet"))

    @cached_property
    def _csv_files(self) -> List[Path]:
        return list(Path(self._local_path).glob("**/*.csv"))

    def dataframe(self):
        raise NotImplementedError(
            "Lightweight transforms do not support the dataframe method."
        )

    def path(self) -> str:
        """Download the dataset's underlying files and return a Path to them."""
        return self._local_path

    def pandas(self) -> "pandas.DataFrame":
        """A Pandas DataFrame containing the full view of the dataset."""
        import pandas as pd

        return (
            pd.concat(map(pd.read_parquet, self._parquet_files), ignore_index=True)
            if self._parquet_files
            else pd.concat(map(pd.read_csv, self._csv_files), ignore_index=True)
        )

    def arrow(self) -> "pyarrow.Table":  # noqa: F821
        """A PyArrow table containing the full view of the dataset."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        from pyarrow import csv

        return (
            pq.ParquetDataset(
                str(self._local_path) + "/", use_legacy_dataset=False
            ).read()
            if self._parquet_files
            else pa.concat_tables([csv.read_csv(p) for p in self._csv_files])
        )

    def polars(
        self, lazy: Optional[bool] = False
    ) -> "polars.DataFrame | polars.LazyFrame":
        """A Polars DataFrame or LazyFrame containing the full view of the dataset.

        Args:
            lazy (bool, optional): Whether to return a LazyFrame or DataFrame. Defaults to False.
        """
        import polars as pl

        if lazy:
            return (
                pl.scan_parquet(
                    f"{self._local_path}/**/*.parquet",
                    rechunk=False,
                    low_memory=True,
                    cache=False,
                )
                if self._parquet_files
                else pl.scan_csv(
                    f"{self._local_path}/**/*.csv",
                    rechunk=False,
                    low_memory=True,
                    cache=False,
                )
            )

        return (
            pl.read_parquet(
                f"{self._local_path}/**/*.parquet",
                rechunk=False,
                low_memory=True,
            )
            if self._parquet_files
            else pl.read_csv(
                f"{self._local_path}/**/*.csv", rechunk=False, low_memory=True
            )
        )


class TransformOutput:
    """The output object passed into Transform objects at runtime."""

    def __init__(self, output: Output, argument_name: str):
        """Initialize the TransformOutput.

        Args:
            output (Output): the output
            argument_name (str): the argument name
        """
        self._fs = None
        self._df = None
        self._argument_name = argument_name
        self.branch = "no-implemented-in-foundry-dev-tools"
        self.path = output.alias
        self.rid = "no-implemented-in-foundry-dev-tools"

    def dataframe(self) -> "pyspark.sql.DataFrame | None":
        """Returns pyspark DataFrame.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                the dataframe of this output or None

        """
        return self._df

    def write_dataframe(self, df: pyspark.sql.DataFrame, **kwargs):
        """Storing dataframe as variable.

        Args:
            df (:external+spark:py:class:`~pyspark.sql.DataFrame`): spark dataframe
            **kwargs: unused

        """
        self._df = df

    def write_pandas(self, pandas_df):
        """Write the given :class:`pandas.DataFrame` to the dataset.

        Args:
            pandas_df (pandas.DataFrame): The dataframe to write.
        """
        return self.write_dataframe(get_spark_session().createDataFrame(pandas_df))

    def _make_filesystem(self):
        if "transforms_output_folder" in foundry_dev_tools.config.Configuration:
            base_path = Path(
                foundry_dev_tools.config.Configuration["transforms_output_folder"]
            ).joinpath(self._argument_name)
            base_path.mkdir(parents=True, exist_ok=True)
            return FileSystem(base_path=base_path)
        return FileSystem()

    def filesystem(self):
        """Returns a temporary filesystem for the output that can be written to.

        Returns:
            an object with read write methods
        """
        if self._fs is None:
            self._fs = self._make_filesystem()
        return self._fs

    def set_mode(self, mode):
        """Not implemented in Foundry DevTools.

        Args:
            mode: write mode
        """


class LightweightTransformOutput(TransformOutput):
    """The output object passed to the user code at runtime.

    Its aim is to mimic a subset of the API of `TransformOutput`.
    """

    def __init__(self, output: Output, argument_name: str):
        super().__init__(output, argument_name)
        self.branch = "no-implemented-in-foundry-dev-tools"
        self.path = output.alias
        self.rid = "no-implemented-in-foundry-dev-tools"
        self.df = None

    def write_pandas(
        self, df: "pandas.DataFrame", *args, **kwargs  # noqa: F821
    ) -> None:
        """Write the given :class:`pandas.DataFrame` to the dataset."""
        self.write_table(df, *args, **kwargs)

    def write_table(self, df, *args, **kwargs) -> None:
        """Write a Pandas DataFrame, Arrow Table, Polars DataFrame or LazyFrame, to a Foundry Dataset.

        Note:
            In case a path is specified, it must match the return value of ``path_for_write_table``.

        Args:
            df: pd.DataFrame, pa.Table, pl.DataFrame, pl.LazyFrame, or pathlib.Path with the data to upload
            *args: not implemented in Foundry DevTools
            **kwargs: not implemented in Foundry DevTools

        Returns:
            None
        """
        if df.__class__.__name__ == "LazyFrame":
            df = df.collect()

        if isinstance(df, (str, Path)):
            if df != self.path_for_write_table:
                raise ValueError(
                    f"Path '{df}' does not match expected path '{self.path_for_write_table}'"
                )

            import pandas as pd

            self.df = pd.read_parquet(df)  # make df pretty printable
        else:
            self.df = df

    @property
    def path_for_write_table(self):
        """Return the path for the dataset's files to be used with write_table."""
        return self.path


class FileStatus(collections.namedtuple("FileStatus", ["path", "size", "modified"])):
    """A :class:`collections.namedtuple` capturing details about a `FoundryFS` file."""


class FileSystem:
    """File System for TransformOutput and TransformInput."""

    def __init__(self, base_path: "str | PathLike | None" = None):
        if base_path:
            self._fs = fs.open_fs(os.fspath(base_path))
        else:
            self._fs = fs.open_fs("mem://")

    def ls(
        self, glob: "str | None" = None, regex: str = ".*", show_hidden: bool = False
    ):
        """Recurses through all directories and lists all files matching the given patterns.

        Starting from the root directory of the dataset.

        Args:
            glob (str | None): A unix file matching pattern. Also supports globstar.
            regex (str | None): A regex pattern against which to match filenames.
            show_hidden (bool): Include hidden files, those prefixed with '.' or '_'.

        Yields:
            :class:`~transforms.api.FileStatus`:
                The logical path, file size (bytes),
                modified timestamp (ms since January 1, 1970 UTC)
        """
        if glob:
            # need to remove trailing slash
            result = [glob.path[1:] for glob in self._fs.glob(glob)]
        else:
            result = [file[1:] for file in self._fs.walk.files(path="/")]
        pattern = re.compile(regex)
        result_after_regex_match = [s for s in result if pattern.match(s)]
        if show_hidden:
            import warnings

            warnings.warn("argument 'show_hidden' not implemented in foundry_dev_tools")
        for result_path in result_after_regex_match:
            yield FileStatus(
                result_path, "size not implemented", "modified not implemented"
            )

    def open(self, path, mode="w", **kwargs):
        """Open file in this filesystem.

        Args:
            path (str): the path to the file
            mode (str): usual file modes
            **kwargs: pass through

        Returns:
            IO:

        """
        self._fs.makedirs(os.path.dirname(path), recreate=True)
        return self._fs.open(path, mode, **kwargs)
