"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # noqa: E501

from __future__ import annotations

import inspect
import os
import re
from functools import cached_property
from os import PathLike
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Literal, NamedTuple

import fs
import pyspark

from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.utils.spark import get_spark_session
from transforms.api._dataset import Input, Output

DECORATOR_TYPE = Literal["spark", "pandas", "transform", "lightweight", "lightweight-pandas", "lightweight-polars"]

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from foundry_dev_tools.utils import api_types


class Transform:
    """A Python Transform.

    Wraps a function and holds the Input and Output context.
    When the function is called, the Input and Output parameters
    are automatically passed.
    """

    def __init__(
        self,
        compute_func: Callable,
        outputs: dict[str, Output] | None = None,
        inputs: dict[str, Input] | None = None,
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
                msg = f"Input '{name}' to transform {self} is not a transforms.api.Input"
                raise TypeError(msg)
        for toutput in self.outputs.values():
            if not isinstance(toutput, Output):
                msg = f"Output '{compute_func.__name__}' of transform {self} is not a transforms.api.Output"
                raise TypeError(
                    msg,
                )

        self._use_context = "ctx" in inspect.getfullargspec(compute_func).args

    def __call__(self, *args, **kwargs):
        """Passthrough call to the underlying compute function."""
        try:
            return self._compute_func(*args, **kwargs)
        except Exception as exc:
            setattr(exc, "__transform_compute_error", True)
            raise

    def compute(self, context: FoundryContext | None = None):  # noqa: ANN202
        """Execute the wrapped transform function."""
        handlers: dict[DECORATOR_TYPE, Callable[[FoundryContext], Any]] = {
            "spark": self._compute_spark,
            "pandas": self._compute_pandas,
            "transform": self._compute_transform,
            "lightweight": self._compute_lightweight,
            "lightweight-pandas": self._compute_lightweight_pandas,
            "lightweight-polars": self._compute_lightweight_polars,
        }

        return handlers[self._type](context or FoundryContext())

    def _compute_spark(
        self,
        context: FoundryContext,
    ) -> pyspark.sql.DataFrame:
        """Execute the wrapped transform function.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                the result of the transforms function

        """
        kwargs = {name: i.init_input(context).dataframe() for name, i in self.inputs.items()}
        if self._use_context:
            kwargs["ctx"] = TransformContext(context)

        output_df = self(**kwargs)

        if not isinstance(output_df, pyspark.sql.DataFrame):
            msg = f"Expected {self} to return a pyspark.sql.DataFrame, instead got {output_df}"
            raise TypeError(msg)

        return output_df

    def _compute_pandas(
        self,
        context: FoundryContext,
    ) -> pd.core.frame.DataFrame:
        """Execute the wrapped transform function.

        Returns:
            :external+spark:py:class:`~pd.core.frame.DataFrame`:
                the result of the transforms function

        """
        kwargs = {name: i.init_input(context).dataframe().toPandas() for name, i in self.inputs.items()}
        if self._use_context:
            kwargs["ctx"] = TransformContext(context)

        output_df = self(**kwargs)
        from foundry_dev_tools._optional.pandas import pd

        if not isinstance(output_df, pd.DataFrame):
            msg = f"Expected {self} to return a pandas.DataFrame, instead got {output_df}"
            raise TypeError(msg)

        return output_df

    def _compute_transform(  # noqa: ANN202
        self,
        context: FoundryContext,
    ):
        # prepare Transform
        inputs = {argument_name: TransformInput(i, context) for argument_name, i in self.inputs.items()}
        outputs = {
            argument_name: TransformOutput(o, argument_name, context) for argument_name, o in self.outputs.items()
        }
        kwargs = {**inputs, **outputs}

        if self._use_context:
            kwargs["ctx"] = TransformContext(context)

        self(**kwargs)

        return {name: i.dataframe() for name, i in outputs.items()}

    def _compute_lightweight(  # noqa: ANN202
        self,
        context: FoundryContext,
    ):
        inputs = {argument_name: LightweightTransformInput(i, context) for argument_name, i in self.inputs.items()}
        outputs = {
            argument_name: LightweightTransformOutput(o, argument_name, context)
            for argument_name, o in self.outputs.items()
        }
        kwargs = {**inputs, **outputs}

        if self._use_context:
            kwargs["ctx"] = TransformContext(context)

        self(**kwargs)

        return {name: i.df for name, i in outputs.items()}

    def _compute_lightweight_pandas(
        self,
        context: FoundryContext,
    ) -> pd.core.frame.DataFrame:
        inputs = {
            argument_name: LightweightTransformInput(i, context).pandas() for argument_name, i in self.inputs.items()
        }
        if self._use_context:
            inputs["ctx"] = TransformContext(context)
        return self(**inputs)

    def _compute_lightweight_polars(
        self,
        context: FoundryContext,
    ) -> pl.DataFrame:
        inputs = {
            argument_name: LightweightTransformInput(i, context).polars() for argument_name, i in self.inputs.items()
        }
        if self._use_context:
            inputs["ctx"] = TransformContext(context)
        return self(**inputs)


class TransformContext:
    """The TransformContext is passed to the transform function if ctx is the first argument."""

    def __init__(self, foundry_ctx: FoundryContext):
        self._foundry_ctx = foundry_ctx

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

        warnings.warn("is_incremental functionality not implemented in Foundry DevTools")
        return False

    @property
    def auth_header(self) -> str:
        return f"Bearer {self._foundry_ctx.token_provider.token}"


class TransformInput:
    """TransformInput class, passed when using @transform decorator."""

    def __init__(self, input_arg: Input, context: FoundryContext):
        self._input_arg = input_arg
        self._context = context

    @cached_property
    def branch(self) -> str:
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._input_arg.branch

    @cached_property
    def _dataset_identity(self) -> api_types.DatasetIdentity:
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._input_arg.get_dataset_identity()

    @cached_property
    def rid(self) -> api_types.Rid:
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._dataset_identity["dataset_rid"]

    @cached_property
    def path(self) -> api_types.FoundryPath:
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._dataset_identity["dataset_path"]

    def dataframe(self) -> pyspark.sql.DataFrame:
        """Returns the dataframe of this input.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                The dataframe for the dataset.
        """
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._input_arg.dataframe()

    def pandas(self) -> pd.core.frame.DataFrame:
        """Returns the pandas dataframe of this transform input.

        Returns:
            :external+pandas:py:class:`~pandas.DataFrame`:
                the pandas dataframe of this transform input.

        """
        return self.dataframe().toPandas()

    def filesystem(self) -> FileSystem:
        """Returns a read-only filesystem object, that has read and write functions.

        Returns:
            FileSystem:
                A `FileSystem` object for reading from `Foundry`.
        """
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return FileSystem(base_path=self._input_arg.get_local_path_to_dataset())


class LightweightTransformInput(TransformInput):
    """LightweightTransformInput class, passed when using @lightweight decorator."""

    def __init__(self, input_arg: Input, context: FoundryContext):
        super().__init__(input_arg, context)

    @cached_property
    def rid(self) -> api_types.Rid:
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._input_arg.get_dataset_identity()["dataset_rid"]

    @cached_property
    def _local_path(self) -> str:
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._input_arg.get_local_path_to_dataset()

    @cached_property
    def branch(self) -> str:
        """The branch of the Input."""
        if not self._input_arg._initialized:  # noqa: SLF001
            self._input_arg.init_input(self._context)
        return self._input_arg.branch

    @cached_property
    def _parquet_files(self) -> list[Path]:
        return list(Path(self._local_path).glob("**/*.parquet"))

    @cached_property
    def _csv_files(self) -> list[Path]:
        return list(Path(self._local_path).glob("**/*.csv"))

    def dataframe(self):
        msg = "Lightweight transforms do not support the dataframe method."
        raise NotImplementedError(msg)

    @property
    def path(self) -> str:
        """Download the dataset's underlying files and return a Path to them."""
        return self._local_path

    def pandas(self) -> pd.DataFrame:
        """A Pandas DataFrame containing the full view of the dataset."""
        from foundry_dev_tools._optional.pandas import pd

        return (
            pd.concat(map(pd.read_parquet, self._parquet_files), ignore_index=True)
            if self._parquet_files
            else pd.concat(map(pd.read_csv, self._csv_files), ignore_index=True)
        )

    def arrow(self) -> pa.Table:
        """A PyArrow table containing the full view of the dataset."""
        from foundry_dev_tools._optional.pyarrow import pa, pq

        return (
            pq.ParquetDataset(str(self._local_path) + "/", use_legacy_dataset=False).read()
            if self._parquet_files
            else pa.concat_tables([pa.csv.read_csv(p) for p in self._csv_files])
        )

    def polars(self, lazy: bool | None = False) -> pl.DataFrame | pl.LazyFrame:
        """A Polars DataFrame or LazyFrame containing the full view of the dataset.

        Args:
            lazy (bool, optional): Whether to return a LazyFrame or DataFrame. Defaults to False.
        """
        from foundry_dev_tools._optional.polars import pl

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
            else pl.read_csv(f"{self._local_path}/**/*.csv", rechunk=False, low_memory=True)
        )


class TransformOutput:
    """The output object passed into Transform objects at runtime."""

    def __init__(self, output: Output, argument_name: str, context: FoundryContext):
        """Initialize the TransformOutput.

        Args:
            output (Output): the output
            argument_name (str): the argument name
            context: a FoundryContext
        """
        self._fs = None
        self._df = None
        self._context = context
        self._argument_name = argument_name
        self.branch = "no-implemented-in-foundry-dev-tools"
        self.path = output.alias
        self.rid = "no-implemented-in-foundry-dev-tools"

    def dataframe(self) -> pyspark.sql.DataFrame | None:
        """Returns pyspark DataFrame.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                the dataframe of this output or None

        """
        return self._df

    def write_dataframe(self, df: pyspark.sql.DataFrame, **kwargs):  # noqa: ARG002
        """Storing dataframe as variable.

        Args:
            df (:external+spark:py:class:`~pyspark.sql.DataFrame`): spark dataframe
            **kwargs: unused

        """
        self._df = df

    def write_pandas(self, pandas_df: pd.core.frame.DataFrame):
        """Write the given :class:`pandas.DataFrame` to the dataset.

        Args:
            pandas_df (pandas.DataFrame): The dataframe to write.
        """
        self.write_dataframe(get_spark_session().createDataFrame(pandas_df))

    def _make_filesystem(self) -> FileSystem:
        if self._context.config.transforms_output_folder is not None:
            base_path = self._context.config.transforms_output_folder.joinpath(
                self._argument_name,
            )
            base_path.mkdir(parents=True, exist_ok=True)
            return FileSystem(base_path=base_path)
        return FileSystem()

    def filesystem(self) -> FileSystem:
        """Returns a temporary filesystem for the output that can be written to.

        Returns:
            an object with read write methods
        """
        if self._fs is None:
            self._fs = self._make_filesystem()
        return self._fs

    def set_mode(self, mode):  # noqa: ANN001
        """Not implemented in Foundry DevTools.

        Args:
            mode: write mode
        """


class LightweightTransformOutput(TransformOutput):
    """The output object passed to the user code at runtime.

    Its aim is to mimic a subset of the API of `TransformOutput`.
    """

    def __init__(self, output: Output, argument_name: str, context: FoundryContext):
        super().__init__(output, argument_name, context)
        self.branch = "no-implemented-in-foundry-dev-tools"
        self.path = output.alias
        self.rid = "no-implemented-in-foundry-dev-tools"
        self.df = None

    def write_pandas(
        self,
        pandas_df: pd.DataFrame,
        *args,
        **kwargs,
    ) -> None:
        """Write the given :class:`pandas.DataFrame` to the dataset."""
        self.write_table(pandas_df, *args, **kwargs)

    def write_table(
        self,
        df: pd.core.frame.DataFrame | pa.Table | pl.DataFrame | pl.LazyFrame,
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> None:
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
        from foundry_dev_tools._optional.polars import pl

        if not pl.__fake__ and isinstance(df, pl.LazyFrame):
            df = df.collect()  # noqa: PD901

        if isinstance(df, str | Path):
            if df != self.path_for_write_table:
                msg = f"Path '{df}' does not match expected path '{self.path_for_write_table}'"
                raise ValueError(msg)

            from foundry_dev_tools._optional.pandas import pd

            self.df = pd.read_parquet(df)  # make df pretty printable
        else:
            self.df = df

    @property
    def path_for_write_table(self) -> str | None:
        """Return the path for the dataset's files to be used with write_table."""
        return self.path


class FileStatus(NamedTuple):
    path: str
    size: str
    modified: str


"""A :class:`collections.namedtuple` capturing details about a `FoundryFS` file."""


class FileSystem:
    """File System for TransformOutput and TransformInput."""

    def __init__(self, base_path: str | PathLike | None = None):
        if base_path:
            self._fs = fs.open_fs(os.fspath(base_path))
        else:
            self._fs = fs.open_fs("mem://")

    def ls(self, glob: str | None = None, regex: str = ".*", show_hidden: bool = False) -> Iterator[FileStatus]:
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
            yield FileStatus(result_path, "size not implemented", "modified not implemented")

    def open(self, path: PathLike[str] | str, mode: str = "w", **kwargs) -> IO:
        """Open file in this filesystem.

        Args:
            path (str): the path to the file
            mode (str): usual file modes
            **kwargs: pass through

        Returns:
            IO:

        """
        self._fs.makedirs(os.path.dirname(path), recreate=True)  # noqa: PTH120
        return self._fs.open(os.fspath(path), mode, **kwargs)
