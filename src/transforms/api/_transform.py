"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # pylint: disable=line-too-long

import collections
import inspect
import os
import re
from typing import Callable, Dict, Optional

import fs
import pandas as pd
import pyspark

from transforms.api import Input, Output

import foundry_dev_tools
from foundry_dev_tools.utils.caches.spark_caches import get_dataset_path
from foundry_dev_tools.utils.spark import get_spark_session


class Transform:
    """A Python Transform.

    Wraps a function and holds the Input and Output context.
    When the function is called, the Input and Output parameters
    are automatically passed.
    """

    def __init__(
        self,
        compute_func: Callable,
        outputs: Dict[str, Output] = None,
        inputs: Dict[str, Input] = None,
        decorator: str = "spark",
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
        for name, toutput in self.outputs.items():
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

    def compute(self):  # pylint: disable=arguments-differ
        """Execute the wrapped transform function."""
        if self._type == "pandas":
            return self._compute_pandas()
        if self._type == "transform":
            return self._compute_transform()
        return self._compute_spark()

    def _compute_spark(
        self,
    ) -> "pyspark.sql.DataFrame":  # pylint: disable=arguments-differ
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

    def _compute_pandas(self):  # pylint: disable=arguments-differ
        """Execute the wrapped transform function.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                the result of the transforms function

        """
        kwargs = {name: i.dataframe().toPandas() for name, i in self.inputs.items()}
        if self._use_context:
            kwargs["ctx"] = TransformContext()

        output_df = self(**kwargs)

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


class TransformContext:  # pylint: disable=too-few-public-methods
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
        # pylint: disable=import-outside-toplevel
        import warnings

        warnings.warn(
            "is_incremental functionality not implemented in Foundry DevTools"
        )
        return False


class TransformInput:
    """TransformInput class, passed when using @transform decorator."""

    def __init__(self, input_arg: Input):
        self._dataframe = input_arg.dataframe()
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
        return self._dataframe

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
        return FileSystem(
            base_path=get_dataset_path(
                foundry_dev_tools.Configuration["cache_dir"], self._dataset_identity
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

    def dataframe(self) -> Optional[pyspark.sql.DataFrame]:
        """Returns pyspark DataFrame.

        Returns:
            :external+spark:py:class:`~pyspark.sql.DataFrame`:
                the dataframe of this output or None

        """
        return self._df

    def write_dataframe(self, df: pyspark.sql.DataFrame, **kwargs):
        # pylint: disable=invalid-name, unused-argument
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
        if "transforms_output_folder" in foundry_dev_tools.Configuration:
            base_path = os.sep.join(
                [
                    foundry_dev_tools.Configuration["transforms_output_folder"],
                    self._argument_name,
                ]
            )
            os.makedirs(base_path, exist_ok=True)
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


class FileStatus(collections.namedtuple("FileStatus", ["path", "size", "modified"])):
    """A :class:`collections.namedtuple` capturing details about a `FoundryFS` file."""


class FileSystem:
    """File System for TransformOutput and TransformInput."""

    def __init__(self, base_path=None):
        if base_path:
            self._fs = fs.open_fs(base_path)
        else:
            self._fs = fs.open_fs("mem://")

    def ls(
        self, glob: Optional[str] = None, regex: str = ".*", show_hidden: bool = False
    ):  # pylint: disable=invalid-name
        """Recurses through all directories and lists all files matching the given patterns.

        Starting from the root directory of the dataset.

        Args:
            glob (Optional[str]): A unix file matching pattern. Also supports globstar.
            regex (Optional[str]): A regex pattern against which to match filenames.
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
            # pylint: disable=import-outside-toplevel
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
        return self._fs.open(path, mode, **kwargs)
