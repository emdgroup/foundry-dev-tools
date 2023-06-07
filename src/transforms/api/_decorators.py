"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # noqa: E501
import warnings

from transforms.api._dataset import Input, Output
from transforms.api._transform import Transform


def transform_df(output, **inputs):
    """Register the wrapped compute function as a dataframe transform.

    The ``transform_df`` decorator is used to construct a :class:`Transform` object from
    a compute function that accepts and returns :class:`pyspark.sql.DataFrame` objects. Similar
    to the :func:`transform` decorator, the input names become the compute function's parameter
    names. However, a ``transform_df`` accepts only a single :class:`Output` spec as a
    positional argument. The return value of the compute function is also a
    :class:`~pyspark.sql.DataFrame` that is automatically written out to the single output
    dataset.

    >>> from transforms.api import transform_df, Input, Output
    >>> @transform_df(
    ...     Output('/path/to/output/dataset'),  # An unnamed Output spec
    ...     first_input=Input('/path/to/first/input/dataset'),
    ...     second_input=Input('/path/to/second/input/dataset'),
    ... )
    ... def my_compute_function(first_input, second_input):
    ...     # type: (pyspark.sql.DataFrame, pyspark.sql.DataFrame) -> pyspark.sql.DataFrame
    ...     return first_input.union(second_input)

    Args:
        output (Output): The single :class:`Output` spec for the transform.
        **inputs (Input): kwargs comprised of named :class:`Input` specs.
    """

    def _transform_df(compute_func):
        return Transform(
            compute_func, {"output": output}, inputs=inputs, decorator="spark"
        )

    return _transform_df


def transform_pandas(output, **inputs):
    """Register the wrapped compute function as a Pandas transform.

    The ``transform_pandas`` decorator is used to construct a :class:`Transform` object from
    a compute function that accepts and returns :class:`pandas.DataFrame` objects. This
    decorator is similar to the :func:`transform_df` decorator,
    however the :class:`pyspark.sql.DataFrame`
    objects are converted to :class:`pandas.DataFrame` object before the computation,
    and converted back afterwards.

    >>> from transforms.api import transform_pandas, Input, Output
    >>> @transform_pandas(
    ...     Output('/path/to/output/dataset'),  # An unnamed Output spec
    ...     first_input=Input('/path/to/first/input/dataset'),
    ...     second_input=Input('/path/to/second/input/dataset'),
    ... )
    ... def my_compute_function(first_input, second_input):
    ...     # type: (pandas.DataFrame, pandas.DataFrame) -> pandas.DataFrame
    ...     return first_input.concat(second_input)

    Args:
        output (Output): The single :class:`Output` spec for the transform.
        **inputs (Input): kwargs comprised of named :class:`Input` specs.
    """

    def _transform_pandas(compute_func):
        return Transform(
            compute_func, {"output": output}, inputs=inputs, decorator="pandas"
        )

    return _transform_pandas


def transform(**kwargs):
    """Wrap up a compute function as a Transform object.

    >>> from transforms.api import transform, Input, Output
    >>> @transform(
    ...     first_input=Input('/path/to/first/input/dataset'),
    ...     second_input=Input('/path/to/second/input/dataset'),
    ...     first_output=Output('/path/to/first/output/dataset'),
    ...     second_output=Output('/path/to/second/output/dataset'),
    ... )
    ... def my_compute_function(first_input, second_input, first_output, second_output):
    ...     # type: (TransformInput, TransformInput, TransformOutput, TransformOutput) -> None
    ...     first_output.write_dataframe(first_input.dataframe())
    ...     second_output.write_dataframe(second_input.dataframe())

    Args:
        **kwargs (Param): kwargs comprised of named :class:`Param` or subclasses.

    Note:
        The compute function is responsible for writing data to its outputs.
    """

    def _transform(compute_func):
        return Transform(
            compute_func,
            outputs={k: v for k, v in kwargs.items() if isinstance(v, Output)},
            inputs={k: v for k, v in kwargs.items() if isinstance(v, Input)},
            decorator="transform",
        )

    return _transform


def incremental(
    require_incremental=False,
    semantic_version=1,
    snapshot_inputs=None,
    allow_retention=False,
):
    """Not implemented in local.

    Args:
        require_incremental: not implemented
        semantic_version: not implemented
        snapshot_inputs: not implemented
        allow_retention: not implemented

    Returns:
        _transform:
    """
    warnings.warn("@incremental functionality not implemented in Foundry DevTools")

    def _transform(compute_func):
        return compute_func

    return _transform
