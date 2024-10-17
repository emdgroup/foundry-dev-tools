"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # noqa: E501

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from transforms.api._dataset import Input, Output
from transforms.api._transform import Transform

if TYPE_CHECKING:
    from collections.abc import Callable

    import pandas as pd
    import polars as pl
    import pyspark.sql


def lightweight(
    _maybe_transform: Transform | None = None,
    *,
    cpu_cores: float | None = None,
    memory_mb: float | None = None,
    memory_gb: float | None = None,
    gpu_type: str | None = None,
    container_image: str | None = None,
    container_tag: str | None = None,
    container_shell_command: str | None = None,
) -> Callable[[Transform], Transform]:
    """Turn a transform into a Lightweight transform.

    A Lightweight transform is a transform that runs without Spark, on a single node. Lightweight transforms are faster
    and more cost-effective for small to medium-sized datasets. Lightweight transforms also provide more methods for
    accessing datasets; however, they only support a subset of the API of a regular transforms, including Pandas and
    the filesystem API. For more information, see the public documentation available at
    https://www.palantir.com/docs/foundry/transforms-python/lightweight-overview

    Args:
        cpu_cores (float, optional): The number of CPU cores to request for the transform's container,
            can be a fraction, not implemented in Foundry DevTools.
        memory_mb (float, optional): The amount of memory to request for the container, in MB,
            not implemented in Foundry DevTools.
        memory_gb (float, optional): The amount of memory to request for the container, in GB,
            not implemented in Foundry DevTools.
        gpu_type (str, optional): The type of GPU to allocate for the transform.
        container_image (str, optional): not implemented in Foundry DevTools
        container_tag (str, optional): not implemented in Foundry DevTools
        container_shell_command (str, optional): not implemented in Foundry DevTools

    Notes:
        Either `memory_gb` or `memory_mb` can be specified, but not both.

        Specifying resources has no effect when running locally.

    Examples:
        >>> @lightweight
        ... @transform(
        ...     my_input=Input('/input'),
        ...     my_output=Output('/output')
        ... )
        ... def compute_func(my_input, my_output):
        ...     my_output.write_table(my_input.polars())

        >>> @lightweight()
        ... @transform(
        ...    my_input=Input('/input'),
        ...    my_output=Output('/output')
        ... )
        ... def compute_func(my_input, my_output):
        ...     for file in my_input.filesystem().ls():
        ...         with my_input.filesystem().open(file.path) as f1:
        ...             with my_output.filesystem().open(file.path, "w") as f2:
        ...                 f2.write(f1.read())
    """
    if container_image or container_tag or container_shell_command:
        msg = (
            "BYOC workflows enabled through container_image, container_tag, and container_shell_command "
            "are not implemented in Foundry DevTools"
        )
        raise NotImplementedError(
            msg,
        )

    if memory_mb is not None and memory_gb is not None:
        msg = "Only one of memory_mb or memory_gb can be specified"
        raise ValueError(msg)

    if cpu_cores or memory_mb or memory_gb or gpu_type:
        warnings.warn("Setting resources for @lightweight transforms will have no effect when running locally.")

    def _lightweight(transform: Transform) -> Transform:
        if not isinstance(transform, Transform):
            msg = (
                "lightweight decorator must be used on a Transform object. "
                "Perhaps you didn't put @lightweight as the top-most decorator?"
            )
            raise TypeError(
                msg,
            )

        if transform._type not in {"transform", "pandas"}:  # noqa: SLF001
            msg = "You can only use @lightweight on @transform or @transform_pandas"
            raise ValueError(msg)

        return Transform(
            transform._compute_func,  # noqa: SLF001
            outputs=transform.outputs,
            inputs=transform.inputs,
            decorator="lightweight-pandas" if transform._type == "pandas" else "lightweight",  # noqa: SLF001
        )

    return _lightweight if _maybe_transform is None else _lightweight(_maybe_transform)


def transform_polars(output: Output, **inputs: Input) -> Callable[[Callable[..., pl.DataFrame]], Transform]:
    """Register the wrapped compute function as a Polars transform.

    Note:
        To use the Polars library, you must add ``polars`` as a **run** dependency in your ``meta.yml`` file .
        For more information, refer to the
        :ref:`section describing the meta.yml file <transforms-python-proj-structure-meta>`.

        The ``transform_polars`` decorator is just a thin wrapper around the ``lightweight`` decorator. Using it
        results in the creation of a lightweight transform which lacks some features of a regular transform.

        This works similarly to the :func:`transform_pandas` decorator, however, instead of Pandas DataFrames, the
        user code is given and is expected to return Polars DataFrames.

        Note that spark profiles can't be used with lightweight transforms, hence, neither with @transforms_polars.

        >>> @transform_polars(
        ...     Output('ri.main.foundry.dataset.out'),  # An unnamed Output spec
        ...     first_input=Input('ri.main.foundry.dataset.in1'),
        ...     second_input=Input('ri.main.foundry.dataset.in2'),
        ... )
        ... def my_compute_function(first_input, second_input):
        ...     # type: (polars.DataFrame, polars.DataFrame) -> polars.DataFrame
        ...     return first_input.join(second_input, on='id', how="inner")

    Args:
        output (Output): The single :class:`Output` spec for the transform.
        **inputs (Input): kwargs comprised of named :class:`Input` specs.
    """

    def _transform_polars(compute_function: Callable[..., pl.DataFrame]) -> Transform:
        return Transform(
            compute_function,
            outputs={"output": output},
            inputs=inputs,
            decorator="lightweight-polars",
        )

    return _transform_polars


def transform_df(output: Output, **inputs: Input) -> Callable[[Callable[..., pyspark.sql.DataFrame]], Transform]:
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

    def _transform_df(compute_func: Callable[..., pyspark.sql.DataFrame]) -> Transform:
        return Transform(compute_func, {"output": output}, inputs=inputs, decorator="spark")

    return _transform_df


def transform_pandas(output: Output, **inputs: Input) -> Callable[[Callable[..., pd.DataFrame]], Transform]:
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

    def _transform_pandas(compute_func: Callable[..., pd.DataFrame]) -> Transform:
        return Transform(compute_func, {"output": output}, inputs=inputs, decorator="pandas")

    return _transform_pandas


def transform(**kwargs: Input | Output) -> Callable[[Callable[..., None]], Transform]:
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

    def _transform(compute_func: Callable[..., None]) -> Transform:
        return Transform(
            compute_func,
            outputs={k: v for k, v in kwargs.items() if isinstance(v, Output)},
            inputs={k: v for k, v in kwargs.items() if isinstance(v, Input)},
            decorator="transform",
        )

    return _transform


def incremental(
    require_incremental: bool = False,  # noqa: ARG001
    semantic_version: int = 1,  # noqa: ARG001
    snapshot_inputs=None,  # noqa: ARG001,ANN001
    allow_retention: bool = False,  # noqa: ARG001
) -> Callable:
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

    def _transform(compute_func: Callable) -> Callable:
        return compute_func

    return _transform
