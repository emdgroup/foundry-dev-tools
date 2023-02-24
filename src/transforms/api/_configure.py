"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # pylint: disable=line-too-long


def configure(*args, **kwargs):
    # pylint: disable=unused-argument
    """Not implemented in local.

    Args:
        args: unused
        kwargs: unused

    Returns:
        the compute_func
    """
    # pylint: disable=import-outside-toplevel
    import warnings

    warnings.warn("@configure functionality not implemented in Foundry DevTools")

    def _configure(compute_func):
        return compute_func

    return _configure
