"""The exposed Function definitions and docstrings is Copyright © 2023 Palantir Technologies Inc. and/or affiliates (“Palantir”). All rights reserved.

https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/
https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api-classes/

"""  # noqa: E501
import warnings


def configure(*args, **kwargs):
    """Not implemented in local.

    Args:
        args: unused
        kwargs: unused

    Returns:
        the compute_func
    """
    warnings.warn("@configure functionality not implemented in Foundry DevTools")

    def _configure(compute_func):
        return compute_func

    return _configure
