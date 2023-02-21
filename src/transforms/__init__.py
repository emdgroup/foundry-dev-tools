# -*- coding: utf-8 -*-
"""The transforms module is a stub for the Foundry API."""
from pkg_resources import DistributionNotFound, get_distribution

try:
    # Change here if project is renamed and does not equal the package name
    DIST_NAME = "foundry_dev_tools"
    __version__ = get_distribution(DIST_NAME).version
except DistributionNotFound:
    __version__ = "unknown"
finally:
    del get_distribution, DistributionNotFound
