from __future__ import annotations

try:
    import foundry_sdk
    import foundry_sdk.v2

    foundry_sdk.__fake__ = False
except ImportError:
    from foundry_dev_tools._optional import FakeModule

    foundry_sdk = FakeModule("foundry_sdk")

__all__ = ["foundry_sdk"]
