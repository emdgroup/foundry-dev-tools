from __future__ import annotations

from types import ModuleType
from typing import Any


class FakeModule(ModuleType):
    __fake__ = True

    def __init__(self, name: str):
        self.msg = f"Missing optional dependency '{name}'. Use conda or pip to install {name}"
        super().__init__(name, self.msg)

    def __getattr__(self, attr: Any) -> Any:  # noqa: ANN401
        if isinstance(attr, str) and attr.startswith("__"):
            object.__getattribute__(self, attr)
        raise ImportError(self.msg)
