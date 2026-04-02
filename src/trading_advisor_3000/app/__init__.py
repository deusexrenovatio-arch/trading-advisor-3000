from __future__ import annotations

"""Legacy compatibility bridge for dual-surface namespace migration.

This package temporarily maps the legacy runtime import root to the canonical
runtime namespace `trading_advisor_3000.product_plane` during phase 4/5
migration windows.
"""

from importlib import import_module
from types import ModuleType
from typing import Any

_CANONICAL_MODULE_NAME = "trading_advisor_3000.product_plane"
_canonical_module: ModuleType = import_module(_CANONICAL_MODULE_NAME)

# Keep nested legacy imports working during compatibility window.
if hasattr(_canonical_module, "__path__"):
    __path__ = _canonical_module.__path__  # type: ignore[assignment]

canonical_all = getattr(_canonical_module, "__all__", None)
if isinstance(canonical_all, list):
    __all__ = list(canonical_all)
elif isinstance(canonical_all, tuple):
    __all__ = list(canonical_all)


def __getattr__(name: str) -> Any:
    return getattr(_canonical_module, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_canonical_module)))
