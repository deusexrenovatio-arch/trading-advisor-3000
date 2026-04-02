from __future__ import annotations

"""Compatibility bridge for the dual-surface rename migration.

This package temporarily maps `trading_advisor_3000.product_plane` to the
current legacy runtime package `trading_advisor_3000.app` until physical
namespace cutover lands in phase 4.
"""

from importlib import import_module
from types import ModuleType
from typing import Any


_LEGACY_MODULE_NAME = "trading_advisor_3000.app"
_legacy_module: ModuleType = import_module(_LEGACY_MODULE_NAME)

# Keep nested imports working during dual-path compatibility mode.
if hasattr(_legacy_module, "__path__"):
    __path__ = _legacy_module.__path__  # type: ignore[assignment]

legacy_all = getattr(_legacy_module, "__all__", None)
if isinstance(legacy_all, list):
    __all__ = list(legacy_all)
elif isinstance(legacy_all, tuple):
    __all__ = list(legacy_all)


def __getattr__(name: str) -> Any:
    return getattr(_legacy_module, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_legacy_module)))
