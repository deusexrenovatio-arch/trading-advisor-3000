from __future__ import annotations

import importlib
import pytest


def test_legacy_namespace_bridge_is_removed() -> None:
    legacy_module = "trading_advisor_3000" + ".app"
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(legacy_module)


def test_product_plane_runtime_import_still_resolves() -> None:
    runtime_module = importlib.import_module("trading_advisor_3000.product_plane.runtime")
    assert runtime_module is not None
