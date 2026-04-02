from __future__ import annotations

import importlib


def test_product_plane_bridge_exposes_legacy_package_path() -> None:
    product_plane = importlib.import_module("trading_advisor_3000.product_plane")
    legacy_app = importlib.import_module("trading_advisor_3000.app")

    assert hasattr(product_plane, "__path__")
    assert list(product_plane.__path__) == list(legacy_app.__path__)


def test_product_plane_bridge_resolves_runtime_submodule() -> None:
    bridged_runtime = importlib.import_module("trading_advisor_3000.product_plane.runtime")
    legacy_runtime = importlib.import_module("trading_advisor_3000.app.runtime")

    assert bridged_runtime.__file__ == legacy_runtime.__file__
