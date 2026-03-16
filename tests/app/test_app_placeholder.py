from __future__ import annotations

from trading_advisor_3000 import __version__, build_app_metadata


def test_placeholder_metadata_contract() -> None:
    payload = build_app_metadata()
    assert payload["name"] == "trading-advisor-3000"
    assert payload["shell_mode"] == "ai-delivery-shell"
    assert payload["domain_logic_enabled"] is False


def test_package_version_declared() -> None:
    assert isinstance(__version__, str)
    assert __version__
