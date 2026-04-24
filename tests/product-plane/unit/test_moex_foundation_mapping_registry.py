from __future__ import annotations

from pathlib import Path

import yaml

from trading_advisor_3000.product_plane.data_plane.moex.foundation import (
    load_mapping_registry,
    load_universe,
    validate_mapping_registry,
    validate_universe_mapping_alignment,
)


def _write_yaml(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding="utf-8")


def test_mapping_registry_rejects_duplicate_active_finam_symbol(tmp_path: Path) -> None:
    config = {
        "version": 1,
        "registry_id": "dup-finam",
        "mappings": [
            {
                "internal_id": "FUT_A",
                "finam_symbol": "A@MOEX",
                "moex_engine": "futures",
                "moex_market": "forts",
                "moex_board": "RFUD",
                "moex_secid": "A1",
                "asset_class": "futures",
                "asset_group": "commodity",
                "mapping_version": 1,
                "is_active": True,
                "activated_at_utc": "2026-04-02T00:00:00Z",
                "deactivated_at_utc": "",
                "change_reason": "initial",
            },
            {
                "internal_id": "FUT_B",
                "finam_symbol": "A@MOEX",
                "moex_engine": "futures",
                "moex_market": "forts",
                "moex_board": "RFUD",
                "moex_secid": "B1",
                "asset_class": "futures",
                "asset_group": "index",
                "mapping_version": 1,
                "is_active": True,
                "activated_at_utc": "2026-04-02T00:00:00Z",
                "deactivated_at_utc": "",
                "change_reason": "initial",
            },
        ],
    }
    path = tmp_path / "mapping.yaml"
    _write_yaml(path, config)
    rows = load_mapping_registry(path)
    try:
        validate_mapping_registry(rows)
    except ValueError as exc:
        assert "duplicate active mapping for finam_symbol" in str(exc)
    else:
        raise AssertionError("expected duplicate active finam symbol failure")


def test_mapping_registry_allows_soft_deactivated_history(tmp_path: Path) -> None:
    mapping_path = Path("configs/moex_foundation/instrument_mapping_registry.v1.yaml")
    universe_path = Path("configs/moex_foundation/universe/moex-futures-priority.v1.yaml")

    mappings = load_mapping_registry(mapping_path)
    universe = load_universe(universe_path)

    validate_mapping_registry(mappings)
    validate_universe_mapping_alignment(universe, mappings)
