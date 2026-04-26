from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.canonical import resolve_moex_t3_storage
from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows


CANONICAL_COLUMNS: dict[str, str] = {
    "contract_id": "string",
    "instrument_id": "string",
    "timeframe": "string",
    "ts": "timestamp",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "bigint",
    "open_interest": "bigint",
}


def test_moex_t3_storage_resolves_authoritative_baseline_manifest(tmp_path: Path) -> None:
    data_root = tmp_path / "trading-advisor-3000-nightly"
    canonical_root = data_root / "canonical" / "moex" / "baseline-4y-current"
    features_root = data_root / "derived" / "moex" / "features"
    indicators_root = data_root / "derived" / "moex" / "indicators"
    bars_path = canonical_root / "canonical_bars.delta"
    provenance_path = canonical_root / "canonical_bar_provenance.delta"
    session_calendar_path = canonical_root / "canonical_session_calendar.delta"
    roll_map_path = canonical_root / "canonical_roll_map.delta"

    row = {
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "15m",
        "ts": "2026-04-10T10:00:00Z",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1000,
        "open_interest": 20000,
    }
    write_delta_table_rows(table_path=bars_path, rows=[row], columns=CANONICAL_COLUMNS)
    write_delta_table_rows(table_path=provenance_path, rows=[row], columns=CANONICAL_COLUMNS)
    features_root.mkdir(parents=True)
    indicators_root.mkdir(parents=True)

    manifest_path = canonical_root / "baseline-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "baseline_id": "moex-baseline-4y-current",
                "storage_mode": "materialized-autonomous",
                "data_root": data_root.as_posix(),
                "storage_layout": {
                    "canonical_root": canonical_root.as_posix(),
                    "derived_root": (data_root / "derived" / "moex").as_posix(),
                    "features_root": features_root.as_posix(),
                    "indicators_root": indicators_root.as_posix(),
                },
                "baseline_paths": {
                    "canonical_bars": bars_path.as_posix(),
                    "canonical_bar_provenance": provenance_path.as_posix(),
                    "canonical_session_calendar": session_calendar_path.as_posix(),
                    "canonical_roll_map": roll_map_path.as_posix(),
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    binding = resolve_moex_t3_storage(baseline_manifest_path=manifest_path)

    assert binding.baseline_id == "moex-baseline-4y-current"
    assert binding.canonical_bars_path == bars_path.resolve()
    assert binding.canonical_session_calendar_path == session_calendar_path.resolve()
    assert binding.canonical_roll_map_path == roll_map_path.resolve()
    assert binding.features_root == features_root.resolve()
    assert binding.indicators_root == indicators_root.resolve()
    assert binding.to_dict()["source"] == "explicit-baseline-manifest"
