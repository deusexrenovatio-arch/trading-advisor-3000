from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.app.data_plane import run_sample_backfill


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"


def test_sample_backfill_builds_canonical_rows_for_whitelist(tmp_path: Path) -> None:
    report = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    assert report["source_rows"] == 4
    assert report["whitelisted_rows"] == 3
    assert report["canonical_rows"] == 2
    output_path = Path(str(report["output_path"]))
    assert output_path.exists()

    rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 2
    assert {row["contract_id"] for row in rows} == {"BR-6.26", "Si-6.26"}
    br_row = next(row for row in rows if row["contract_id"] == "BR-6.26")
    assert br_row["instrument_id"] == "BR"
    assert br_row["ts"] == "2026-03-16T10:00:00Z"
    assert br_row["close"] == 82.6
    assert br_row["open_interest"] == 21000
    assert "canonical_bars" in report["delta_schema_manifest"]


def test_sample_backfill_rejects_invalid_payload(tmp_path: Path) -> None:
    source = tmp_path / "invalid.jsonl"
    source.write_text(
        '{"contract_id":"BR-6.26","instrument_id":"BR","timeframe":"15m","ts_open":"2026-03-16T10:00:00Z","ts_close":"2026-03-16T10:15:00Z","open":82.1,"high":82.8,"low":81.9,"close":82.4,"volume":"1500","open_interest":21000}\n',
        encoding="utf-8",
    )

    try:
        run_sample_backfill(
            source_path=source,
            output_dir=tmp_path,
            whitelist_contracts={"BR-6.26"},
        )
    except ValueError as exc:
        assert "volume must be integer" in str(exc)
    else:
        raise AssertionError("expected ValueError")
