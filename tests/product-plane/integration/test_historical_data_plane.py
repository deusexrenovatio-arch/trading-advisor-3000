from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.data_plane import run_sample_backfill


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"


def test_sample_backfill_builds_canonical_rows_for_whitelist(tmp_path: Path) -> None:
    report = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    assert report["source_rows"] == 4
    assert report["whitelisted_rows"] == 3
    assert report["canonical_rows"] == 2
    assert report["incremental_rows"] == 2
    assert report["stale_rows"] == 1
    output_path = Path(str(report["output_path"]))
    assert output_path.exists()
    assert (output_path / "_delta_log").exists()

    rows = read_delta_table_rows(output_path)
    assert len(rows) == 2
    assert {row["contract_id"] for row in rows} == {"BR-6.26", "Si-6.26"}
    br_row = next(row for row in rows if row["contract_id"] == "BR-6.26")
    assert br_row["instrument_id"] == "BR"
    assert br_row["ts"] == "2026-03-16T10:00:00Z"
    assert br_row["close"] == 82.6
    assert br_row["open_interest"] == 21000
    assert (Path(str(report["output_paths"]["canonical_instruments"])) / "_delta_log").exists()
    assert (Path(str(report["output_paths"]["canonical_contracts"])) / "_delta_log").exists()
    assert (Path(str(report["output_paths"]["canonical_session_calendar"])) / "_delta_log").exists()
    assert (Path(str(report["output_paths"]["canonical_roll_map"])) / "_delta_log").exists()
    assert "canonical_bars" in report["delta_schema_manifest"]
    assert "canonical_instruments" in report["delta_schema_manifest"]
    assert "canonical_contracts" in report["delta_schema_manifest"]
    assert "canonical_session_calendar" in report["delta_schema_manifest"]
    assert "canonical_roll_map" in report["delta_schema_manifest"]


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


def test_sample_backfill_is_incremental_append_only_and_idempotent(tmp_path: Path) -> None:
    first = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )
    second = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    raw_path = Path(str(second["output_paths"]["raw_market_backfill"]))
    raw_rows = read_delta_table_rows(raw_path)
    assert first["incremental_rows"] == 2
    assert second["incremental_rows"] == 0
    assert second["deduplicated_rows"] >= 2
    assert len(raw_rows) == 2


def test_sample_backfill_disprover_fails_when_physical_delta_data_is_deleted(tmp_path: Path) -> None:
    report = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    bars_path = Path(str(report["output_paths"]["canonical_bars"]))
    data_files = [item for item in bars_path.rglob("*.parquet") if item.is_file()]
    assert data_files, "expected physical Delta parquet files"
    data_files[0].unlink()

    # Manifest-level metadata remains available, but runtime read must fail without physical table data.
    assert "canonical_bars" in report["delta_schema_manifest"]
    assert (bars_path / "_delta_log").exists()
    try:
        read_delta_table_rows(bars_path)
    except Exception:
        return
    raise AssertionError("expected Delta runtime read failure after deleting physical output")
