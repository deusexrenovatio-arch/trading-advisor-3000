from __future__ import annotations

from pathlib import Path

from .canonical import build_canonical_dataset, run_data_quality_checks
from .delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_row_batches,
    write_delta_table_rows,
)
from .ingestion import ingest_raw_backfill
from .schemas import historical_data_delta_schema_manifest


def _fixture_session_intervals_for_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    sessions = sorted({(str(row["instrument_id"]), str(row["ts_open"])[:10]) for row in rows})
    return [
        {
            "instrument_id": instrument_id,
            "session_date": session_date,
            "interval_id": f"{instrument_id}-{session_date}-regular-1",
            "interval_seq": 1,
            "expected_open_ts": f"{session_date}T10:00:00Z",
            "expected_close_ts": f"{session_date}T18:45:00Z",
            "session_class": "regular",
            "interval_type": "regular_trading",
            "policy_id": "sample-official-session-fixture-v1",
            "source_id": "sample-official-session-fixture",
            "source_document_hash": "sha256:sample-fixture",
        }
        for instrument_id, session_date in sessions
    ]


def _fixture_bar_provenance_for_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "contract_id": row["contract_id"],
            "instrument_id": row["instrument_id"],
            "timeframe": row["timeframe"],
            "ts": row["ts_open"],
            "bar_start_ts": row["ts_open"],
            "bar_end_ts": row["ts_close"],
            "session_interval_id": (f"{row['instrument_id']}-{str(row['ts_open'])[:10]}-regular-1"),
            "source_provider": "sample_fixture",
            "source_timeframe": row["timeframe"],
            "source_interval": 1,
            "source_run_id": "sample-backfill",
            "source_ingest_run_id": "sample-backfill",
            "source_row_count": 1,
            "source_ts_open_first": row["ts_open"],
            "source_ts_close_last": row["ts_close"],
            "open_interest_imputed": False,
            "build_run_id": "sample-backfill",
            "built_at_utc": "2026-01-01T00:00:00Z",
        }
        for row in rows
    ]


def run_sample_backfill(
    *,
    source_path: Path,
    output_dir: Path,
    whitelist_contracts: set[str],
) -> dict[str, object]:
    delta_schema_manifest = historical_data_delta_schema_manifest()
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_output_path = output_dir / "raw_market_backfill.delta"
    existing_raw_rows = (
        read_delta_table_rows(raw_output_path) if has_delta_log(raw_output_path) else []
    )

    ingestion_batch = ingest_raw_backfill(
        source_path,
        whitelist_contracts=whitelist_contracts,
        existing_rows=existing_raw_rows,
    )
    write_delta_table_rows(
        table_path=raw_output_path,
        rows=ingestion_batch.rows,
        columns=delta_schema_manifest["raw_market_backfill"]["columns"],
    )

    session_intervals = _fixture_session_intervals_for_rows(ingestion_batch.rows)
    bar_provenance = _fixture_bar_provenance_for_rows(ingestion_batch.rows)
    dataset = build_canonical_dataset(
        ingestion_batch.rows,
        session_intervals=session_intervals,
    )
    quality_errors = run_data_quality_checks(dataset.bars, whitelist_contracts=whitelist_contracts)
    if quality_errors:
        raise ValueError("data quality failed: " + "; ".join(quality_errors))

    bars_output_path = output_dir / "canonical_bars.delta"
    bar_provenance_output_path = output_dir / "canonical_bar_provenance.delta"
    instruments_output_path = output_dir / "canonical_instruments.delta"
    contracts_output_path = output_dir / "canonical_contracts.delta"
    session_intervals_output_path = output_dir / "canonical_session_intervals.delta"
    session_calendar_output_path = output_dir / "canonical_session_calendar.delta"
    roll_map_output_path = output_dir / "canonical_roll_map.delta"

    write_delta_table_row_batches(
        table_path=bars_output_path,
        row_batches=iter([[item.to_dict() for item in dataset.bars]]),
        columns=delta_schema_manifest["canonical_bars"]["columns"],
        max_rows_per_delta_write=65_536,
    )
    write_delta_table_row_batches(
        table_path=bar_provenance_output_path,
        row_batches=iter([bar_provenance]),
        columns=delta_schema_manifest["canonical_bar_provenance"]["columns"],
        max_rows_per_delta_write=65_536,
    )
    write_delta_table_row_batches(
        table_path=instruments_output_path,
        row_batches=iter([[item.to_dict() for item in dataset.instruments]]),
        columns=delta_schema_manifest["canonical_instruments"]["columns"],
        max_rows_per_delta_write=65_536,
    )
    write_delta_table_row_batches(
        table_path=contracts_output_path,
        row_batches=iter([[item.to_dict() for item in dataset.contracts]]),
        columns=delta_schema_manifest["canonical_contracts"]["columns"],
        max_rows_per_delta_write=65_536,
    )
    write_delta_table_row_batches(
        table_path=session_intervals_output_path,
        row_batches=iter([session_intervals]),
        columns=delta_schema_manifest["canonical_session_intervals"]["columns"],
        max_rows_per_delta_write=65_536,
    )
    write_delta_table_row_batches(
        table_path=session_calendar_output_path,
        row_batches=iter([[item.to_dict() for item in dataset.session_calendar]]),
        columns=delta_schema_manifest["canonical_session_calendar"]["columns"],
        max_rows_per_delta_write=65_536,
    )
    write_delta_table_row_batches(
        table_path=roll_map_output_path,
        row_batches=iter([[item.to_dict() for item in dataset.roll_map]]),
        columns=delta_schema_manifest["canonical_roll_map"]["columns"],
        max_rows_per_delta_write=65_536,
    )

    return {
        "source_rows": ingestion_batch.source_rows,
        "whitelisted_rows": ingestion_batch.whitelisted_rows,
        "canonical_rows": len(dataset.bars),
        "incremental_rows": ingestion_batch.incremental_rows,
        "deduplicated_rows": ingestion_batch.deduplicated_rows,
        "stale_rows": ingestion_batch.stale_rows,
        "watermark_by_key": ingestion_batch.watermark_by_key,
        "output_path": bars_output_path.as_posix(),
        "output_paths": {
            "raw_market_backfill": raw_output_path.as_posix(),
            "canonical_bars": bars_output_path.as_posix(),
            "canonical_bar_provenance": bar_provenance_output_path.as_posix(),
            "canonical_instruments": instruments_output_path.as_posix(),
            "canonical_contracts": contracts_output_path.as_posix(),
            "canonical_session_intervals": session_intervals_output_path.as_posix(),
            "canonical_session_calendar": session_calendar_output_path.as_posix(),
            "canonical_roll_map": roll_map_output_path.as_posix(),
        },
        "delta_schema_manifest": delta_schema_manifest,
    }
