from __future__ import annotations

from pathlib import Path

from .canonical import build_canonical_dataset, run_data_quality_checks
from .delta_runtime import has_delta_log, read_delta_table_rows, write_delta_table_rows
from .ingestion import ingest_raw_backfill
from .schemas import phase2a_delta_schema_manifest


def run_sample_backfill(
    *,
    source_path: Path,
    output_dir: Path,
    whitelist_contracts: set[str],
) -> dict[str, object]:
    delta_schema_manifest = phase2a_delta_schema_manifest()
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_output_path = output_dir / "raw_market_backfill.delta"
    existing_raw_rows = read_delta_table_rows(raw_output_path) if has_delta_log(raw_output_path) else []

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

    dataset = build_canonical_dataset(ingestion_batch.rows)
    quality_errors = run_data_quality_checks(dataset.bars, whitelist_contracts=whitelist_contracts)
    if quality_errors:
        raise ValueError("data quality failed: " + "; ".join(quality_errors))

    bars_output_path = output_dir / "canonical_bars.delta"
    instruments_output_path = output_dir / "canonical_instruments.delta"
    contracts_output_path = output_dir / "canonical_contracts.delta"
    session_calendar_output_path = output_dir / "canonical_session_calendar.delta"
    roll_map_output_path = output_dir / "canonical_roll_map.delta"

    write_delta_table_rows(
        table_path=bars_output_path,
        rows=[item.to_dict() for item in dataset.bars],
        columns=delta_schema_manifest["canonical_bars"]["columns"],
    )
    write_delta_table_rows(
        table_path=instruments_output_path,
        rows=[item.to_dict() for item in dataset.instruments],
        columns=delta_schema_manifest["canonical_instruments"]["columns"],
    )
    write_delta_table_rows(
        table_path=contracts_output_path,
        rows=[item.to_dict() for item in dataset.contracts],
        columns=delta_schema_manifest["canonical_contracts"]["columns"],
    )
    write_delta_table_rows(
        table_path=session_calendar_output_path,
        rows=[item.to_dict() for item in dataset.session_calendar],
        columns=delta_schema_manifest["canonical_session_calendar"]["columns"],
    )
    write_delta_table_rows(
        table_path=roll_map_output_path,
        rows=[item.to_dict() for item in dataset.roll_map],
        columns=delta_schema_manifest["canonical_roll_map"]["columns"],
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
            "canonical_instruments": instruments_output_path.as_posix(),
            "canonical_contracts": contracts_output_path.as_posix(),
            "canonical_session_calendar": session_calendar_output_path.as_posix(),
            "canonical_roll_map": roll_map_output_path.as_posix(),
        },
        "delta_schema_manifest": delta_schema_manifest,
    }
