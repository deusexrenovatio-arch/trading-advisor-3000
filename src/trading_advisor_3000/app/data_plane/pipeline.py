from __future__ import annotations

import json
from pathlib import Path

from .canonical import build_canonical_dataset, run_data_quality_checks
from .ingestion import ingest_raw_backfill
from .schemas import phase2a_delta_schema_manifest


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        payload = json.loads(raw)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _append_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.touch(exist_ok=True)
        return
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def run_sample_backfill(
    *,
    source_path: Path,
    output_dir: Path,
    whitelist_contracts: set[str],
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_output_path = output_dir / "raw_market_backfill.sample.jsonl"
    existing_raw_rows = _read_jsonl(raw_output_path)

    ingestion_batch = ingest_raw_backfill(
        source_path,
        whitelist_contracts=whitelist_contracts,
        existing_rows=existing_raw_rows,
    )
    _append_jsonl(raw_output_path, ingestion_batch.new_rows)

    dataset = build_canonical_dataset(ingestion_batch.rows)
    quality_errors = run_data_quality_checks(dataset.bars, whitelist_contracts=whitelist_contracts)
    if quality_errors:
        raise ValueError("data quality failed: " + "; ".join(quality_errors))

    bars_output_path = output_dir / "canonical_bars.sample.jsonl"
    instruments_output_path = output_dir / "canonical_instruments.sample.jsonl"
    contracts_output_path = output_dir / "canonical_contracts.sample.jsonl"
    session_calendar_output_path = output_dir / "canonical_session_calendar.sample.jsonl"
    roll_map_output_path = output_dir / "canonical_roll_map.sample.jsonl"

    _write_jsonl(bars_output_path, [item.to_dict() for item in dataset.bars])
    _write_jsonl(instruments_output_path, [item.to_dict() for item in dataset.instruments])
    _write_jsonl(contracts_output_path, [item.to_dict() for item in dataset.contracts])
    _write_jsonl(session_calendar_output_path, [item.to_dict() for item in dataset.session_calendar])
    _write_jsonl(roll_map_output_path, [item.to_dict() for item in dataset.roll_map])

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
        "delta_schema_manifest": phase2a_delta_schema_manifest(),
    }
