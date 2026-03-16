from __future__ import annotations

import json
from pathlib import Path

from .canonical import build_canonical_bars, run_data_quality_checks
from .ingestion import ingest_raw_backfill
from .schemas import phase2a_delta_schema_manifest


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def run_sample_backfill(
    *,
    source_path: Path,
    output_dir: Path,
    whitelist_contracts: set[str],
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_rows = ingest_raw_backfill(source_path, whitelist_contracts=whitelist_contracts)
    canonical = build_canonical_bars(raw_rows)
    quality_errors = run_data_quality_checks(canonical, whitelist_contracts=whitelist_contracts)
    if quality_errors:
        raise ValueError("data quality failed: " + "; ".join(quality_errors))

    output_path = output_dir / "canonical_bars.sample.jsonl"
    _write_jsonl(output_path, [item.to_dict() for item in canonical])

    return {
        "raw_rows": len(raw_rows),
        "canonical_rows": len(canonical),
        "output_path": output_path.as_posix(),
        "delta_schema_manifest": phase2a_delta_schema_manifest(),
    }
