from __future__ import annotations

import json
from pathlib import Path

from .canonical import build_canonical_dataset, run_data_quality_checks
from .ingestion import ingest_raw_backfill
from .providers import (
    DEFAULT_MOEX_ISS_BASE_URL,
    build_phase9_dataset_version,
    default_phase9_pilot_universe,
    fetch_moex_historical_bars,
    get_phase9_provider_contract,
)
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


def run_phase9_historical_bootstrap(
    *,
    source_path: Path,
    output_dir: Path,
    provider_id: str = "moex-history",
) -> dict[str, object]:
    provider = get_phase9_provider_contract(provider_id)
    if provider.role != "historical_source":
        raise ValueError(f"provider is not configured as historical source: {provider_id}")

    pilot_universe = default_phase9_pilot_universe()
    if pilot_universe.historical_provider_id != provider_id:
        raise ValueError("pilot universe historical_provider_id does not match provider_id")

    report = run_sample_backfill(
        source_path=source_path,
        output_dir=output_dir,
        whitelist_contracts=pilot_universe.whitelist_contracts(),
    )
    dataset_version = build_phase9_dataset_version(
        provider_id=provider_id,
        pilot_universe=pilot_universe,
        watermark_by_key=dict(report["watermark_by_key"]),
    )
    return {
        **report,
        "provider": provider.to_dict(),
        "pilot_universe": pilot_universe.to_dict(),
        "dataset_version": dataset_version,
        "source_path": source_path.as_posix(),
        "output_dir": output_dir.as_posix(),
        "source_kind": "jsonl-import",
    }


def run_phase9_moex_historical_bootstrap(
    *,
    output_dir: Path,
    from_date: str,
    till_date: str,
    timeframe: str = "15m",
    provider_id: str = "moex-history",
    base_url: str = DEFAULT_MOEX_ISS_BASE_URL,
) -> dict[str, object]:
    provider = get_phase9_provider_contract(provider_id)
    if provider.role != "historical_source":
        raise ValueError(f"provider is not configured as historical source: {provider_id}")

    pilot_universe = default_phase9_pilot_universe()
    if pilot_universe.historical_provider_id != provider_id:
        raise ValueError("pilot universe historical_provider_id does not match provider_id")

    output_dir.mkdir(parents=True, exist_ok=True)
    fetched_rows: list[dict[str, object]] = []
    resolved_secids: dict[str, str] = {}
    source_urls: list[str] = []
    for contract_id in pilot_universe.contract_ids:
        fetched = fetch_moex_historical_bars(
            contract_id=contract_id,
            timeframe=timeframe,
            from_date=from_date,
            till_date=till_date,
            base_url=base_url,
        )
        fetched_rows.extend(fetched.rows)
        resolved_secids[contract_id] = fetched.secid
        source_urls.append(fetched.source_url)

    source_path = output_dir / "phase9-moex-history.fetch.jsonl"
    _write_jsonl(source_path, fetched_rows)
    report = run_phase9_historical_bootstrap(
        source_path=source_path,
        output_dir=output_dir,
        provider_id=provider_id,
    )
    return {
        **report,
        "from_date": from_date,
        "till_date": till_date,
        "timeframe": timeframe,
        "source_kind": "moex-iss",
        "source_urls": source_urls,
        "resolved_secids": resolved_secids,
        "source_path": source_path.as_posix(),
    }
