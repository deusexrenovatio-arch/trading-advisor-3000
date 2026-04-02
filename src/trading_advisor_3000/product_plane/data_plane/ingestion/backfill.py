from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


REQUIRED_FIELDS = {
    "contract_id",
    "instrument_id",
    "timeframe",
    "ts_open",
    "ts_close",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
}
ALLOWED_TIMEFRAMES = {"5m", "15m", "1h"}


@dataclass(frozen=True)
class IngestionBatch:
    rows: list[dict[str, object]]
    new_rows: list[dict[str, object]]
    source_rows: int
    whitelisted_rows: int
    incremental_rows: int
    deduplicated_rows: int
    stale_rows: int
    watermark_by_key: dict[str, str]


def _assert_record_shape(record: dict[str, object], *, line_no: int) -> None:
    extra = sorted(set(record) - REQUIRED_FIELDS)
    if extra:
        raise ValueError(f"line {line_no}: unsupported fields: {', '.join(extra)}")
    missing = sorted(REQUIRED_FIELDS - set(record))
    if missing:
        raise ValueError(f"line {line_no}: missing required fields: {', '.join(missing)}")

    for name in ("contract_id", "instrument_id", "timeframe", "ts_open", "ts_close"):
        value = record.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"line {line_no}: {name} must be non-empty string")

    timeframe = record["timeframe"]
    if timeframe not in ALLOWED_TIMEFRAMES:
        raise ValueError(f"line {line_no}: unsupported timeframe: {timeframe}")

    for name in ("open", "high", "low", "close"):
        value = record.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"line {line_no}: {name} must be number")

    volume = record.get("volume")
    if isinstance(volume, bool) or not isinstance(volume, int):
        raise ValueError(f"line {line_no}: volume must be integer")
    if volume < 0:
        raise ValueError(f"line {line_no}: volume must be non-negative")

    open_interest = record.get("open_interest")
    if isinstance(open_interest, bool) or not isinstance(open_interest, int):
        raise ValueError(f"line {line_no}: open_interest must be integer")
    if open_interest < 0:
        raise ValueError(f"line {line_no}: open_interest must be non-negative")


def _incremental_key(record: dict[str, object]) -> tuple[str, str]:
    return str(record["contract_id"]), str(record["timeframe"])


def _row_signature(record: dict[str, object]) -> str:
    return json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compute_watermarks(rows: list[dict[str, object]]) -> dict[tuple[str, str], str]:
    watermarks: dict[tuple[str, str], str] = {}
    for row in rows:
        key = _incremental_key(row)
        ts_close = str(row["ts_close"])
        current = watermarks.get(key)
        if current is None or ts_close > current:
            watermarks[key] = ts_close
    return watermarks


def ingest_raw_backfill(
    source_path: Path,
    *,
    whitelist_contracts: set[str],
    existing_rows: list[dict[str, object]] | None = None,
) -> IngestionBatch:
    existing_rows = list(existing_rows or [])
    rows = list(existing_rows)
    new_rows: list[dict[str, object]] = []
    source_rows = 0
    whitelisted_rows = 0
    deduplicated_rows = 0
    stale_rows = 0
    seen_signatures = {_row_signature(item) for item in existing_rows}
    watermarks = _compute_watermarks(existing_rows)

    for line_no, raw in enumerate(source_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        source_rows += 1
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError(f"line {line_no}: record must be object")
        _assert_record_shape(payload, line_no=line_no)
        if payload["contract_id"] not in whitelist_contracts:
            continue
        whitelisted_rows += 1

        signature = _row_signature(payload)
        if signature in seen_signatures:
            deduplicated_rows += 1
            continue

        key = _incremental_key(payload)
        ts_close = str(payload["ts_close"])
        watermark = watermarks.get(key)
        if watermark is not None and ts_close <= watermark:
            stale_rows += 1
            continue

        seen_signatures.add(signature)
        watermarks[key] = ts_close
        rows.append(payload)
        new_rows.append(payload)

    watermarks_text = {f"{key[0]}|{key[1]}": value for key, value in sorted(watermarks.items())}
    return IngestionBatch(
        rows=rows,
        new_rows=new_rows,
        source_rows=source_rows,
        whitelisted_rows=whitelisted_rows,
        incremental_rows=len(new_rows),
        deduplicated_rows=deduplicated_rows,
        stale_rows=stale_rows,
        watermark_by_key=watermarks_text,
    )
