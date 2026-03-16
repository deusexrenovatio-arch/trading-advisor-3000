from __future__ import annotations

import json
from pathlib import Path


REQUIRED_FIELDS = {
    "contract_id",
    "timeframe",
    "ts_open",
    "ts_close",
    "open",
    "high",
    "low",
    "close",
    "volume",
}
ALLOWED_TIMEFRAMES = {"5m", "15m", "1h"}


def _assert_record_shape(record: dict[str, object], *, line_no: int) -> None:
    extra = sorted(set(record) - REQUIRED_FIELDS)
    if extra:
        raise ValueError(f"line {line_no}: unsupported fields: {', '.join(extra)}")
    missing = sorted(REQUIRED_FIELDS - set(record))
    if missing:
        raise ValueError(f"line {line_no}: missing required fields: {', '.join(missing)}")

    for name in ("contract_id", "timeframe", "ts_open", "ts_close"):
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


def ingest_raw_backfill(source_path: Path, *, whitelist_contracts: set[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line_no, raw in enumerate(source_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError(f"line {line_no}: record must be object")
        _assert_record_shape(payload, line_no=line_no)
        if payload["contract_id"] not in whitelist_contracts:
            continue
        rows.append(payload)
    return rows
