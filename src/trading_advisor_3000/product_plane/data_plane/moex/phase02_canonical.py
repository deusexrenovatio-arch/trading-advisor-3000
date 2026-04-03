from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_rows,
)


TARGET_TIMEFRAMES: tuple[Timeframe, ...] = (
    Timeframe.M5,
    Timeframe.M15,
    Timeframe.H1,
    Timeframe.H4,
    Timeframe.D1,
    Timeframe.W1,
)

TARGET_MINUTES_BY_TIMEFRAME: dict[Timeframe, int] = {
    Timeframe.M5: 5,
    Timeframe.M15: 15,
    Timeframe.H1: 60,
    Timeframe.H4: 240,
    Timeframe.D1: 1440,
    Timeframe.W1: 10080,
}

SOURCE_MINUTES_BY_LABEL: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
}

CANONICAL_BAR_COLUMNS: dict[str, str] = {
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

PROVENANCE_COLUMNS: dict[str, str] = {
    "contract_id": "string",
    "instrument_id": "string",
    "timeframe": "string",
    "ts": "timestamp",
    "source_provider": "string",
    "source_timeframe": "string",
    "source_interval": "int",
    "source_run_id": "string",
    "source_ingest_run_id": "string",
    "source_row_count": "int",
    "source_ts_open_first": "timestamp",
    "source_ts_close_last": "timestamp",
    "open_interest_imputed": "int",
    "build_run_id": "string",
    "built_at_utc": "timestamp",
}

REQUIRED_PROVENANCE_FIELDS: tuple[str, ...] = (
    "source_provider",
    "source_timeframe",
    "source_interval",
    "source_run_id",
)


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str) -> datetime:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("timestamp must be non-empty")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_string(payload: dict[str, object], key: str, *, row_index: int) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"raw row[{row_index}] `{key}` must be a non-empty string")
    return value.strip()


def _require_int(payload: dict[str, object], key: str, *, row_index: int) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"raw row[{row_index}] `{key}` must be an integer")
    return int(value)


def _require_number(payload: dict[str, object], key: str, *, row_index: int) -> float:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"raw row[{row_index}] `{key}` must be a number")
    return float(value)


def _parse_provenance(value: object, *, row_index: int) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError(f"raw row[{row_index}] `provenance_json` must decode into a JSON object")
        return {str(key): item for key, item in payload.items()}
    if value is None:
        return {}
    raise ValueError(f"raw row[{row_index}] `provenance_json` must be object/string/null")


def _normalize_source_interval(
    *,
    source_timeframe: str,
    source_interval_raw: object,
    row_index: int,
) -> int:
    if source_timeframe not in SOURCE_MINUTES_BY_LABEL:
        raise ValueError(
            f"raw row[{row_index}] unsupported source timeframe `{source_timeframe}`; "
            f"allowed={', '.join(sorted(SOURCE_MINUTES_BY_LABEL))}"
        )
    expected_minutes = SOURCE_MINUTES_BY_LABEL[source_timeframe]

    if source_interval_raw is None:
        return expected_minutes
    if isinstance(source_interval_raw, bool) or not isinstance(source_interval_raw, int):
        raise ValueError(f"raw row[{row_index}] `source_interval` must be integer or null")
    if source_interval_raw <= 0:
        raise ValueError(f"raw row[{row_index}] `source_interval` must be > 0")
    # MOEX source_interval encoding is not uniform across frames (e.g. 1d=24, 1w=7).
    # Keep deterministic resampling by using canonical minutes derived from source_timeframe.
    return expected_minutes


@dataclass(frozen=True)
class RawCandle:
    contract_id: str
    instrument_id: str
    source_timeframe: str
    source_interval: int
    ts_open: str
    ts_close: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: int
    open_interest_imputed: bool
    source_provider: str
    source_run_id: str
    source_ingest_run_id: str


@dataclass(frozen=True)
class CanonicalProvenance:
    contract_id: str
    instrument_id: str
    timeframe: str
    ts: str
    source_provider: str
    source_timeframe: str
    source_interval: int
    source_run_id: str
    source_ingest_run_id: str
    source_row_count: int
    source_ts_open_first: str
    source_ts_close_last: str
    open_interest_imputed: int
    build_run_id: str
    built_at_utc: str

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "ts": self.ts,
            "source_provider": self.source_provider,
            "source_timeframe": self.source_timeframe,
            "source_interval": self.source_interval,
            "source_run_id": self.source_run_id,
            "source_ingest_run_id": self.source_ingest_run_id,
            "source_row_count": self.source_row_count,
            "source_ts_open_first": self.source_ts_open_first,
            "source_ts_close_last": self.source_ts_close_last,
            "open_interest_imputed": self.open_interest_imputed,
            "build_run_id": self.build_run_id,
            "built_at_utc": self.built_at_utc,
        }


@dataclass(frozen=True)
class ResamplingSkip:
    contract_id: str
    instrument_id: str
    timeframe: str
    target_minutes: int
    available_intervals: tuple[int, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "target_minutes": self.target_minutes,
            "available_intervals": list(self.available_intervals),
        }


def _normalize_raw_rows(rows: list[dict[str, object]]) -> list[RawCandle]:
    normalized: list[RawCandle] = []
    for row_index, payload in enumerate(rows):
        if not isinstance(payload, dict):
            raise ValueError(f"raw row[{row_index}] must be a JSON object")

        source_timeframe = _require_string(payload, "timeframe", row_index=row_index)
        source_interval = _normalize_source_interval(
            source_timeframe=source_timeframe,
            source_interval_raw=payload.get("source_interval"),
            row_index=row_index,
        )
        open_interest_raw = payload.get("open_interest")
        open_interest_imputed = open_interest_raw is None
        if open_interest_imputed:
            open_interest = 0
        else:
            if isinstance(open_interest_raw, bool) or not isinstance(open_interest_raw, int):
                raise ValueError(f"raw row[{row_index}] `open_interest` must be integer or null")
            if open_interest_raw < 0:
                raise ValueError(f"raw row[{row_index}] `open_interest` must be non-negative")
            open_interest = int(open_interest_raw)

        provenance = _parse_provenance(payload.get("provenance_json"), row_index=row_index)
        source_provider = str(provenance.get("source_provider", "")).strip()
        source_run_id = str(provenance.get("run_id", "")).strip()

        normalized.append(
            RawCandle(
                contract_id=_require_string(payload, "finam_symbol", row_index=row_index),
                instrument_id=_require_string(payload, "internal_id", row_index=row_index),
                source_timeframe=source_timeframe,
                source_interval=source_interval,
                ts_open=_require_string(payload, "ts_open", row_index=row_index),
                ts_close=_require_string(payload, "ts_close", row_index=row_index),
                open=_require_number(payload, "open", row_index=row_index),
                high=_require_number(payload, "high", row_index=row_index),
                low=_require_number(payload, "low", row_index=row_index),
                close=_require_number(payload, "close", row_index=row_index),
                volume=_require_int(payload, "volume", row_index=row_index),
                open_interest=open_interest,
                open_interest_imputed=open_interest_imputed,
                source_provider=source_provider,
                source_run_id=source_run_id,
                source_ingest_run_id=_require_string(payload, "ingest_run_id", row_index=row_index),
            )
        )
    return _deduplicate_raw_rows(normalized)


def _deduplicate_raw_rows(rows: list[RawCandle]) -> list[RawCandle]:
    dedup: dict[tuple[str, str, int, str], RawCandle] = {}
    for row in rows:
        key = (
            row.contract_id,
            row.instrument_id,
            row.source_interval,
            row.ts_open,
        )
        current = dedup.get(key)
        if current is None:
            dedup[key] = row
            continue
        if row.ts_close > current.ts_close:
            dedup[key] = row
    return sorted(
        dedup.values(),
        key=lambda item: (item.contract_id, item.instrument_id, item.source_interval, item.ts_open, item.ts_close),
    )


def _floor_to_bucket(ts_open: str, *, bucket_minutes: int) -> str:
    value = _parse_iso_utc(ts_open)
    epoch_seconds = int(value.timestamp())
    bucket_seconds = bucket_minutes * 60
    floored = (epoch_seconds // bucket_seconds) * bucket_seconds
    return _to_iso_utc(datetime.fromtimestamp(floored, tz=UTC))


def _select_source_interval(
    *,
    available_intervals: set[int],
    target_minutes: int,
) -> int | None:
    if target_minutes in available_intervals:
        return target_minutes
    compatible = sorted(item for item in available_intervals if target_minutes % item == 0)
    if not compatible:
        return None
    # Prefer the coarsest compatible interval to reduce noise and keep rollups deterministic.
    return compatible[-1]


def _group_by_contract(rows: list[RawCandle]) -> dict[tuple[str, str], list[RawCandle]]:
    grouped: dict[tuple[str, str], list[RawCandle]] = {}
    for row in rows:
        key = (row.contract_id, row.instrument_id)
        grouped.setdefault(key, []).append(row)
    for key in grouped:
        grouped[key] = sorted(grouped[key], key=lambda item: (item.source_interval, item.ts_open, item.ts_close))
    return grouped


def _resample_rows_for_target(
    rows: list[RawCandle],
    *,
    timeframe: Timeframe,
    build_run_id: str,
    built_at_utc: str,
    source_interval_override: int | None = None,
) -> tuple[list[CanonicalBar], list[CanonicalProvenance]]:
    target_minutes = TARGET_MINUTES_BY_TIMEFRAME[timeframe]
    source_interval = source_interval_override
    if source_interval is None:
        available_intervals = {item.source_interval for item in rows}
        source_interval = _select_source_interval(
            available_intervals=available_intervals,
            target_minutes=target_minutes,
        )
    if source_interval is None:
        return [], []
    source_rows = [item for item in rows if item.source_interval == source_interval]
    if not source_rows:
        return [], []

    buckets: dict[str, list[RawCandle]] = {}
    for row in source_rows:
        bucket_ts = _floor_to_bucket(row.ts_open, bucket_minutes=target_minutes)
        buckets.setdefault(bucket_ts, []).append(row)

    canonical_rows: list[CanonicalBar] = []
    provenance_rows: list[CanonicalProvenance] = []

    for bucket_ts, bucket_rows in sorted(buckets.items()):
        ordered_rows = sorted(bucket_rows, key=lambda item: (item.ts_open, item.ts_close))
        first = ordered_rows[0]
        last = ordered_rows[-1]
        volume = sum(item.volume for item in ordered_rows)
        high = max(item.high for item in ordered_rows)
        low = min(item.low for item in ordered_rows)
        bar = CanonicalBar.from_dict(
            {
                "contract_id": first.contract_id,
                "instrument_id": first.instrument_id,
                "timeframe": timeframe.value,
                "ts": bucket_ts,
                "open": first.open,
                "high": high,
                "low": low,
                "close": last.close,
                "volume": volume,
                "open_interest": last.open_interest,
            }
        )
        canonical_rows.append(bar)
        provenance_rows.append(
            CanonicalProvenance(
                contract_id=bar.contract_id,
                instrument_id=bar.instrument_id,
                timeframe=bar.timeframe.value,
                ts=bar.ts,
                source_provider=last.source_provider,
                source_timeframe=last.source_timeframe,
                source_interval=last.source_interval,
                source_run_id=last.source_run_id,
                source_ingest_run_id=last.source_ingest_run_id,
                source_row_count=len(ordered_rows),
                source_ts_open_first=first.ts_open,
                source_ts_close_last=last.ts_close,
                open_interest_imputed=1 if any(item.open_interest_imputed for item in ordered_rows) else 0,
                build_run_id=build_run_id,
                built_at_utc=built_at_utc,
            )
        )
    return canonical_rows, provenance_rows


def _build_phase02_canonical_outputs_with_diagnostics(
    *,
    raw_rows: list[dict[str, object]],
    build_run_id: str,
    built_at_utc: str,
) -> tuple[list[CanonicalBar], list[CanonicalProvenance], list[ResamplingSkip]]:
    normalized_rows = _normalize_raw_rows(raw_rows)
    grouped_rows = _group_by_contract(normalized_rows)
    canonical_rows: list[CanonicalBar] = []
    provenance_rows: list[CanonicalProvenance] = []
    resampling_skips: list[ResamplingSkip] = []

    for (contract_id, instrument_id), rows in sorted(grouped_rows.items()):
        available_intervals = {item.source_interval for item in rows}
        for timeframe in TARGET_TIMEFRAMES:
            target_minutes = TARGET_MINUTES_BY_TIMEFRAME[timeframe]
            source_interval = _select_source_interval(
                available_intervals=available_intervals,
                target_minutes=target_minutes,
            )
            if source_interval is None:
                resampling_skips.append(
                    ResamplingSkip(
                        contract_id=contract_id,
                        instrument_id=instrument_id,
                        timeframe=timeframe.value,
                        target_minutes=target_minutes,
                        available_intervals=tuple(sorted(available_intervals)),
                    )
                )
                continue
            bars, provenance = _resample_rows_for_target(
                rows,
                timeframe=timeframe,
                build_run_id=build_run_id,
                built_at_utc=built_at_utc,
                source_interval_override=source_interval,
            )
            canonical_rows.extend(bars)
            provenance_rows.extend(provenance)

    canonical_rows = sorted(
        canonical_rows,
        key=lambda item: (item.contract_id, item.instrument_id, item.timeframe.value, item.ts),
    )
    provenance_rows = sorted(
        provenance_rows,
        key=lambda item: (item.contract_id, item.instrument_id, item.timeframe, item.ts),
    )
    return canonical_rows, provenance_rows, resampling_skips


def build_phase02_canonical_outputs(
    *,
    raw_rows: list[dict[str, object]],
    build_run_id: str,
    built_at_utc: str,
) -> tuple[list[CanonicalBar], list[CanonicalProvenance]]:
    canonical_rows, provenance_rows, _ = _build_phase02_canonical_outputs_with_diagnostics(
        raw_rows=raw_rows,
        build_run_id=build_run_id,
        built_at_utc=built_at_utc,
    )
    return canonical_rows, provenance_rows


def _sample_errors(values: list[str], *, limit: int = 10) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
        if len(unique) >= limit:
            break
    return unique


def _summarize_resampling_skips(skips: list[ResamplingSkip]) -> dict[str, object]:
    by_timeframe: dict[str, int] = {}
    for item in skips:
        by_timeframe[item.timeframe] = by_timeframe.get(item.timeframe, 0) + 1
    samples = [item.to_dict() for item in skips[:20]]
    return {
        "count": len(skips),
        "by_timeframe": dict(sorted(by_timeframe.items())),
        "samples": samples,
    }


def run_qc_gates(
    *,
    bars: list[CanonicalBar],
    provenance_rows: list[CanonicalProvenance],
    run_id: str,
) -> dict[str, object]:
    unique_errors: list[str] = []
    monotonic_errors: list[str] = []
    ohlcv_errors: list[str] = []
    provenance_errors: list[str] = []

    seen_keys: set[tuple[str, str, str]] = set()
    last_ts_by_key: dict[tuple[str, str], str] = {}
    provenance_by_key: dict[tuple[str, str, str], CanonicalProvenance] = {}
    duplicate_provenance: list[str] = []

    for row in provenance_rows:
        key = (row.contract_id, row.timeframe, row.ts)
        if key in provenance_by_key:
            duplicate_provenance.append(f"{row.contract_id}/{row.timeframe}/{row.ts}")
        else:
            provenance_by_key[key] = row

    for bar in bars:
        key = (bar.contract_id, bar.timeframe.value, bar.ts)
        if key in seen_keys:
            unique_errors.append(f"duplicate key: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        seen_keys.add(key)

        monotonic_key = (bar.contract_id, bar.timeframe.value)
        previous_ts = last_ts_by_key.get(monotonic_key)
        if previous_ts is not None and bar.ts < previous_ts:
            monotonic_errors.append(
                f"non-monotonic timeline: {bar.contract_id}/{bar.timeframe.value} ({previous_ts} -> {bar.ts})"
            )
        last_ts_by_key[monotonic_key] = bar.ts

        if bar.high < max(bar.open, bar.close):
            ohlcv_errors.append(f"high violation: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        if bar.low > min(bar.open, bar.close):
            ohlcv_errors.append(f"low violation: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        if bar.volume < 0:
            ohlcv_errors.append(f"negative volume: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        if bar.open_interest < 0:
            ohlcv_errors.append(f"negative open_interest: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")

        provenance = provenance_by_key.get(key)
        if provenance is None:
            provenance_errors.append(f"missing provenance: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
            continue
        if not provenance.source_provider.strip():
            provenance_errors.append(f"missing source_provider: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        if not provenance.source_timeframe.strip():
            provenance_errors.append(f"missing source_timeframe: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        if provenance.source_interval <= 0:
            provenance_errors.append(f"invalid source_interval: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        if not provenance.source_run_id.strip():
            provenance_errors.append(f"missing source_run_id: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}")
        if not provenance.source_ingest_run_id.strip():
            provenance_errors.append(
                f"missing source_ingest_run_id: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )

    for item in duplicate_provenance:
        provenance_errors.append(f"duplicate provenance key: {item}")

    gate_results = [
        {
            "gate": "unique_bar_key",
            "status": "PASS" if not unique_errors else "FAIL",
            "violations": len(unique_errors),
            "samples": _sample_errors(unique_errors),
        },
        {
            "gate": "timeline_monotonicity",
            "status": "PASS" if not monotonic_errors else "FAIL",
            "violations": len(monotonic_errors),
            "samples": _sample_errors(monotonic_errors),
        },
        {
            "gate": "ohlcv_validity",
            "status": "PASS" if not ohlcv_errors else "FAIL",
            "violations": len(ohlcv_errors),
            "samples": _sample_errors(ohlcv_errors),
        },
        {
            "gate": "provenance_completeness",
            "status": "PASS" if not provenance_errors else "FAIL",
            "violations": len(provenance_errors),
            "samples": _sample_errors(provenance_errors),
        },
    ]
    failed_gates = [item["gate"] for item in gate_results if item["status"] == "FAIL"]
    return {
        "run_id": run_id,
        "generated_at_utc": _utc_now_iso(),
        "status": "PASS" if not failed_gates else "FAIL",
        "publish_decision": "publish" if not failed_gates else "blocked",
        "failed_gates": failed_gates,
        "gate_results": gate_results,
    }


def run_contract_compatibility_check(
    *,
    bars: list[CanonicalBar],
    repo_root: Path,
) -> dict[str, object]:
    schema_path = (
        repo_root
        / "src"
        / "trading_advisor_3000"
        / "product_plane"
        / "contracts"
        / "schemas"
        / "canonical_bar.v1.json"
    )
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    required = payload.get("required")
    properties = payload.get("properties")
    additional_properties = bool(payload.get("additionalProperties", True))

    if not isinstance(required, list) or not isinstance(properties, dict):
        raise ValueError("canonical_bar.v1 schema must define `required` list and `properties` object")

    required_fields = {str(item) for item in required}
    property_fields = {str(item) for item in properties}
    allowed_timeframes = set(payload["properties"]["timeframe"]["enum"])

    enum_values = {item.value for item in Timeframe}
    errors: list[str] = []
    if enum_values != allowed_timeframes:
        errors.append(
            "timeframe enum mismatch: "
            f"python={sorted(enum_values)} schema={sorted(allowed_timeframes)}"
        )

    if required_fields != set(CANONICAL_BAR_COLUMNS):
        errors.append(
            "canonical schema required fields mismatch with runtime columns: "
            f"schema={sorted(required_fields)} runtime={sorted(CANONICAL_BAR_COLUMNS)}"
        )

    if property_fields != set(CANONICAL_BAR_COLUMNS):
        errors.append(
            "canonical schema properties mismatch with runtime columns: "
            f"schema={sorted(property_fields)} runtime={sorted(CANONICAL_BAR_COLUMNS)}"
        )

    for index, bar in enumerate(bars):
        row = bar.to_dict()
        keys = set(row)
        missing = sorted(required_fields - keys)
        extra = sorted(keys - required_fields)
        if missing:
            errors.append(f"row[{index}] missing required fields: {', '.join(missing)}")
        if extra and not additional_properties:
            errors.append(f"row[{index}] has unsupported fields: {', '.join(extra)}")
        if row["timeframe"] not in allowed_timeframes:
            errors.append(f"row[{index}] unsupported timeframe: {row['timeframe']}")
        try:
            CanonicalBar.from_dict(dict(row))
        except Exception as exc:  # noqa: BLE001 - explicit contract compatibility capture
            errors.append(f"row[{index}] canonical contract parse failed: {exc}")

    return {
        "schema_path": schema_path.as_posix(),
        "status": "PASS" if not errors else "FAIL",
        "errors": _sample_errors(errors, limit=20),
        "checked_rows": len(bars),
        "required_fields": sorted(required_fields),
        "allowed_timeframes": sorted(allowed_timeframes),
    }


def run_runtime_decoupling_check(*, repo_root: Path) -> dict[str, object]:
    runtime_root = repo_root / "src" / "trading_advisor_3000" / "app" / "runtime"
    if not runtime_root.exists():
        raise FileNotFoundError(f"runtime root not found: {runtime_root.as_posix()}")

    forbidden_tokens = (
        "pyspark",
        "sparksession",
        "trading_advisor_3000.spark_jobs",
        "delta-spark",
    )
    violations: list[dict[str, str]] = []
    checked_files = 0

    for path in sorted(runtime_root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        normalized = text.lower()
        checked_files += 1
        for token in forbidden_tokens:
            if token in normalized:
                violations.append(
                    {
                        "file": path.as_posix(),
                        "token": token,
                    }
                )

    return {
        "status": "PASS" if not violations else "FAIL",
        "runtime_root": runtime_root.as_posix(),
        "checked_files": checked_files,
        "violations": violations,
        "forbidden_tokens": list(forbidden_tokens),
    }


def _json_write(path: Path, payload: dict[str, object] | list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_snapshot_payload(
    *,
    bars: list[CanonicalBar],
    provenance_rows: list[CanonicalProvenance],
) -> tuple[dict[str, object], dict[str, object]]:
    rows = [item.to_dict() for item in bars]
    keys = [f"{item['contract_id']}|{item['timeframe']}|{item['ts']}" for item in rows]
    counts_by_timeframe: dict[str, int] = {}
    for row in rows:
        timeframe = str(row["timeframe"])
        counts_by_timeframe[timeframe] = counts_by_timeframe.get(timeframe, 0) + 1

    source_intervals_by_timeframe: dict[str, list[int]] = {}
    for row in provenance_rows:
        bucket = source_intervals_by_timeframe.setdefault(row.timeframe, [])
        if row.source_interval not in bucket:
            bucket.append(row.source_interval)
    for key in source_intervals_by_timeframe:
        source_intervals_by_timeframe[key] = sorted(source_intervals_by_timeframe[key])

    canonical_snapshot = {
        "rows": rows,
    }
    target_timeframe_values = tuple(item.value for item in TARGET_TIMEFRAMES)
    resampling_snapshot = {
        "counts_by_timeframe": {key: counts_by_timeframe.get(key, 0) for key in target_timeframe_values},
        "stable_keys": keys,
        "source_intervals_by_timeframe": source_intervals_by_timeframe,
    }
    return canonical_snapshot, resampling_snapshot


def _discover_real_bindings(raw_rows: list[dict[str, object]]) -> list[str]:
    bindings: set[str] = set()
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        raw = row.get("provenance_json")
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
        elif isinstance(raw, dict):
            payload = raw
        else:
            continue
        if not isinstance(payload, dict):
            continue
        discovery_url = str(payload.get("discovery_url", "")).strip()
        if discovery_url:
            bindings.add(discovery_url)
        source_provider = str(payload.get("source_provider", "")).strip()
        if source_provider:
            bindings.add(source_provider)
    return sorted(bindings)


def run_phase02_canonical(
    *,
    raw_table_path: Path,
    output_dir: Path,
    run_id: str,
    repo_root: Path | None = None,
) -> dict[str, object]:
    repo_root = repo_root.resolve() if repo_root else Path(__file__).resolve().parents[5]
    if not has_delta_log(raw_table_path):
        raise FileNotFoundError(f"phase-02 raw source table missing `_delta_log`: {raw_table_path.as_posix()}")

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_rows = read_delta_table_rows(raw_table_path)
    built_at_utc = _utc_now_iso()
    canonical_rows, provenance_rows, resampling_skips = _build_phase02_canonical_outputs_with_diagnostics(
        raw_rows=raw_rows,
        build_run_id=run_id,
        built_at_utc=built_at_utc,
    )

    qc_report = run_qc_gates(
        bars=canonical_rows,
        provenance_rows=provenance_rows,
        run_id=run_id,
    )
    contract_report = run_contract_compatibility_check(
        bars=canonical_rows,
        repo_root=repo_root,
    )
    runtime_report = run_runtime_decoupling_check(repo_root=repo_root)

    canonical_snapshot, resampling_snapshot = _build_snapshot_payload(
        bars=canonical_rows,
        provenance_rows=provenance_rows,
    )

    qc_path = output_dir / "qc-report.json"
    contract_path = output_dir / "contract-compatibility-report.json"
    runtime_path = output_dir / "runtime-decoupling-proof.json"
    canonical_snapshot_path = output_dir / "canonical-snapshot.json"
    resampling_snapshot_path = output_dir / "resampling-snapshot.json"

    _json_write(qc_path, qc_report)
    _json_write(contract_path, contract_report)
    _json_write(runtime_path, runtime_report)
    _json_write(canonical_snapshot_path, canonical_snapshot)
    _json_write(resampling_snapshot_path, resampling_snapshot)

    publish_allowed = (
        qc_report["status"] == "PASS"
        and contract_report["status"] == "PASS"
        and runtime_report["status"] == "PASS"
    )

    output_paths: dict[str, str] = {}
    if publish_allowed:
        bars_path = output_dir / "delta" / "canonical_bars.delta"
        provenance_path = output_dir / "delta" / "canonical_bar_provenance.delta"
        write_delta_table_rows(
            table_path=bars_path,
            rows=[item.to_dict() for item in canonical_rows],
            columns=CANONICAL_BAR_COLUMNS,
        )
        write_delta_table_rows(
            table_path=provenance_path,
            rows=[item.to_dict() for item in provenance_rows],
            columns=PROVENANCE_COLUMNS,
        )
        output_paths = {
            "canonical_bars": bars_path.as_posix(),
            "canonical_bar_provenance": provenance_path.as_posix(),
        }

    report: dict[str, object] = {
        "run_id": run_id,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "status": "PASS" if publish_allowed else "BLOCKED",
        "publish_decision": "publish" if publish_allowed else "blocked",
        "raw_table_path": raw_table_path.as_posix(),
        "output_dir": output_dir.as_posix(),
        "source_rows": len(raw_rows),
        "canonical_rows": len(canonical_rows),
        "provenance_rows": len(provenance_rows),
        "target_timeframes": [item.value for item in TARGET_TIMEFRAMES],
        "resampling_skips": _summarize_resampling_skips(resampling_skips),
        "output_paths": output_paths,
        "artifact_paths": {
            "canonical_snapshot": canonical_snapshot_path.as_posix(),
            "resampling_snapshot": resampling_snapshot_path.as_posix(),
            "qc_report": qc_path.as_posix(),
            "contract_compatibility_report": contract_path.as_posix(),
            "runtime_decoupling_proof": runtime_path.as_posix(),
        },
        "qc_report": qc_report,
        "contract_compatibility_report": contract_report,
        "runtime_decoupling_proof": runtime_report,
        "real_bindings": _discover_real_bindings(raw_rows),
    }

    _json_write(output_dir / "phase02-canonical-report.json", report)
    return report
