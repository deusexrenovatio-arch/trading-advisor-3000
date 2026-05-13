from __future__ import annotations

# ruff: noqa: E501
import json
import os
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    has_delta_log,
)
from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import (
    STATUS_PASS_NOOP,
    build_parity_manifest_v1,
    normalize_changed_windows,
)
from trading_advisor_3000.product_plane.data_plane.moex.session_buckets import (
    CANONICAL_BUCKET_POLICY_VERSION,
    CanonicalBucket,
    build_canonical_bucket_rows,
)
from trading_advisor_3000.spark_jobs.moex_canonical_publish_job import (
    run_moex_canonical_publish_spark_delta_job,
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

SIDECAR_ROLL_REASON = "max_open_interest_then_latest_ts_close"

RAW_SCOPE_COLUMNS: tuple[str, ...] = (
    "internal_id",
    "finam_symbol",
    "moex_secid",
    "timeframe",
    "source_interval",
    "ts_open",
    "ts_close",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "ingest_run_id",
    "provenance_json",
)

RAW_INTERVAL_PROJECTION_COLUMNS: tuple[str, ...] = (
    "internal_id",
    "finam_symbol",
    "timeframe",
    "source_interval",
)

STATUS_PASS = "PASS"
STATUS_BLOCKED = "BLOCKED"
CANONICAL_MERGE_SCOPED_DELETE_INSERT = "scoped_delete_insert"
SPARK_PUBLISH_SUBPROCESS_TIMEOUT_SECONDS = 1800


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
            raise ValueError(
                f"raw row[{row_index}] `provenance_json` must decode into a JSON object"
            )
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


@dataclass(frozen=True)
class ChangedWindowScope:
    internal_id: str
    source_timeframe: str
    source_interval: int
    moex_secid: str
    window_start_utc: str
    window_end_utc: str
    incremental_rows: int

    @property
    def key(self) -> tuple[str, str, int, str]:
        return (
            self.internal_id,
            self.source_timeframe,
            self.source_interval,
            self.moex_secid,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "internal_id": self.internal_id,
            "source_timeframe": self.source_timeframe,
            "source_interval": self.source_interval,
            "moex_secid": self.moex_secid,
            "window_start_utc": self.window_start_utc,
            "window_end_utc": self.window_end_utc,
            "incremental_rows": self.incremental_rows,
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
        key=lambda item: (
            item.contract_id,
            item.instrument_id,
            item.source_interval,
            item.ts_open,
            item.ts_close,
        ),
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
        grouped[key] = sorted(
            grouped[key], key=lambda item: (item.source_interval, item.ts_open, item.ts_close)
        )
    return grouped


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

    ordered_bars = sorted(
        bars,
        key=lambda item: (item.contract_id, item.instrument_id, item.timeframe.value, item.ts),
    )
    for bar in ordered_bars:
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
            ohlcv_errors.append(
                f"negative volume: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )
        if bar.open_interest < 0:
            ohlcv_errors.append(
                f"negative open_interest: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )

        provenance = provenance_by_key.get(key)
        if provenance is None:
            provenance_errors.append(
                f"missing provenance: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )
            continue
        if not provenance.source_provider.strip():
            provenance_errors.append(
                f"missing source_provider: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )
        if not provenance.source_timeframe.strip():
            provenance_errors.append(
                f"missing source_timeframe: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )
        if provenance.source_interval <= 0:
            provenance_errors.append(
                f"invalid source_interval: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )
        if not provenance.source_run_id.strip():
            provenance_errors.append(
                f"missing source_run_id: {bar.contract_id}/{bar.timeframe.value}/{bar.ts}"
            )
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
        raise ValueError(
            "canonical_bar.v1 schema must define `required` list and `properties` object"
        )

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
    candidate_runtime_roots = (
        repo_root / "src" / "trading_advisor_3000" / "product_plane" / "runtime",
        repo_root / "src" / "trading_advisor_3000" / "app" / "runtime",
    )
    runtime_root = next((path for path in candidate_runtime_roots if path.exists()), None)
    if runtime_root is None:
        checked = ", ".join(path.as_posix() for path in candidate_runtime_roots)
        raise FileNotFoundError(f"runtime root not found; checked: {checked}")

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
        "counts_by_timeframe": {
            key: counts_by_timeframe.get(key, 0) for key in target_timeframe_values
        },
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


def _canonical_bar_key(bar: CanonicalBar) -> tuple[str, str, str, str]:
    return (bar.contract_id, bar.instrument_id, bar.timeframe.value, bar.ts)


def _canonical_provenance_key(row: CanonicalProvenance) -> tuple[str, str, str, str]:
    return (row.contract_id, row.instrument_id, row.timeframe, row.ts)


def _canonical_provenance_from_dict(
    payload: dict[str, object], *, row_index: int
) -> CanonicalProvenance:
    def _require_text(key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"provenance row[{row_index}] `{key}` must be a non-empty string")
        return value.strip()

    def _require_int_value(key: str) -> int:
        value = payload.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"provenance row[{row_index}] `{key}` must be integer")
        return int(value)

    return CanonicalProvenance(
        contract_id=_require_text("contract_id"),
        instrument_id=_require_text("instrument_id"),
        timeframe=_require_text("timeframe"),
        ts=_require_text("ts"),
        source_provider=_require_text("source_provider"),
        source_timeframe=_require_text("source_timeframe"),
        source_interval=_require_int_value("source_interval"),
        source_run_id=_require_text("source_run_id"),
        source_ingest_run_id=_require_text("source_ingest_run_id"),
        source_row_count=_require_int_value("source_row_count"),
        source_ts_open_first=_require_text("source_ts_open_first"),
        source_ts_close_last=_require_text("source_ts_close_last"),
        open_interest_imputed=_require_int_value("open_interest_imputed"),
        build_run_id=_require_text("build_run_id"),
        built_at_utc=_require_text("built_at_utc"),
    )


def _canonical_provenance_from_dict_lenient(
    payload: dict[str, object], *, row_index: int
) -> CanonicalProvenance:
    def _text(key: str) -> str:
        value = payload.get(key)
        if value is None:
            return ""
        return str(value).strip()

    def _int_value(key: str) -> int:
        value = payload.get(key)
        if value is None or value == "":
            return 0
        if isinstance(value, bool):
            raise ValueError(f"provenance row[{row_index}] `{key}` must not be boolean")
        return int(value)

    return CanonicalProvenance(
        contract_id=_text("contract_id"),
        instrument_id=_text("instrument_id"),
        timeframe=_text("timeframe"),
        ts=_text("ts"),
        source_provider=_text("source_provider"),
        source_timeframe=_text("source_timeframe"),
        source_interval=_int_value("source_interval"),
        source_run_id=_text("source_run_id"),
        source_ingest_run_id=_text("source_ingest_run_id"),
        source_row_count=_int_value("source_row_count"),
        source_ts_open_first=_text("source_ts_open_first"),
        source_ts_close_last=_text("source_ts_close_last"),
        open_interest_imputed=_int_value("open_interest_imputed"),
        build_run_id=_text("build_run_id"),
        built_at_utc=_text("built_at_utc"),
    )


def _build_selected_source_interval_map_from_available_intervals(
    available_intervals_by_contract: dict[tuple[str, str], set[int]],
) -> dict[tuple[str, str, str], int]:
    selected: dict[tuple[str, str, str], int] = {}
    for (contract_id, instrument_id), available_intervals in sorted(
        available_intervals_by_contract.items()
    ):
        for timeframe in TARGET_TIMEFRAMES:
            target_minutes = TARGET_MINUTES_BY_TIMEFRAME[timeframe]
            source_interval = _select_source_interval(
                available_intervals=available_intervals,
                target_minutes=target_minutes,
            )
            if source_interval is None:
                continue
            selected[(contract_id, instrument_id, timeframe.value)] = source_interval
    return selected


def _build_available_intervals_by_contract(
    rows: list[RawCandle],
) -> dict[tuple[str, str], set[int]]:
    grouped_rows = _group_by_contract(rows)
    return {
        (contract_id, instrument_id): {item.source_interval for item in contract_rows}
        for (contract_id, instrument_id), contract_rows in grouped_rows.items()
    }


def _build_selected_source_interval_map(rows: list[RawCandle]) -> dict[tuple[str, str, str], int]:
    return _build_selected_source_interval_map_from_available_intervals(
        _build_available_intervals_by_contract(rows)
    )


def _build_available_intervals_by_contract_from_projection(
    rows: list[dict[str, object]],
) -> dict[tuple[str, str], set[int]]:
    available_intervals_by_contract: dict[tuple[str, str], set[int]] = {}
    for row_index, payload in enumerate(rows):
        if not isinstance(payload, dict):
            continue
        contract_id = str(payload.get("finam_symbol", "")).strip()
        instrument_id = str(payload.get("internal_id", "")).strip()
        source_timeframe = str(payload.get("timeframe", "")).strip()
        if not contract_id or not instrument_id or not source_timeframe:
            continue
        try:
            source_interval = _normalize_source_interval(
                source_timeframe=source_timeframe,
                source_interval_raw=payload.get("source_interval"),
                row_index=row_index,
            )
        except ValueError:
            continue
        available_intervals_by_contract.setdefault((contract_id, instrument_id), set()).add(
            source_interval
        )
    return available_intervals_by_contract


def _build_selected_source_interval_map_from_projection(
    rows: list[dict[str, object]],
) -> dict[tuple[str, str, str], int]:
    return _build_selected_source_interval_map_from_available_intervals(
        _build_available_intervals_by_contract_from_projection(rows)
    )


def _build_resampling_skips_from_available_intervals(
    available_intervals_by_contract: dict[tuple[str, str], set[int]],
    *,
    selected_source_intervals: dict[tuple[str, str, str], int],
) -> list[ResamplingSkip]:
    skips: list[ResamplingSkip] = []
    for (contract_id, instrument_id), available_intervals in sorted(
        available_intervals_by_contract.items()
    ):
        sorted_intervals = tuple(sorted(available_intervals))
        for timeframe in TARGET_TIMEFRAMES:
            if (contract_id, instrument_id, timeframe.value) in selected_source_intervals:
                continue
            skips.append(
                ResamplingSkip(
                    contract_id=contract_id,
                    instrument_id=instrument_id,
                    timeframe=timeframe.value,
                    target_minutes=TARGET_MINUTES_BY_TIMEFRAME[timeframe],
                    available_intervals=sorted_intervals,
                )
            )
    return skips


def _build_available_intervals_by_changed_window(
    changed_windows: list[ChangedWindowScope],
) -> dict[tuple[str, str], set[int]]:
    available_intervals_by_contract: dict[tuple[str, str], set[int]] = {}
    for window in changed_windows:
        available_intervals_by_contract.setdefault(
            (window.moex_secid, window.internal_id), set()
        ).add(window.source_interval)
    return available_intervals_by_contract


def _build_resampling_skips(
    rows: list[RawCandle],
    *,
    selected_source_intervals: dict[tuple[str, str, str], int],
) -> list[ResamplingSkip]:
    return _build_resampling_skips_from_available_intervals(
        _build_available_intervals_by_contract(rows),
        selected_source_intervals=selected_source_intervals,
    )


def _raw_candle_to_dict(row: RawCandle) -> dict[str, object]:
    return {
        "contract_id": row.contract_id,
        "instrument_id": row.instrument_id,
        "source_timeframe": row.source_timeframe,
        "source_interval": row.source_interval,
        "ts_open": row.ts_open,
        "ts_close": row.ts_close,
        "open": row.open,
        "high": row.high,
        "low": row.low,
        "close": row.close,
        "volume": row.volume,
        "open_interest": row.open_interest,
        "open_interest_imputed": row.open_interest_imputed,
        "source_provider": row.source_provider,
        "source_run_id": row.source_run_id,
        "source_ingest_run_id": row.source_ingest_run_id,
    }


def _jsonl_write(path: Path, rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _selected_source_interval_rows(
    selected_source_intervals: dict[tuple[str, str, str], int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for (contract_id, instrument_id, timeframe), source_interval in sorted(
        selected_source_intervals.items()
    ):
        rows.append(
            {
                "contract_id": contract_id,
                "moex_secid": contract_id,
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "target_minutes": TARGET_MINUTES_BY_TIMEFRAME[Timeframe(timeframe)],
                "source_interval": source_interval,
            }
        )
    return rows


def _publish_scope_rows(
    *,
    changed_windows: list[ChangedWindowScope],
    selected_source_intervals: dict[tuple[str, str, str], int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for window in changed_windows:
        for (contract_id, instrument_id, timeframe), source_interval in sorted(
            selected_source_intervals.items()
        ):
            if contract_id != window.moex_secid:
                continue
            if instrument_id != window.internal_id:
                continue
            if source_interval != window.source_interval:
                continue
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "timeframe": timeframe,
                    "target_minutes": TARGET_MINUTES_BY_TIMEFRAME[Timeframe(timeframe)],
                    "window_start_utc": window.window_start_utc,
                    "window_end_utc": window.window_end_utc,
                }
            )
    return rows


def _spark_profile() -> str:
    explicit = os.environ.get("TA3000_MOEX_CANONICALIZATION_SPARK_PROFILE", "").strip().lower()
    if explicit:
        if explicit not in {"local", "docker"}:
            raise RuntimeError(
                "TA3000_MOEX_CANONICALIZATION_SPARK_PROFILE must be `local` or `docker`"
            )
        return explicit
    if os.name == "nt":
        return "docker"
    return "local"


def _run_spark_canonicalization(
    *,
    raw_table_path: Path,
    changed_windows: list[ChangedWindowScope],
    selected_source_intervals: dict[tuple[str, str, str], int],
    canonical_buckets: list[CanonicalBucket],
    output_dir: Path,
    run_id: str,
    built_at_utc: str,
    repo_root: Path,
) -> dict[str, object]:
    spark_dir = output_dir / ".spark-canonicalization"
    changed_windows_path = spark_dir / "changed-windows.jsonl"
    selected_source_intervals_path = spark_dir / "selected-source-intervals.jsonl"
    publish_scope_path = spark_dir / "publish-scope.jsonl"
    canonical_buckets_path = spark_dir / "canonical-buckets.jsonl"
    spark_output_dir = spark_dir / "scoped-output"
    spark_report_path = spark_dir / "spark-execution-report.json"

    _jsonl_write(changed_windows_path, (window.to_dict() for window in changed_windows))
    _jsonl_write(
        selected_source_intervals_path,
        _selected_source_interval_rows(selected_source_intervals),
    )
    _jsonl_write(
        publish_scope_path,
        _publish_scope_rows(
            changed_windows=changed_windows,
            selected_source_intervals=selected_source_intervals,
        ),
    )
    _jsonl_write(canonical_buckets_path, (row.to_dict() for row in canonical_buckets))

    command = [
        sys.executable,
        (repo_root / "scripts" / "run_moex_historical_canonical_route_spark.py").as_posix(),
        "--profile",
        _spark_profile(),
        "--raw-table-path",
        raw_table_path.as_posix(),
        "--changed-windows-jsonl",
        changed_windows_path.as_posix(),
        "--selected-source-intervals-jsonl",
        selected_source_intervals_path.as_posix(),
        "--canonical-buckets-jsonl",
        canonical_buckets_path.as_posix(),
        "--output-dir",
        spark_output_dir.as_posix(),
        "--run-id",
        run_id,
        "--built-at-utc",
        built_at_utc,
        "--output-json",
        spark_report_path.as_posix(),
    ]
    completed = subprocess.run(
        command,
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"spark canonicalization failed: {detail}")

    payload = json.loads(spark_report_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("spark canonicalization report must be JSON object")
    payload.setdefault("bucket_mode", "canonical_bucket_map")
    payload.setdefault("canonical_bucket_rows", len(canonical_buckets))
    payload.setdefault("policy_version", CANONICAL_BUCKET_POLICY_VERSION)
    output_paths = payload.setdefault("output_paths", {})
    if isinstance(output_paths, dict):
        output_paths.setdefault("canonical_buckets", canonical_buckets_path.as_posix())
    return payload


def _run_spark_canonical_publish(
    *,
    staged_bars_path: Path,
    staged_provenance_path: Path,
    publish_scope_path: Path | None,
    target_bars_path: Path,
    target_provenance_path: Path,
    session_calendar_path: Path,
    roll_map_path: Path,
    output_dir: Path,
    run_id: str,
    repo_root: Path,
) -> dict[str, object]:
    spark_dir = output_dir / ".spark-canonical-publish"
    spark_report_path = spark_dir / "spark-publish-report.json"
    profile = _spark_profile()
    if profile == "local":
        report = run_moex_canonical_publish_spark_delta_job(
            staged_bars_path=staged_bars_path,
            staged_provenance_path=staged_provenance_path,
            publish_scope_path=publish_scope_path,
            target_bars_path=target_bars_path,
            target_provenance_path=target_provenance_path,
            session_calendar_path=session_calendar_path,
            roll_map_path=roll_map_path,
            output_dir=spark_dir,
            run_id=run_id,
        )
        _json_write(spark_report_path, report)
        return report

    command = [
        sys.executable,
        (repo_root / "scripts" / "run_moex_canonical_publish_spark.py").as_posix(),
        "--profile",
        profile,
        "--staged-bars-path",
        staged_bars_path.as_posix(),
        "--staged-provenance-path",
        staged_provenance_path.as_posix(),
        "--publish-scope-path",
        publish_scope_path.as_posix() if publish_scope_path is not None else "",
        "--target-bars-path",
        target_bars_path.as_posix(),
        "--target-provenance-path",
        target_provenance_path.as_posix(),
        "--session-calendar-path",
        session_calendar_path.as_posix(),
        "--roll-map-path",
        roll_map_path.as_posix(),
        "--output-dir",
        spark_dir.as_posix(),
        "--run-id",
        run_id,
        "--output-json",
        spark_report_path.as_posix(),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=SPARK_PUBLISH_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            "spark canonical publish failed: subprocess timed out after "
            f"{SPARK_PUBLISH_SUBPROCESS_TIMEOUT_SECONDS} seconds"
        ) from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"spark canonical publish failed: {detail}")

    payload = json.loads(spark_report_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("spark canonical publish report must be JSON object")
    return payload


def _prepare_changed_window_scope(raw_windows: list[dict[str, object]]) -> list[ChangedWindowScope]:
    scopes: list[ChangedWindowScope] = []
    for row in raw_windows:
        source_timeframe = str(row["source_timeframe"]).strip()
        source_interval = _normalize_source_interval(
            source_timeframe=source_timeframe,
            source_interval_raw=row["source_interval"],
            row_index=0,
        )
        scopes.append(
            ChangedWindowScope(
                internal_id=str(row["internal_id"]).strip(),
                source_timeframe=source_timeframe,
                source_interval=source_interval,
                moex_secid=str(row["moex_secid"]).strip(),
                window_start_utc=str(row["window_start_utc"]).strip(),
                window_end_utc=str(row["window_end_utc"]).strip(),
                incremental_rows=int(row["incremental_rows"]),
            )
        )
    return scopes


def _build_internal_id_filters(internal_ids: set[str]) -> list[list[tuple[str, str, object]]]:
    return [
        [("internal_id", "=", internal_id)]
        for internal_id in sorted(item for item in internal_ids if item)
    ]


def _build_scoped_raw_read_filters(
    changed_windows: list[ChangedWindowScope],
) -> list[list[tuple[str, str, object]]]:
    filters: list[list[tuple[str, str, object]]] = []
    for window in changed_windows:
        window_start = _parse_iso_utc(window.window_start_utc)
        window_end = _parse_iso_utc(window.window_end_utc)
        filters.append(
            [
                ("internal_id", "=", window.internal_id),
                ("timeframe", "=", window.source_timeframe),
                ("ts_close", ">=", window_start),
                ("ts_close", "<=", window_end),
            ]
        )
    return filters


def _report_source_row_count(
    *,
    raw_ingest_run_report: dict[str, object],
    raw_table_path: Path,
) -> int:
    value = raw_ingest_run_report.get("source_rows")
    if value is not None and not isinstance(value, bool):
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
    return count_delta_table_rows(raw_table_path)


def _report_real_bindings(
    *,
    raw_ingest_run_report: dict[str, object],
    fallback_rows: list[dict[str, object]],
) -> list[str]:
    report_values = raw_ingest_run_report.get("real_bindings")
    bindings: set[str] = set()
    if isinstance(report_values, (list, tuple)):
        for item in report_values:
            text = str(item).strip()
            if text:
                bindings.add(text)
    if not bindings:
        bindings.update(_discover_real_bindings(fallback_rows))
    return sorted(bindings)


def _extract_row_moex_secid(row: dict[str, object]) -> str:
    moex_secid = str(row.get("moex_secid", "")).strip()
    if moex_secid:
        return moex_secid
    return str(row.get("finam_symbol", "")).strip()


def _scope_raw_rows_to_changed_windows(
    *,
    raw_rows: list[dict[str, object]],
    changed_windows: list[ChangedWindowScope],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if not changed_windows:
        return [], []

    windows_by_key: dict[tuple[str, str, int, str], list[ChangedWindowScope]] = {}
    for window in changed_windows:
        windows_by_key.setdefault(window.key, []).append(window)

    scoped_rows: list[dict[str, object]] = []
    matched_keys: set[tuple[str, str, int, str, str, str]] = set()
    matched_window_entries: set[tuple[str, str, int, str, str, str]] = set()
    unmatched_windows: list[dict[str, object]] = []

    for row_index, raw_row in enumerate(raw_rows):
        if not isinstance(raw_row, dict):
            continue
        internal_id = str(raw_row.get("internal_id", "")).strip()
        source_timeframe = str(raw_row.get("timeframe", "")).strip()
        moex_secid = _extract_row_moex_secid(raw_row)
        if not internal_id or not source_timeframe or not moex_secid:
            continue
        try:
            source_interval = _normalize_source_interval(
                source_timeframe=source_timeframe,
                source_interval_raw=raw_row.get("source_interval"),
                row_index=row_index,
            )
        except ValueError:
            continue
        candidates = windows_by_key.get(
            (internal_id, source_timeframe, source_interval, moex_secid)
        )
        if not candidates:
            continue
        ts_close_raw = raw_row.get("ts_close")
        if not isinstance(ts_close_raw, str) or not ts_close_raw.strip():
            continue
        ts_close = _parse_iso_utc(ts_close_raw)
        for window in candidates:
            start = _parse_iso_utc(window.window_start_utc)
            end = _parse_iso_utc(window.window_end_utc)
            if start <= ts_close <= end:
                row_signature = json.dumps(
                    raw_row, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                dedup_key = (
                    internal_id,
                    source_timeframe,
                    source_interval,
                    moex_secid,
                    row_signature,
                    str(ts_close_raw),
                )
                if dedup_key not in matched_keys:
                    scoped_rows.append(dict(raw_row))
                    matched_keys.add(dedup_key)
                matched_window_entries.add(
                    (
                        window.internal_id,
                        window.source_timeframe,
                        window.source_interval,
                        window.moex_secid,
                        window.window_start_utc,
                        window.window_end_utc,
                    )
                )
                break

    for window in changed_windows:
        key = (
            window.internal_id,
            window.source_timeframe,
            window.source_interval,
            window.moex_secid,
            window.window_start_utc,
            window.window_end_utc,
        )
        if key not in matched_window_entries:
            unmatched_windows.append(window.to_dict())

    return scoped_rows, unmatched_windows


def _validate_changed_window_width(
    *,
    changed_windows: list[ChangedWindowScope],
    max_changed_window_days: int | None,
) -> None:
    if max_changed_window_days is None:
        return
    if max_changed_window_days <= 0:
        raise ValueError("`max_changed_window_days` must be > 0")
    max_seconds = max_changed_window_days * 24 * 60 * 60
    for window in changed_windows:
        start = _parse_iso_utc(window.window_start_utc)
        end = _parse_iso_utc(window.window_end_utc)
        width_seconds = (end - start).total_seconds()
        if width_seconds > max_seconds:
            raise ValueError(
                "canonical refresh window is wider than allowed for baseline update: "
                f"{window.internal_id}/{window.source_timeframe}/{window.moex_secid} "
                f"{window.window_start_utc}->{window.window_end_utc} "
                f"width_days={width_seconds / 86400:.2f} max_days={max_changed_window_days}"
            )


def _build_raw_parity_report(
    *,
    run_id: str,
    changed_windows: list[ChangedWindowScope],
    scoped_raw_rows: list[dict[str, object]],
    unmatched_windows: list[dict[str, object]],
) -> dict[str, object]:
    duplicate_errors: list[str] = []
    timestamp_drift_errors: list[str] = []

    seen_keys: set[tuple[str, str, int, str, str, str]] = set()
    for row_index, row in enumerate(scoped_raw_rows):
        if not isinstance(row, dict):
            continue
        internal_id = str(row.get("internal_id", "")).strip()
        source_timeframe = str(row.get("timeframe", "")).strip()
        moex_secid = _extract_row_moex_secid(row)
        try:
            source_interval = _normalize_source_interval(
                source_timeframe=source_timeframe,
                source_interval_raw=row.get("source_interval"),
                row_index=row_index,
            )
        except ValueError:
            continue
        ts_open = str(row.get("ts_open", "")).strip()
        ts_close = str(row.get("ts_close", "")).strip()
        if not ts_open or not ts_close:
            continue
        key = (internal_id, source_timeframe, source_interval, moex_secid, ts_open, ts_close)
        if key in seen_keys:
            duplicate_errors.append(
                f"duplicate raw key: {internal_id}/{source_timeframe}/{moex_secid}/{source_interval}/{ts_open}/{ts_close}"
            )
        seen_keys.add(key)
        if _parse_iso_utc(ts_close) < _parse_iso_utc(ts_open):
            timestamp_drift_errors.append(
                f"invalid raw window ordering: {internal_id}/{source_timeframe}/{moex_secid}/{ts_open}->{ts_close}"
            )

    failures: list[str] = []
    if unmatched_windows:
        failures.append("missing_window_rows")
    if duplicate_errors:
        failures.append("duplicate_rows")
    if timestamp_drift_errors:
        failures.append("timestamp_drift")

    return {
        "run_id": run_id,
        "status": STATUS_PASS if not failures else "FAIL",
        "window_count": len(changed_windows),
        "scoped_source_rows": len(scoped_raw_rows),
        "unmatched_windows_count": len(unmatched_windows),
        "failure_classes": failures,
        "samples": {
            "unmatched_windows": unmatched_windows[:20],
            "duplicate_rows": _sample_errors(duplicate_errors, limit=20),
            "timestamp_drift": _sample_errors(timestamp_drift_errors, limit=20),
        },
    }


def _build_canonical_parity_report(
    *,
    run_id: str,
    scoped_bars: list[CanonicalBar],
    final_bars: list[CanonicalBar],
    affected_keys: set[tuple[str, str, str, str]],
) -> dict[str, object]:
    expected_by_key = {_canonical_bar_key(item): item.to_dict() for item in scoped_bars}
    final_by_key: dict[tuple[str, str, str, str], dict[str, object]] = {}
    duplicate_errors: list[str] = []
    timestamp_drift_errors: list[str] = []
    for row in final_bars:
        key = _canonical_bar_key(row)
        if key not in affected_keys:
            continue
        if key in final_by_key:
            duplicate_errors.append(f"duplicate canonical key: {key[0]}/{key[2]}/{key[3]}")
        final_by_key[key] = row.to_dict()
        timeframe = Timeframe(str(row.timeframe.value))
        expected_ts = _floor_to_bucket(
            row.ts, bucket_minutes=TARGET_MINUTES_BY_TIMEFRAME[timeframe]
        )
        if expected_ts != row.ts:
            timestamp_drift_errors.append(f"timestamp drift: {key[0]}/{key[2]}/{key[3]}")

    missing_bar_errors: list[str] = []
    aggregation_mismatch_errors: list[str] = []
    for key, expected in expected_by_key.items():
        final_row = final_by_key.get(key)
        if final_row is None:
            missing_bar_errors.append(f"missing canonical bar: {key[0]}/{key[2]}/{key[3]}")
            continue
        comparable_fields = ("open", "high", "low", "close", "volume", "open_interest")
        for field_name in comparable_fields:
            if expected[field_name] != final_row[field_name]:
                aggregation_mismatch_errors.append(
                    f"aggregation mismatch {key[0]}/{key[2]}/{key[3]} field={field_name}"
                )
                break

    failures: list[str] = []
    if missing_bar_errors:
        failures.append("missing_bar")
    if duplicate_errors:
        failures.append("duplicate_rows")
    if timestamp_drift_errors:
        failures.append("timestamp_drift")
    if aggregation_mismatch_errors:
        failures.append("aggregation_mismatch")

    return {
        "run_id": run_id,
        "status": STATUS_PASS if not failures else "FAIL",
        "affected_key_count": len(affected_keys),
        "expected_scope_rows": len(scoped_bars),
        "resolved_scope_rows": len(final_by_key),
        "failure_classes": failures,
        "samples": {
            "missing_bar": _sample_errors(missing_bar_errors, limit=20),
            "duplicate_rows": _sample_errors(duplicate_errors, limit=20),
            "timestamp_drift": _sample_errors(timestamp_drift_errors, limit=20),
            "aggregation_mismatch": _sample_errors(aggregation_mismatch_errors, limit=20),
        },
    }


def _status_for_publish_decision(
    *, publish_allowed: bool, changed_windows: list[ChangedWindowScope]
) -> str:
    if not publish_allowed:
        return STATUS_BLOCKED
    if not changed_windows:
        return STATUS_PASS_NOOP
    return STATUS_PASS


def _int_report_value(report: dict[str, object] | None, key: str) -> int:
    if not isinstance(report, dict):
        return 0
    value = report.get(key)
    if isinstance(value, bool):
        return 0
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _build_spark_raw_parity_report(
    *,
    run_id: str,
    changed_windows: list[ChangedWindowScope],
    spark_execution_report: dict[str, object] | None,
) -> dict[str, object]:
    scoped_source_rows = _int_report_value(spark_execution_report, "source_rows")
    unmatched_windows_count = _int_report_value(spark_execution_report, "unmatched_window_rows")
    failures: list[str] = []
    if changed_windows and scoped_source_rows <= 0:
        failures.append("missing_scoped_source_rows")
    if unmatched_windows_count > 0:
        failures.append("unmatched_windows")
    samples: dict[str, object] = {}
    if isinstance(spark_execution_report, dict):
        unmatched = spark_execution_report.get("unmatched_windows")
        if isinstance(unmatched, list):
            samples["unmatched_windows"] = unmatched[:20]
    return {
        "run_id": run_id,
        "runtime_owner": "spark_delta",
        "status": STATUS_PASS if not failures else "FAIL",
        "window_count": len(changed_windows),
        "scoped_source_rows": scoped_source_rows,
        "unmatched_windows_count": unmatched_windows_count,
        "failure_classes": failures,
        "samples": samples,
    }


def _build_spark_canonical_parity_report(
    *,
    run_id: str,
    spark_execution_report: dict[str, object] | None,
) -> dict[str, object]:
    source_rows = _int_report_value(spark_execution_report, "source_rows")
    canonical_rows = _int_report_value(spark_execution_report, "canonical_rows")
    provenance_rows = _int_report_value(spark_execution_report, "provenance_rows")
    failures: list[str] = []
    if source_rows > 0 and canonical_rows <= 0:
        failures.append("missing_canonical_rows")
    if canonical_rows != provenance_rows:
        failures.append("canonical_provenance_row_count_mismatch")
    return {
        "run_id": run_id,
        "runtime_owner": "spark_delta",
        "status": STATUS_PASS if not failures else "FAIL",
        "affected_key_count": canonical_rows,
        "expected_scope_rows": canonical_rows,
        "resolved_scope_rows": canonical_rows,
        "failure_classes": failures,
        "samples": {},
    }


def _real_bindings_from_spark_report(
    *,
    raw_ingest_run_report: dict[str, object],
    spark_execution_report: dict[str, object] | None,
) -> list[str]:
    bindings = set(
        _report_real_bindings(raw_ingest_run_report=raw_ingest_run_report, fallback_rows=[])
    )
    if isinstance(spark_execution_report, dict):
        providers = spark_execution_report.get("source_providers")
        if isinstance(providers, (list, tuple)):
            for provider in providers:
                text = str(provider).strip()
                if text:
                    bindings.add(text)
    return sorted(bindings)


def _run_scoped_spark_delta_publish_route(
    *,
    raw_table_path: Path,
    output_dir: Path,
    run_id: str,
    raw_ingest_run_report: dict[str, object],
    repo_root: Path,
    bars_path: Path,
    provenance_path: Path,
    session_calendar_path: Path,
    roll_map_path: Path,
    changed_window_scope: list[ChangedWindowScope],
    changed_window_set_manifest: dict[str, object],
    source_rows: int,
    max_changed_window_days: int | None,
) -> dict[str, object]:
    built_at_utc = _utc_now_iso()
    available_intervals_by_contract = _build_available_intervals_by_changed_window(
        changed_window_scope
    )
    selected_source_intervals = _build_selected_source_interval_map_from_available_intervals(
        available_intervals_by_contract
    )
    resampling_skips = _build_resampling_skips_from_available_intervals(
        available_intervals_by_contract,
        selected_source_intervals=selected_source_intervals,
    )
    canonical_buckets = build_canonical_bucket_rows(
        changed_windows=changed_window_scope,
        selected_source_intervals=selected_source_intervals,
        available_intervals_by_contract=available_intervals_by_contract,
        target_timeframes=tuple(item.value for item in TARGET_TIMEFRAMES),
    )

    spark_execution_report: dict[str, object] | None = None
    scoped_bars_path: Path | None = None
    scoped_provenance_path: Path | None = None
    publish_scope_path: Path | None = None
    if changed_window_scope and selected_source_intervals:
        spark_execution_report = _run_spark_canonicalization(
            raw_table_path=raw_table_path,
            changed_windows=changed_window_scope,
            selected_source_intervals=selected_source_intervals,
            canonical_buckets=canonical_buckets,
            output_dir=output_dir,
            run_id=run_id,
            built_at_utc=built_at_utc,
            repo_root=repo_root,
        )
        spark_output_paths = spark_execution_report.get("output_paths", {})
        if isinstance(spark_output_paths, dict):
            raw_bars_path = str(spark_output_paths.get("canonical_bars", "")).strip()
            raw_provenance_path = str(
                spark_output_paths.get("canonical_bar_provenance", "")
            ).strip()
            scoped_bars_path = Path(raw_bars_path) if raw_bars_path else None
            scoped_provenance_path = Path(raw_provenance_path) if raw_provenance_path else None
        publish_scope_path = output_dir / ".spark-canonicalization" / "publish-scope.jsonl"

    raw_parity_report = _build_spark_raw_parity_report(
        run_id=run_id,
        changed_windows=changed_window_scope,
        spark_execution_report=spark_execution_report,
    )
    canonical_parity_report = _build_spark_canonical_parity_report(
        run_id=run_id,
        spark_execution_report=spark_execution_report,
    )
    contract_report: dict[str, object] = {
        "runtime_owner": "spark_delta",
        "status": "NOT_RUN",
        "errors": [],
        "checked_rows": _int_report_value(spark_execution_report, "canonical_rows"),
        "mode": "pending_spark_publish_contract_report",
    }
    runtime_report = run_runtime_decoupling_check(repo_root=repo_root)

    qc_report: dict[str, object] = {
        "run_id": run_id,
        "runtime_owner": "spark_delta",
        "status": "PASS",
        "publish_decision": "publish",
        "failed_gates": [],
        "gate_results": [],
    }
    publish_allowed = (
        raw_parity_report["status"] == STATUS_PASS
        and canonical_parity_report["status"] == STATUS_PASS
        and runtime_report["status"] == "PASS"
    )

    spark_publish_report: dict[str, object] | None = None
    if publish_allowed and spark_execution_report is not None:
        if scoped_bars_path is None or scoped_provenance_path is None:
            raise RuntimeError("scoped canonical publish requires Spark output paths")
        spark_publish_report = _run_spark_canonical_publish(
            staged_bars_path=scoped_bars_path,
            staged_provenance_path=scoped_provenance_path,
            publish_scope_path=publish_scope_path,
            target_bars_path=bars_path,
            target_provenance_path=provenance_path,
            session_calendar_path=session_calendar_path,
            roll_map_path=roll_map_path,
            output_dir=output_dir,
            run_id=run_id,
            repo_root=repo_root,
        )
        qc_payload = spark_publish_report.get("qc_report")
        if isinstance(qc_payload, dict):
            qc_report = qc_payload
        else:
            qc_report = {
                "run_id": run_id,
                "runtime_owner": "spark_delta",
                "status": "FAIL",
                "publish_decision": "blocked",
                "failed_gates": ["spark_publish_qc_report"],
                "gate_results": [],
            }
        contract_payload = spark_publish_report.get("contract_compatibility_report")
        if isinstance(contract_payload, dict):
            contract_report = contract_payload
        else:
            contract_report = {
                "runtime_owner": "spark_delta",
                "status": "FAIL",
                "checked_rows": _int_report_value(spark_execution_report, "canonical_rows"),
                "errors": ["spark publish report missing contract_compatibility_report"],
                "mode": "spark_publish_contract_report_missing",
            }
        qc_passed = (
            isinstance(qc_payload, dict) and str(qc_payload.get("status", "")).strip() == "PASS"
        )
        contract_passed = (
            isinstance(contract_payload, dict)
            and str(contract_payload.get("status", "")).strip() == "PASS"
        )
        publish_allowed = (
            publish_allowed
            and str(spark_publish_report.get("publish_decision", "")).strip() == "publish"
            and qc_passed
            and contract_passed
        )
    else:
        qc_report = {
            "run_id": run_id,
            "runtime_owner": "spark_delta",
            "status": "FAIL" if not publish_allowed else "PASS",
            "publish_decision": "blocked" if not publish_allowed else "publish",
            "failed_gates": [] if publish_allowed else ["pre_publish_spark_parity"],
            "gate_results": [],
        }

    sidecar_report: dict[str, object] = {
        "mode": "skipped",
        "mutation_applied": False,
        "refreshed_session_calendar_rows": 0,
        "refreshed_roll_map_rows": 0,
        "affected_session_rows": 0,
        "overlap_session_rows": 0,
        "overlap_policy": "",
    }
    if isinstance(spark_publish_report, dict) and isinstance(
        spark_publish_report.get("sidecar_refresh"), dict
    ):
        sidecar_report = dict(spark_publish_report["sidecar_refresh"])

    changed_window_set_path = output_dir / "changed-window-set-manifest.json"
    raw_parity_path = output_dir / "raw-parity-report.json"
    canonical_parity_path = output_dir / "canonical-parity-report.json"
    qc_path = output_dir / "qc-report.json"
    contract_path = output_dir / "contract-compatibility-report.json"
    runtime_path = output_dir / "runtime-decoupling-proof.json"
    canonical_snapshot_path = output_dir / "canonical-snapshot.json"
    resampling_snapshot_path = output_dir / "resampling-snapshot.json"

    canonical_snapshot = {
        "runtime_owner": "spark_delta",
        "mode": "not_materialized_in_python_hot_path",
        "scoped_canonical_rows": _int_report_value(spark_execution_report, "canonical_rows"),
    }
    resampling_snapshot = {
        "runtime_owner": "spark_delta",
        "mode": "changed-window-source-intervals",
        "resampling_skips": _summarize_resampling_skips(resampling_skips),
    }

    _json_write(changed_window_set_path, changed_window_set_manifest)
    _json_write(raw_parity_path, raw_parity_report)
    _json_write(canonical_parity_path, canonical_parity_report)
    _json_write(qc_path, qc_report)
    _json_write(contract_path, contract_report)
    _json_write(runtime_path, runtime_report)
    _json_write(canonical_snapshot_path, canonical_snapshot)
    _json_write(resampling_snapshot_path, resampling_snapshot)

    output_paths: dict[str, str] = {}
    if publish_allowed and isinstance(spark_publish_report, dict):
        spark_output_paths = spark_publish_report.get("output_paths")
        spark_delta_log = spark_publish_report.get("delta_log")
        if isinstance(spark_output_paths, dict) and isinstance(spark_delta_log, dict):
            for key, value in spark_output_paths.items():
                delta_payload = spark_delta_log.get(str(key), {})
                if isinstance(delta_payload, dict) and bool(delta_payload.get("delta_log")):
                    output_paths[str(key)] = str(value)

    mutation_applied = (
        bool(spark_publish_report.get("mutation_applied", False))
        if isinstance(spark_publish_report, dict)
        else False
    )
    status = _status_for_publish_decision(
        publish_allowed=publish_allowed,
        changed_windows=changed_window_scope,
    )
    canonical_rows = (
        _int_report_value(spark_publish_report, "canonical_rows")
        if isinstance(spark_publish_report, dict)
        else 0
    )
    provenance_rows = (
        _int_report_value(spark_publish_report, "provenance_rows")
        if isinstance(spark_publish_report, dict)
        else 0
    )

    report: dict[str, object] = {
        "run_id": run_id,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "canonicalization_engine": "spark",
        "canonical_publish_engine": "spark_delta",
        "bucket_mode": "canonical_bucket_map",
        "canonical_bucket_rows": len(canonical_buckets),
        "policy_version": CANONICAL_BUCKET_POLICY_VERSION,
        "status": status,
        "publish_decision": "publish" if publish_allowed else "blocked",
        "raw_table_path": raw_table_path.as_posix(),
        "output_dir": output_dir.as_posix(),
        "source_rows": source_rows,
        "scoped_source_rows": _int_report_value(spark_execution_report, "source_rows"),
        "changed_windows_count": len(changed_window_scope),
        "changed_windows_hash_sha256": changed_window_set_manifest["changed_windows_hash_sha256"],
        "canonical_merge_strategy": CANONICAL_MERGE_SCOPED_DELETE_INSERT,
        "max_changed_window_days": max_changed_window_days,
        "affected_key_count": _int_report_value(spark_execution_report, "canonical_rows"),
        "mutation_applied": mutation_applied,
        "canonical_rows": canonical_rows,
        "provenance_rows": provenance_rows,
        "sidecar_refresh": {
            "mode": sidecar_report.get("mode"),
            "mutation_applied": bool(sidecar_report.get("mutation_applied", False)),
            "refreshed_session_calendar_rows": int(
                sidecar_report.get("refreshed_session_calendar_rows", 0) or 0
            ),
            "refreshed_roll_map_rows": int(sidecar_report.get("refreshed_roll_map_rows", 0) or 0),
            "affected_session_rows": int(sidecar_report.get("affected_session_rows", 0) or 0),
            "overlap_session_rows": int(sidecar_report.get("overlap_session_rows", 0) or 0),
            "overlap_policy": str(sidecar_report.get("overlap_policy", "")),
        },
        "scoped_canonical_rows": _int_report_value(spark_execution_report, "canonical_rows"),
        "target_timeframes": [item.value for item in TARGET_TIMEFRAMES],
        "resampling_skips": _summarize_resampling_skips(resampling_skips),
        "output_paths": output_paths,
        "artifact_paths": {
            "changed_window_set_manifest": changed_window_set_path.as_posix(),
            "raw_parity_report": raw_parity_path.as_posix(),
            "canonical_parity_report": canonical_parity_path.as_posix(),
            "canonical_snapshot": canonical_snapshot_path.as_posix(),
            "resampling_snapshot": resampling_snapshot_path.as_posix(),
            "qc_report": qc_path.as_posix(),
            "contract_compatibility_report": contract_path.as_posix(),
            "runtime_decoupling_proof": runtime_path.as_posix(),
        },
        "changed_window_set_manifest": changed_window_set_manifest,
        "raw_parity_report": raw_parity_report,
        "canonical_parity_report": canonical_parity_report,
        "qc_report": qc_report,
        "contract_compatibility_report": contract_report,
        "runtime_decoupling_proof": runtime_report,
        "real_bindings": _real_bindings_from_spark_report(
            raw_ingest_run_report=raw_ingest_run_report,
            spark_execution_report=spark_execution_report,
        ),
    }
    if spark_execution_report is not None:
        report["artifact_paths"]["spark_execution_report"] = (
            output_dir / ".spark-canonicalization" / "spark-execution-report.json"
        ).as_posix()
        report["artifact_paths"]["canonical_buckets_jsonl"] = (
            output_dir / ".spark-canonicalization" / "canonical-buckets.jsonl"
        ).as_posix()
        report["spark_execution_report"] = spark_execution_report
    if spark_publish_report is not None:
        report["artifact_paths"]["spark_publish_report"] = (
            output_dir / ".spark-canonical-publish" / "spark-publish-report.json"
        ).as_posix()
        report["spark_publish_report"] = spark_publish_report

    _json_write(output_dir / "canonical-refresh-report.json", report)
    return report


def run_historical_canonical_route(
    *,
    raw_table_path: Path,
    output_dir: Path,
    run_id: str,
    raw_ingest_run_report: dict[str, object],
    repo_root: Path | None = None,
    canonical_bars_path: Path | None = None,
    canonical_provenance_path: Path | None = None,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    canonical_merge_strategy: str = CANONICAL_MERGE_SCOPED_DELETE_INSERT,
    max_changed_window_days: int | None = None,
) -> dict[str, object]:
    if not isinstance(raw_ingest_run_report, dict):
        raise ValueError("`raw_ingest_run_report` must be object")
    repo_root = repo_root.resolve() if repo_root else Path(__file__).resolve().parents[5]
    if not has_delta_log(raw_table_path):
        raise FileNotFoundError(
            f"phase-02 raw source table missing `_delta_log`: {raw_table_path.as_posix()}"
        )
    if canonical_merge_strategy != CANONICAL_MERGE_SCOPED_DELETE_INSERT:
        raise ValueError(
            "phase-02 canonical route only supports scoped_delete_insert Spark/Delta publish"
        )

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    bars_path = (canonical_bars_path or (output_dir / "delta" / "canonical_bars.delta")).resolve()
    provenance_path = (
        canonical_provenance_path or (output_dir / "delta" / "canonical_bar_provenance.delta")
    ).resolve()
    session_calendar_path = (
        canonical_session_calendar_path
        or (output_dir / "delta" / "canonical_session_calendar.delta")
    ).resolve()
    roll_map_path = (
        canonical_roll_map_path or (output_dir / "delta" / "canonical_roll_map.delta")
    ).resolve()

    changed_windows_raw = raw_ingest_run_report.get("changed_windows", [])
    if not isinstance(changed_windows_raw, (list, tuple)):
        raise ValueError("`raw_ingest_run_report.changed_windows` must be list-like")
    normalized_changed_windows = normalize_changed_windows(list(changed_windows_raw))
    changed_window_scope = _prepare_changed_window_scope(normalized_changed_windows)
    _validate_changed_window_width(
        changed_windows=changed_window_scope,
        max_changed_window_days=max_changed_window_days,
    )

    changed_window_set_manifest = build_parity_manifest_v1(
        run_id=run_id,
        raw_ingest_run_report=raw_ingest_run_report,
        changed_windows=normalized_changed_windows,
    )

    raw_report_status = str(raw_ingest_run_report.get("status", "")).strip()
    if raw_report_status not in {STATUS_PASS, STATUS_PASS_NOOP}:
        raise ValueError(
            "phase-02 requires raw ingest status PASS or PASS-NOOP before canonical execution; "
            f"got `{raw_report_status or 'EMPTY'}`"
        )
    if raw_report_status == STATUS_PASS and not changed_window_scope:
        raise ValueError(
            "phase-02 scope mismatch: raw ingest status PASS requires non-empty changed_windows"
        )
    if raw_report_status == STATUS_PASS_NOOP and changed_window_scope:
        raise ValueError(
            "phase-02 scope mismatch: raw ingest status PASS-NOOP requires empty changed_windows"
        )

    source_rows = _report_source_row_count(
        raw_ingest_run_report=raw_ingest_run_report,
        raw_table_path=raw_table_path,
    )
    if raw_report_status == STATUS_PASS_NOOP:
        changed_window_set_path = output_dir / "changed-window-set-manifest.json"
        runtime_path = output_dir / "runtime-decoupling-proof.json"
        runtime_report = run_runtime_decoupling_check(repo_root=repo_root)
        runtime_pass = str(runtime_report.get("status", "")).strip() == STATUS_PASS
        noop_status = STATUS_PASS_NOOP if runtime_pass else "BLOCKED"
        noop_publish_decision = "publish" if runtime_pass else "blocked"
        noop_failed_gates = [] if runtime_pass else ["runtime_decoupling"]
        _json_write(changed_window_set_path, changed_window_set_manifest)
        _json_write(runtime_path, runtime_report)

        output_paths: dict[str, str] = {}
        if has_delta_log(bars_path):
            output_paths["canonical_bars"] = bars_path.as_posix()
        if has_delta_log(provenance_path):
            output_paths["canonical_bar_provenance"] = provenance_path.as_posix()
        if has_delta_log(session_calendar_path):
            output_paths["canonical_session_calendar"] = session_calendar_path.as_posix()
        if has_delta_log(roll_map_path):
            output_paths["canonical_roll_map"] = roll_map_path.as_posix()

        report: dict[str, object] = {
            "run_id": run_id,
            "route_signal": "worker:phase-only",
            "proof_class": "staging-real",
            "canonicalization_engine": "spark",
            "canonical_publish_engine": "spark_delta",
            "status": noop_status,
            "publish_decision": noop_publish_decision,
            "raw_table_path": raw_table_path.as_posix(),
            "output_dir": output_dir.as_posix(),
            "source_rows": source_rows,
            "scoped_source_rows": 0,
            "changed_windows_count": 0,
            "changed_windows_hash_sha256": changed_window_set_manifest[
                "changed_windows_hash_sha256"
            ],
            "canonical_merge_strategy": CANONICAL_MERGE_SCOPED_DELETE_INSERT,
            "max_changed_window_days": max_changed_window_days,
            "affected_key_count": 0,
            "mutation_applied": False,
            "canonical_rows": count_delta_table_rows(bars_path) if has_delta_log(bars_path) else 0,
            "provenance_rows": count_delta_table_rows(provenance_path)
            if has_delta_log(provenance_path)
            else 0,
            "sidecar_refresh": {
                "mode": "noop"
                if has_delta_log(session_calendar_path) and has_delta_log(roll_map_path)
                else "skipped",
                "mutation_applied": False,
                "refreshed_session_calendar_rows": 0,
                "refreshed_roll_map_rows": 0,
                "affected_session_rows": 0,
                "overlap_session_rows": 0,
                "overlap_policy": "",
            },
            "scoped_canonical_rows": 0,
            "target_timeframes": [item.value for item in TARGET_TIMEFRAMES],
            "resampling_skips": {},
            "output_paths": output_paths,
            "artifact_paths": {
                "changed_window_set_manifest": changed_window_set_path.as_posix(),
                "runtime_decoupling_proof": runtime_path.as_posix(),
            },
            "changed_window_set_manifest": changed_window_set_manifest,
            "raw_parity_report": {
                "run_id": run_id,
                "status": STATUS_PASS,
                "window_count": 0,
                "scoped_source_rows": 0,
                "unmatched_windows_count": 0,
                "failure_classes": [],
                "samples": {},
            },
            "canonical_parity_report": {
                "run_id": run_id,
                "status": STATUS_PASS,
                "affected_key_count": 0,
                "expected_scope_rows": 0,
                "resolved_scope_rows": 0,
                "failure_classes": [],
                "samples": {},
            },
            "qc_report": {
                "run_id": run_id,
                "runtime_owner": "spark_delta",
                "status": "PASS" if runtime_pass else "FAIL",
                "publish_decision": noop_publish_decision,
                "failed_gates": noop_failed_gates,
                "gate_results": [],
            },
            "contract_compatibility_report": {
                "status": "SKIPPED",
                "checked_rows": 0,
                "errors": [],
                "reason": "noop refresh did not produce Spark canonical rows",
            },
            "runtime_decoupling_proof": runtime_report,
            "real_bindings": _report_real_bindings(
                raw_ingest_run_report=raw_ingest_run_report,
                fallback_rows=[],
            ),
        }
        _json_write(output_dir / "canonical-refresh-report.json", report)
        return report

    return _run_scoped_spark_delta_publish_route(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id=run_id,
        raw_ingest_run_report=raw_ingest_run_report,
        repo_root=repo_root,
        bars_path=bars_path,
        provenance_path=provenance_path,
        session_calendar_path=session_calendar_path,
        roll_map_path=roll_map_path,
        changed_window_scope=changed_window_scope,
        changed_window_set_manifest=changed_window_set_manifest,
        source_rows=source_rows,
        max_changed_window_days=max_changed_window_days,
    )
