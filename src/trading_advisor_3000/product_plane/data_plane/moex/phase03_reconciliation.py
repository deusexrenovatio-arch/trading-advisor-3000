from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
from statistics import median

import yaml

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_rows,
)


MANDATORY_RECONCILIATION_DIMENSIONS: tuple[str, ...] = (
    "close_drift_bps",
    "volume_drift_ratio",
    "missing_bars_ratio",
    "lag_class",
)

MANDATORY_FINAM_METADATA_FIELDS: tuple[str, ...] = (
    "source_ts_utc",
    "received_at_utc",
    "archive_batch_id",
    "source_provider",
    "source_binding",
)

FINAM_ARCHIVE_COLUMNS: dict[str, str] = {
    "contract_id": "string",
    "instrument_id": "string",
    "timeframe": "string",
    "ts": "timestamp",
    "close": "double",
    "volume": "bigint",
    "source_ts_utc": "timestamp",
    "received_at_utc": "timestamp",
    "latency_seconds": "double",
    "lag_class": "string",
    "archive_batch_id": "string",
    "source_provider": "string",
    "source_binding": "string",
    "ingest_run_id": "string",
    "ingested_at_utc": "timestamp",
}

RECONCILIATION_METRICS_COLUMNS: dict[str, str] = {
    "contract_id": "string",
    "instrument_id": "string",
    "timeframe": "string",
    "ts": "timestamp",
    "asset_group": "string",
    "moex_close": "double",
    "finam_close": "double",
    "close_drift_bps": "double",
    "close_hard_threshold_bps": "double",
    "close_hard_violation": "int",
    "moex_volume": "bigint",
    "finam_volume": "bigint",
    "volume_drift_ratio": "double",
    "volume_hard_threshold": "double",
    "volume_hard_violation": "int",
    "moex_lag_class": "string",
    "finam_lag_class": "string",
    "lag_class_mismatch": "int",
    "missing_in_finam": "int",
    "unexpected_in_finam": "int",
    "archive_batch_id": "string",
    "run_id": "string",
    "generated_at_utc": "timestamp",
}


@dataclass(frozen=True)
class ThresholdPolicy:
    policy_id: str
    version: int
    required_dimensions: tuple[str, ...]
    lag_class_seconds: dict[str, int]
    close_drift_hard_by_asset_group: dict[str, float]
    volume_drift_hard_by_timeframe: dict[str, float]
    missing_bars_ratio_hard_max: float
    lag_class_mismatch_ratio_hard_max: float
    require_alert_simulation: bool
    hard_violation_requires_incident: bool
    escalation_channels: tuple[str, ...]


@dataclass(frozen=True)
class FinamArchiveSnapshot:
    contract_id: str
    instrument_id: str
    timeframe: str
    ts: str
    close: float
    volume: int
    source_ts_utc: str
    received_at_utc: str
    latency_seconds: float
    lag_class: str
    archive_batch_id: str
    source_provider: str
    source_binding: str
    ingest_run_id: str
    ingested_at_utc: str

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "ts": self.ts,
            "close": self.close,
            "volume": self.volume,
            "source_ts_utc": self.source_ts_utc,
            "received_at_utc": self.received_at_utc,
            "latency_seconds": self.latency_seconds,
            "lag_class": self.lag_class,
            "archive_batch_id": self.archive_batch_id,
            "source_provider": self.source_provider,
            "source_binding": self.source_binding,
            "ingest_run_id": self.ingest_run_id,
            "ingested_at_utc": self.ingested_at_utc,
        }


@dataclass(frozen=True)
class MoexOverlapRow:
    contract_id: str
    instrument_id: str
    timeframe: str
    ts: str
    close: float
    volume: int
    lag_class: str
    source_provider: str


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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _fingerprint_finam_source(path: Path) -> dict[str, object]:
    resolved = path.resolve()
    if has_delta_log(resolved):
        files = sorted(item for item in resolved.rglob("*") if item.is_file())
        rows: list[dict[str, object]] = []
        aggregate = hashlib.sha256()
        for item in files:
            rel = item.relative_to(resolved).as_posix()
            size = item.stat().st_size
            file_hash = _sha256_file(item)
            rows.append({"path": rel, "size_bytes": size, "sha256": file_hash})
            aggregate.update(f"{rel}:{size}:{file_hash}\n".encode("utf-8"))
        return {
            "source_kind": "delta",
            "source_root": resolved.as_posix(),
            "file_count": len(rows),
            "fingerprint_sha256": aggregate.hexdigest(),
            "files": rows,
        }
    return {
        "source_kind": "file",
        "source_path": resolved.as_posix(),
        "size_bytes": resolved.stat().st_size,
        "fingerprint_sha256": _sha256_file(resolved),
    }


def _require_text(payload: dict[str, object], key: str, *, row_index: int, aliases: tuple[str, ...] = ()) -> str:
    for name in (key, *aliases):
        value = payload.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    alias_text = f" (aliases: {', '.join(aliases)})" if aliases else ""
    raise ValueError(f"row[{row_index}] `{key}` must be a non-empty string{alias_text}")


def _require_number(payload: dict[str, object], key: str, *, row_index: int, aliases: tuple[str, ...] = ()) -> float:
    for name in (key, *aliases):
        value = payload.get(name)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
    alias_text = f" (aliases: {', '.join(aliases)})" if aliases else ""
    raise ValueError(f"row[{row_index}] `{key}` must be a number{alias_text}")


def _require_int(payload: dict[str, object], key: str, *, row_index: int, aliases: tuple[str, ...] = ()) -> int:
    for name in (key, *aliases):
        value = payload.get(name)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return int(value)
        if isinstance(value, float) and value.is_integer():
            return int(value)
    alias_text = f" (aliases: {', '.join(aliases)})" if aliases else ""
    raise ValueError(f"row[{row_index}] `{key}` must be an integer{alias_text}")


def _load_snapshot_source_rows(path: Path) -> list[dict[str, object]]:
    resolved = path.resolve()
    if has_delta_log(resolved):
        rows = read_delta_table_rows(resolved)
        return [row for row in rows if isinstance(row, dict)]

    suffix = resolved.suffix.lower()
    if suffix == ".json":
        payload = json.loads(resolved.read_text(encoding="utf-8-sig"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            rows = payload.get("rows")
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        raise ValueError("finam snapshot json must be list[object] or {'rows': list[object]}")

    if suffix == ".csv":
        with resolved.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [
                {str(key): value for key, value in row.items()}
                for row in reader
                if isinstance(row, dict)
            ]

    raise ValueError(f"unsupported finam archive format: {resolved.as_posix()} (expected .json/.csv or delta table)")


def _classify_lag(*, latency_seconds: float, lag_class_seconds: dict[str, int]) -> str:
    low_max = int(lag_class_seconds["low"])
    medium_max = int(lag_class_seconds["medium"])
    if latency_seconds <= low_max:
        return "low"
    if latency_seconds <= medium_max:
        return "medium"
    return "high"


def load_phase03_threshold_policy(path: Path) -> ThresholdPolicy:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("threshold policy must be a YAML object")

    policy_id = str(payload.get("policy_id", "")).strip()
    if not policy_id:
        raise ValueError("threshold policy requires non-empty `policy_id`")

    version_raw = payload.get("version")
    if isinstance(version_raw, bool) or not isinstance(version_raw, int) or version_raw <= 0:
        raise ValueError("threshold policy `version` must be positive integer")

    required_raw = payload.get("required_dimensions")
    if not isinstance(required_raw, list):
        raise ValueError("threshold policy `required_dimensions` must be list")
    required_dimensions = tuple(str(item).strip() for item in required_raw if str(item).strip())

    missing_dimensions = sorted(set(MANDATORY_RECONCILIATION_DIMENSIONS) - set(required_dimensions))
    if missing_dimensions:
        missing_text = ", ".join(missing_dimensions)
        raise ValueError(f"threshold policy missing mandatory dimensions: {missing_text}")

    lag_raw = payload.get("lag_class_seconds")
    if not isinstance(lag_raw, dict):
        raise ValueError("threshold policy `lag_class_seconds` must be object")
    low_raw = lag_raw.get("low")
    medium_raw = lag_raw.get("medium")
    if isinstance(low_raw, bool) or not isinstance(low_raw, int) or low_raw <= 0:
        raise ValueError("threshold policy `lag_class_seconds.low` must be positive integer")
    if isinstance(medium_raw, bool) or not isinstance(medium_raw, int) or medium_raw < low_raw:
        raise ValueError("threshold policy `lag_class_seconds.medium` must be integer >= low")

    close_raw = payload.get("close_drift_bps")
    if not isinstance(close_raw, dict):
        raise ValueError("threshold policy `close_drift_bps` must be object")
    close_by_group_raw = close_raw.get("hard_by_asset_group")
    if not isinstance(close_by_group_raw, dict) or not close_by_group_raw:
        raise ValueError("threshold policy `close_drift_bps.hard_by_asset_group` must be non-empty object")
    close_by_group: dict[str, float] = {}
    for key, value in close_by_group_raw.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) <= 0:
            raise ValueError(f"close drift threshold for asset_group `{key}` must be > 0")
        close_by_group[str(key).strip()] = float(value)
    if "default" not in close_by_group:
        raise ValueError("close drift thresholds require `default` fallback")

    volume_raw = payload.get("volume_drift_ratio")
    if not isinstance(volume_raw, dict):
        raise ValueError("threshold policy `volume_drift_ratio` must be object")
    volume_by_tf_raw = volume_raw.get("hard_by_timeframe")
    if not isinstance(volume_by_tf_raw, dict) or not volume_by_tf_raw:
        raise ValueError("threshold policy `volume_drift_ratio.hard_by_timeframe` must be non-empty object")
    volume_by_timeframe: dict[str, float] = {}
    for key, value in volume_by_tf_raw.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) <= 0:
            raise ValueError(f"volume drift threshold for timeframe `{key}` must be > 0")
        volume_by_timeframe[str(key).strip()] = float(value)

    missing_raw = payload.get("missing_bars_ratio")
    if not isinstance(missing_raw, dict):
        raise ValueError("threshold policy `missing_bars_ratio` must be object")
    missing_hard = missing_raw.get("hard_max")
    if isinstance(missing_hard, bool) or not isinstance(missing_hard, (int, float)) or not (0 <= float(missing_hard) < 1):
        raise ValueError("threshold policy `missing_bars_ratio.hard_max` must be in [0, 1)")

    lag_mismatch_raw = payload.get("lag_class_mismatch_ratio")
    if not isinstance(lag_mismatch_raw, dict):
        raise ValueError("threshold policy `lag_class_mismatch_ratio` must be object")
    lag_mismatch_hard = lag_mismatch_raw.get("hard_max")
    if (
        isinstance(lag_mismatch_hard, bool)
        or not isinstance(lag_mismatch_hard, (int, float))
        or not (0 <= float(lag_mismatch_hard) <= 1)
    ):
        raise ValueError("threshold policy `lag_class_mismatch_ratio.hard_max` must be in [0, 1]")

    escalation_raw = payload.get("escalation")
    if not isinstance(escalation_raw, dict):
        raise ValueError("threshold policy `escalation` must be object")
    require_alert_simulation = bool(escalation_raw.get("require_alert_simulation", True))
    hard_violation_requires_incident = bool(escalation_raw.get("hard_violation_requires_incident", True))
    channels_raw = escalation_raw.get("channels")
    if not isinstance(channels_raw, list) or not channels_raw:
        raise ValueError("threshold policy `escalation.channels` must be non-empty list")
    channels = tuple(str(item).strip() for item in channels_raw if str(item).strip())
    if not channels:
        raise ValueError("threshold policy `escalation.channels` must contain non-empty strings")

    return ThresholdPolicy(
        policy_id=policy_id,
        version=int(version_raw),
        required_dimensions=required_dimensions,
        lag_class_seconds={"low": int(low_raw), "medium": int(medium_raw)},
        close_drift_hard_by_asset_group=close_by_group,
        volume_drift_hard_by_timeframe=volume_by_timeframe,
        missing_bars_ratio_hard_max=float(missing_hard),
        lag_class_mismatch_ratio_hard_max=float(lag_mismatch_hard),
        require_alert_simulation=require_alert_simulation,
        hard_violation_requires_incident=hard_violation_requires_incident,
        escalation_channels=channels,
    )


def _normalize_finam_snapshots(
    *,
    rows: list[dict[str, object]],
    run_id: str,
    ingested_at_utc: str,
    lag_class_seconds: dict[str, int],
) -> list[FinamArchiveSnapshot]:
    normalized: list[FinamArchiveSnapshot] = []
    errors: list[str] = []
    for row_index, payload in enumerate(rows):
        if not isinstance(payload, dict):
            errors.append(f"row[{row_index}] must be object")
            continue
        try:
            contract_id = _require_text(payload, "contract_id", row_index=row_index, aliases=("finam_symbol",))
            instrument_id = _require_text(payload, "instrument_id", row_index=row_index, aliases=("internal_id",))
            timeframe = _require_text(payload, "timeframe", row_index=row_index)
            ts = _require_text(payload, "ts", row_index=row_index)
            close = _require_number(payload, "close", row_index=row_index)
            volume = _require_int(payload, "volume", row_index=row_index)
            if volume < 0:
                raise ValueError(f"row[{row_index}] `volume` must be non-negative")

            source_ts_utc = _require_text(
                payload,
                "source_ts_utc",
                row_index=row_index,
            )
            received_at_utc = _require_text(
                payload,
                "received_at_utc",
                row_index=row_index,
            )
            source_dt = _parse_iso_utc(source_ts_utc)
            received_dt = _parse_iso_utc(received_at_utc)
            latency_seconds = max(0.0, (received_dt - source_dt).total_seconds())
            lag_class = _classify_lag(latency_seconds=latency_seconds, lag_class_seconds=lag_class_seconds)
            archive_batch_id = _require_text(payload, "archive_batch_id", row_index=row_index)
            source_provider = _require_text(payload, "source_provider", row_index=row_index)
            source_binding = _require_text(payload, "source_binding", row_index=row_index)

            normalized.append(
                FinamArchiveSnapshot(
                    contract_id=contract_id,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    ts=ts,
                    close=close,
                    volume=volume,
                    source_ts_utc=_to_iso_utc(source_dt),
                    received_at_utc=_to_iso_utc(received_dt),
                    latency_seconds=round(latency_seconds, 6),
                    lag_class=lag_class,
                    archive_batch_id=archive_batch_id,
                    source_provider=source_provider,
                    source_binding=source_binding,
                    ingest_run_id=run_id,
                    ingested_at_utc=ingested_at_utc,
                )
            )
        except ValueError as exc:
            errors.append(str(exc))
    if errors:
        joined = "; ".join(_sample_errors(errors, limit=15))
        raise ValueError(f"invalid Finam archive snapshot payload: {joined}")
    dedup: dict[tuple[str, str, str], FinamArchiveSnapshot] = {}
    for row in normalized:
        key = (row.contract_id, row.timeframe, row.ts)
        current = dedup.get(key)
        if current is None or row.received_at_utc > current.received_at_utc:
            dedup[key] = row
    return [
        dedup[key]
        for key in sorted(dedup)
    ]


def ingest_finam_archive_snapshots(
    *,
    source_path: Path,
    table_path: Path,
    run_id: str,
    policy: ThresholdPolicy,
) -> tuple[list[FinamArchiveSnapshot], dict[str, object], dict[str, object]]:
    raw_rows = _load_snapshot_source_rows(source_path)
    ingested_at_utc = _utc_now_iso()
    snapshots = _normalize_finam_snapshots(
        rows=raw_rows,
        run_id=run_id,
        ingested_at_utc=ingested_at_utc,
        lag_class_seconds=policy.lag_class_seconds,
    )
    write_delta_table_rows(
        table_path=table_path,
        rows=[row.to_dict() for row in snapshots],
        columns=FINAM_ARCHIVE_COLUMNS,
    )

    latencies = [row.latency_seconds for row in snapshots]
    lag_class_counts: dict[str, int] = {}
    providers: set[str] = set()
    bindings: set[str] = set()
    source_min = ""
    source_max = ""

    for row in snapshots:
        lag_class_counts[row.lag_class] = lag_class_counts.get(row.lag_class, 0) + 1
        if row.source_provider:
            providers.add(row.source_provider)
        if row.source_binding:
            bindings.add(row.source_binding)
        if not source_min or row.source_ts_utc < source_min:
            source_min = row.source_ts_utc
        if not source_max or row.source_ts_utc > source_max:
            source_max = row.source_ts_utc

    source_fingerprint = _fingerprint_finam_source(source_path)
    report = {
        "run_id": run_id,
        "source_path": source_path.resolve().as_posix(),
        "finam_archive_table_path": table_path.resolve().as_posix(),
        "source_fingerprint": source_fingerprint,
        "snapshots_received": len(raw_rows),
        "snapshots_ingested": len(snapshots),
        "ingested_at_utc": ingested_at_utc,
        "latency_summary_seconds": {
            "min": round(min(latencies), 6) if latencies else 0.0,
            "median": round(median(latencies), 6) if latencies else 0.0,
            "max": round(max(latencies), 6) if latencies else 0.0,
        },
        "lag_class_counts": lag_class_counts,
        "source_window_utc": {
            "begin": source_min,
            "end": source_max,
        },
        "source_providers": sorted(providers),
        "source_bindings": sorted(bindings),
        "mandatory_metadata_fields": list(MANDATORY_FINAM_METADATA_FIELDS),
        "metadata_fail_closed": True,
    }

    provenance_artifact = {
        "run_id": run_id,
        "captured_at_utc": ingested_at_utc,
        "source_capture": source_fingerprint,
        "records": {
            "snapshots_received": len(raw_rows),
            "snapshots_ingested": len(snapshots),
        },
        "mandatory_metadata_fields": list(MANDATORY_FINAM_METADATA_FIELDS),
        "source_window_utc": {
            "begin": source_min,
            "end": source_max,
        },
        "source_providers": sorted(providers),
        "source_bindings": sorted(bindings),
        "archive_batch_ids": sorted({row.archive_batch_id for row in snapshots}),
    }
    return snapshots, report, provenance_artifact


def _load_moex_asset_groups(mapping_registry_path: Path) -> dict[str, str]:
    payload = yaml.safe_load(mapping_registry_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("mapping registry must be yaml object")
    mappings = payload.get("mappings")
    if not isinstance(mappings, list):
        raise ValueError("mapping registry `mappings` must be list")
    out: dict[str, str] = {}
    for row in mappings:
        if not isinstance(row, dict):
            continue
        if bool(row.get("is_active")) is not True:
            continue
        finam_symbol = str(row.get("finam_symbol", "")).strip()
        asset_group = str(row.get("asset_group", "")).strip()
        if finam_symbol and asset_group:
            out[finam_symbol] = asset_group
    return out


def _load_moex_overlap_rows(
    *,
    canonical_bars_path: Path,
    canonical_provenance_path: Path,
    policy: ThresholdPolicy,
) -> list[MoexOverlapRow]:
    bars = read_delta_table_rows(canonical_bars_path)
    provenance_rows = read_delta_table_rows(canonical_provenance_path)
    provenance_by_key: dict[tuple[str, str, str], dict[str, object]] = {}

    for row in provenance_rows:
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("contract_id", "")).strip(),
            str(row.get("timeframe", "")).strip(),
            str(row.get("ts", "")).strip(),
        )
        if not all(key):
            continue
        provenance_by_key[key] = row

    normalized: list[MoexOverlapRow] = []
    errors: list[str] = []
    for row_index, row in enumerate(bars):
        if not isinstance(row, dict):
            errors.append(f"canonical row[{row_index}] must be object")
            continue
        try:
            contract_id = _require_text(row, "contract_id", row_index=row_index)
            instrument_id = _require_text(row, "instrument_id", row_index=row_index)
            timeframe = _require_text(row, "timeframe", row_index=row_index)
            ts = _require_text(row, "ts", row_index=row_index)
            close = _require_number(row, "close", row_index=row_index)
            volume = _require_int(row, "volume", row_index=row_index)
            if volume < 0:
                raise ValueError(f"canonical row[{row_index}] `volume` must be non-negative")

            provenance = provenance_by_key.get((contract_id, timeframe, ts))
            if provenance is None:
                raise ValueError(
                    f"canonical row[{row_index}] missing provenance for key {contract_id}/{timeframe}/{ts}"
                )
            source_ts = _require_text(
                provenance,
                "source_ts_close_last",
                row_index=row_index,
            )
            built_at = _require_text(
                provenance,
                "built_at_utc",
                row_index=row_index,
            )
            latency = max(0.0, (_parse_iso_utc(built_at) - _parse_iso_utc(source_ts)).total_seconds())
            lag_class = _classify_lag(latency_seconds=latency, lag_class_seconds=policy.lag_class_seconds)
            source_provider = str(provenance.get("source_provider", "")).strip()
            normalized.append(
                MoexOverlapRow(
                    contract_id=contract_id,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    ts=ts,
                    close=close,
                    volume=volume,
                    lag_class=lag_class,
                    source_provider=source_provider,
                )
            )
        except ValueError as exc:
            errors.append(str(exc))
    if errors:
        joined = "; ".join(_sample_errors(errors, limit=15))
        raise ValueError(f"invalid MOEX overlap rows: {joined}")

    return sorted(
        normalized,
        key=lambda row: (row.contract_id, row.instrument_id, row.timeframe, row.ts),
    )


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return float(ordered[index])


def _json_write(path: Path, payload: dict[str, object] | list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _validate_required_dimensions(policy: ThresholdPolicy) -> dict[str, object]:
    missing = sorted(set(MANDATORY_RECONCILIATION_DIMENSIONS) - set(policy.required_dimensions))
    return {
        "gate": "required_dimensions",
        "status": "PASS" if not missing else "FAIL",
        "missing_dimensions": missing,
    }


def _build_alert_simulation(
    *,
    run_id: str,
    hard_failed_gates: list[str],
    policy: ThresholdPolicy,
) -> tuple[dict[str, object], dict[str, object]]:
    incidents: list[dict[str, object]] = []
    for gate in hard_failed_gates:
        incidents.append(
            {
                "incident_id": f"{run_id}:{gate}",
                "gate": gate,
                "severity": "P1",
                "status": "OPEN",
            }
        )

    events: list[dict[str, object]] = []
    if incidents:
        for incident in incidents:
            events.append(
                {
                    "stage": "incident_opened",
                    "incident_id": incident["incident_id"],
                    "gate": incident["gate"],
                    "at_utc": _utc_now_iso(),
                }
            )
            for channel in policy.escalation_channels:
                events.append(
                    {
                        "stage": "escalated",
                        "incident_id": incident["incident_id"],
                        "channel": channel,
                        "at_utc": _utc_now_iso(),
                    }
                )
    else:
        events.append(
            {
                "stage": "no_hard_violations",
                "at_utc": _utc_now_iso(),
            }
        )

    alert_simulation = {
        "run_id": run_id,
        "executed": True,
        "incidents": incidents,
        "incident_count": len(incidents),
    }
    escalation_trace = {
        "run_id": run_id,
        "executed": True,
        "events": events,
    }
    return alert_simulation, escalation_trace


def run_phase03_reconciliation(
    *,
    canonical_bars_path: Path,
    canonical_provenance_path: Path,
    finam_archive_source_path: Path,
    threshold_policy_path: Path,
    mapping_registry_path: Path,
    output_dir: Path,
    run_id: str,
    allow_degraded_publish: bool = False,
) -> dict[str, object]:
    policy = load_phase03_threshold_policy(threshold_policy_path)
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at_utc = _utc_now_iso()

    finam_table_path = output_dir / "delta" / "finam_archive_snapshots.delta"
    finam_snapshots, finam_ingest_report, finam_provenance_artifact = ingest_finam_archive_snapshots(
        source_path=finam_archive_source_path,
        table_path=finam_table_path,
        run_id=run_id,
        policy=policy,
    )
    moex_rows = _load_moex_overlap_rows(
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        policy=policy,
    )
    asset_group_by_contract = _load_moex_asset_groups(mapping_registry_path)

    moex_by_key = {
        (row.contract_id, row.timeframe, row.ts): row
        for row in moex_rows
    }
    finam_by_key = {
        (row.contract_id, row.timeframe, row.ts): row
        for row in finam_snapshots
    }
    moex_keys = set(moex_by_key)
    finam_keys = set(finam_by_key)
    matched_keys = sorted(moex_keys & finam_keys)
    missing_in_finam = sorted(moex_keys - finam_keys)
    unexpected_in_finam = sorted(finam_keys - moex_keys)

    close_drifts: list[float] = []
    volume_drifts: list[float] = []
    lag_mismatch_count = 0
    close_hard_violations: list[str] = []
    volume_hard_violations: list[str] = []
    metrics_rows: list[dict[str, object]] = []
    archive_batch_ids: set[str] = set()

    for key in matched_keys:
        moex = moex_by_key[key]
        finam = finam_by_key[key]
        archive_batch_ids.add(finam.archive_batch_id)
        asset_group = asset_group_by_contract.get(moex.contract_id, "default")

        denominator = abs(moex.close) if abs(moex.close) > 1e-9 else 1.0
        close_drift_bps = abs(finam.close - moex.close) * 10000.0 / denominator
        volume_drift_ratio = abs(finam.volume - moex.volume) / max(moex.volume, 1)
        close_drifts.append(close_drift_bps)
        volume_drifts.append(volume_drift_ratio)

        close_threshold = policy.close_drift_hard_by_asset_group.get(
            asset_group,
            policy.close_drift_hard_by_asset_group["default"],
        )
        volume_threshold = policy.volume_drift_hard_by_timeframe.get(moex.timeframe)
        if volume_threshold is None:
            raise ValueError(f"threshold policy missing volume ratio threshold for timeframe `{moex.timeframe}`")

        close_hard = close_drift_bps > close_threshold
        volume_hard = volume_drift_ratio > volume_threshold
        lag_mismatch = 1 if moex.lag_class != finam.lag_class else 0
        if lag_mismatch:
            lag_mismatch_count += 1

        if close_hard:
            close_hard_violations.append(f"{moex.contract_id}/{moex.timeframe}/{moex.ts}")
        if volume_hard:
            volume_hard_violations.append(f"{moex.contract_id}/{moex.timeframe}/{moex.ts}")

        metrics_rows.append(
            {
                "contract_id": moex.contract_id,
                "instrument_id": moex.instrument_id,
                "timeframe": moex.timeframe,
                "ts": moex.ts,
                "asset_group": asset_group,
                "moex_close": moex.close,
                "finam_close": finam.close,
                "close_drift_bps": round(close_drift_bps, 6),
                "close_hard_threshold_bps": close_threshold,
                "close_hard_violation": 1 if close_hard else 0,
                "moex_volume": moex.volume,
                "finam_volume": finam.volume,
                "volume_drift_ratio": round(volume_drift_ratio, 6),
                "volume_hard_threshold": volume_threshold,
                "volume_hard_violation": 1 if volume_hard else 0,
                "moex_lag_class": moex.lag_class,
                "finam_lag_class": finam.lag_class,
                "lag_class_mismatch": lag_mismatch,
                "missing_in_finam": 0,
                "unexpected_in_finam": 0,
                "archive_batch_id": finam.archive_batch_id,
                "run_id": run_id,
                "generated_at_utc": generated_at_utc,
            }
        )

    for key in missing_in_finam:
        moex = moex_by_key[key]
        metrics_rows.append(
            {
                "contract_id": moex.contract_id,
                "instrument_id": moex.instrument_id,
                "timeframe": moex.timeframe,
                "ts": moex.ts,
                "asset_group": asset_group_by_contract.get(moex.contract_id, "default"),
                "moex_close": moex.close,
                "finam_close": None,
                "close_drift_bps": None,
                "close_hard_threshold_bps": None,
                "close_hard_violation": 0,
                "moex_volume": moex.volume,
                "finam_volume": None,
                "volume_drift_ratio": None,
                "volume_hard_threshold": None,
                "volume_hard_violation": 0,
                "moex_lag_class": moex.lag_class,
                "finam_lag_class": None,
                "lag_class_mismatch": 0,
                "missing_in_finam": 1,
                "unexpected_in_finam": 0,
                "archive_batch_id": "",
                "run_id": run_id,
                "generated_at_utc": generated_at_utc,
            }
        )

    for key in unexpected_in_finam:
        finam = finam_by_key[key]
        archive_batch_ids.add(finam.archive_batch_id)
        metrics_rows.append(
            {
                "contract_id": finam.contract_id,
                "instrument_id": finam.instrument_id,
                "timeframe": finam.timeframe,
                "ts": finam.ts,
                "asset_group": asset_group_by_contract.get(finam.contract_id, "default"),
                "moex_close": None,
                "finam_close": finam.close,
                "close_drift_bps": None,
                "close_hard_threshold_bps": None,
                "close_hard_violation": 0,
                "moex_volume": None,
                "finam_volume": finam.volume,
                "volume_drift_ratio": None,
                "volume_hard_threshold": None,
                "volume_hard_violation": 0,
                "moex_lag_class": None,
                "finam_lag_class": finam.lag_class,
                "lag_class_mismatch": 0,
                "missing_in_finam": 0,
                "unexpected_in_finam": 1,
                "archive_batch_id": finam.archive_batch_id,
                "run_id": run_id,
                "generated_at_utc": generated_at_utc,
            }
        )

    total_moex = len(moex_keys)
    total_matched = len(matched_keys)
    missing_ratio = (len(missing_in_finam) / total_moex) if total_moex else 0.0
    lag_mismatch_ratio = (lag_mismatch_count / total_matched) if total_matched else 0.0
    close_hard = bool(close_hard_violations)
    volume_hard = bool(volume_hard_violations)
    missing_hard = missing_ratio > policy.missing_bars_ratio_hard_max
    lag_hard = lag_mismatch_ratio > policy.lag_class_mismatch_ratio_hard_max

    required_dimensions_gate = _validate_required_dimensions(policy)
    gates = [
        required_dimensions_gate,
        {
            "gate": "close_drift_bps",
            "status": "FAIL" if close_hard else "PASS",
            "hard_threshold_mode": "asset_group",
            "violations": len(close_hard_violations),
            "samples": _sample_errors(close_hard_violations),
            "summary": {
                "p95_bps": round(_p95(close_drifts), 6),
                "max_bps": round(max(close_drifts), 6) if close_drifts else 0.0,
            },
        },
        {
            "gate": "volume_drift_ratio",
            "status": "FAIL" if volume_hard else "PASS",
            "hard_threshold_mode": "timeframe",
            "violations": len(volume_hard_violations),
            "samples": _sample_errors(volume_hard_violations),
            "summary": {
                "p95_ratio": round(_p95(volume_drifts), 6),
                "max_ratio": round(max(volume_drifts), 6) if volume_drifts else 0.0,
            },
        },
        {
            "gate": "missing_bars_ratio",
            "status": "FAIL" if missing_hard else "PASS",
            "hard_max": policy.missing_bars_ratio_hard_max,
            "value": round(missing_ratio, 6),
            "violations": len(missing_in_finam),
            "samples": _sample_errors([f"{contract}/{timeframe}/{ts}" for contract, timeframe, ts in missing_in_finam]),
        },
        {
            "gate": "lag_class",
            "status": "FAIL" if lag_hard else "PASS",
            "hard_max": policy.lag_class_mismatch_ratio_hard_max,
            "value": round(lag_mismatch_ratio, 6),
            "violations": lag_mismatch_count,
            "samples": _sample_errors(
                [
                    f"{key[0]}/{key[1]}/{key[2]}"
                    for key in matched_keys
                    if moex_by_key[key].lag_class != finam_by_key[key].lag_class
                ]
            ),
        },
    ]
    hard_failed_gates = [str(gate["gate"]) for gate in gates if gate.get("status") == "FAIL"]

    alert_simulation, escalation_trace = _build_alert_simulation(
        run_id=run_id,
        hard_failed_gates=hard_failed_gates,
        policy=policy,
    )

    if policy.require_alert_simulation and not alert_simulation.get("executed"):
        hard_failed_gates.append("alert_simulation_missing")

    if hard_failed_gates:
        if policy.hard_violation_requires_incident and not alert_simulation["incidents"]:
            status = "BLOCKED"
            publish_decision = "blocked"
        elif allow_degraded_publish:
            status = "DEGRADED"
            publish_decision = "degraded_with_incident"
        else:
            status = "BLOCKED"
            publish_decision = "blocked"
    else:
        status = "PASS"
        publish_decision = "publish"

    metrics_table_path = output_dir / "delta" / "reconciliation_metrics.delta"
    write_delta_table_rows(
        table_path=metrics_table_path,
        rows=metrics_rows,
        columns=RECONCILIATION_METRICS_COLUMNS,
    )

    finam_report_path = output_dir / "finam-archive-ingest-report.json"
    finam_provenance_path = output_dir / "finam-archive-provenance.json"
    overlap_metrics_path = output_dir / "overlap-metrics.json"
    alert_path = output_dir / "alert-simulation.json"
    escalation_path = output_dir / "escalation-trace.json"

    finam_ingest_report["provenance_artifact_path"] = finam_provenance_path.as_posix()
    _json_write(finam_report_path, finam_ingest_report)
    _json_write(finam_provenance_path, finam_provenance_artifact)
    _json_write(
        overlap_metrics_path,
        {
            "run_id": run_id,
            "status": "FAIL" if hard_failed_gates else "PASS",
            "matched_rows": total_matched,
            "missing_in_finam": len(missing_in_finam),
            "unexpected_in_finam": len(unexpected_in_finam),
            "missing_ratio": round(missing_ratio, 6),
            "lag_mismatch_ratio": round(lag_mismatch_ratio, 6),
            "close_drift_p95_bps": round(_p95(close_drifts), 6),
            "volume_drift_p95_ratio": round(_p95(volume_drifts), 6),
            "gate_results": gates,
        },
    )
    _json_write(alert_path, alert_simulation)
    _json_write(escalation_path, escalation_trace)

    artifact_paths = {
        "finam_archive_ingest_report": finam_report_path.as_posix(),
        "finam_archive_provenance": finam_provenance_path.as_posix(),
        "overlap_metrics": overlap_metrics_path.as_posix(),
        "alert_simulation": alert_path.as_posix(),
        "escalation_trace": escalation_path.as_posix(),
    }
    artifacts_complete = all(Path(path).exists() for path in artifact_paths.values())

    moex_bindings = sorted({row.source_provider for row in moex_rows if row.source_provider})
    finam_bindings: set[str] = set()
    for row in finam_snapshots:
        if row.source_provider:
            finam_bindings.add(row.source_provider)
        if row.source_binding:
            finam_bindings.add(row.source_binding)
    real_bindings = sorted(set(moex_bindings) | finam_bindings)

    report: dict[str, object] = {
        "run_id": run_id,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "status": status,
        "publish_decision": publish_decision,
        "generated_at_utc": generated_at_utc,
        "policy": {
            "policy_id": policy.policy_id,
            "version": policy.version,
            "required_dimensions": list(policy.required_dimensions),
        },
        "input_paths": {
            "canonical_bars": canonical_bars_path.resolve().as_posix(),
            "canonical_provenance": canonical_provenance_path.resolve().as_posix(),
            "finam_archive_source": finam_archive_source_path.resolve().as_posix(),
            "mapping_registry": mapping_registry_path.resolve().as_posix(),
            "threshold_policy": threshold_policy_path.resolve().as_posix(),
        },
        "overlap_window_utc": {
            "begin": min(
                [row.ts for row in moex_rows] + [row.ts for row in finam_snapshots]
            )
            if moex_rows or finam_snapshots
            else "",
            "end": max(
                [row.ts for row in moex_rows] + [row.ts for row in finam_snapshots]
            )
            if moex_rows or finam_snapshots
            else "",
        },
        "counts": {
            "moex_rows": len(moex_rows),
            "finam_rows": len(finam_snapshots),
            "matched_rows": total_matched,
            "missing_in_finam": len(missing_in_finam),
            "unexpected_in_finam": len(unexpected_in_finam),
        },
        "gate_results": gates,
        "hard_failed_gates": hard_failed_gates,
        "alert_simulation": alert_simulation,
        "escalation_trace": escalation_trace,
        "artifact_paths": artifact_paths,
        "output_paths": {
            "finam_archive_snapshots": finam_table_path.as_posix(),
            "reconciliation_metrics": metrics_table_path.as_posix(),
        },
        "archive_batch_ids": sorted(archive_batch_ids),
        "artifacts_complete": artifacts_complete,
        "real_bindings": real_bindings,
    }
    _json_write(output_dir / "reconciliation-report.json", report)
    return report
