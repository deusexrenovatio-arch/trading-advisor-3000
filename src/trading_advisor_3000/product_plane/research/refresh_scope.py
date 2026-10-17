"""Dirty ranges between the accepted Data and Research snapshot boundaries."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    capture_table,
    current_snapshot,
    load_snapshot,
    open_pinned_table,
    runtime_provenance,
)
from trading_advisor_3000.product_plane.research.data_snapshots import (
    _parameters,
    snapshot_registry,
)


def _stamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _identity(reference: Mapping | None) -> tuple | None:
    return (reference["table_id"], reference["delta_version"]) if reference else None


def _tail(reference: Mapping) -> datetime | None:
    """Every active nonempty file must supply a valid temporal upper bound."""
    table = open_pinned_table(reference)
    fields = {field.name for field in table.schema().fields}
    columns = [name for name in ("bar_end_ts", "ts_close", "ts", "end_utc") if name in fields]
    if not columns:
        return None
    actions = table.get_add_actions(flatten=True)
    if not actions.num_rows:
        return None
    for column in columns:
        name = f"max.{column}"
        if name not in actions.schema.names:
            continue
        values = actions.column(name).to_pylist()
        if all(value is not None for value in values):
            return max(_stamp(value) for value in values)
    raise SnapshotUnavailableError("Incomplete temporal file statistics")


def resolve_refresh_scope(
    *,
    data_manifest_path: Path,
    materialized_output_dir: Path,
    dataset_version: str,
    timeframes: Sequence[str],
    configuration: Mapping | None = None,
) -> dict | None:
    """Return one frozen scope before either Dagster sensor can decide NOOP."""
    data = load_snapshot(data_manifest_path)
    root = data_manifest_path.parent.parent
    accepted_data = current_snapshot(root)
    if not accepted_data or (accepted_data["snapshot_id"], accepted_data["manifest_sha256"]) != (
        data["snapshot_id"],
        data["manifest_sha256"],
    ):
        raise ValueError("Refresh scope Data parent is not accepted current")
    full = {"refresh_scope_version": 1, "force_full_refresh": True, "windows": []}
    if data["parameters"].get("downstream_scope", {}).get("version") != 1:
        return {**full, "reason": "legacy_bound_data_scope"}
    research = current_snapshot(
        snapshot_registry(materialized_output_dir, dataset_version, "pit_active_front")
    )
    if (
        research is None
        or research["parameters"].get("refresh_scope_version") != 1
        or len(research["parents"]) != 1
        or set(research["parameters"].get("timeframes", [])) != set(timeframes)
    ):
        return {**full, "reason": "missing_or_legacy_research_scope"}
    if configuration is not None:
        ignored = {
            "changed_windows",
            "refresh_scope_version",
            "force_full_refresh",
            "data_snapshot_manifest_path",
        }
        if (
            any(
                research["parameters"].get(key) != value
                for key, value in _parameters(configuration).items()
                if key not in ignored
            )
            or research["parameters"].get("runtime_provenance") != runtime_provenance()
        ):
            return {**full, "reason": "research_semantics_changed"}
    volume_profile_name = Path(
        str((configuration or {}).get("volume_profile_raw_1m_table_path", "raw_moex_history.delta"))
    ).stem
    volume_profile_path = str((configuration or {}).get("volume_profile_raw_1m_table_path") or "")
    if volume_profile_path and volume_profile_name not in data["tables"]:
        current_vp = capture_table(Path(volume_profile_path))
        accepted_vp = research["tables"].get(volume_profile_name)
        if (
            _identity(current_vp) != _identity(accepted_vp)
            or not accepted_vp
            or Path(str(current_vp["table_path"])).resolve()
            != Path(str(accepted_vp["table_path"])).resolve()
        ):
            return {**full, "reason": "unbounded_external_volume_profile"}
    if any(
        not (match := re.fullmatch(r"(\d+)([mhdw])", timeframe))
        or int(match[1]) * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[match[2]] > 604800
        for timeframe in timeframes
    ):
        return {**full, "reason": "unproven_bar_period"}
    scopes, seen, cursor = [], set(), data
    anchor = research["parents"][0]
    try:
        while cursor["snapshot_id"] != anchor:
            if cursor["snapshot_id"] in seen:
                raise ValueError("Data snapshot ancestry cycle")
            seen.add(cursor["snapshot_id"])
            scope = cursor["parameters"].get("downstream_scope", {})
            if scope.get("version") != 1 or scope.get("full_refresh"):
                return {**full, "reason": "unbounded_data_scope"}
            if len(cursor["parents"]) != 1:
                return {**full, "reason": "missing_data_ancestry"}
            parent = load_snapshot(root / "manifests" / f"{cursor['parents'][0]}.json")
            for name in cursor["tables"].keys() | parent["tables"].keys():
                if not (
                    name.startswith("canonical_")
                    or name == "raw_moex_history"
                    or name == volume_profile_name
                    or "open_interest" in name
                    or "daily_oi" in name
                ):
                    continue
                actual = cursor["tables"].get(name)
                if _identity(actual) != _identity(parent["tables"].get(name)):
                    if actual is None or _identity(
                        scope.get("covered_tables", {}).get(name)
                    ) != _identity(actual):
                        return {**full, "reason": f"unbounded_dependency:{name}"}
            scopes.append(scope)
            cursor = parent
    except (FileNotFoundError, SnapshotUnavailableError):
        return {**full, "reason": "data_ancestry_unavailable"}
    windows = [dict(window) for scope in scopes for window in scope.get("windows", [])]
    intervals = [item for scope in scopes for item in scope.get("effective_intervals", [])]
    if not windows and not intervals:
        return {"refresh_scope_version": 1, "force_full_refresh": False, "windows": []}
    references = [
        reference for manifest in (data, research) for reference in manifest["tables"].values()
    ]
    try:
        references.extend(capture_table(path) for path in materialized_output_dir.glob("*.delta"))
        tails = [value for reference in references if (value := _tail(reference)) is not None]
        tails.extend(_stamp(window["end_ts"]) for window in windows)
        if not tails:
            return {**full, "reason": "temporal_tail_unavailable"}
        end = max(tails)
    except (FileNotFoundError, SnapshotUnavailableError):
        return {**full, "reason": "incomplete_temporal_tail"}
    aliases: dict[str, set[str]] = {}
    for window in windows:
        actual = str(window["instrument_id"])
        normalized = actual.removeprefix("FUT_").removesuffix("@MOEX")
        aliases.setdefault(normalized, set()).add(actual)
    for interval in intervals:
        normalized = str(interval["instrument_id"]).removeprefix("FUT_").removesuffix("@MOEX")
        candidates = aliases.get(normalized, set())
        if len(candidates) != 1:
            return {**full, "reason": "unproven_economics_identity"}
        for timeframe in timeframes:
            windows.append(
                {
                    "instrument_id": next(iter(candidates)),
                    "timeframe": timeframe,
                    "start_ts": interval["effective_from_ts"],
                    "end_ts": _iso(end),
                }
            )
    grouped: dict[tuple[str, str], datetime] = {}
    for window in windows:
        # Calendar/session envelopes require a halo for existing weekly anchors.
        start = _stamp(window["start_ts"]).replace(hour=0, minute=0, second=0, microsecond=0)
        start -= timedelta(days=7)
        # Daily coverage classifies weekly usage; MTF and minute-volume inputs
        # can also affect other requested periods of the same instrument.
        for timeframe in timeframes:
            key = str(window["instrument_id"]), str(timeframe)
            grouped[key] = min(grouped.get(key, start), start)
    return {
        "refresh_scope_version": 1,
        "force_full_refresh": False,
        "windows": [
            {
                "instrument_id": instrument,
                "timeframe": timeframe,
                "start_ts": _iso(start),
                "end_ts": _iso(end),
            }
            for (instrument, timeframe), start in sorted(grouped.items())
        ],
    }
