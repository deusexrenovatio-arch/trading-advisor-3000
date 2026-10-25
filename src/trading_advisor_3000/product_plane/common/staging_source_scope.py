"""Validate actual producer configuration and input-byte upper bounds before work."""

from __future__ import annotations

import dataclasses
import json
import os
from collections.abc import Mapping
from contextlib import nullcontext
from datetime import date, datetime
from enum import Enum
from pathlib import Path

from .staging_execution import StagingExecutionError, _plan, _stamp, digest


def parameter_value(value):
    if isinstance(value, Path):
        return value.resolve().as_posix()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return parameter_value(dataclasses.asdict(value))
    if isinstance(value, Enum):
        return parameter_value(value.value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): parameter_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        items = [parameter_value(item) for item in value]
        return sorted(items, key=str) if isinstance(value, (set, frozenset)) else items
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise StagingExecutionError("unsupported protected producer parameter type")


def require_staging_parameters(*, stage, output_path, parameters, input_until_utc=None):
    plan = _plan(output_path)
    if plan is None:
        return
    expected = plan.get("producer_parameters", {}).get(stage)
    actual = parameter_value(parameters)
    if plan.get("mode") == "reference" and stage == "run_moex_baseline_update":
        expected = _reference_input_parameters(plan, actual, expected)
    if expected is None or digest(actual) != digest(expected):
        raise StagingExecutionError("actual producer parameters differ from protected staging plan")
    if input_until_utc is not None:
        scope = plan["scope"]
        if not _stamp(scope["start_utc"]) < _stamp(input_until_utc) <= _stamp(scope["end_utc"]):
            raise StagingExecutionError("producer input cutoff exceeds protected staging scope")


def _reference_input_parameters(plan, actual, expected):
    """Renew one native input pin, authenticated by the actual daily root and publication."""
    from ..data_plane.moex.staging_input_facts import registered_history_refresh_kwargs
    from . import staging_execution as guard
    from .data_snapshots import capture_table, current_snapshot

    if (
        expected is None
        or expected.get("daily_open_interest_path") is None
        or expected.get("daily_open_interest_version") is None
    ):
        return expected
    allowed = dict(expected)
    allowed["daily_open_interest_version"] = actual.get("daily_open_interest_version")
    if digest(actual) != digest(allowed):
        raise StagingExecutionError("reference producer behavior differs from accepted parameters")
    scope = guard._ACTIVE.get()
    if scope is None or digest(scope.plan) != digest(plan):
        raise StagingExecutionError("reference input renewal requires its actual scheduled root")
    guard._live_run(scope)
    guard._deadline(plan, scope.reservation)
    if scope.instance is None:
        from dagster import DagsterInstance

        owner = DagsterInstance.from_config(os.environ["DAGSTER_HOME"])
    else:
        owner = nullcontext(scope.instance)
    with owner as instance:
        run = instance.get_run_by_id(scope.run.run_id)
        if (
            run.job_name != plan["root_job"]
            or run.run_id != scope.reservation.run_config["root_run_id"]
        ):
            raise StagingExecutionError("reference input renewal requires the recorded root job")
        tick = guard._reference_tick(plan, instance, run)
        recorded = run.run_config.get("ops", {}).get("moex_baseline_update", {}).get("config", {})
    path, version = (
        actual.get("daily_open_interest_path"),
        actual.get("daily_open_interest_version"),
    )
    if (
        type(version) is not int
        or version < 0
        or type(recorded.get("daily_open_interest_version")) is not int
        or recorded.get("daily_open_interest_version") != version
        or parameter_value(Path(recorded.get("daily_open_interest_path", ""))) != path
        or path != expected["daily_open_interest_path"]
    ):
        raise StagingExecutionError("reference OI pin differs from actual recorded schedule config")
    owner_parameters = parameter_value(registered_history_refresh_kwargs(Path(plan["data_root"])))
    if not owner_parameters or any(
        actual.get(key) != value for key, value in owner_parameters.items()
    ):
        raise StagingExecutionError(
            "reference input pin/behavior differs from native owner metadata"
        )
    snapshot = current_snapshot(Path(actual["canonical_bars_path"]).parent / ".data_snapshots")
    native = capture_table(Path(path), version=version)
    if (
        snapshot is None
        or snapshot["tables"].get(Path(path).stem) != native
        or capture_table(Path(path)) != native
    ):
        raise StagingExecutionError("reference OI pin is not the currently accepted native input")
    binding = {
        "plan_sha256": digest(plan),
        "root_run_id": run.run_id,
        "root_config_sha256": digest(run.run_config),
        "tick_id": str(tick.tick_id),
        "source_snapshot_id": snapshot["snapshot_id"],
        "source_manifest_sha256": snapshot["manifest_sha256"],
        "input": native,
    }
    with guard._quota(plan) as quota:
        key = guard._key(
            "staging-reference-input-v1", scope.reservation.run_id, "daily_open_interest"
        )
        guard._add(quota, key, binding)
        if quota.get_run_by_id(key).run_config != binding:
            raise StagingExecutionError(
                "reference input pin changed inside an admitted daily cycle"
            )
    return allowed


def require_staging_source_bytes(*, output_path, input_paths, input_values=()):
    plan = _plan(output_path)
    if plan is None or "max_source_bytes" not in plan["scope"]:
        return
    from deltalake import DeltaTable

    from .data_snapshots import _file_path, open_read_table, snapshot_table_reference

    cap, total, seen = plan["scope"]["max_source_bytes"], 0, set()
    for supplied in input_paths:
        if supplied is None:
            continue
        path, version = supplied if isinstance(supplied, tuple) else (supplied, None)
        if version is not None and (type(version) is not int or version < 0):
            raise StagingExecutionError("source byte cap requires a valid pinned input version")
        path = Path(path).resolve()
        if (path / "_delta_log").is_dir():
            reference = snapshot_table_reference(path)
            if (
                reference is not None
                and version is not None
                and reference["delta_version"] != version
            ):
                raise StagingExecutionError(
                    "source byte cap explicit version differs from snapshot"
                )
            table = (
                open_read_table(path) if version is None else DeltaTable(str(path), version=version)
            )
            actions = table.get_add_actions()
            expected_total = sum(int(size) for size in actions.column("size_bytes").to_pylist())
            entries = [
                (str(_file_path(uri)), _file_path(uri).stat().st_size) for uri in table.file_uris()
            ]
            if sum(size for _, size in entries) != expected_total:
                raise StagingExecutionError("source file sizes differ from native Delta metadata")
        elif path.is_file():
            entries = [(str(path), path.stat().st_size)]
        elif not path.exists():
            # Native optional/empty target semantics remain with their owner.
            continue
        else:
            raise StagingExecutionError("protected staging source input is missing")
        for name, size in entries:
            if name not in seen:
                total += int(size)
                seen.add(name)
            if total > cap:
                raise StagingExecutionError("native staging source bytes exceed protected cap")
    encoder = json.JSONEncoder(default=parameter_value, allow_nan=False)
    for value in input_values:
        for piece in encoder.iterencode(value):
            total += len(piece.encode("utf-8"))
            if total > cap:
                raise StagingExecutionError("native staging source bytes exceed protected cap")
