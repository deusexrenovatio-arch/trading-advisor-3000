"""Read-only disposition of one real reference schedule graph; never admission proof."""

from __future__ import annotations

import re
import time
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

from dagster import DagsterEventType, RunsFilter
from dagster._core.definitions.run_status_sensor_definition import RunStatusSensorCursor
from dagster._core.event_api import RunStatusChangeRecordsFilter
from dagster._core.scheduler.instigation import InstigatorType

from ..common.staging_contracts import digest
from ..common.staging_route_proof import JOBS, STAGES

SENSORS = (
    "moex_cf_catch_up_after_moex_baseline_sensor",
    "research_data_prep_after_moex_cf_catch_up_sensor",
)


class _Pending(ValueError):
    pass


class _SensorBlocked(ValueError):
    def __init__(self, message, evidence):
        super().__init__(message)
        self.evidence = evidence


def _repository(run):
    if run.remote_job_origin is None:
        raise ValueError("reference observation requires a recorded remote job origin")
    return run.remote_job_origin.repository_origin.get_id()


def _states(instance, kind, name, run):
    states = [
        state
        for state in instance.all_instigator_state(instigator_type=kind)
        if state.name == name and state.repository_origin_id == _repository(run)
    ]
    if len(states) != 1:
        raise ValueError(f"reference instigator is missing or ambiguous: {name}")
    return states[0]


def _schedule(instance, root):
    name = "moex_baseline_daily_update_schedule"
    state = _states(instance, InstigatorType.SCHEDULE, name, root)
    for tick in instance.get_ticks(state.instigator_origin_id, state.selector_id, limit=100):
        if root.run_id in tick.run_ids and tick.status.value == "SUCCESS":
            return {
                "name": name,
                "tick_id": str(tick.tick_id),
                "status": "SUCCESS",
                "timestamp": tick.timestamp,
                "run_id": root.run_id,
            }
    raise _Pending("awaiting the root's actual successful schedule tick")


def _tick_matches(instance, tick, upstream, child):
    if child is not None and child.run_id in tick.run_ids:
        return True
    if upstream.run_id in tick.origin_run_ids:
        return True
    if not tick.cursor or not RunStatusSensorCursor.is_valid(tick.cursor):
        return False
    cursor = RunStatusSensorCursor.from_json(tick.cursor)
    if cursor.update_timestamp is not None:
        return False  # Legacy run-sharded IDs cannot establish an exact global event identity.
    records = instance.fetch_run_status_changes(
        RunStatusChangeRecordsFilter(
            event_type=DagsterEventType.RUN_SUCCESS, storage_ids=[cursor.record_id]
        ),
        limit=1,
    ).records
    return len(records) == 1 and records[0].run_id == upstream.run_id


def _edge(instance, upstream, child, sensor):
    state = _states(instance, InstigatorType.SENSOR, sensor, upstream)
    if not state.is_running:
        raise ValueError(f"required reference sensor is stopped: {sensor}")
    ticks = instance.get_ticks(state.instigator_origin_id, state.selector_id, limit=100)
    tick = next((item for item in ticks if _tick_matches(instance, item, upstream, child)), None)
    if tick is None or tick.status.value == "STARTED":
        raise _Pending(f"awaiting exact native sensor disposition: {sensor}")
    result = {
        "name": sensor,
        "tick_id": str(tick.tick_id),
        "status": tick.status.value,
        "upstream_run_id": upstream.run_id,
        "skip_reason": tick.skip_reason or "",
    }
    if tick.status.value == "FAILURE" or result["skip_reason"].startswith("BLOCKED:"):
        raise _SensorBlocked(
            f"reference sensor blocked the graph: {sensor}: {result['skip_reason']}", result
        )
    if child is not None:
        if tick.status.value != "SUCCESS" or child.run_id not in tick.run_ids:
            raise _SensorBlocked(
                "recorded child is not bound to its actual successful sensor tick", result
            )
        result["child_run_id"] = child.run_id
        return result
    if (
        sensor == SENSORS[0]
        and tick.status.value == "SKIPPED"
        and re.fullmatch(r"NOOP: .+; admission=[0-9a-f]{64}", result["skip_reason"])
    ):
        return result
    raise _SensorBlocked(
        f"sensor produced no supported child or explicit native NOOP: {sensor}", result
    )


def _config(run, asset):
    return run.run_config.get("ops", {}).get(asset, {}).get("config", {})


def _snapshots(plan, root, research_run, data_root):
    from ..common.data_snapshots import current_snapshot, load_snapshot, open_pinned_table
    from ..research.data_snapshots import snapshot_registry

    def inside(value):
        path = Path(value).resolve()
        if not path.is_relative_to(data_root):
            raise ValueError("reference snapshot path escapes its isolated data root")
        return path

    baseline = _config(root, "moex_baseline_update")
    data_registry = inside(baseline["canonical_bars_path"]).parent / ".data_snapshots"
    config = _config(research_run, "research_datasets") if research_run is not None else {}
    request = plan["consumer"]
    output = inside(config.get("materialized_output_dir") or data_root / "research/gold/current")
    registry = snapshot_registry(
        output,
        config.get("dataset_version") or request["dataset_version"],
        "pit_active_front"
        if config.get("series_mode") == "continuous_front"
        else request["contour_id"],
    )
    data, research = current_snapshot(data_registry), current_snapshot(registry)
    if not data or not research:
        raise ValueError("reference cycle has no accepted Data and Research publication")
    logical = baseline.get("run_id") or baseline.get("canonical_run_id")
    if not logical or data["run_id"] != logical:
        raise ValueError("accepted Data does not belong to the actual baseline logical run")

    verified = set()

    def pins(manifest):
        if not manifest["quality_checks"] or any(
            status not in {"PASS", "PASS-NOOP"} for status in manifest["quality_checks"].values()
        ):
            raise ValueError("reference accepted snapshot quality checks did not pass")
        for reference in manifest["tables"].values():
            inside(reference["table_path"])
            identity = digest(reference)
            if identity not in verified:
                open_pinned_table(reference)
                verified.add(identity)
        return {
            "snapshot_id": manifest["snapshot_id"],
            "manifest_sha256": manifest["manifest_sha256"],
            "run_id": manifest["run_id"],
            "table_pins": deepcopy(manifest["tables"]),
        }

    data_pins, research_pins = pins(data), pins(research)
    previous = research_pins
    if research_run is not None:
        if (
            research["run_id"] != research_run.run_id
            or data["snapshot_id"] not in research["parents"]
        ):
            raise ValueError("Research publication is not bound to the actual reference graph")
        admission = config["refresh_admission"]
        if admission["data_parent"]["snapshot_id"] != data["snapshot_id"]:
            raise ValueError("reference Research consumed another Data snapshot")
        parent = admission["research_parent"]
        manifest = load_snapshot(inside(parent["manifest_path"]))
        if any(manifest[key] != parent[key] for key in ("snapshot_id", "manifest_sha256")):
            raise ValueError("reference Research input identity changed")
        previous = pins(manifest)
    if current_snapshot(data_registry)["snapshot_id"] != data["snapshot_id"] or (
        current_snapshot(registry)["snapshot_id"] != research["snapshot_id"]
    ):
        raise ValueError("accepted reference snapshots changed during observation")
    return (
        {"data": data_pins, "research": previous},
        {"data": data_pins, "research": research_pins},
    )


def observe_reference_cycle(plan, root_run_id, instance, data_root):
    """Read actual native runs/ticks/pins; callers own activation, halt and admission."""
    result = {
        "schema_version": "staging_reference_cycle.v1",
        "mode": "reference",
        "root_run_id": root_run_id,
        "task_id": plan.get("task_id"),
        "runs": {},
        "sensor_edges": [],
        "observed_at_utc": datetime.now(UTC).isoformat(),
    }
    cutoff = None
    try:
        root_path = Path(data_root).resolve()
        if plan.get("mode", "candidate") != "reference" or plan["stage"] != "acceptance":
            raise ValueError("reference observation cannot establish candidate admission")
        if root_path != Path(plan["data_root"]).resolve():
            raise ValueError("reference observation data root differs from the protected plan")
        cutoff = datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00")).timestamp()
        root = instance.get_run_by_id(root_run_id)
        if root is None or root.job_name != JOBS[0]:
            raise ValueError("actual reference baseline run is missing")
        authority = root.tags.get("ta3000/reference_authorization")
        if not authority:
            raise ValueError("reference run has no recorded protected activation")
        stages, ended = set(), []
        upstream = None
        for index, job in enumerate(JOBS):
            if upstream is not None:
                children = instance.get_runs(
                    filters=RunsFilter(
                        job_name=job, tags={"ta3000/upstream_run_id": upstream.run_id}
                    ),
                    limit=2,
                )
                if len(children) > 1:
                    raise ValueError("reference graph contains duplicate child runs")
                run = children[0] if children else None
                edge = _edge(instance, upstream, run, SENSORS[index - 1])
                result["sensor_edges"].append(edge)
                if run is None:
                    inputs, outputs = _snapshots(plan, root, None, root_path)
                    result.update(
                        status="PASS-NOOP",
                        reason=edge["skip_reason"],
                        inputs=inputs,
                        outputs=outputs,
                    )
                    break
            else:
                run = root
            if (
                run.tags.get("ta3000/staging_plan_sha256") != digest(plan)
                or run.tags.get("ta3000/candidate_head_sha") != plan["candidate"]["head_sha"]
                or run.tags.get("ta3000/staging_root_run_id") != root.run_id
                or run.tags.get("ta3000/reference_authorization") != authority
                or (
                    upstream is not None
                    and (
                        run.tags.get("ta3000/upstream_job") != upstream.job_name
                        or _repository(run) != _repository(root)
                    )
                )
            ):
                raise ValueError("reference run belongs to another plan, artifact or graph")
            result["runs"][job] = {
                "run_id": run.run_id,
                "status": run.status.value,
                "upstream_run_id": upstream.run_id if upstream else None,
            }
            if run.status.value in {"FAILURE", "CANCELED", "CANCELING"}:
                raise ValueError(f"reference producer failed: {job}: {run.status.value}")
            stats = instance.get_run_stats(run.run_id)
            if run is root:
                if stats.start_time is None:
                    raise _Pending("actual reference root start event is not visible")
                cutoff = min(cutoff, stats.start_time + plan["max_runtime_seconds"])
            if run.status.value != "SUCCESS" or stats.end_time is None:
                raise _Pending(f"reference producer has not completed: {job}")
            ended.append(stats.end_time)
            steps = instance.get_run_step_stats(run.run_id)
            if not steps or any(step.status.value != "SUCCESS" for step in steps):
                raise ValueError("reference producer lacks successful native step evidence")
            stages.update(step.step_key for step in steps)
            if run is root:
                if "moex_baseline_update" not in stages:
                    raise ValueError("reference root did not execute its baseline producer")
                result["schedule"] = _schedule(instance, root)
            upstream = run
        else:
            if not set(STAGES).issubset(stages):
                raise ValueError("reference graph lacks required producer/publication steps")
            inputs, outputs = _snapshots(plan, root, upstream, root_path)
            result.update(
                status="PASS",
                reason="actual scheduled reference graph published",
                inputs=inputs,
                outputs=outputs,
            )
        if ended and max(ended) > cutoff:
            raise ValueError("reference graph exceeded its protected execution deadline")
    except _Pending as exc:
        result.update(
            status="BLOCKED" if cutoff is not None and time.time() >= cutoff else "PENDING",
            reason=str(exc),
        )
    except _SensorBlocked as exc:
        result["sensor_edges"].append(exc.evidence)
        result.update(status="BLOCKED", reason=str(exc))
    except Exception as exc:
        result.update(status="BLOCKED", reason=str(exc))
    return result
