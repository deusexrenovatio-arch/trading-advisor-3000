"""Read-only native production NOOP witness; never execution or admission proof."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def native_noop(instance, root_run_id, scheduled_timestamp, cron):
    from dagster import DagsterRunStatus, RunsFilter
    from dagster._core.scheduler.instigation import InstigatorType
    from dagster._core.storage.tags import SCHEDULED_EXECUTION_TIME_TAG

    from trading_advisor_3000.product_plane.interfaces.staging_observation import (
        JOBS,
        SENSORS,
        _edge,
        _states,
        _tick_matches,
    )

    root = instance.get_run_by_id(root_run_id)
    if root is None or root.job_name != JOBS[0] or root.status.value != "SUCCESS":
        raise ValueError("Production root is not its actual successful baseline job")
    instant = datetime.fromisoformat(root.tags[SCHEDULED_EXECUTION_TIME_TAG].replace("Z", "+00:00"))
    if (
        instant.tzinfo is None
        or instant.timestamp() != scheduled_timestamp
        or scheduled_timestamp > time.time()
    ):
        raise ValueError("Production root belongs to another actual schedule instant")
    state = _states(instance, InstigatorType.SCHEDULE, "moex_baseline_daily_update_schedule", root)
    ticks = [
        tick
        for tick in instance.get_ticks(state.instigator_origin_id, state.selector_id, limit=100)
        if tick.status.value == "SUCCESS"
        and root_run_id in tick.run_ids
        and tick.timestamp == scheduled_timestamp
    ]
    if not state.is_running or state.instigator_data.cron_schedule != cron or len(ticks) != 1:
        raise ValueError("Production root lacks its unique successful native schedule tick")
    active = [
        status
        for status in DagsterRunStatus
        if status.value in {"QUEUED", "STARTING", "STARTED", "CANCELING"}
    ]
    if instance.get_runs(filters=RunsFilter(statuses=active), limit=1):
        raise ValueError("Production has active native work")
    if instance.get_runs(
        filters=RunsFilter(job_name=JOBS[1], tags={"ta3000/upstream_run_id": root_run_id}), limit=1
    ):
        return {
            "status": "NOT_NOOP",
            "reason": "Actual CF child exists",
            "root_run_id": root_run_id,
        }
    sensor = _states(instance, InstigatorType.SENSOR, SENSORS[0], root)
    history = instance.get_ticks(sensor.instigator_origin_id, sensor.selector_id, limit=100)
    disposition = None
    for tick in history:
        if tick.origin_run_ids and set(tick.origin_run_ids) != {root_run_id}:
            raise ValueError("Production sensor origin changed after the root disposition")
        if not _tick_matches(instance, tick, root, None):
            raise ValueError("Production sensor advanced away from the exact root disposition")
        if (
            tick.status.value == "SKIPPED"
            and not tick.run_ids
            and not tick.origin_run_ids
            and tick.skip_reason == "Sensor function returned an empty result"
        ):
            if disposition is None:
                disposition = tick.cursor
            elif disposition != tick.cursor:
                raise ValueError("Production sensor cursor changed after its NOOP disposition")
            continue
        if disposition is not None and tick.cursor != disposition:
            raise ValueError("Production sensor cursor changed after its NOOP disposition")
        selected = tick
        break
    else:
        raise ValueError("Production sensor has no exact explicit native NOOP disposition")

    class ActualTickView:
        def __getattr__(self, name):
            return getattr(instance, name)

        def get_ticks(self, *args, **kwargs):
            return [selected]  # The actual retained native object, never a synthetic TickData.

    edge = _edge(ActualTickView(), root, None, SENSORS[0])
    native_tick = ticks[0]
    return {
        "status": "PASS-NOOP",
        "root_run_id": root_run_id,
        "compute_attempts": 0,
        "schedule": {
            "id": state.instigator_origin_id + ":" + str(float(native_tick.timestamp)),
            "timestamp": native_tick.timestamp,
            "run_id": root_run_id,
            "status": "SUCCESS",
        },
        "sensor": {**edge, "cursor": selected.cursor, "origin_id": selected.instigator_origin_id},
    }


CORE = ("history/runs.db", "history/runs/index.db", "schedules/schedules.db")


def _source_facts(source, declared, deadline):
    if (
        not isinstance(declared, list)
        or any(not isinstance(name, str) for name in declared)
        or len(set(declared)) != len(declared)
    ):
        raise ValueError("Original SQLite family declaration must be explicit and unique")
    files = {name: source / name for name in CORE}
    for name in CORE:
        if Path(str(files[name]) + "-journal").exists():
            raise ValueError("Original rollback journal appeared")
        wal = Path(str(files[name]) + "-wal")
        if wal.exists():
            files[name + "-wal"] = wal
    if set(files) != set(declared):
        raise ValueError("Original main/WAL family set changed")
    if sum(path.stat().st_size for path in files.values()) > 384 * 1024**2:
        raise ValueError("Original metadata exceeds explicit 384MiB read bound")
    values = {}
    for name, path in sorted(files.items()):
        if (
            path.is_symlink()
            or any(parent.is_symlink() for parent in path.parents if parent != source.parent)
            or not path.resolve().is_relative_to(source.resolve())
        ):
            raise ValueError("Unsafe original SQLite family path")
        before = path.stat()
        checksum = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(512 * 1024):
                if time.monotonic() >= deadline:
                    raise ValueError("Readonly metadata observation deadline exceeded")
                checksum.update(chunk)
        after = path.stat()

        def identity(stat):
            return stat.st_size, stat.st_mtime_ns, stat.st_ino, stat.st_dev

        if identity(before) != identity(after):
            raise ValueError("Original SQLite family changed while hashing")
        values[name] = {
            "bytes": before.st_size,
            "mtime_ns": before.st_mtime_ns,
            "inode": before.st_ino,
            "device": before.st_dev,
            "sha256": checksum.hexdigest(),
        }
    return values


def observe(source_home, alias_home, declared, root_run_id, scheduled_timestamp, cron):
    from dagster import DagsterInstance

    from trading_advisor_3000.product_plane.common.staging_contracts import digest
    from trading_advisor_3000.product_plane.common.staging_observation import _source_config

    started, deadline = time.monotonic(), time.monotonic() + 25
    source_home, alias_home = source_home.resolve(), alias_home.resolve()
    mounts = {
        line.split()[4]: line.split()[5].split(",")
        for line in Path("/proc/self/mountinfo").read_text().splitlines()
    }
    if "ro" not in mounts.get(str(source_home), ()) or not str(alias_home).startswith("/tmp/"):
        raise ValueError("Observer requires kernel-readonly source and owned tmpfs alias root")
    config = _source_config(source_home)
    before = _source_facts(source_home, declared, deadline)
    for name in declared:
        original, alias = source_home / name, alias_home / name
        if (
            "ro" not in mounts.get(str(alias), ())
            or original.stat().st_ino != alias.stat().st_ino
            or original.stat().st_dev != alias.stat().st_dev
        ):
            raise ValueError("Native metadata alias is not the original kernel-readonly file")
    with DagsterInstance.from_config(str(alias_home)) as instance:

        def classify():
            try:
                return native_noop(instance, root_run_id, scheduled_timestamp, cron)
            except (ValueError, KeyError) as exc:
                return {
                    "status": "BLOCKED_NOT_NOOP",
                    "reason": str(exc),
                    "root_run_id": root_run_id,
                    "compute_attempts": 0,
                }

        result, repeated = classify(), classify()
    after = _source_facts(source_home, declared, deadline)
    if (
        before != after
        or _source_config(source_home) != config
        or digest(result) != digest(repeated)
    ):
        raise ValueError("Original source family or exact native facts changed during observation")
    result["metadata_observation"] = {
        "observation_complete": True,
        "source_configuration_sha256": config,
        "capture_method": "kernel-readonly-native-aliases-with-private-derived-aux",
        "source_family_before": before,
        "source_family_after_observation": after,
        "source_read_bound_bytes": 384 * 1024**2,
        "tmpfs_bound_bytes": 256 * 1024**2,
        "immutable": False,
        "original_rw": False,
        "elapsed_seconds": time.monotonic() - started,
        "selected_native_facts_sha256": digest(repeated),
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-home", type=Path, default=Path("/production-home"))
    parser.add_argument("--alias-home", type=Path, default=Path("/tmp/production-noop-home"))
    parser.add_argument("--source-files", required=True)
    parser.add_argument("--root-run-id", required=True)
    parser.add_argument("--scheduled-timestamp", required=True, type=float)
    parser.add_argument("--cron", required=True)
    args = parser.parse_args()
    try:
        print(
            json.dumps(
                observe(
                    args.source_home,
                    args.alias_home,
                    json.loads(args.source_files),
                    args.root_run_id,
                    args.scheduled_timestamp,
                    args.cron,
                )
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "BLOCKED",
                    "reason": str(exc)[:1000],
                    "error_type": type(exc).__name__,
                    "compute_attempts": 0,
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
