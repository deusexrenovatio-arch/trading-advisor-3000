"""Early contract checks and process ownership for the native Research route."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import threading
import urllib.request
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from trading_advisor_3000.product_plane.common.data_snapshots import atomic_json


def _store_contracts():
    from trading_advisor_3000.spark_jobs.continuous_front_indicator_sidecar_job import (
        _sidecar_storage_contract,
    )

    from .continuous_front import continuous_front_store_contract
    from .continuous_front_indicators.contracts import continuous_front_indicator_store_contract
    from .datasets import research_dataset_store_contract
    from .derived_indicators import research_derived_indicator_store_contract
    from .derived_indicators.source_frames import research_derived_source_frame_store_contract
    from .indicators import indicator_store_contract

    contracts = {}
    for factory in (
        continuous_front_store_contract,
        continuous_front_indicator_store_contract,
        research_dataset_store_contract,
        research_derived_indicator_store_contract,
        research_derived_source_frame_store_contract,
        indicator_store_contract,
    ):
        contracts.update(factory())
    # The Spark sidecar activates yearly storage separately from its shared
    # business contract. Validate the owning writer's physical layout.
    contracts.update(_sidecar_storage_contract(continuous_front_indicator_store_contract()))
    return contracts


def _table_layout(path):
    from deltalake import DeltaTable

    table = DeltaTable(str(path))
    return tuple(table.metadata().partition_columns), {f.name for f in table.schema().fields}


def preflight(config: Mapping) -> None:
    """Check metadata/contracts only; never load historical bar payloads."""
    for window in config.get("changed_windows", ()):
        for key in ("instrument_id", "timeframe", "start_ts", "end_ts"):
            if not str(window.get(key) or "").strip():
                raise ValueError(f"research refresh window requires `{key}`")
        from datetime import datetime as dt

        start = dt.fromisoformat(str(window["start_ts"]).replace("Z", "+00:00"))
        end = dt.fromisoformat(str(window["end_ts"]).replace("Z", "+00:00"))
        start = start.replace(tzinfo=UTC) if start.tzinfo is None else start
        end = end.replace(tzinfo=UTC) if end.tzinfo is None else end
        # Existing changed windows are closed intervals; one corrected bar is
        # valid when start == end. Legacy naive timestamps retain UTC semantics.
        if start > end:
            raise ValueError("research refresh window requires an ordered UTC interval")
    contracts = _store_contracts()
    root = Path(str(config.get("materialized_output_dir") or "."))
    for name, contract in contracts.items():
        partitions = tuple(contract.get("partition_by", contract.get("partition_columns", ())))
        declared = set(contract.get("columns", ())) | set(contract.get("physical_columns", ()))
        if declared and set(partitions) - declared:
            raise ValueError(f"{name}: writer contract omits partition columns")
        path = root / f"{name}.delta"
        if not (path / "_delta_log").exists():
            continue
        actual, columns = _table_layout(path)
        # Legacy CF/report writers support existing unpartitioned targets.
        # Activated physical layouts (including every yearly writer) are strict.
        legacy_unpartitioned = (
            not actual
            and not contract.get("physical_columns")
            and "partition_columns" not in contract
        )
        if actual != partitions and not legacy_unpartitioned:
            raise ValueError(f"{name}: use an isolated layout migration before calculation")
        if set(partitions) - columns:
            raise ValueError(f"{name}: stored schema omits partition columns")
        unique_keys = re.findall(r"unique\(([^)]+)\)", " ".join(contract.get("constraints", ())))
        required_keys = {key.strip() for group in unique_keys for key in group.split(",")}
        if required_keys - columns:
            raise ValueError(f"{name}: stored schema omits logical key columns")
    from . import campaigns
    from .derived_indicators.registry import build_derived_indicator_profile_registry
    from .indicators.registry import build_indicator_profile_registry

    if not callable(getattr(campaigns, "validate_research_contracts", None)):
        raise ValueError("research summary contract validator is unavailable")
    for field, registry in (
        ("indicator_profile_version", build_indicator_profile_registry()),
        ("derived_indicator_profile_version", build_derived_indicator_profile_registry()),
    ):
        if config.get(field):
            registry.get(str(config[field]))


def _process_start(pid: int) -> str | None:
    try:
        # Linux /proc stat field 22; process names can contain spaces/parentheses.
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except FileNotFoundError:
        return None
    except (OSError, IndexError):
        return "UNKNOWN"


def worker_identity(pid: int | None = None) -> dict:
    pid = os.getpid() if pid is None else pid
    generation = _process_start(1)
    if generation not in (None, "UNKNOWN"):
        try:
            generation = (
                Path("/proc/sys/kernel/random/boot_id").read_text().strip() + ":" + generation
            )
        except OSError:
            generation = "UNKNOWN"
    return {
        "owner": os.environ.get("TA3000_RESEARCH_WORKER_OWNER") or socket.gethostname(),
        "generation": generation,
        "pid": pid,
        "process_start": _process_start(pid),
    }


def worker_state(identity, *, owner, generation, process_start) -> str:
    if (
        identity.get("owner") != owner
        or not identity.get("generation")
        or generation in (None, "UNKNOWN")
    ):
        return "UNKNOWN"
    if identity["generation"] != generation:
        return "DEAD"
    if identity.get("process_start") in (None, "UNKNOWN") or process_start == "UNKNOWN":
        return "UNKNOWN"
    return "ALIVE" if identity["process_start"] == process_start else "DEAD"


def local_worker_state(identity) -> str:
    local = worker_identity()
    state = worker_state(
        identity,
        owner=local["owner"],
        generation=local["generation"],
        process_start=_process_start(int(identity.get("pid", -1))),
    )
    # A child step can survive a crashed multiprocess run owner. Wait for both.
    if state == "DEAD" and identity.get("stage_worker"):
        child = identity["stage_worker"]
        child_state = worker_state(
            child,
            owner=local["owner"],
            generation=local["generation"],
            process_start=_process_start(int(child.get("pid", -1))),
        )
        if child_state != "DEAD":
            return child_state
    return state


@contextmanager
def worker_lease(root: Path, run_id: str, stage: str):
    path = root / ".research_workers" / (hashlib.sha256(run_id.encode()).hexdigest() + ".json")
    # The existing jobs use Dagster's multiprocess executor: the op's parent
    # is the durable run worker, so monitoring also covers gaps between steps.
    lease = {
        **worker_identity(os.getppid()),
        "stage_worker": worker_identity(),
        "run_id": run_id,
        "stage": stage,
        "state": "ACTIVE",
    }
    stopped = threading.Event()

    def heartbeat():
        lease["heartbeat_at_utc"] = datetime.now(UTC).isoformat()
        atomic_json(path, lease)

    def pulse():
        while not stopped.wait(15):
            try:
                heartbeat()
            except OSError:
                # Missing heartbeat alone never proves worker death.
                return

    heartbeat()
    thread = threading.Thread(target=pulse, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join()
        lease["stage_state"] = "FINISHED"
        heartbeat()


def recover_indicator_lock(root: Path) -> bool:
    path = root / ".research_indicator_frames.lock.json"
    if not path.exists():
        return False
    # Kernel locks serialize recoverers and release even on process death.
    with (root / ".research_indicator_frames.recovery").open("a+b") as guard:
        if os.name == "nt":
            import msvcrt

            guard.seek(0)
            if not guard.read(1):
                guard.write(b"\0")
                guard.flush()
            guard.seek(0)

            def lock():
                msvcrt.locking(guard.fileno(), msvcrt.LK_NBLCK, 1)

            def unlock():
                msvcrt.locking(guard.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            def lock():
                fcntl.flock(guard.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            def unlock():
                fcntl.flock(guard.fileno(), fcntl.LOCK_UN)

        try:
            lock()
        except OSError:
            return False
        try:
            if not path.exists():
                return False
            content = path.read_bytes()
            if local_worker_state(json.loads(content)) != "DEAD":
                return False
            if path.read_bytes() != content:
                raise RuntimeError("indicator writer lock changed during recovery")
            path.unlink()
            return True
        finally:
            guard.seek(0)
            unlock()


def reconcile_workers(instance, root: Path) -> list[str]:
    lost = []
    for path in sorted((root / ".research_workers").glob("*.json")):
        lease = json.loads(path.read_text())
        if lease.get("state") != "ACTIVE":
            continue
        run = instance.get_run_by_id(lease["run_id"])
        if run and str(run.status.value) in {"SUCCESS", "FAILURE", "CANCELED"}:
            lease["state"] = "TERMINAL"
            atomic_json(path, lease)
            continue
        if local_worker_state(lease) != "DEAD":
            continue
        if run and str(run.status.value) in {"STARTED", "STARTING", "CANCELING"}:
            instance.report_run_failed(
                run, message=f"Confirmed lost Research worker: {lease.get('stage', 'unknown')}"
            )
            lost.append(lease["run_id"])
        lease["state"] = "LOST"
        atomic_json(path, lease)
    recover_indicator_lock(root)
    return lost


def assert_restart_safe(runs) -> None:
    active = [r["runId"] for r in runs if r.get("status") not in {"SUCCESS", "FAILURE", "CANCELED"}]
    if active:
        raise RuntimeError(f"container restart blocked by active Dagster runs: {active}")


def _graphql(url, query, variables=None):
    request = urllib.request.Request(
        url,
        data=json.dumps({"query": query, "variables": variables or {}}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.load(response)
    if payload.get("errors"):
        raise RuntimeError(str(payload["errors"]))
    return payload["data"]


def check_restart(url) -> None:
    # Query every nonterminal state explicitly. Never infer safety from an
    # empty recent-runs page that might omit an old STARTED worker.
    result = _graphql(
        url,
        """{ runsOrError(filter: {statuses: [NOT_STARTED, QUEUED,
      STARTING, STARTED, CANCELING]}) { __typename ... on Runs { results { runId status } } } }""",
    )["runsOrError"]
    if result["__typename"] != "Runs":
        raise RuntimeError("cannot establish Dagster restart safety")
    assert_restart_safe(result["results"])


def resume(url, run_id):
    result = _graphql(
        url,
        """mutation Resume($id: String!) {
      launchRunReexecution(reexecutionParams: {parentRunId: $id, strategy: FROM_FAILURE}) {
        __typename ... on LaunchRunSuccess { run { runId status } }
        ... on PythonError { message } ... on UnauthorizedError { message }
      }
    }""",
        {"id": run_id},
    )["launchRunReexecution"]
    if result["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(str(result))
    return result["run"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "check-restart", "resume", "reconcile"))
    parser.add_argument("--url", default="http://127.0.0.1:3000/graphql")
    parser.add_argument("--run-id")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--root", type=Path)
    args = parser.parse_args()
    if args.command == "check-restart":
        check_restart(args.url)
    elif args.command == "resume":
        if not args.run_id:
            parser.error("resume requires --run-id")
        print(json.dumps(resume(args.url, args.run_id)))
    elif args.command == "preflight":
        if not args.config:
            parser.error("preflight requires --config (resolved Research op configuration)")
        preflight(json.loads(args.config.read_text()))
    else:
        from dagster import DagsterInstance

        if not args.root:
            parser.error("reconcile requires --root")
        with DagsterInstance.get() as instance:
            print(json.dumps(reconcile_workers(instance, args.root)))


if __name__ == "__main__":
    main()
