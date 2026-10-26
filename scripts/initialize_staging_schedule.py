"""One-time native acceptance schedule checkpoint, never a fabricated tick/run.

The protected host must stop its verified own acceptance daemon first, inspect
the exact HOME volume/image, and mount its short-lived context plus lease RO.
This metadata container has only its own HOME RW and append-only quota access;
no writable dataset, private key or Docker socket. Restart the daemon afterwards
to discard native in-memory iteration times. Dry-run is the default.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from trading_advisor_3000.product_plane.common.staging_contracts import (  # noqa: E402
    digest,
    load_plan,
)

SCHEDULE = "moex_baseline_daily_update_schedule"


def selected_instant(plan, *, now=None):
    from dagster._utils.schedules import cron_string_iterator

    from trading_advisor_3000.dagster_defs.moex_historical_assets import (
        MOEX_BASELINE_DAILY_CRON,
        MOEX_HISTORICAL_EXECUTION_TIMEZONE,
    )
    from trading_advisor_3000.product_plane.common.staging_route_proof import JOBS

    if (
        plan["stage"] != "acceptance"
        or plan.get("mode", "candidate") != "candidate"
        or not plan["require_scheduled"]
        or plan["attempt_kind"] != "acceptance"
        or plan["root_job"] != JOBS[0]
        or set(plan["jobs"]) != set(JOBS)
    ):
        raise ValueError("Checkpoint initialization is only for a fresh acceptance candidate")
    source = plan["source"]["native_schedule"]
    if (
        source["cron"] != MOEX_BASELINE_DAILY_CRON
        or source["timezone"] != MOEX_HISTORICAL_EXECUTION_TIMEZONE
    ):
        raise ValueError("Declared schedule differs from the protected native definition")
    instant = datetime.fromisoformat(source["logical_scheduled_at_utc"].replace("Z", "+00:00"))
    now = now or datetime.now(UTC)
    native = next(
        cron_string_iterator(
            now.timestamp(),
            source["cron"],
            source["timezone"],
            ascending=False,
        )
    )
    if instant.tzinfo is None or instant.timestamp() != native.timestamp():
        raise ValueError("Only the most recent actual elapsed cron instant is supported")
    return instant.timestamp()


def verify_host_context(plan, plan_path, context_path, host_state, home, *, now=None):
    """Validate a protected host inspection request; it is not independent mount proof."""
    if context_path.is_symlink():
        raise ValueError("Host context cannot be a symlink")
    context_path = context_path.resolve()
    if (
        not context_path.is_relative_to(plan_path.resolve().parent)
        or home.resolve().is_relative_to(context_path.parent)
        or context_path.parent.is_relative_to(home.resolve())
    ):
        raise ValueError("Host context must be protected and independent of writable HOME")
    context = json.loads(context_path.read_text("utf-8"))
    lock = json.loads((host_state / "heavy-compute.lock").read_text("utf-8"))
    marker = json.loads((plan_path.parent / "root-marker.json").read_text("utf-8"))
    if marker != {
        "schema_version": "staging_root.v1",
        "plan_path": str(plan_path),
        "plan_digest": digest(plan),
    }:
        raise ValueError("Protected plan/root binding changed")
    expected = dict(project="ta3000-acceptance", plan_sha256=digest(plan))
    if (
        context.get("schema_version") != "staging_schedule_initialization_context.v1"
        or any(context.get(k) != v or lock.get(k) != v for k, v in expected.items())
        or lock.get("schema_version") != "staging_host_lock.v1"
        or context.get("lease_nonce") != lock.get("lease_nonce")
        or not re.fullmatch(r"[0-9a-f]{32}", str(context.get("lease_nonce", "")))
        or context.get("daemon_stopped") is not True
        or context.get("home_volume") != "ta3000-acceptance_staging-home"
        or context.get("home_path") != str(home)
        or context.get("image_digest") != plan["candidate"]["image_digest"]
    ):
        raise ValueError("Host inspection/own acceptance lease binding is missing or changed")
    verified = datetime.fromisoformat(context["verified_at_utc"].replace("Z", "+00:00"))
    expires = datetime.fromisoformat(context["expires_at_utc"].replace("Z", "+00:00"))
    now = now or datetime.now(UTC)
    if (
        verified.tzinfo is None
        or expires.tzinfo is None
        or not verified <= now < expires
        or not 0 < (expires - verified).total_seconds() <= 30
    ):
        raise ValueError("Host inspection request expired or has an invalid lifetime")
    return context


def initialize(instance, quota, plan, *, apply=False, context=None):
    from dagster import DagsterRunStatus, RunsFilter
    from dagster._core.scheduler.instigation import InstigatorType, ScheduleInstigatorData

    from trading_advisor_3000.product_plane.common.staging_execution import (
        _add,
        _deadline,
        _key,
    )

    _deadline(plan)
    selected = selected_instant(plan)
    states = [
        state
        for state in instance.all_instigator_state(instigator_type=InstigatorType.SCHEDULE)
        if state.name == SCHEDULE
    ]
    if len(states) != 1:
        raise ValueError("Exactly one existing native acceptance schedule record is required")
    state = states[0]
    old = state.instigator_data
    if (
        not state.is_running
        or not isinstance(old, ScheduleInstigatorData)
        or old.cron_schedule != plan["source"]["native_schedule"]["cron"]
        or old.start_timestamp is None
        or old.start_timestamp <= selected
        or (old.last_iteration_timestamp is not None and old.last_iteration_timestamp <= selected)
    ):
        raise ValueError("Schedule is not a fresh, unchanged native initial checkpoint")
    if instance.get_ticks(state.instigator_origin_id, state.selector_id, limit=1):
        raise ValueError("Existing schedule ticks prohibit initialization or reset")
    if any(instance.get_runs(filters=RunsFilter(job_name=job), limit=1) for job in plan["jobs"]):
        raise ValueError("Existing graph runs prohibit initialization or retry")
    active = RunsFilter(
        statuses=[
            status
            for status in DagsterRunStatus
            if status.value in {"QUEUED", "STARTING", "STARTED", "CANCELING"}
        ]
    )
    if instance.get_runs(filters=active, limit=1):
        raise ValueError("Acceptance HOME contains active work")
    # Native _slots always fills slot zero first, and accepted spend is append-only.
    # Three exact lookups cover this task even after extensions or HOME/SHA changes.
    if any(
        quota.get_run_by_id(_key("staging-root-v1", plan["task_id"], kind, 0)) is not None
        for kind in ("small", "acceptance", "rebuild")
    ):
        raise ValueError("Claimed task attempts prohibit initialization")
    claim_id = _key("staging-schedule-initialization-v1", plan["task_id"])
    if quota.get_run_by_id(claim_id) is not None:
        raise ValueError("One-time initialization already claimed; no automatic reset/retry")
    checkpoint = selected - 1.0
    result = dict(
        status="DRY_RUN",
        plan_sha256=digest(plan),
        schedule=state.name,
        selector_id=state.selector_id,
        logical_scheduled_timestamp=selected,
        checkpoint_timestamp=checkpoint,
        creates_runtime_ticks_or_runs=False,
        quota_control_claim_created=False,
        daemon_restart_required=True,
        compute_attempts=0,
    )
    if apply:
        if context is None:
            raise ValueError("Apply requires the protected host inspection context")
        expires = datetime.fromisoformat(context["expires_at_utc"].replace("Z", "+00:00"))
        if datetime.now(UTC) >= expires:
            raise ValueError("Host inspection expired before initialization claim")
        if (
            _add(
                quota,
                claim_id,
                dict(
                    task_id=plan["task_id"],
                    plan_sha256=digest(plan),
                    selector_id=state.selector_id,
                    checkpoint=checkpoint,
                    host_context_sha256=digest(context),
                ),
            )
            is None
        ):
            raise ValueError("Concurrent initialization already claimed")
        if instance.get_instigator_state(state.instigator_origin_id, state.selector_id) != state:
            raise ValueError("Native schedule changed after inspection; initialization halted")
        if datetime.now(UTC) >= expires:
            raise ValueError("Host inspection expired after claim; explicit recovery required")
        instance.update_instigator_state(
            state.with_data(
                old._replace(
                    start_timestamp=checkpoint,
                    last_iteration_timestamp=checkpoint,
                )
            )
        )
        result["status"] = "INITIALIZED_NATIVE_CHECKPOINT"
        result["quota_control_claim_created"] = True
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, default=Path("/staging-control/plan.json"))
    parser.add_argument(
        "--host-context", type=Path, default=Path("/staging-control/schedule-initialization.json")
    )
    parser.add_argument("--host-state", type=Path, default=Path("/staging-host-state"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        from dagster import DagsterInstance

        from trading_advisor_3000.product_plane.common.staging_execution import _quota

        plan = load_plan(args.plan)
        home = Path(os.environ["DAGSTER_HOME"])
        quota_home = Path(plan["quota_home"]).resolve()
        if (
            home != Path("/opt/dagster/dagster_home")
            or not (home / "history/runs.db").is_file()
            or not (home / "schedules/schedules.db").is_file()
            or quota_home.is_relative_to(home)
            or home.is_relative_to(quota_home)
        ):
            raise ValueError("Initializer requires the existing independent container HOME")
        context = verify_host_context(plan, args.plan, args.host_context, args.host_state, home)
        with DagsterInstance.from_config(str(home)) as instance, _quota(plan) as quota:
            context = verify_host_context(plan, args.plan, args.host_context, args.host_state, home)
            print(json.dumps(initialize(instance, quota, plan, apply=args.apply, context=context)))
        return 0
    except (ValueError, RuntimeError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
