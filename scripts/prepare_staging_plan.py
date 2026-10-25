"""Prepare bounded control metadata; never execute producers or issue acceptance.

Run in an isolated metadata container with the frozen candidate and seed read-only,
without signing keys, owner credentials or Docker access. Print the proposed plan;
the protected host validates it before installing plan.json and its root marker.
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import json
import sys
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
BASELINE_STEPS = (
    "run_moex_baseline_update",
    "run_moex_raw_ingest_spark_delta_job",
    "run_moex_contract_economics_spark_job",
    "run_moex_canonicalization_spark_delta_job",
    "run_moex_canonical_publish_spark_delta_job",
)
RESEARCH_STEPS = (
    "run_continuous_front_spark_job",
    "run_research_bar_views_spark_job",
    "materialize_indicator_frames",
    "materialize_derived_indicator_frames",
    "run_research_derived_source_frames_spark_job",
    "run_continuous_front_indicator_pandas_job",
    "run_continuous_front_indicator_sidecar_spark_job",
)
JOBS = ("moex_baseline_update_job", "moex_cf_catch_up_job", "research_data_prep_job")


@lru_cache(maxsize=1)
def contracts():
    path = ROOT / "src/trading_advisor_3000/product_plane/common/staging_contracts.py"
    spec = importlib.util.spec_from_file_location("protected_plan_contracts", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Captured(BaseException):
    pass


def capture_parameters(assets, op_config):
    """Stop at the native producer boundary, before its body or admission runs."""
    from trading_advisor_3000.product_plane.common.staging_source_scope import parameter_value
    from trading_advisor_3000.product_plane.data_plane.moex.baseline_update import (
        _baseline_update_timeframes,
    )

    original, captured = assets.run_moex_baseline_update, {}

    def capture(**kwargs):
        bound = inspect.signature(original).bind(**kwargs)
        bound.apply_defaults()
        captured.update(bound.arguments)
        raise _Captured

    assets.run_moex_baseline_update = capture
    try:
        function = inspect.unwrap(assets.moex_baseline_update.op.compute_fn.decorated_fn)
        function(SimpleNamespace(op_config=op_config))
        raise ValueError("Native asset did not reach the protected parameter capture")
    except _Captured:
        pass
    finally:
        assets.run_moex_baseline_update = original
    cutoff = captured.pop("ingest_till_utc")
    captured.pop("run_id")
    captured["timeframes"] = _baseline_update_timeframes(captured["timeframes"])
    return parameter_value(captured), cutoff


def scheduled_instant(assets, value):
    from dagster._utils.schedules import cron_string_iterator

    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        raise ValueError("Preparation requires an aware native cron instant")
    native = next(
        cron_string_iterator(
            instant.timestamp(),
            assets.MOEX_BASELINE_DAILY_CRON,
            assets.MOEX_HISTORICAL_EXECUTION_TIMEZONE,
        )
    )
    if native.timestamp() != instant.timestamp():
        raise ValueError("Selected time is not an actual native schedule instant")
    now = datetime.now(UTC)
    if (
        instant > now
        and instant.timestamp()
        != next(
            cron_string_iterator(
                now.timestamp(),
                assets.MOEX_BASELINE_DAILY_CRON,
                assets.MOEX_HISTORICAL_EXECUTION_TIMEZONE,
            )
        ).timestamp()
    ):
        raise ValueError("A future proposal must name the next native cron instant")
    return instant


def native_schedule_metadata(assets, instant, run_config):
    """Carry the complete Linux-native config without host path reconstruction."""
    return dict(
        cron=assets.MOEX_BASELINE_DAILY_CRON,
        timezone=assets.MOEX_HISTORICAL_EXECUTION_TIMEZONE,
        logical_scheduled_at_utc=instant.astimezone(UTC).isoformat(),
        run_config=run_config,
        preparation="metadata-only; not execution or acceptance",
    )


def native_root_run_config(assets, instant, stage, deployed_root):
    daily = assets._build_baseline_daily_schedule_run_config(
        SimpleNamespace(scheduled_execution_time=instant)
    )
    if stage != "branch":
        return daily
    from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (
        build_moex_baseline_run_config_for_instance,
        load_moex_runtime_instances_registry,
    )

    instance = load_moex_runtime_instances_registry(repo_root=deployed_root).instance(
        "moex_branch_staging"
    )
    clock = daily["ops"]["moex_baseline_update"]["config"]
    branch = build_moex_baseline_run_config_for_instance(
        instance, run_id=clock["run_id"], ingest_till_utc=clock["ingest_till_utc"]
    )
    # The native owner's closed-session cutoff and lag are one temporal contract.
    # Applying the manual branch lag again would drop the just-closed daily candle.
    branch["ops"]["moex_baseline_update"]["config"]["stability_lag_minutes"] = clock[
        "stability_lag_minutes"
    ]
    return branch


def prepare(args):
    sys.path.insert(0, str(args.deployed_root / "src"))
    from trading_advisor_3000.dagster_defs import moex_historical_assets as assets
    from trading_advisor_3000.product_plane.common.data_snapshots import (
        current_snapshot,
        load_snapshot,
    )
    from trading_advisor_3000.product_plane.common.staging_seed import _frozen_accepted
    from trading_advisor_3000.product_plane.data_plane.moex.foundation import load_universe
    from trading_advisor_3000.product_plane.research.io import ResearchSliceRequest

    if not Path(assets.__file__).resolve().is_relative_to((args.deployed_root / "src").resolve()):
        raise ValueError("Native configuration was imported from another artifact")
    root = args.data_root.resolve()
    if assets._route_artifact_root().resolve() != root:
        raise ValueError("Native configured data root differs from the requested staging root")
    instant = scheduled_instant(assets, args.scheduled_at)
    if instant >= datetime.fromisoformat(args.deadline.replace("Z", "+00:00")):
        raise ValueError("Proposed schedule must occur before the protected deadline")
    run_config = native_root_run_config(assets, instant, args.stage, args.deployed_root)
    config = run_config["ops"]["moex_baseline_update"]["config"]
    parameters, cutoff = capture_parameters(assets, config)
    source_path = args.research_manifest.resolve()
    if not source_path.is_relative_to(root):
        raise ValueError("Source manifest must belong to the independent staging root")
    source = load_snapshot(source_path)
    data_registry = root / "canonical/moex/baseline-4y-current/.data_snapshots"
    if len(source["parents"]) != 1:
        raise ValueError("Research requires its exact immutable accepted Data parent")
    data = load_snapshot(data_registry / "manifests" / f"{source['parents'][0]}.json")
    current = current_snapshot(source_path.parent.parent)
    if current is None or (
        current["manifest_sha256"] != source["manifest_sha256"]
        and not _frozen_accepted(root, source, data)
    ):
        raise ValueError("Source is not a current or protected observed accepted checkpoint")
    for manifest in (source, data):
        if datetime.fromisoformat(manifest["created_at_utc"].replace("Z", "+00:00")) > instant:
            raise ValueError("Source checkpoint is newer than the declared native schedule instant")
        if not manifest["quality_checks"] or any(
            v not in {"PASS", "PASS-NOOP"} for v in manifest["quality_checks"].values()
        ):
            raise ValueError("Source checkpoints are not accepted")
    consumer = json.loads(args.consumer.read_text("utf-8"))
    request = ResearchSliceRequest(**consumer)
    instruments = [
        s.internal_id for s in load_universe(Path(parameters["universe_path"])) if s.is_active
    ]
    if (
        not request.instrument_ids
        or not set(request.instrument_ids).issubset(instruments)
        or request.timeframe not in parameters["timeframes"]
        or not request.price_columns
        or request.dataset_version != source["dataset_version"]
    ):
        raise ValueError("Consumer must select an explicit compatible native slice")
    candidate = json.loads(args.candidate.read_text("utf-8"))
    if candidate.get("schema_version") != "staging_candidate.v1":
        raise ValueError("Use the immutable build_staging_candidate output")
    jobs = JOBS if args.stage == "acceptance" else JOBS[:1]
    steps = dict.fromkeys(BASELINE_STEPS, 1)
    if args.stage == "acceptance":
        steps.update(dict.fromkeys(RESEARCH_STEPS, 1))
        steps["materialize_research_dataset"] = 2
    end = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
    plan = dict(
        schema_version="staging_plan.v1",
        task_id=args.task_id,
        stage=args.stage,
        attempt_kind="acceptance" if args.stage == "acceptance" else "small",
        candidate=candidate["candidate"],
        data_root=str(root),
        quota_home=str(args.quota_home),
        deadline_utc=args.deadline,
        max_runtime_seconds=args.max_runtime_seconds,
        limits=dict(small=2, acceptance=1, rebuild=0),
        root_job=JOBS[0],
        jobs={
            job: dict(upstream_job=jobs[i - 1] if i else None, max_runs=1)
            for i, job in enumerate(jobs)
        },
        native_steps=steps,
        scope=dict(
            instrument_ids=instruments,
            timeframes=parameters["timeframes"],
            start_utc=(end - timedelta(days=parameters["max_changed_window_days"])).isoformat(),
            end_utc=cutoff,
            max_changed_window_days=parameters["max_changed_window_days"],
            max_source_bytes=args.max_source_bytes,
            full_history=False,
        ),
        require_scheduled=args.stage == "acceptance",
        consumer=consumer,
        required_scenarios=[],
        verifier_revision=args.verifier_revision,
        producer_parameters={"run_moex_baseline_update": parameters},
        source=dict(
            research_manifest_path=str(source_path),
            snapshot_id=source["snapshot_id"],
            manifest_sha256=source["manifest_sha256"],
            native_schedule=native_schedule_metadata(assets, instant, run_config),
        ),
    )
    if args.window_start or args.window_end:
        plan["execution_window"] = dict(start_utc=args.window_start, end_utc=args.window_end)
    return contracts().validate_document("plan", plan)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "research-manifest", "consumer", "data-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("task-id", "scheduled-at", "deadline", "verifier-revision"):
        parser.add_argument("--" + name, required=True)
    for name in ("max-runtime-seconds", "max-source-bytes"):
        parser.add_argument("--" + name, type=int, required=True)
    parser.add_argument("--stage", choices=("branch", "acceptance"), default="acceptance")
    parser.add_argument("--deployed-root", type=Path, default=Path("/workspace"))
    parser.add_argument("--quota-home", type=Path, default=Path("/staging-quota"))
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        deadline = datetime.fromisoformat(args.deadline.replace("Z", "+00:00"))
        if deadline.tzinfo is None or deadline <= datetime.now(UTC):
            raise ValueError("Protected plan deadline must be an unexpired aware timestamp")
        if args.output and (
            args.output.exists() or args.output.resolve().is_relative_to(args.data_root.resolve())
        ):
            raise ValueError("Plan output must be new and outside writable data")
        plan = prepare(args)
        encoded = json.dumps(plan, indent=2) + "\n"
        if args.output:
            with args.output.open("x", encoding="utf-8") as stream:
                stream.write(encoded)
        else:
            print(encoded, end="")
        return 0
    except (ValueError, OSError, KeyError, TypeError, RuntimeError) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
