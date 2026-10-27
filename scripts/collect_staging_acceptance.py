"""Read actual native Dagster/Delta evidence and invoke a fresh public consumer."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from trading_advisor_3000.product_plane.common.staging_acceptance import (
    STAGES,
    validate_native_result,
)


def consumer(plan, manifest):
    from trading_advisor_3000.product_plane.common.data_snapshots import snapshot_read_scope
    from trading_advisor_3000.product_plane.research.io import (
        ResearchSliceRequest,
        load_backtest_frames,
    )

    root = Path("/ta3000-data/moex-historical/research/gold/current")
    request = ResearchSliceRequest(**plan["consumer"])
    if (
        not request.instrument_ids
        or not request.price_columns
        or not request.indicator_columns
        or not request.derived_columns
    ):
        raise ValueError("Explicit price/L1/L2 consumer coverage required")
    with snapshot_read_scope(manifest):
        frames, _, hit = load_backtest_frames(
            dataset_output_dir=root,
            indicator_output_dir=root,
            derived_indicator_output_dir=root,
            request=request,
            cache=None,
        )
    columns = (*request.price_columns, *request.indicator_columns, *request.derived_columns)
    if (
        hit
        or len(frames) != len(request.instrument_ids)
        or any(
            len(f.frame) == 0
            or any(c not in f.frame or not f.frame[c].notna().any() for c in columns)
            for f in frames
        )
    ):
        raise ValueError("Consumer missing populated instrument/column coverage")
    return {
        "entrypoint": "research.io.load_backtest_frames",
        "rows": sum(len(f.frame) for f in frames),
        "snapshot_id": manifest["snapshot_id"],
        "cache_hit": False,
    }


def native_state(instance, plan):
    from dagster import RunsFilter

    runs = {}
    for job in plan["jobs"]:
        actual = instance.get_runs(
            filters=RunsFilter(job_name=job, tags={"ta3000/staging_root": plan["root_run_id"]}),
            limit=2,
        )
        if len(actual) > 1:
            raise ValueError("Duplicate native job")
        if actual:
            run = actual[0]
            runs[job] = {
                "run_id": run.run_id,
                "status": run.status.value,
                "upstream_run_id": run.tags.get("ta3000/upstream_run_id"),
            }
    statuses = {r["status"] for r in runs.values()}
    if statuses.intersection({"FAILURE", "CANCELED", "CANCELING"}):
        status = "FAILED"
    elif len(runs) == len(plan["jobs"]) and statuses == {"SUCCESS"}:
        status = "SUCCESS"
    else:
        status = "PENDING"
    return {"status": status, "runs": runs}


def baseline_changed_windows(materialization):
    if materialization.asset_key.to_user_string() != "moex_baseline_update":
        return 0
    count = materialization.metadata.get("effective_changed_windows")
    return int(count.value) if count is not None else 0


def observe(plan, instance):
    from trading_advisor_3000.product_plane.common.data_snapshots import (
        current_snapshot,
        open_pinned_table,
    )

    state = native_state(instance, plan)
    if state["status"] != "SUCCESS":
        raise ValueError("Native graph incomplete or failed")
    runs, stages, changed, upstream = state["runs"], set(), 0, None
    for job in plan["jobs"]:
        run = runs[job]
        if upstream and run["upstream_run_id"] != upstream:
            raise ValueError("Broken actual native lineage")
        upstream = run["run_id"]
        for entry in instance.all_logs(upstream):
            event = entry.dagster_event
            if event is not None and event.is_step_materialization:
                materialization = event.event_specific_data.materialization
                stages.add(materialization.asset_key.to_user_string())
                changed = max(changed, baseline_changed_windows(materialization))
    value = {
        **{
            k: plan[k]
            for k in (
                "base_sha",
                "head_sha",
                "tree_sha",
                "image_digest",
                "ci_run_id",
                "ci_attempt",
                "input_pins",
                "stage",
            )
        },
        "version": 1,
        "evidence_level": "short",
        "status": "PASS",
        "runs": runs,
        "executed_stages": sorted(stages),
        "changed_windows": changed,
    }
    if plan["target"] == "data":
        from deltalake import DeltaTable

        table = DeltaTable(plan["data_table"])
        manifest = json.loads(Path(plan["data_manifest"]).read_text())
        if table.count() < 1 or not manifest.get("row_counts"):
            raise ValueError("Native Data publication/readback empty")
        value.update(data_rows=table.count(), qc={"native_data_job": "PASS"})
        return value
    registry = Path(plan["research_registry"])
    manifest = current_snapshot(registry)
    if manifest is None or manifest["run_id"] != upstream:
        raise ValueError("Published Research belongs to another chain")
    data_registry = (
        Path("/staging-control/fixture/canonical/.data_snapshots")
        if plan["stage"] == "branch"
        else Path("/ta3000-data/moex-historical/canonical/moex/baseline-4y-current/.data_snapshots")
    )
    data = current_snapshot(data_registry)
    if data is None or manifest["parents"] != [data["snapshot_id"]]:
        raise ValueError("Research does not descend from actual Data publication")
    if not manifest["quality_checks"] or any(
        v != "PASS" for v in manifest["quality_checks"].values()
    ):
        raise ValueError("Published native QC incomplete")
    for reference in manifest["tables"].values():
        open_pinned_table(reference)
    if plan["stage"] == "acceptance" and (
        not set(STAGES).issubset(stages)
        or changed < 1
        or data["snapshot_id"] == plan["input_pins"]["data"]
    ):
        raise ValueError("Full nonempty update/Research/QC/publication missing")
    request = subprocess.run(
        [sys.executable, __file__, "consumer", "/staging-control/plan.json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    value.update(
        consumer=json.loads(request.stdout),
        qc=manifest["quality_checks"],
        snapshot_id=manifest["snapshot_id"],
    )
    if current_snapshot(registry) != manifest or current_snapshot(data_registry) != data:
        raise ValueError("Publication changed during fresh consumer readback")
    return validate_native_result(value) if plan["stage"] == "acceptance" else value


if __name__ == "__main__":
    from dagster import DagsterInstance

    plan = json.loads(Path(sys.argv[2]).read_text())
    if sys.argv[1] == "consumer":
        from trading_advisor_3000.product_plane.common.data_snapshots import current_snapshot

        result = consumer(plan, current_snapshot(Path(plan["research_registry"])))
    else:
        with DagsterInstance.get() as instance:
            result = (
                native_state(instance, plan) if sys.argv[1] == "status" else observe(plan, instance)
            )
    print(json.dumps(result))
