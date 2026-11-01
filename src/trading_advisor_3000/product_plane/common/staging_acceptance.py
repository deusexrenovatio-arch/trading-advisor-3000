"""Product-owned contract for the native Data/Research acceptance result."""

from __future__ import annotations

import re

JOBS = ("moex_baseline_update_job", "moex_cf_catch_up_job", "research_data_prep_job")
STAGES = (
    "moex_baseline_update",
    "continuous_front_bars",
    "research_datasets",
    "research_indicator_frames",
    "research_derived_indicator_frames",
    "continuous_front_indicator_acceptance_report",
    "research_data_snapshot",
)


def validate_native_result(value):
    if value.get("stage") != "acceptance" or value.get("status") != "PASS":
        raise ValueError("Full native acceptance required; branch cannot grant admission")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value.get("image_digest", "")) or not value.get(
        "input_pins"
    ):
        raise ValueError("Missing actual image/input pins")
    upstream, ids = None, set()
    for name in JOBS:
        run = value.get("runs", {}).get(name, {})
        if run.get("status") != "SUCCESS" or not run.get("run_id") or run["run_id"] in ids:
            raise ValueError("Missing successful distinct native jobs")
        if upstream and run.get("upstream_run_id") != upstream:
            raise ValueError("Broken upstream lineage")
        ids.add(run["run_id"])
        upstream = run["run_id"]
    if (
        not set(STAGES).issubset(value.get("executed_stages", []))
        or value.get("changed_windows", 0) < 1
    ):
        raise ValueError("Real nonempty route execution incomplete")
    if not value.get("qc") or any(v != "PASS" for v in value["qc"].values()):
        raise ValueError("Native QC/publication incomplete")
    consumer = value.get("consumer", {})
    if (
        consumer.get("entrypoint") != "research.io.load_backtest_frames"
        or consumer.get("cache_hit") is not False
        or consumer.get("rows", 0) < 1
        or not value.get("snapshot_id")
        or consumer.get("snapshot_id") != value["snapshot_id"]
    ):
        raise ValueError("Fresh normal consumer proof missing")
    return {**value, "evidence_level": "native"}
