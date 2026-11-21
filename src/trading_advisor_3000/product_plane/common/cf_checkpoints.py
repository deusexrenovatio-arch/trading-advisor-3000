"""Shared native CF checkpoint contract consumed by Data recovery and Research."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .data_snapshots import (
    _hash,
    assert_tables_unchanged,
    current_snapshot,
    load_snapshot,
    open_pinned_table,
)

CONTINUOUS_FRONT_TABLES = (
    "continuous_front_bars",
    "continuous_front_roll_events",
    "continuous_front_adjustment_ladder",
    "continuous_front_qc_report",
)


def snapshot_registry(output_dir: Path, dataset_version: str, contour_id: str) -> Path:
    scope = hashlib.sha256(json.dumps([dataset_version, contour_id]).encode()).hexdigest()[:24]
    return output_dir / ".data_snapshots" / scope


def _registry(config: Mapping[str, Any]) -> Path:
    contour = (
        "pit_active_front" if config.get("series_mode") == "continuous_front" else "native_tradable"
    )
    return snapshot_registry(
        Path(config["materialized_output_dir"]), str(config["dataset_version"]), contour
    )


def _pending_path(context: Any, config: Mapping[str, Any], run_id: str | None = None) -> Path:
    # Dagster run IDs identify attempts; campaign_run_id is often reused nightly.
    run_id = run_id or str(context.run_id)
    token = hashlib.sha256(run_id.encode()).hexdigest()
    return _registry(config) / "pending" / f"{token}.json"


def _parent(root, manifest=None):
    manifest = manifest or current_snapshot(root)
    if manifest is None:
        return None
    return {
        "registry_root": Path(root).resolve().as_posix(),
        "manifest_path": (Path(root) / "manifests" / f"{manifest['snapshot_id']}.json")
        .resolve()
        .as_posix(),
        "snapshot_id": manifest["snapshot_id"],
        "manifest_sha256": manifest["manifest_sha256"],
    }


def _read_cf_checkpoint(instance, source_id, config):
    run = instance.get_run_by_id(source_id) if instance and source_id else None
    stage = "continuous_front_bars"
    receipt = config.get("refresh_admission")
    if (
        not run
        or run.job_name != "moex_cf_catch_up_job"
        or run.status.value != "SUCCESS"
        or not any(
            step.step_key == stage and step.status.value == "SUCCESS"
            for step in instance.get_run_step_stats(source_id)
        )
    ):
        raise ValueError("daily refresh upstream CF run/step/admission changed")
    source = json.loads(_pending_path(run, config, source_id).read_text(encoding="utf-8"))
    outputs = CONTINUOUS_FRONT_TABLES
    if (
        source["run_id"] != source_id
        or source["parameters"].get("refresh_admission") != receipt
        or source["quality_owners"].get(stage) != source_id
        or source["quality_checks"].get(stage) != "PASS"
    ):
        raise ValueError("daily refresh upstream CF checkpoint lineage or QC changed")
    if config.get("operation") == "daily" and (
        run.run_config.get("ops", {}).get(stage, {}).get("config", {}).get("refresh_admission")
        != receipt
        or source["expected_current_id"] != receipt["research_parent"]["snapshot_id"]
        or source["parents"] != [receipt["data_parent"]["snapshot_id"]]
        or source["parent_manifest_sha256"] != receipt["data_parent"]["manifest_sha256"]
    ):
        raise ValueError("daily refresh upstream CF admission or parents changed")
    result = source["quality_evidence"][stage].get("cf_result", {})
    if (
        result.get("status") != "PASS"
        or set(result.get("rows_by_table", {})) != set(outputs)
        or result.get("dataset_version") != config["dataset_version"]
        or not result.get("qc_rows")
        or any(row.get("status") != "PASS" for row in result["qc_rows"])
        or set(result.get("output_paths", {})) != set(outputs)
    ):
        raise ValueError("daily refresh upstream CF result incomplete or QC failed")
    for name in outputs:
        expected = Path(config["materialized_output_dir"]) / f"{name}.delta"
        ref = source["tables"].get(name)
        if not ref or any(
            Path(path).resolve() != expected.resolve()
            for path in (ref["table_path"], result["output_paths"][name])
        ):
            raise ValueError(f"daily refresh upstream CF checkpoint path or pin changed: {name}")
    return source


def recovery_cf_checkpoint(instance, run_id, *, research, data_root, research_root):
    """Verify the native producer before its four outputs may seed a later daily CF."""
    from dagster._core.scheduler.instigation import TickStatus, create_snapshot_id

    run = instance.get_run_by_id(run_id)
    tags = run.tags if run else {}
    baseline = (
        instance.get_run_by_id(tags["ta3000/upstream_run_id"])
        if tags.get("ta3000/upstream_run_id")
        else None
    )
    if (
        not baseline
        or baseline.job_name != "moex_baseline_update_job"
        or baseline.status.value != "SUCCESS"
        or baseline.tags.get("dagster/schedule_name") != "moex_baseline_daily_update_schedule"
        or tags.get("ta3000/upstream_job") != baseline.job_name
        or tags.get("dagster/sensor_name") != "moex_cf_catch_up_after_moex_baseline_sensor"
        or not run.remote_job_origin
        or not baseline.remote_job_origin
        or run.remote_job_origin.repository_origin != baseline.remote_job_origin.repository_origin
    ):
        raise ValueError("recovery requires a successful scheduled baseline and CF")
    repo = run.remote_job_origin.repository_origin
    if (repo.code_location_origin.location_name, repo.repository_name) != (
        "trading_advisor_3000.dagster_defs:product_plane_definitions",
        "__repository__",
    ):
        raise ValueError("recovery CF code location differs from the nightly owner")
    ticks = {}
    for native, tag in ((baseline, "dagster/schedule_name"), (run, "dagster/sensor_name")):
        origin = repo.get_instigator_origin(native.tags[tag])
        created = instance.get_run_record_by_id(native.run_id).create_timestamp.timestamp()
        matches = [
            tick.tick_id
            for tick in instance.get_ticks(
                origin.get_id(),
                create_snapshot_id(origin.get_selector()),
                before=created + 300,
                after=created - 86400,
                statuses=[TickStatus.SUCCESS],
            )
            if tick.instigator_origin_id == origin.get_id() and native.run_id in tick.run_ids
        ]
        if len(matches) != 1:
            raise ValueError(f"recovery requires the successful owner tick for {native.run_id}")
        ticks[native.tags[tag]] = matches[0]
    config = run.run_config["ops"]["continuous_front_bars"]["config"]
    receipt = config["refresh_admission"]
    if (
        _registry(config).resolve() != Path(research_root).resolve()
        or config.get("operation") != "daily"
        or config.get("force_full_refresh") is not False
        or receipt["operation"] != "daily"
        or receipt["outcome"] != "SCOPED"
        or not receipt["effective_windows"]
        or config["changed_windows"] != receipt["effective_windows"]
        or receipt["admission_id"]
        != _hash({k: v for k, v in receipt.items() if k != "admission_id"})
        or tags.get("ta3000/refresh_admission_id") != receipt["admission_id"]
        or receipt["research_parent"] != _parent(research_root, research)
    ):
        raise ValueError("recovery CF admission differs from accepted Research")
    source = _read_cf_checkpoint(instance, run_id, config)
    data = load_snapshot(
        data_root / "manifests" / (receipt["data_parent"]["snapshot_id"] + ".json")
    )
    if (
        receipt["data_parent"] != _parent(data_root, data)
        or receipt["inputs"] != data["tables"]
        or data["parents"] != research["parents"]
        or data["parameters"]["downstream_scope"].get("full_refresh") is not False
        or config["data_snapshot_manifest_path"] != receipt["data_parent"]["manifest_path"]
        or baseline.run_config["ops"]["moex_baseline_update"]["config"]["run_id"] != data["run_id"]
        or source["semantic_compute_contract"]
        != research["parameters"]["semantic_compute_contract"]
    ):
        raise ValueError("recovery CF Data ancestor or numerical contract differs")
    names = CONTINUOUS_FRONT_TABLES
    if set(source["tables"]) != set(research["tables"]) or any(
        ref != (data["tables"][name] if name in data["tables"] else research["tables"][name])
        for name, ref in source["tables"].items()
        if name not in names
    ):
        raise ValueError("recovery CF starting table vector changed")
    for ref in data["tables"].values():
        open_pinned_table(ref)
    outputs = {name: source["tables"][name] for name in names}
    if any(outputs[name]["table_id"] != research["tables"][name]["table_id"] for name in names):
        raise ValueError("recovery CF output table identity changed")
    assert_tables_unchanged(outputs)
    path = _pending_path(run, config, run_id)
    body = path.read_bytes()
    if json.loads(body) != source:
        raise ValueError("recovery CF checkpoint changed during validation")
    return data, {
        "run_id": run_id,
        "owner_ticks": ticks,
        "admission_id": receipt["admission_id"],
        "checkpoint_sha256": hashlib.sha256(body).hexdigest(),
    }


def recorded_cf_outputs(binding, research, checkpoint_data):
    """Open the exact native step receipt bound by the Data recovery owner."""
    run_id = binding["run_id"]
    root = Path(research["parameters"]["materialized_output_dir"])
    registry = snapshot_registry(root, research["dataset_version"], "pit_active_front")
    body = (
        registry / "pending" / (hashlib.sha256(run_id.encode()).hexdigest() + ".json")
    ).read_bytes()
    if hashlib.sha256(body).hexdigest() != binding["checkpoint_sha256"]:
        raise ValueError("recovery CF checkpoint contents changed")
    source = json.loads(body)
    stage = "continuous_front_bars"
    receipt = source["parameters"]["refresh_admission"]
    if (
        source["run_id"] != run_id
        or source["quality_owners"].get(stage) != run_id
        or source["quality_checks"].get(stage) != "PASS"
        or source["expected_current_id"] != research["snapshot_id"]
        or source["parents"] != [checkpoint_data["snapshot_id"]]
        or source["parent_manifest_sha256"] != checkpoint_data["manifest_sha256"]
        or receipt["admission_id"] != binding["admission_id"]
        or receipt["inputs"] != checkpoint_data["tables"]
    ):
        raise ValueError("recovery CF checkpoint lineage changed")
    outputs = {name: source["tables"][name] for name in CONTINUOUS_FRONT_TABLES}
    for name, ref in outputs.items():
        if (
            Path(ref["table_path"]).resolve() != (root / (name + ".delta")).resolve()
            or ref["table_id"] != research["tables"][name]["table_id"]
        ):
            raise ValueError("recovery CF output identity changed")
        open_pinned_table(ref)
    return outputs
