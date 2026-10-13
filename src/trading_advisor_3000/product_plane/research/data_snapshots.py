"""Snapshot checkpoints for the existing data-prep asset graph."""

from __future__ import annotations

import functools
import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.common.data_snapshots import (
    assert_tables_unchanged,
    atomic_json,
    capture_table,
    create_snapshot,
    current_snapshot,
    load_snapshot,
    materialization_snapshot_scope,
    publish_snapshot,
    runtime_provenance,
    save_snapshot,
)
from trading_advisor_3000.product_plane.research.operations import preflight, worker_lease

_STAGE_OUTPUTS: dict[str, tuple[str, ...]] = {}


def snapshot_registry(output_dir: Path, dataset_version: str, contour_id: str) -> Path:
    scope = hashlib.sha256(json.dumps([dataset_version, contour_id]).encode()).hexdigest()[:24]
    return output_dir / ".data_snapshots" / scope


def _configuration(context: Any) -> dict[str, Any]:
    op_context = getattr(context, "op_execution_context", context)
    return dict(op_context.op_config)


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


def _input_tables(
    config: Mapping[str, Any], parent: Mapping[str, Any] | None
) -> dict[str, dict[str, Any]]:
    canonical = Path(config["canonical_output_dir"])
    roots = [canonical]
    raw_root = str(config.get("data_snapshot_raw_output_dir") or "")
    if raw_root:
        roots.append(Path(raw_root))
    elif "canonical" in canonical.parts:
        parts = list(canonical.parts)
        parts[parts.index("canonical")] = "raw"
        roots.append(Path(*parts))
    elif (canonical.parent / "raw").exists():
        roots.append(canonical.parent / "raw")
    paths = [
        path
        for root in roots
        for path in sorted(root.glob("*.delta"))
        if (path / "_delta_log").exists()
    ]
    for key in ("canonical_contract_economics_path", "volume_profile_raw_1m_table_path"):
        text = str(config.get(key) or "")
        if text and (Path(text) / "_delta_log").exists():
            paths.append(Path(text))
    tables: dict[str, dict[str, Any]] = dict(parent["tables"]) if parent else {}
    if parent:
        assert_tables_unchanged(tables)
    for path in paths:
        if path.stem in tables:
            if Path(tables[path.stem]["table_path"]).resolve() != path.resolve():
                raise ValueError(f"snapshot input name collision: {path.stem}")
            continue
        tables[path.stem] = capture_table(path)
    return tables


def _parameters(config: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "universe_id",
        "dataset_name",
        "validation_plan",
        "series_mode",
        "timeframes",
        "base_timeframe",
        "warmup_bars",
        "split_method",
        "dataset_contract_ids",
        "dataset_instrument_ids",
        "changed_windows",
        "continuous_front_policy",
        "indicator_set_version",
        "indicator_profile_version",
        "derived_indicator_set_version",
        "derived_indicator_profile_version",
        "continuous_front_indicator_qc_mode",
        "continuous_front_indicator_sidecar_materialization_mode",
        "volume_profile_tick_size_by_instrument",
        "start_ts",
        "end_ts",
        "dataset_version",
        "canonical_output_dir",
        "registry_root",
        "materialized_output_dir",
        "canonical_contract_economics_path",
        "volume_profile_raw_1m_table_path",
        "data_snapshot_raw_output_dir",
        "data_snapshot_manifest_path",
    )
    result = {key: config[key] for key in keys if key in config}
    if "volume_profile_tick_size_by_instrument" in result:
        result["volume_profile_tick_size_by_instrument"] = {
            str(key).strip(): float(value)
            for key, value in result["volume_profile_tick_size_by_instrument"].items()
            if isinstance(value, (int, float)) and math.isfinite(value) and value > 0
        }
    return json.loads(json.dumps(result, allow_nan=False))


def _initial(context: Any, config: Mapping[str, Any]) -> dict[str, Any]:
    path = _pending_path(context, config)
    parameters = _parameters(config)
    provenance = runtime_provenance()
    if path.exists():
        pending = json.loads(path.read_text(encoding="utf-8"))
        if (
            pending["parameters"] != parameters
            or pending["code_version"] != str(config["code_version"])
            or pending["runtime_provenance"] != provenance
        ):
            raise ValueError("snapshot resume requires the same parameters and code")
        _assert_current_data_parent(config, pending)
        return pending
    op_context = getattr(context, "op_execution_context", context)
    parent_run_id = getattr(getattr(op_context, "dagster_run", None), "parent_run_id", None)
    if parent_run_id:
        source = _pending_path(context, config, parent_run_id)
        if not source.exists():
            raise ValueError("Research resume requires the parent attempt checkpoint")
        pending = json.loads(source.read_text(encoding="utf-8"))
        if (
            pending["parameters"] != parameters
            or pending["code_version"] != str(config["code_version"])
            or pending["runtime_provenance"] != provenance
        ):
            raise ValueError("snapshot resume requires the same parameters and code")
        source_run = op_context.instance.get_run_by_id(parent_run_id)
        if source_run is None or source_run.status.value not in {"FAILURE", "CANCELED"}:
            raise ValueError("Research resume requires a terminal failed/canceled parent")
        successful = {
            step.step_key
            for step in op_context.instance.get_run_step_stats(parent_run_id)
            if step.status.value == "SUCCESS"
        }
        owners = pending.get("quality_owners", {})
        reusable = {
            stage
            for stage, status in pending["quality_checks"].items()
            if status == "PASS"
            and (stage in successful or owners.get(stage) != parent_run_id and stage in owners)
        }
        # Outputs of failed/unexecuted stages may contain partial new commits.
        # They are rewritten by the native failed stage, never reused as inputs.
        reusable_outputs = {name for stage in reusable for name in _STAGE_OUTPUTS.get(stage, ())}
        for stage, outputs in _STAGE_OUTPUTS.items():
            if stage not in reusable:
                for name in outputs:
                    if name not in reusable_outputs:
                        pending["tables"].pop(name, None)
        pending["quality_checks"] = {stage: "PASS" for stage in reusable}
        pending["quality_evidence"] = {
            stage: value
            for stage, value in pending["quality_evidence"].items()
            if stage in reusable
        }
        pending["quality_owners"] = {stage: owners.get(stage, parent_run_id) for stage in reusable}
        _assert_quality_owners(op_context.instance, pending, current_run_id=str(context.run_id))
        _assert_current_data_parent(config, pending)
        assert_tables_unchanged(pending["tables"])
        current = current_snapshot(_registry(config))
        if (current["snapshot_id"] if current else None) != pending["expected_current_id"]:
            raise ValueError("accepted Research changed before resume")
        pending["recovery_lineage"] = [*pending.get("recovery_lineage", ()), parent_run_id]
        pending["run_id"] = str(context.run_id)
        preflight(config)
        atomic_json(path, pending)
        return pending
    preflight(config)
    current = current_snapshot(_registry(config))
    parent_path = str(config.get("data_snapshot_manifest_path") or "")
    parent = (
        load_snapshot(Path(parent_path))
        if parent_path
        else current_snapshot(Path(config["canonical_output_dir"]) / ".data_snapshots")
    )
    if parent_path:
        accepted = current_snapshot(Path(config["canonical_output_dir"]) / ".data_snapshots")
        if not accepted or (accepted["snapshot_id"], accepted["manifest_sha256"]) != (
            parent["snapshot_id"],
            parent["manifest_sha256"],
        ):
            raise ValueError("the explicit Data parent is not the accepted current snapshot")
    tables = _input_tables(config, parent)
    # Partial rebuild jobs also pin pre-existing upstream research materializations.
    for path in sorted(Path(config["materialized_output_dir"]).glob("*.delta")):
        if (
            path.stem
            in {
                "research_datasets",
                "research_instrument_tree",
                "research_bar_views",
                "research_indicator_frames",
                "research_derived_source_frames",
                "research_derived_indicator_frames",
            }
            or path.stem.startswith(("continuous_front_", "cf_indicator_", "indicator_roll_"))
        ) and (path / "_delta_log").exists():
            tables[path.stem] = capture_table(path)
    return {
        "run_id": str(context.run_id),
        "dataset_version": str(config["dataset_version"]),
        "code_version": str(config["code_version"]),
        "parameters": parameters,
        "window": {key: config.get(key, "") for key in ("start_ts", "end_ts")},
        "tables": tables,
        "quality_checks": {},
        "runtime_provenance": provenance,
        "parents": [parent["snapshot_id"]] if parent else [],
        "parent_manifest_sha256": parent["manifest_sha256"] if parent else None,
        "quality_evidence": {},
        "quality_owners": {},
        "expected_current_id": current["snapshot_id"] if current else None,
    }


def _assert_quality_owners(
    instance: Any, pending: Mapping[str, Any], *, current_run_id: str
) -> None:
    for stage, owner in pending.get("quality_owners", {}).items():
        if owner == current_run_id:
            continue
        if not any(
            step.step_key == stage and step.status.value == "SUCCESS"
            for step in instance.get_run_step_stats(owner)
        ):
            raise ValueError(f"Research resume lacks native successful step evidence: {stage}")


def _assert_current_data_parent(config: Mapping[str, Any], pending: Mapping[str, Any]) -> None:
    accepted = current_snapshot(Path(config["canonical_output_dir"]) / ".data_snapshots")
    if not accepted or (
        pending["parents"] != [accepted["snapshot_id"]]
        or pending.get("parent_manifest_sha256") != accepted["manifest_sha256"]
    ):
        raise ValueError("accepted Data parent changed during research calculation")


def snapshot_materialization(output_tables: Sequence[str]) -> Callable:
    """Pin upstream inputs and checkpoint completed outputs after each owning asset."""

    def decorate(function: Callable) -> Callable:
        _STAGE_OUTPUTS[function.__name__] = tuple(output_tables)

        @functools.wraps(function)
        def wrapped(context: Any, *args: Any, **kwargs: Any) -> Any:
            config = _configuration(context)
            if not config.get("data_snapshot_enabled", False):
                op_context = getattr(context, "op_execution_context", context)
                instance = getattr(op_context, "instance", None)
                run = instance.get_run_by_id(str(context.run_id)) if instance else None
                if run is not None and run.parent_run_id:
                    raise ValueError("verified Research resume requires an enabled snapshot owner")
                preflight(config)
                with worker_lease(
                    Path(config["materialized_output_dir"]), str(context.run_id), function.__name__
                ):
                    return function(context, *args, **kwargs)
            pending = _initial(context, config)
            root = Path(config["materialized_output_dir"])
            writable = [root / f"{name}.delta" for name in output_tables]
            inputs = {
                name: ref for name, ref in pending["tables"].items() if name not in output_tables
            }
            assert_tables_unchanged(inputs)
            view = (
                create_snapshot(
                    tables=inputs,
                    required_tables=tuple(inputs),
                    dataset_version=pending["dataset_version"],
                    run_id=pending["run_id"],
                    code_version=pending["code_version"],
                    quality_checks={"inputs": "PASS"},
                )
                if inputs
                else None
            )
            with worker_lease(root, str(context.run_id), function.__name__):
                with materialization_snapshot_scope(view, writable_paths=writable) as reads:
                    result = function(context, *args, **kwargs)
            inputs = reads.plain_tables()
            assert_tables_unchanged(inputs)
            if not isinstance(result, Mapping):
                raise ValueError(f"snapshot stage requires QC result: {function.__name__}")
            if isinstance(result, Mapping):
                admissible_statuses = {"PASS", "PASS-NOOP"}
                if (
                    function.__name__ == "continuous_front_bars"
                    and config.get("series_mode") != "continuous_front"
                ):
                    admissible_statuses.add("SKIP")
                if ("success" in result and result["success"] is not True) or (
                    "status" in result and result["status"] not in admissible_statuses
                ):
                    raise ValueError(f"snapshot stage failed QC: {function.__name__}")
                sidecar = result.get("sidecar_result", {})
                if ("success" in sidecar and sidecar["success"] is not True) or (
                    "status" in sidecar
                    and sidecar["status"]
                    not in (
                        {"PASS", "PASS-NOOP"}
                        if config.get("series_mode") == "continuous_front"
                        else {"PASS", "PASS-NOOP", "SKIPPED"}
                    )
                ):
                    raise ValueError("snapshot sidecar failed QC")
                if (
                    config.get("series_mode") == "continuous_front"
                    and sidecar
                    and sidecar.get("publish_status") != "accepted"
                ):
                    raise ValueError("snapshot requires accepted continuous-front sidecar")
            pending["tables"].update(inputs)
            for name, path in zip(output_tables, writable, strict=True):
                if (path / "_delta_log").exists():
                    pending["tables"][name] = capture_table(path)
            pending["quality_checks"][function.__name__] = "PASS"
            pending.setdefault("quality_owners", {})[function.__name__] = str(context.run_id)
            if isinstance(result, Mapping):
                pending["quality_evidence"][function.__name__] = {
                    key: result[key]
                    for key in ("status", "success", "row_count", "report_path")
                    if key in result
                }
                sidecar = result.get("sidecar_result", {})
                pending["quality_evidence"][function.__name__].update(
                    {
                        key: sidecar[key]
                        for key in (
                            "status",
                            "publish_status",
                            "rule_set_version",
                            "rule_set_hash",
                            "adapter_bundle_hash",
                            "qc_mode",
                            "skipped_qc_groups",
                            "sidecar_refresh_mode",
                        )
                        if key in sidecar
                    }
                )
            atomic_json(_pending_path(context, config), pending)
            return result

        return wrapped

    return decorate


def publish_research_snapshot(context: Any, *, acceptance: Mapping[str, Any]) -> dict[str, Any]:
    config = _configuration(context)
    if not config.get("data_snapshot_enabled", False):
        return {"status": "SKIPPED", "reason": "snapshot_owner_not_enabled"}
    pending = _initial(context, config)
    op_context = getattr(context, "op_execution_context", context)
    _assert_quality_owners(op_context.instance, pending, current_run_id=str(context.run_id))
    _assert_current_data_parent(config, pending)
    if not pending["parents"]:
        raise ValueError("Research publication requires an accepted Data parent snapshot")
    required = [
        "canonical_bars",
        "canonical_session_calendar",
        "canonical_roll_map",
        "research_datasets",
        "research_instrument_tree",
        "research_bar_views",
        "research_indicator_frames",
        "research_derived_indicator_frames",
    ]
    raw = [name for name in pending["tables"] if name.startswith("raw_")]
    if not raw:
        raise ValueError(
            "Data/research snapshot requires raw input lineage; "
            "configure data_snapshot_raw_output_dir"
        )
    required.extend(raw)
    checks = pending["quality_checks"]
    stages = {
        "continuous_front_bars",
        "research_datasets",
        "research_indicator_frames",
        "research_derived_indicator_frames",
        "continuous_front_indicator_acceptance_report",
    }
    if stages - set(checks) or any(checks[name] != "PASS" for name in stages):
        raise ValueError("snapshot requires all data-prep stages from the verified attempt lineage")
    if config.get("series_mode") == "continuous_front":
        status = str(acceptance.get("sidecar_result", {}).get("status", ""))
        checks["continuous_front_acceptance"] = status
        required.extend(
            (
                "continuous_front_bars",
                "continuous_front_roll_events",
                "continuous_front_adjustment_ladder",
                "continuous_front_qc_report",
                "continuous_front_indicator_acceptance_report",
                "cf_indicator_input_frame",
                "indicator_roll_rules",
                "continuous_front_indicator_frames",
                "continuous_front_derived_indicator_frames",
                "continuous_front_indicator_qc_observations",
                "continuous_front_indicator_run_manifest",
            )
        )
    assert_tables_unchanged(pending["tables"])
    if runtime_provenance() != pending["runtime_provenance"]:
        raise ValueError("snapshot code/dependencies changed during calculation")
    manifest = create_snapshot(
        tables=pending["tables"],
        required_tables=required,
        dataset_version=pending["dataset_version"],
        run_id=pending["run_id"],
        code_version=pending["code_version"],
        parameters={
            **pending["parameters"],
            "runtime_provenance": pending["runtime_provenance"],
            "quality_evidence": pending["quality_evidence"],
            "quality_owners": pending.get("quality_owners", {}),
            "recovery_lineage": pending.get("recovery_lineage", []),
        },
        quality_checks=checks,
        window=pending["window"],
        parents=pending["parents"],
    )
    root = _registry(config)
    current = current_snapshot(root)
    if current and current["run_id"] == pending["run_id"]:
        if (
            current["tables"] != pending["tables"]
            or current["quality_checks"] != checks
            or current["parameters"] != manifest["parameters"]
        ):
            raise ValueError("accepted snapshot for this run differs from its checkpoint")
        path = root / "manifests" / f"{current['snapshot_id']}.json"
        return {
            "snapshot_id": current["snapshot_id"],
            "snapshot_manifest_path": path.as_posix(),
            "manifest_sha256": current["manifest_sha256"],
            "table_count": len(current["tables"]),
        }
    path = save_snapshot(root, manifest)
    _assert_current_data_parent(config, pending)
    publish_snapshot(root, path, expected_current_id=pending["expected_current_id"])
    return {
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_manifest_path": path.as_posix(),
        "manifest_sha256": manifest["manifest_sha256"],
        "table_count": len(manifest["tables"]),
    }
