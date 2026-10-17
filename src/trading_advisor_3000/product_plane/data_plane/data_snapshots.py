"""Raw-to-canonical snapshot boundary owned by the existing baseline route."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.common.data_snapshots import (
    assert_tables_unchanged,
    capture_table,
    create_snapshot,
    current_snapshot,
    publish_snapshot,
    runtime_provenance,
    save_snapshot,
)


def capture_data_inputs(paths: Sequence[Path], roots: Sequence[Path]) -> dict[str, dict[str, Any]]:
    candidates = {*paths, *(path for root in roots for path in root.glob("*.delta"))}
    tables = {}
    for path in sorted(candidates):
        if not (path / "_delta_log").exists():
            continue
        if (
            path.stem in tables
            and Path(tables[path.stem]["table_path"]).resolve() != path.resolve()
        ):
            raise ValueError(f"Data snapshot table name collision: {path.stem}")
        tables[path.stem] = capture_table(path)
    return tables


def data_input_snapshot(*, tables: Mapping, run_id: str) -> dict[str, Any]:
    return create_snapshot(
        tables=tables,
        required_tables=tuple(tables),
        dataset_version="moex-baseline",
        run_id=run_id,
        code_version="moex-baseline-update",
        quality_checks={"input_binding": "PASS"},
    )


def _downstream_scope(*, report: Mapping, tables: Mapping) -> dict:
    """Bind each producer's dirty range only to its exact published outputs."""
    cf = report.get("cf_catch_up") or {}
    windows = [dict(window) for window in cf.get("windows", [])]
    covered = {}
    economics = report.get("economics_refresh") or {}
    economics_scope = economics.get("downstream_scope") or {}
    sidecar = report.get("canonical_report", {}).get("sidecar_refresh")
    sidecar_scope = sidecar.get("dependency_scope", {}) if isinstance(sidecar, Mapping) else {}
    for scope, bindings, names in (
        (
            economics_scope,
            economics.get("output_references", {}),
            {
                "canonical_contract_economics",
                "canonical_fx_rates",
                "canonical_asset_risk_parameters",
            },
        ),
        (
            sidecar_scope,
            sidecar_scope.get("output_delta_versions", {}),
            {"canonical_session_calendar", "canonical_roll_map"},
        ),
    ):
        if scope.get("version") != 1 or scope.get("full_refresh"):
            continue
        for name in names:
            reference, actual = bindings.get(name), tables.get(name)
            if (
                reference
                and actual
                and all(reference[key] == actual[key] for key in ("table_id", "delta_version"))
            ):
                covered[name] = {key: actual[key] for key in ("table_id", "delta_version")}
        for window in scope.get("windows", []):
            for timeframe in (
                [window["timeframe"]] if "timeframe" in window else cf.get("target_timeframes", [])
            ):
                windows.append({**window, "timeframe": timeframe})
    # OHLC/provenance scopes come from the canonical owner's raw window set,
    # with bindings captured by that same publisher, never from a calendar range.
    if cf.get("windows"):
        bindings = sidecar_scope.get("output_delta_versions", {})
        for name in ("canonical_bars", "canonical_bar_provenance"):
            reference, actual = bindings.get(name), tables.get(name)
            if (
                reference
                and actual
                and all(reference[key] == actual[key] for key in ("table_id", "delta_version"))
            ):
                covered[name] = {key: actual[key] for key in ("table_id", "delta_version")}
        for name, reference in report.get("windowed_input_references", {}).items():
            actual = tables.get(name)
            if actual and all(
                reference[key] == actual[key] for key in ("table_id", "delta_version")
            ):
                covered[name] = {key: actual[key] for key in ("table_id", "delta_version")}
    return {
        "version": 1,
        "full_refresh": False,
        "covered_tables": covered,
        "windows": windows,
        "effective_intervals": economics_scope.get("effective_intervals", []),
    }


def publish_data_snapshot(
    *,
    inputs: Mapping,
    canonical_paths: Sequence[Path],
    canonical_roots: Sequence[Path],
    report: Mapping[str, Any],
    expected_current_id: str | None,
    provenance: Mapping,
) -> dict[str, Any]:
    if report["status"] != "PASS" or report["publish_decision"] != "publish":
        raise ValueError("Data snapshot requires accepted baseline QC")
    assert_tables_unchanged(inputs)
    if runtime_provenance() != provenance:
        raise ValueError("Data snapshot code/dependencies changed during calculation")
    tables = {**inputs, **capture_data_inputs(canonical_paths, canonical_roots)}
    for name, reference in report["economics_refresh"].get("output_references", {}).items():
        actual = tables.get(name)
        if actual is None or any(
            actual[key] != reference[key] for key in ("table_id", "delta_version", "table_path")
        ):
            raise ValueError(f"Economics publication output changed: {name}")
    required = [path.stem for path in canonical_paths]
    required.extend(name for name in inputs if name.startswith("raw_"))
    if not any(name.startswith("raw_") for name in inputs):
        raise ValueError("Data snapshot requires raw lineage")
    checks = {
        "baseline": report["status"],
        "canonical": report["canonical_report"]["status"],
        "economics": report["economics_refresh"]["status"],
    }
    manifest = create_snapshot(
        tables=tables,
        required_tables=required,
        dataset_version="moex-baseline",
        run_id=str(report["run_id"]),
        code_version="moex-baseline-update",
        quality_checks=checks,
        parents=[expected_current_id] if expected_current_id else [],
        parameters={
            "runtime_provenance": dict(provenance),
            "refresh_window_days": report["refresh_window_days"],
            "changed_windows": report["cf_catch_up"]["source_changed_windows"],
            "downstream_scope": _downstream_scope(report=report, tables=tables),
            "economics_mode": report["economics_mode"],
            "economics_stage_inputs": dict(
                report["economics_refresh"].get("snapshot_inputs") or {}
            ),
        },
    )
    root = canonical_paths[0].parent / ".data_snapshots"
    path = save_snapshot(root, manifest)
    publish_snapshot(root, path, expected_current_id=expected_current_id)
    return {
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_manifest_path": path.as_posix(),
        "manifest_sha256": manifest["manifest_sha256"],
        "table_count": len(tables),
    }


def accepted_data_snapshot_id(canonical_root: Path) -> str | None:
    current = current_snapshot(canonical_root / ".data_snapshots")
    return current["snapshot_id"] if current else None
