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
        parameters={
            "runtime_provenance": dict(provenance),
            "refresh_window_days": report["refresh_window_days"],
            "changed_windows": report["cf_catch_up"]["source_changed_windows"],
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
