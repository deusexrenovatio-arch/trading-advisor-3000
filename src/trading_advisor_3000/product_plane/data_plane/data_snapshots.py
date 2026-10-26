"""Raw-to-canonical snapshot boundary owned by the existing baseline route."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.common.data_snapshots import (
    _recovery_digest,
    _recovery_identity,
    _recovery_scope,
    assert_tables_unchanged,
    capture_table,
    create_snapshot,
    current_snapshot,
    load_snapshot,
    publish_snapshot,
    runtime_provenance,
    save_snapshot,
    validate_recovery_bridge,
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


def _downstream_scope(
    *, report: Mapping, tables: Mapping, previous_tables: Mapping | None = None
) -> dict:
    """Bind each producer's dirty range only to its exact published outputs."""
    if report.get("unproven_starting_references"):
        return {
            "version": 2,
            "full_refresh": True,
            "covered_tables": {},
            "windows": [],
            "effective_intervals": [],
            "reason": "unproven_starting_references",
            "unproven_tables": list(report["unproven_starting_references"]),
        }
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
    raw_scope = report.get("raw_ingest_scope") or {}
    raw_name = "raw_moex_history"
    before, actual = (previous_tables or {}).get(raw_name), tables.get(raw_name)
    if before and actual and before != actual:
        original, final = raw_scope.get("input_reference"), raw_scope.get("output_reference")
        if not (
            type(raw_scope.get("version")) is int
            and raw_scope.get("version") == 1
            and raw_scope.get("proven") is True
            and original == before
            and final == actual
        ):
            covered.pop(raw_name, None)
    protocol = report.get("canonical_report", {}).get("publish_protocol") or {}
    pre_versions, target_paths = (
        protocol.get("pre_publish_versions", {}),
        protocol.get("target_paths", {}),
    )
    for name in (
        "canonical_bars",
        "canonical_bar_provenance",
        "canonical_session_calendar",
        "canonical_roll_map",
    ):
        before, actual = (previous_tables or {}).get(name), tables.get(name)
        if before and actual and before != actual:
            proof = protocol.get("commit_owner_verified", {}).get(name) or {}
            if (
                not isinstance(proof, Mapping)
                or proof.get("verified") is not True
                or any(
                    proof.get(key) != actual.get(key)
                    for key in ("table_path", "table_id", "delta_version")
                )
                or pre_versions.get(name) != before["delta_version"]
                or Path(str(target_paths.get(name) or "")).resolve()
                != Path(before["table_path"]).resolve()
                or actual["table_id"] != before["table_id"]
            ):
                covered.pop(name, None)
    oi = report.get("oi_support_scope") or {}
    reference = oi.get("output_reference") or {}
    name = Path(str(reference.get("table_path") or "")).stem
    actual = tables.get(name)
    before = (previous_tables or {}).get(name)
    original = oi.get("input_reference") or {}
    prior_bound = before and all(
        before.get(key) == original.get(key) for key in ("table_path", "table_id", "delta_version")
    )
    if (
        type(oi.get("version")) is int
        and oi.get("version") == 1
        and oi.get("scope_proven") is True
        and prior_bound
        and oi.get("changed_keys")
        and oi.get("windows")
        and actual
        and all(
            reference.get(key) == actual.get(key)
            for key in ("table_path", "table_id", "delta_version")
        )
    ):
        covered[name] = {key: actual[key] for key in ("table_id", "delta_version")}
    return {
        "version": 2,
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
    root = canonical_paths[0].parent / ".data_snapshots"
    previous_tables = (
        load_snapshot(root / "manifests" / f"{expected_current_id}.json")["tables"]
        if expected_current_id
        else {}
    )
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
            "downstream_scope": _downstream_scope(
                report=report, tables=tables, previous_tables=previous_tables
            ),
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


def recover_data_scope(
    *,
    canonical_root,
    research_root,
    not_before,
    expected_data_id,
    expected_research_id,
    publish=False,
    spark_session_factory=None,
):
    """Explicit audit; optional metadata-only publication by the existing owner.

    Does not accept a caller-supplied report. Publication always runs the native
    producer itself. Data CAS is single-registry; a changed Research binding
    leaves any already published bridge unusable, never a different permission.
    """
    from uuid import uuid4

    from trading_advisor_3000.spark_jobs.snapshot_diff_job import compare_snapshot_tables, iso

    root = Path(canonical_root) / ".data_snapshots"
    data, research = current_snapshot(root), current_snapshot(Path(research_root))
    if (
        not data
        or not research
        or data["snapshot_id"] != expected_data_id
        or research["snapshot_id"] != expected_research_id
        or len(research["parents"]) != 1
    ):
        raise ValueError("recovery accepted parents changed")
    if any(value not in {"PASS", "PASS-NOOP"} for value in data["quality_checks"].values()) or any(
        value != "PASS" for value in research["quality_checks"].values()
    ):
        raise ValueError("recovery requires accepted QC")
    source = load_snapshot(root / "manifests" / f"{research['parents'][0]}.json")
    if any(research["tables"].get(name) != ref for name, ref in source["tables"].items()):
        raise ValueError("recovery Research inputs differ from its exact Data anchor")
    outputs = {
        name: ref for name, ref in research["tables"].items() if name not in source["tables"]
    }
    assert_tables_unchanged(data["tables"])
    assert_tables_unchanged(outputs)
    provenance = runtime_provenance()
    native = compare_snapshot_tables(
        source_tables=source["tables"],
        target_tables=data["tables"],
        timeframes=research["parameters"]["timeframes"],
        not_before=not_before,
        spark_session_factory=spark_session_factory,
    )
    report = {
        "schema_version": 1,
        "source": _recovery_identity(source),
        "target": _recovery_identity(data),
        "research": _recovery_identity(research),
        "not_before": iso(not_before),
        "timeframes": list(research["parameters"]["timeframes"]),
        **native,
        "runtime_provenance": provenance,
    }
    report["report_sha256"] = _recovery_digest(report)
    manifest = create_snapshot(
        tables=data["tables"],
        required_tables=data["required_tables"],
        dataset_version=data["dataset_version"],
        run_id="scope-recovery-" + uuid4().hex,
        code_version="metadata-only-scope-recovery.v1",
        quality_checks={**data["quality_checks"], "scope_recovery": "PASS"},
        parents=[data["snapshot_id"]],
        parameters={
            **data["parameters"],
            "downstream_scope": _recovery_scope(report, data["tables"]),
        },
        window=data["window"],
    )
    validate_recovery_bridge(manifest, root=root, research=research)
    assert_tables_unchanged(data["tables"])
    assert_tables_unchanged(outputs)
    if (
        runtime_provenance() != provenance
        or _recovery_identity(current_snapshot(root)) != _recovery_identity(data)
        or _recovery_identity(current_snapshot(Path(research_root))) != _recovery_identity(research)
    ):
        raise ValueError("recovery source or accepted parents changed during audit")
    if publish:
        path = save_snapshot(root, manifest)
        publish_snapshot(root, path, expected_current_id=data["snapshot_id"])
        if _recovery_identity(current_snapshot(Path(research_root))) != _recovery_identity(
            research
        ):
            raise ValueError("Research changed after Data CAS; recovery bridge is unusable")
    return {"published": publish, "report": report, "manifest": manifest}


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Data snapshot owner operations")
    commands = parser.add_subparsers(dest="operation", required=True)
    recover = commands.add_parser(
        "recover-scope", help="explicit native audit; no numerical materialization"
    )
    recover.add_argument("--canonical-root", type=Path, required=True)
    recover.add_argument("--research-root", type=Path, required=True)
    recover.add_argument("--not-before", required=True)
    recover.add_argument("--expected-data-id", required=True)
    recover.add_argument("--expected-research-id", required=True)
    recover.add_argument(
        "--publish",
        action="store_true",
        help="publish a metadata-only Data bridge after the native audit",
    )
    recover.add_argument("--report-path", type=Path)
    args = parser.parse_args()
    result = recover_data_scope(
        canonical_root=args.canonical_root,
        research_root=args.research_root,
        not_before=args.not_before,
        expected_data_id=args.expected_data_id,
        expected_research_id=args.expected_research_id,
        publish=args.publish,
    )
    if args.report_path:
        from trading_advisor_3000.product_plane.common.data_snapshots import atomic_json

        atomic_json(args.report_path, result)
    report = result["report"]
    print(
        json.dumps(
            {
                "published": result["published"],
                "snapshot_id": result["manifest"]["snapshot_id"],
                "report_sha256": report["report_sha256"],
                "windows": len(report["windows"]),
                "preserved_prefix_rows": report["preserved_prefix_rows"],
                "start_ts": min((window["start_ts"] for window in report["windows"]), default=None),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
