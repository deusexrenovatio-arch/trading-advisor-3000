"""Explicit native rebuild migration; never reinterpret incompatible Research."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path

from trading_advisor_3000.product_plane.common.data_snapshots import (
    atomic_json,
    current_snapshot,
    load_snapshot,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import (
    load_schema,
    validate_schema,
)

from .compute_contract import semantic_compute_contract, validate_contract
from .data_snapshots import runtime_provenance
from .hash_compatibility import REQUIRED_QUALITY
from .semantic_fingerprint import digest

_DOMAIN_KEYS = {
    "dataset_version",
    "dataset_name",
    "universe_id",
    "timeframes",
    "base_timeframe",
    "dataset_instrument_ids",
    "start_ts",
    "end_ts",
    "warmup_bars",
    "continuous_front_policy",
    "indicator_set_version",
    "indicator_profile_version",
    "derived_indicator_set_version",
    "derived_indicator_profile_version",
    "volume_profile_tick_size_by_instrument",
}
_REQUIRED_DOMAIN = {"dataset_version", "timeframes", "dataset_instrument_ids", "start_ts", "end_ts"}
_SCHEMA = Path(__file__).parents[1] / "contracts/schemas/research_contract_migration.v1.json"


def _json(value):
    return json.loads(json.dumps(value, allow_nan=False))


def _accepted(path: Path) -> dict:
    manifest = load_snapshot(path)
    accepted = current_snapshot(path.parent.parent)
    if accepted is None or any(
        accepted[key] != manifest[key] for key in ("snapshot_id", "manifest_sha256")
    ):
        raise ValueError("migration source must be the accepted current manifest")
    return manifest


def _isolated(target: Path, sources) -> None:
    for source in sources:
        source = Path(source).resolve()
        if target == source or target in source.parents or source in target.parents:
            raise ValueError(f"migration target overlaps source: {source}")
    if target.exists():
        raise ValueError("migration requires a fresh nonexistent target; plans cannot be reused")


def plan_contract_migration(
    *, accepted_research_manifest_path, data_manifest_path, target_root, domain: Mapping
) -> dict:
    """Read-only plan binding a bounded explicit rebuild to immutable inputs."""
    from trading_advisor_3000.dagster_defs.research_assets import (
        build_research_data_prep_run_config,
    )

    source_path = Path(accepted_research_manifest_path).resolve()
    data_path = Path(data_manifest_path).resolve()
    target = Path(target_root).resolve()
    source, data = _accepted(source_path), _accepted(data_path)
    if source["parameters"].get("series_mode") != "continuous_front":
        raise ValueError("migration supports the continuous-front Research contract")
    if not REQUIRED_QUALITY.issubset(source["quality_checks"]) or any(
        source["quality_checks"][key] != "PASS" for key in REQUIRED_QUALITY
    ):
        raise ValueError("migration source requires accepted data-prep quality")
    domain = _json(dict(domain))
    if set(domain) - _DOMAIN_KEYS or _REQUIRED_DOMAIN - set(domain):
        raise ValueError(
            "migration domain must explicitly declare bounds and cannot override owners"
        )
    if any(not domain[key] for key in _REQUIRED_DOMAIN):
        raise ValueError("migration domain must declare nonempty instruments/timeframes/bounds")
    if not set(domain["timeframes"]) <= {"15m", "1h", "4h", "1d"}:
        raise ValueError("migration domain timeframe is outside the governed profile")
    from .refresh_scope import _stamp

    if _stamp(domain["start_ts"]) >= _stamp(domain["end_ts"]):
        raise ValueError("migration domain requires ordered time bounds")
    if data_path.parent.name != "manifests" or data_path.parent.parent.name != ".data_snapshots":
        raise ValueError("migration Data manifest must use the canonical snapshot registry")
    canonical = data_path.parents[2]
    raw = data["tables"].get("raw_moex_history")
    if raw is None:
        raise ValueError("migration requires pinned raw lineage and volume-profile input")
    sources = [source_path.parent.parent, canonical, Path(raw["table_path"]).parent]
    sources.extend(
        ref["table_path"] for manifest in (source, data) for ref in manifest["tables"].values()
    )
    sources.extend(
        source["parameters"][key]
        for key in ("materialized_output_dir", "registry_root", "canonical_output_dir")
        if source["parameters"].get(key)
    )
    _isolated(target, sources)
    run_config = build_research_data_prep_run_config(
        operation="rebuild",
        canonical_output_dir=canonical,
        materialized_output_dir=target / "research/gold/current",
        results_output_dir=target / "results",
        data_snapshot_manifest_path=data_path,
        data_snapshot_raw_output_dir=Path(raw["table_path"]).parent,
        volume_profile_raw_1m_table_path=raw["table_path"],
        series_mode="continuous_front",
        campaign_run_id="research-contract-migration",
        code_version="research-contract-migration",
        start_strategy_cascade=False,
        **domain,
    )
    config = run_config["ops"]["continuous_front_bars"]["config"]
    for key in ("materialized_output_dir", "results_output_dir", "registry_root"):
        if not Path(config[key]).resolve().is_relative_to(target):
            raise ValueError(f"migration output escapes isolated root: {key}")
    from .operations import preflight

    preflight(config)
    old = validate_contract(source["parameters"].get("semantic_compute_contract"))
    current = validate_contract(semantic_compute_contract(config))
    changed = sorted(key for key in current if key != "digest" and old.get(key) != current[key])
    if not changed:
        raise ValueError("Research compute contract is already current; migration is unnecessary")
    payload = {
        "schema_version": 1,
        "request": {
            "accepted_research_manifest_path": source_path.as_posix(),
            "data_manifest_path": data_path.as_posix(),
            "target_root": target.as_posix(),
            "domain": domain,
        },
        "source_snapshot_id": source["snapshot_id"],
        "source_manifest_sha256": source["manifest_sha256"],
        "target_compute_digest": current["digest"],
        "target_runtime_digest": digest(runtime_provenance()),
        "changed_components": changed,
        "run_config": _json(run_config),
    }
    plan = {**payload, "plan_id": digest(payload)}
    validate_schema(load_schema(_SCHEMA), plan)
    return plan


def apply_contract_migration(plan: Mapping) -> dict:
    """Consume a fresh plan via existing complete native assets and publisher."""
    from dagster import DagsterInstance, materialize

    from trading_advisor_3000.dagster_defs.research_assets import (
        RESEARCH_ASSETS,
        RESEARCH_DATA_PREP_JOB_ASSETS,
    )

    plan = _json(dict(plan))
    validate_schema(load_schema(_SCHEMA), plan)
    if plan["plan_id"] != digest({k: v for k, v in plan.items() if k != "plan_id"}):
        raise ValueError("migration plan content changed")
    if plan_contract_migration(**plan["request"]) != plan:
        raise ValueError("migration source, target contract, domain or native pins changed")
    # Claim a new root only after all validation; a racing/consumed plan cannot
    # enter a second writer. Partial failures retain their isolated diagnostics.
    root = Path(plan["request"]["target_root"])
    root.mkdir(parents=True, exist_ok=False)
    atomic_json(root / "migration-plan.json", plan, immutable=True)
    runtime = root / "runtime"
    runtime.mkdir()
    with DagsterInstance.local_temp(str(runtime)) as instance:
        result = materialize(
            assets=list(RESEARCH_ASSETS),
            selection=list(RESEARCH_DATA_PREP_JOB_ASSETS),
            run_config=plan["run_config"],
            instance=instance,
            raise_on_error=True,
        )
        if not result.success:
            raise ValueError("native Research migration did not succeed")
        published = result.output_for_node("research_data_snapshot")
    manifest_path = Path(published["snapshot_manifest_path"])
    manifest = _accepted(manifest_path)
    if (
        manifest["parameters"]["semantic_compute_contract"]["digest"]
        != plan["target_compute_digest"]
    ):
        raise ValueError("migration publication differs from the planned target contract")
    _accepted(Path(plan["request"]["accepted_research_manifest_path"]))
    receipt = {
        "status": "PASS",
        "scope": "isolated_verification",
        "plan_id": plan["plan_id"],
        "source_snapshot_id": plan["source_snapshot_id"],
        "data_snapshot": published,
        "selected_assets": list(RESEARCH_DATA_PREP_JOB_ASSETS),
    }
    atomic_json(root / "migration-receipt.json", receipt, immutable=True)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--request", type=Path, help="Plan from a JSON request; write nothing")
    operation.add_argument("--apply-plan", type=Path, help="Apply a previously exported plan")
    args = parser.parse_args()
    path = args.request or args.apply_plan
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    result = (
        plan_contract_migration(**payload) if args.request else apply_contract_migration(payload)
    )
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
