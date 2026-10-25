"""Protected observation/signing of the existing scheduled producer-consumer route."""

from __future__ import annotations

# ruff: noqa: E402
import argparse
import importlib.util
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
if sys.argv[1:2] != ["consumer"]:
    sys.path.insert(0, str(ROOT / "src"))
from build_staging_candidate import verify_deployed_files


@lru_cache(maxsize=1)
def _contracts():
    path = ROOT / "src/trading_advisor_3000/product_plane/common/staging_contracts.py"
    spec = importlib.util.spec_from_file_location("protected_staging_contracts", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def digest(value):
    return _contracts().digest(value)


def load_plan(path):
    return _contracts().load_plan(path)


def validate_document(kind, value):
    return _contracts().validate_document(kind, value)


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


def observed_mutations(before: dict, after: dict) -> list[dict]:
    """Report native pinned storage changes separately from producer counters."""
    result = []
    for name, current in after["tables"].items():
        previous = before["tables"].get(name)
        if (
            previous is not None
            and current["table_id"] == previous["table_id"]
            and current["delta_version"] > previous["delta_version"]
            and current.get("active_files_sha256")
            and current["active_files_sha256"] != previous.get("active_files_sha256")
        ):
            result.append(
                {
                    "table": name,
                    "from_version": previous["delta_version"],
                    "to_version": current["delta_version"],
                    "before_files_sha256": previous.get("active_files_sha256"),
                    "after_files_sha256": current["active_files_sha256"],
                    "before_rows": previous.get("row_count"),
                    "after_rows": current.get("row_count"),
                }
            )
    return result


def candidate_consumer(
    plan_path: Path,
    manifest_path: Path,
    deployed_root: Path = Path("/workspace"),
    *,
    verify_compute_contract: bool = False,
) -> dict:
    """This fresh process deliberately imports the candidate's public consumer."""
    sys.path.insert(0, str(deployed_root / "src"))
    from trading_advisor_3000.product_plane.common.data_snapshots import (
        load_snapshot,
        snapshot_read_scope,
    )
    from trading_advisor_3000.product_plane.research.io import (
        ResearchSliceRequest,
        load_backtest_frames,
    )

    plan = load_plan(plan_path)
    if plan.get("required_scenarios"):
        raise ValueError("Scenario-specific proof is not implemented; acceptance remains blocked")
    manifest = load_snapshot(manifest_path)
    compute = None
    if verify_compute_contract:
        from trading_advisor_3000.product_plane.research.compute_contract import (
            compare_contracts,
            semantic_compute_contract,
        )

        desired = semantic_compute_contract(manifest["parameters"])
        if not compare_contracts(manifest["parameters"].get("semantic_compute_contract"), desired):
            raise ValueError("Unknown/changed compute contract requires explicit transition proof")
        compute = {"status": "PASS", "digest": desired["digest"]}
    request = ResearchSliceRequest(**plan["consumer"])
    if not request.instrument_ids or not request.timeframe or not request.price_columns:
        raise ValueError("Consumer proof requires an explicit bounded native slice")
    output = Path(plan["data_root"]) / "research/gold/current"
    with snapshot_read_scope(manifest):
        frames, _, hit = load_backtest_frames(
            dataset_output_dir=output,
            indicator_output_dir=output,
            derived_indicator_output_dir=output,
            request=request,
            cache=None,
        )
    rows = sum(len(frame.frame) for frame in frames)
    if hit or rows < 1:
        raise ValueError("Normal candidate consumer returned no fresh rows")
    requested = (*request.price_columns, *request.indicator_columns, *request.derived_columns)
    for column in requested:
        if not any(column in item.frame and item.frame[column].notna().any() for item in frames):
            raise ValueError(f"Consumer did not read requested populated column: {column}")
    return {
        "status": "PASS",
        "entrypoint": "research.io.load_backtest_frames",
        "cache_hit": False,
        "snapshot_id": manifest["snapshot_id"],
        "rows": rows,
        "process_id": str(os.getpid()),
        "read_at_utc": datetime.now(UTC).isoformat(),
        "digest": digest(
            [item.frame.to_json(orient="split", date_format="iso") for item in frames]
        ),
        "compute_contract_preflight": compute,
    }


def preflight(plan_path: Path, deployed_root: Path, execution_manifest: Path) -> dict:
    """Read the pinned seed through the actual consumer before allocating compute."""
    from trading_advisor_3000.product_plane.common.data_snapshots import (
        assert_tables_unchanged,
        current_snapshot,
        load_snapshot,
    )
    from trading_advisor_3000.product_plane.common.staging_observation import readonly_home_snapshot
    from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_registry

    plan = load_plan(plan_path)
    if plan.get("required_scenarios"):
        raise ValueError("Scenario-specific proof is not implemented; no compute is authorized")
    code = verify_deployed_files(deployed_root, json.loads(execution_manifest.read_text("utf-8")))
    if code != plan["candidate"]["executable_sha256"]:
        raise ValueError("Actual mounted executable differs from candidate")
    source_path = Path(plan.get("source", {}).get("research_manifest_path", ""))
    source = load_snapshot(source_path)
    if any(plan["source"].get(key) != source[key] for key in ("snapshot_id", "manifest_sha256")):
        raise ValueError("Source snapshot differs from protected seed identity")
    if plan.get("mode", "candidate") == "candidate":
        root = Path(plan["data_root"])
        registry = snapshot_registry(
            root / "research/gold/current",
            plan["consumer"]["dataset_version"],
            plan["consumer"].get("contour_id", "native_tradable"),
        )
        current = current_snapshot(registry)
        data = current_snapshot(root / "canonical/moex/baseline-4y-current/.data_snapshots")
        if (
            current is None
            or data is None
            or any(current[k] != source[k] for k in ("snapshot_id", "manifest_sha256"))
            or source["parents"] != [data["snapshot_id"]]
            or any(source["tables"].get(name) != ref for name, ref in data["tables"].items())
        ):
            raise ValueError("Candidate inputs differ from the protected accepted baseline")
        assert_tables_unchanged(source["tables"])
    # Prove observation capacity before spending the only native attempt.
    with readonly_home_snapshot(Path(os.environ["DAGSTER_HOME"])) as (_, metadata):
        pass
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            str(Path(__file__).resolve()),
            "consumer",
            "--plan",
            str(plan_path),
            "--manifest",
            str(source_path),
            "--deployed-root",
            str(deployed_root),
            "--verify-compute-contract",
        ],
        cwd=deployed_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if result.returncode:
        raise ValueError(f"Fresh source consumer preflight failed: {result.stderr[-1500:]}")
    return {
        "status": "PREFLIGHT_PASS",
        "plan_sha256": digest(plan),
        "source": {key: source[key] for key in ("snapshot_id", "manifest_sha256")},
        "consumer": json.loads(result.stdout),
        "compute_attempts": 0,
        "metadata_observation": metadata,
    }


def reference_cycle(plan_path: Path, root_run_id: str, deployed_root: Path) -> dict:
    from dagster import DagsterInstance

    from trading_advisor_3000.product_plane.common.data_snapshots import current_snapshot
    from trading_advisor_3000.product_plane.common.staging_observation import readonly_home_snapshot
    from trading_advisor_3000.product_plane.interfaces.staging_observation import (
        observe_reference_cycle,
    )
    from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_registry

    plan = load_plan(plan_path)
    if not root_run_id:
        raise ValueError("Reference observation requires the actual recorded root ID")
    with (
        readonly_home_snapshot(Path(os.environ["DAGSTER_HOME"]), run_ids=[root_run_id]) as (
            home,
            metadata,
        ),
        DagsterInstance.from_config(str(home)) as instance,
    ):
        result = observe_reference_cycle(plan, root_run_id, instance, plan["data_root"])
    result["metadata_observation"] = metadata
    if result["status"] in {"PASS", "PASS-NOOP"}:
        registry = snapshot_registry(
            Path(plan["data_root"]) / "research/gold/current",
            plan["consumer"]["dataset_version"],
            plan["consumer"].get("contour_id", "native_tradable"),
        )
        accepted = result["outputs"]["research"]
        path = registry / "manifests" / f"{accepted['snapshot_id']}.json"
        consumer = subprocess.run(
            [
                sys.executable,
                "-I",
                str(Path(__file__).resolve()),
                "consumer",
                "--plan",
                str(plan_path),
                "--manifest",
                str(path),
                "--deployed-root",
                str(deployed_root),
            ],
            cwd=deployed_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=180,
        )
        if consumer.returncode:
            raise ValueError("Fresh reference candidate consumer failed")
        result["consumer"] = json.loads(consumer.stdout)
        current = current_snapshot(registry)
        if current is None or any(
            current[k] != accepted[k] for k in ("snapshot_id", "manifest_sha256")
        ):
            raise ValueError("Reference publication changed during fresh consumer read")
    return result


def observe_reattest(
    plan_path: Path, original_path: Path, deployed_root: Path, execution_manifest: Path
) -> dict:
    """Fresh applicability readback of one immutable original computation."""
    from dagster import DagsterInstance

    from trading_advisor_3000.product_plane.common.data_snapshots import (
        current_snapshot,
        load_snapshot,
        open_pinned_table,
    )
    from trading_advisor_3000.product_plane.common.staging_observation import readonly_home_snapshot
    from trading_advisor_3000.product_plane.common.staging_route_proof import (
        verify_acceptance_route,
    )
    from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_registry

    plan = load_plan(plan_path)
    signed = json.loads(original_path.read_text("utf-8"))
    keys = json.loads((plan_path.parent / "operator-keyring.json").read_text("utf-8"))["keys"]
    original = verify_acceptance_route(
        signed,
        keys,
        now=datetime.fromisoformat(signed["payload"]["issued_at_utc"].replace("Z", "+00:00")),
    )
    if "compute_origin" in original or original["plan_sha256"] != digest(plan):
        raise ValueError("Reattestation requires the unchanged original computation plan")
    actual_code = verify_deployed_files(
        deployed_root, json.loads(execution_manifest.read_text("utf-8"))
    )
    if actual_code != original["candidate"]["executable_sha256"]:
        raise ValueError("Original compute artifact changed")
    source = load_snapshot(Path(plan["source"]["research_manifest_path"]))
    if any(source[k] != original["source"][k] for k in ("snapshot_id", "manifest_sha256")):
        raise ValueError("Original source identity changed")
    root = Path(plan["data_root"])
    registry = snapshot_registry(
        root / "research/gold/current",
        plan["consumer"]["dataset_version"],
        plan["consumer"].get("contour_id", "native_tradable"),
    )
    research = current_snapshot(registry)
    data = current_snapshot(root / "canonical/moex/baseline-4y-current/.data_snapshots")
    if (
        research is None
        or data is None
        or any(research[k] != original["outputs"][k] for k in ("snapshot_id", "manifest_sha256"))
        or data["snapshot_id"] != original["outputs"]["data_snapshot_id"]
        or research["parents"] != [data["snapshot_id"]]
        or any(research["tables"].get(name) != ref for name, ref in data["tables"].items())
        or not data["quality_checks"]
        or any(value != "PASS" for value in data["quality_checks"].values())
        or digest(research["tables"]) != original["outputs"]["pins_sha256"]
        or research["quality_checks"] != original["qc"]["checks"]
    ):
        raise ValueError("Original accepted output/pins/quality changed")
    for manifest in (source, data, research):
        for reference in manifest["tables"].values():
            open_pinned_table(reference)
    ids = [original["runs"][job]["run_id"] for job in JOBS]
    with (
        readonly_home_snapshot(Path(os.environ["DAGSTER_HOME"]), run_ids=ids) as (home, metadata),
        DagsterInstance.from_config(str(home)) as instance,
    ):
        for job, identifier in zip(JOBS, ids, strict=True):
            run = instance.get_run_by_id(identifier)
            if (
                run is None
                or run.job_name != job
                or run.status.value != "SUCCESS"
                or run.tags.get("ta3000/staging_plan_sha256") != digest(plan)
                or run.tags.get("ta3000/candidate_head_sha") != original["candidate"]["head_sha"]
            ):
                raise ValueError("Original completed native graph changed")
    path = registry / "manifests" / f"{research['snapshot_id']}.json"
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            str(Path(__file__).resolve()),
            "consumer",
            "--plan",
            str(plan_path),
            "--manifest",
            str(path),
            "--deployed-root",
            str(deployed_root),
        ],
        cwd=deployed_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if result.returncode:
        raise ValueError("Fresh reattestation candidate consumer failed")
    current_research = current_snapshot(registry)
    current_data = current_snapshot(root / "canonical/moex/baseline-4y-current/.data_snapshots")
    if any(
        current is None
        or any(current[key] != before[key] for key in ("snapshot_id", "manifest_sha256"))
        for current, before in ((current_research, research), (current_data, data))
    ):
        raise ValueError("Original publication changed during fresh consumer read")
    return {
        "status": "APPLICABLE",
        "original_receipt_sha256": digest(signed),
        "source": original["source"],
        "outputs": original["outputs"],
        "consumer": json.loads(result.stdout),
        "metadata_observation": metadata,
        "compute_attempts": 0,
    }


def observe(plan_path: Path, deployed_root: Path, execution_manifest: Path) -> dict:
    from dagster import DagsterEventType, DagsterInstance, RunsFilter

    from trading_advisor_3000.product_plane.common.data_snapshots import (
        current_snapshot,
        load_snapshot,
        open_pinned_table,
    )
    from trading_advisor_3000.product_plane.common.staging_execution import _reference_tick
    from trading_advisor_3000.product_plane.common.staging_observation import readonly_home_snapshot
    from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_registry

    plan = load_plan(plan_path)
    if plan.get("required_scenarios"):
        raise ValueError("Scenario-specific proof is not implemented; no acceptance can be issued")
    if (
        plan["stage"] != "acceptance"
        or not plan["require_scheduled"]
        or plan.get("mode", "candidate") != "candidate"
    ):
        raise ValueError("Acceptance requires the actual scheduled staging route")
    code = verify_deployed_files(deployed_root, json.loads(execution_manifest.read_text("utf-8")))
    if code != plan["candidate"]["executable_sha256"]:
        raise ValueError("Actual mounted executable differs from candidate")
    now = datetime.now(UTC)
    if now >= datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00")):
        raise ValueError("Protected task deadline expired")
    source_path = Path(plan.get("source", {}).get("research_manifest_path", ""))
    source = load_snapshot(source_path)
    if any(plan["source"].get(key) != source[key] for key in ("snapshot_id", "manifest_sha256")):
        raise ValueError("Source snapshot differs from protected seed identity")
    for reference in source["tables"].values():
        open_pinned_table(reference)
    data_root = Path(plan["data_root"])
    registry = snapshot_registry(
        data_root / "research/gold/current",
        plan["consumer"]["dataset_version"],
        plan["consumer"].get("contour_id", "native_tradable"),
    )
    research = current_snapshot(registry)
    if research is None or research["snapshot_id"] == source["snapshot_id"]:
        raise ValueError("PENDING: candidate has not published a new Research snapshot")
    data = current_snapshot(data_root / "canonical/moex/baseline-4y-current/.data_snapshots")
    if (
        data is None
        or research["parents"] != [data["snapshot_id"]]
        or any(research["tables"].get(name) != ref for name, ref in data["tables"].items())
    ):
        raise ValueError("Published Research does not descend from current accepted Data")
    if not research["quality_checks"] or any(
        value != "PASS" for value in research["quality_checks"].values()
    ):
        raise ValueError("Published snapshot quality checks are incomplete")
    for reference in research["tables"].values():
        open_pinned_table(reference)
    mutations = observed_mutations(source, research)
    if not any(
        item["table"] in {"raw_moex_history", "canonical_bars", "canonical_contract_economics"}
        for item in mutations
    ):
        raise ValueError(
            "Actual native Data mutation is missing; counters or a new ID are insufficient"
        )
    bindings, stages = {}, set()
    with (
        readonly_home_snapshot(Path(os.environ["DAGSTER_HOME"])) as (home, home_observation),
        DagsterInstance.from_config(str(home)) as instance,
    ):
        runs = instance.get_runs(
            filters=RunsFilter(job_name=JOBS[0], tags={"ta3000/staging_plan_sha256": digest(plan)}),
            limit=3,
        )
        if len(runs) != 1:
            raise ValueError("Expected one bounded root run for the protected acceptance plan")
        upstream, changed, root = None, 0, runs[0]
        for name in JOBS:
            if upstream:
                matches = instance.get_runs(
                    filters=RunsFilter(
                        job_name=name, tags={"ta3000/upstream_run_id": upstream.run_id}
                    ),
                    limit=3,
                )
                if len(matches) != 1:
                    raise ValueError(f"PENDING: expected one recorded child job: {name}")
                run = matches[0]
            else:
                run = root
            if run.status.value != "SUCCESS":
                raise ValueError(f"PENDING: {name} is {run.status.value}")
            if (
                run.tags.get("ta3000/staging_plan_sha256") != digest(plan)
                or run.tags.get("ta3000/candidate_head_sha") != plan["candidate"]["head_sha"]
                or run.tags.get("ta3000/staging_root_run_id") != root.run_id
            ):
                raise ValueError("Actual run belongs to another plan/candidate/root")
            stats = instance.get_run_step_stats(run.run_id)
            stages.update(step.step_key for step in stats if step.status.value == "SUCCESS")
            if any(step.status.value != "SUCCESS" for step in stats):
                raise ValueError("Failed or skipped producer step cannot establish route proof")
            bindings[name] = {
                "run_id": run.run_id,
                "status": "SUCCESS",
                "candidate_head_sha": plan["candidate"]["head_sha"],
                "upstream_run_id": upstream.run_id if upstream else None,
            }
            for event in instance.all_logs(
                run.run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION
            ):
                materialization = event.dagster_event.event_specific_data.materialization
                metadata = materialization.metadata
                changed = max(
                    changed, int(getattr(metadata.get("effective_changed_windows"), "value", 0))
                )
            upstream = run
        if research["run_id"] != upstream.run_id:
            raise ValueError("Published Research was not produced by the recorded candidate chain")
        tick = _reference_tick(plan, instance, root)
        tick_proof = dict(
            name="moex_baseline_daily_update_schedule",
            status="SUCCESS",
            run_id=root.run_id,
            tick_id=str(tick.tick_id),
            timestamp=tick.timestamp,
        )
    if changed < 1 or not set(STAGES).issubset(stages):
        raise ValueError("Nonempty execution of every required production stage is missing")
    path = registry / "manifests" / f"{research['snapshot_id']}.json"
    from trading_advisor_3000.product_plane.common.staging_execution import _key, _quota

    with _quota(plan) as quota:
        reservations = [
            quota.get_run_by_id(
                _key("staging-root-v1", plan["task_id"], plan["attempt_kind"], slot)
            )
            for slot in range(plan["limits"][plan["attempt_kind"]])
        ]
        reservations = [
            item
            for item in reservations
            if item is not None
            and item.run_config.get("root_run_id") == root.run_id
            and item.run_config.get("plan_sha256") == digest(plan)
        ]
        if len(reservations) != 1:
            raise ValueError("Real durable root reservation is missing")
        reservation = reservations[0]
        if (
            now.timestamp()
            >= float(reservation.run_config["started_at"]) + plan["max_runtime_seconds"]
        ):
            raise ValueError("Candidate exceeded the bounded graph runtime")
        claims, reserved = {}, {}
        for entry, limit in plan["native_steps"].items():
            entries = [
                quota.get_run_by_id(_key("staging-native-v1", reservation.run_id, entry, slot))
                for slot in range(limit)
            ]
            reserved[entry] = sum(item is not None for item in entries)
            claims[entry] = sum(
                item is not None
                and quota.get_run_by_id(_key("staging-claim-consumed-v1", item.run_id)) is not None
                for item in entries
            )
        if not any(claims.values()):
            raise ValueError("No durable native computation claims exist")
        budget = {
            "status": "PASS",
            "plan_sha256": digest(plan),
            "root_reservation_id": reservation.run_id,
            "native_claims": {entry: count for entry, count in claims.items() if count},
            "observed_native_entries": claims,
            "reserved_native_entries": reserved,
        }
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            str(Path(__file__).resolve()),
            "consumer",
            "--plan",
            str(plan_path),
            "--manifest",
            str(path),
        ],
        cwd=deployed_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=min(180, plan["max_runtime_seconds"]),
    )
    if result.returncode:
        raise ValueError(f"Fresh normal consumer failed: {result.stderr[-1500:]}")
    consumer = json.loads(result.stdout)
    # A second read prevents signing a pointer that changed during consumer verification.
    if any(
        current is None
        or any(current[key] != before[key] for key in ("snapshot_id", "manifest_sha256"))
        for current, before in (
            (current_snapshot(registry), research),
            (
                current_snapshot(data_root / "canonical/moex/baseline-4y-current/.data_snapshots"),
                data,
            ),
        )
    ):
        raise ValueError("Published snapshot changed during independent consumer proof")
    return validate_document(
        "receipt",
        {
            "schema_version": "staging_receipt.v1",
            "task_id": plan["task_id"],
            "stage": "acceptance",
            "candidate": plan["candidate"],
            "plan_sha256": digest(plan),
            "issued_at_utc": datetime.now(UTC).isoformat(),
            "expires_at_utc": min(
                now + timedelta(hours=24),
                datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00")),
            ).isoformat(),
            "verifier_revision": plan["verifier_revision"],
            "source": {
                "snapshot_id": source["snapshot_id"],
                "manifest_sha256": source["manifest_sha256"],
            },
            "outputs": {
                "snapshot_id": research["snapshot_id"],
                "manifest_sha256": research["manifest_sha256"],
                "data_snapshot_id": data["snapshot_id"],
                "pins_sha256": digest(research["tables"]),
            },
            "runs": bindings,
            "schedule": tick_proof,
            "changed_windows": changed,
            "qc": {"status": "PASS", "checks": research["quality_checks"]},
            "consumer": consumer,
            "coverage": {
                "status": "PASS",
                "required_stages": list(STAGES),
                "executed_stages": sorted(stages),
                "required_scenarios": [],
                "executed_scenarios": [],
                "native_mutations": mutations,
            },
            "runtime": {
                "mode": plan.get("mode", "candidate"),
                "code_sha256": code,
                "config_sha256": plan["candidate"]["config_sha256"],
                "image_digest": plan["candidate"]["image_digest"],
                "budget": budget,
                "metadata_observation": home_observation,
            },
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", choices=("observe", "consumer", "preflight", "reference-cycle", "reattest")
    )
    parser.add_argument("--root-run-id")
    parser.add_argument("--verify-compute-contract", action="store_true")
    parser.add_argument("--original-receipt", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--deployed-root", type=Path, default=Path("/workspace"))
    parser.add_argument(
        "--execution-manifest", type=Path, default=Path("/staging-control/execution-manifest.json")
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if args.mode == "consumer":
            value = candidate_consumer(
                args.plan,
                args.manifest,
                args.deployed_root,
                verify_compute_contract=args.verify_compute_contract,
            )
        elif args.mode == "preflight":
            value = preflight(args.plan, args.deployed_root, args.execution_manifest)
        elif args.mode == "reference-cycle":
            value = reference_cycle(args.plan, args.root_run_id, args.deployed_root)
        elif args.mode == "reattest":
            value = observe_reattest(
                args.plan, args.original_receipt, args.deployed_root, args.execution_manifest
            )
        elif args.mode == "observe":
            value = observe(args.plan, args.deployed_root, args.execution_manifest)
        else:
            raise ValueError("Use the protected host supervisor to verify runtime and sign")
        if args.output:
            args.output.write_text(json.dumps(value, indent=2), encoding="utf-8")
        else:
            print(json.dumps(value))
        return 0
    except (ValueError, OSError, TypeError, KeyError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
