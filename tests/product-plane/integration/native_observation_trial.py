"""Explicit isolated A-F native trial. No production registry or publication entrypoint."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from dagster import DagsterInstance
from fastapi.testclient import TestClient

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    atomic_json,
    capture_table,
    create_snapshot,
    save_snapshot,
)
from trading_advisor_3000.product_plane.data_plane.moex import (
    observation_materialization as writer,
)
from trading_advisor_3000.product_plane.data_plane.moex.observation_acquisition import (
    pin,
    verify_source_bundle,
)
from trading_advisor_3000.product_plane.data_plane.moex.observation_job import (
    native_observation_materialization_job,
)
from trading_advisor_3000.product_plane.data_plane.native_observations import (
    NativeObservationRead,
    NativeObservationSnapshot,
    completed_candidate,
)
from trading_advisor_3000.product_plane.interfaces.data_inspector import create_app


def _command_directory(root, command_id):
    return root / "commands" / hashlib.sha256(command_id.encode()).hexdigest()


def _physical_state(graph):
    return {
        name: capture_table(Path(reference["table_path"]))
        for name, reference in graph["manifest"]["tables"].items()
    }


def run_native_checks(root: Path) -> dict:
    root = root.resolve()
    if root != Path("/trial") or not (root / "scope.json").is_file():
        raise ValueError("native proof requires only its explicit isolated trial mount")
    acquisition = json.loads((root / "acquisition-result.json").read_bytes())
    source, assignments = verify_source_bundle(
        root, acquisition["source_manifest"], acquisition["assignments"]
    )
    original = json.loads((root / "baseline-01-result.json").read_bytes())
    baseline_pin = original["completion"]
    baseline = NativeObservationSnapshot(root, baseline_pin)
    baseline_graph = baseline.verify()
    receipts_before = sorted((root / "source/http_receipts").glob("*.json"))
    baseline_physical = _physical_state(baseline_graph)
    runtime = root / "runtime" / "dagster-acceptance"
    runtime.mkdir(parents=True, exist_ok=True)
    os.environ["DAGSTER_HOME"] = str(root / "runtime/dagster-home")
    runs = []
    with DagsterInstance.local_temp(
        tempdir=str(runtime), overrides={"telemetry": {"enabled": False}}
    ) as instance:

        def invoke(command_id, *, parent=None, overlay=None, fault=None, success=True):
            input_path = root / "command-inputs" / (command_id + ".json")
            if input_path.exists():
                intent = json.loads(input_path.read_bytes())
                if intent["parent"] != parent or intent["overlay"] != overlay:
                    raise ValueError("trial command input conflict")
            else:
                intent = writer.build_observation_command(
                    root,
                    command_id=command_id,
                    source_pin=acquisition["source_manifest"],
                    assignment_pin=acquisition["assignments"],
                    parent=parent,
                    overlay=overlay,
                )
                atomic_json(input_path, intent, immutable=True)
            config = {"root": str(root), "intent_path": input_path.relative_to(root).as_posix()}
            if fault:
                config["fault"] = fault
            result = native_observation_materialization_job.execute_in_process(
                instance=instance,
                run_config={"ops": {"materialize_native_observations": {"config": config}}},
                raise_on_error=False,
            )
            runs.append(
                {
                    "command_id": command_id,
                    "fault": fault,
                    "dagster_run_id": result.run_id,
                    "success": result.success,
                }
            )
            assert result.success is success, (command_id, result.success)
            return (
                result.output_for_node("materialize_native_observations")
                if result.success
                else None
            )

        replay = invoke("baseline-01")
        assert replay["replayed"] and replay["completion"] == baseline_pin
        assert _physical_state(baseline.verify()) == baseline_physical
        with patch.object(writer, "producer_code_sha256", return_value="0" * 64):
            upgraded_replay = invoke("baseline-01")
        assert upgraded_replay["replayed"] and upgraded_replay["completion"] == baseline_pin
        conflict = copy.deepcopy(baseline_graph["intent"])
        conflict["selection"][0]["row_limit"] -= 1
        try:
            writer.materialize_observation_command(root, conflict)
        except ValueError:
            pass
        else:
            raise AssertionError("changed input under the old command key was accepted")

        baseline_intent = baseline_graph["intent"]
        target_request = next(
            q
            for q in baseline_intent["selection"]
            if q["timeframe"] == "1m" and q["context"]["market"] == "shares"
        )
        target = baseline.read(target_request).table
        key = {
            field: target[field][0].as_py()
            for field in baseline_graph["metadata"]["storage_layout"]["native_bars"]["grain"]
        }
        before_value = target["volume"][0].as_py()
        overlay_value = before_value + 1
        overlay = {
            "schema_version": 1,
            "simulation_id": "named-volume-correction-01",
            "parent_completion_sha256": baseline_pin["sha256"],
            "changes": [{"key": key, "field": "volume", "new_value": str(overlay_value)}],
        }
        overlay_path = root / "overlays/volume-correction-01.json"
        atomic_json(overlay_path, overlay, immutable=True)
        corrected = invoke("correction-01", parent=baseline_pin, overlay=pin(root, overlay_path))
        corrected_reader = NativeObservationSnapshot(root, corrected["completion"])
        corrected_graph = corrected_reader.verify()
        assert (
            corrected_graph["manifest"]["tables"]["raw_observations"]
            == baseline_graph["manifest"]["tables"]["raw_observations"]
        )
        assert (
            corrected_graph["manifest"]["tables"]["quarantine"]
            == baseline_graph["manifest"]["tables"]["quarantine"]
        )
        assert corrected_reader.read(target_request).table["volume"][0].as_py() == overlay_value
        assert baseline.read(target_request).table["volume"][0].as_py() == before_value
        corrected_wire = corrected_reader.read(target_request).to_ipc(corrected_reader.namespace)
        invoke("baseline-01")
        assert (
            corrected_reader.read(target_request).to_ipc(corrected_reader.namespace)
            == corrected_wire
        )

        invoke("partial-01", fault="after_native_bars", success=False)
        partial_completion = _command_directory(root, "partial-01") / "completion.json"
        assert not partial_completion.exists()
        try:
            NativeObservationSnapshot(
                root, {"path": partial_completion.relative_to(root).as_posix(), "sha256": "0" * 64}
            )
        except SnapshotUnavailableError:
            pass
        else:
            raise AssertionError("a partial generation became readable")
        partial_recovery = invoke("partial-01")
        NativeObservationSnapshot(root, partial_recovery["completion"]).read(target_request)

        invoke("saved-graph-01", fault="after_snapshot", success=False)
        saved_dir = root / "attempts" / hashlib.sha256(b"saved-graph-01").hexdigest()
        saved_path = next(saved_dir.glob("*/manifests/*.json"))
        saved_manifest = json.loads(saved_path.read_bytes())
        saved_state = _physical_state({"manifest": saved_manifest})
        recovered_saved = invoke("saved-graph-01")
        assert recovered_saved["recovered"]
        saved_graph = completed_candidate(root, recovered_saved["completion"])
        assert saved_graph["manifest"]["snapshot_id"] == saved_manifest["snapshot_id"]
        assert _physical_state(saved_graph) == saved_state

        invoke("lost-ack-01", fault="after_completion", success=False)
        lost_path = _command_directory(root, "lost-ack-01") / "completion.json"
        lost_pin = pin(root, lost_path)
        lost_state = _physical_state(completed_candidate(root, lost_pin))
        reconciled = invoke("lost-ack-01")
        assert reconciled["replayed"] and reconciled["completion"] == lost_pin
        assert _physical_state(completed_candidate(root, lost_pin)) == lost_state

        lock_path = _command_directory(root, "baseline-01") / "writer.lock"
        holder_code = (
            "import fcntl,sys,time; f=open(sys.argv[1],'a+b'); "
            "fcntl.flock(f,fcntl.LOCK_EX); print('LOCKED',flush=True); time.sleep(30)"
        )
        holder = subprocess.Popen(
            [sys.executable, "-c", holder_code, str(lock_path)], stdout=subprocess.PIPE, text=True
        )
        try:
            assert holder.stdout.readline().strip() == "LOCKED"
            invoke("baseline-01", success=False)
        finally:
            holder.terminate()
            holder.wait(timeout=5)
        assert invoke("baseline-01")["completion"] == baseline_pin

        invoke("ambiguous-01", fault="after_snapshot", success=False)
        ambiguous_dir = root / "attempts" / hashlib.sha256(b"ambiguous-01").hexdigest()
        one = json.loads(next(ambiguous_dir.glob("*/manifests/*.json")).read_bytes())
        duplicate_attempt = ambiguous_dir / uuid4().hex
        duplicate_attempt.mkdir()
        duplicate = create_snapshot(
            tables=one["tables"],
            required_tables=one["required_tables"],
            dataset_version=one["dataset_version"],
            run_id=duplicate_attempt.name,
            code_version=one["code_version"],
            quality_checks=one["quality_checks"],
            parameters=one["parameters"],
            window=one["window"],
            parents=one["parents"],
        )
        save_snapshot(duplicate_attempt, duplicate)
        invoke("ambiguous-01", success=False)
        assert not (_command_directory(root, "ambiguous-01") / "completion.json").exists()

        api_reads = []
        with TestClient(create_app(data_root=root, observation_reader=corrected_reader)) as client:
            for query in corrected_graph["intent"]["selection"]:
                response = client.post("/api/data-inspector/native-observation-read", json=query)
                assert response.status_code == 200, (
                    response.text[:300] if response.status_code != 200 else ""
                )
                decoded = NativeObservationRead.from_ipc(
                    response.content, corrected_reader.namespace
                )
                assert decoded.table.equals(corrected_reader.read(query).table, check_metadata=True)
                api_reads.append(
                    {
                        "instrument_id": query["instrument_id"],
                        "timeframe": query["timeframe"],
                        "rows": decoded.table.num_rows,
                        "wire_bytes": len(response.content),
                    }
                )
            for change in [
                {"known_at": "2026-09-16T00:00:00Z"},
                {"series_type": "continuous_front"},
                {"row_limit": 1},
                {"byte_limit": 1},
                {"columns": ["series_id"]},
            ]:
                query = {**target_request, **change}
                assert (
                    client.post(
                        "/api/data-inspector/native-observation-read", json=query
                    ).status_code
                    == 422
                )
        assert _physical_state(baseline.verify()) == baseline_physical
        assert sorted((root / "source/http_receipts").glob("*.json")) == receipts_before
        pointers = [
            p.relative_to(root).as_posix()
            for name in ["published_current.json", "accepted_current.json", "latest.json"]
            for p in root.rglob(name)
        ]
        assert not pointers
        report = {
            "status": "NATIVE_PROOFS_PASS_OLD_READER_PENDING",
            "A": {
                "status": "PASS",
                **original["oracle"],
                "source_requests": acquisition["requests"],
                "source_pages": acquisition["pages"],
            },
            "B": {
                "status": "PASS",
                "same_receipt": True,
                "producer_upgrade_replay": True,
                "changed_input_rejected": True,
                "no_new_source_requests_or_delta_versions": True,
            },
            "C": {
                "status": "PASS",
                "simulation_id": overlay["simulation_id"],
                "changed_native_keys": 1,
                "affected_qc_groups": 1,
                "baseline": baseline_pin,
                "corrected": corrected["completion"],
                "raw_and_quarantine_pins_reused": True,
            },
            "D": {
                "status": "PASS",
                "partial_unreadable_then_recovered": True,
                "saved_graph_reused_without_delta_writes": True,
                "lost_ack_reconciled": True,
                "OS_lock_busy_and_process_death_release": True,
                "multiple_saved_graphs_rejected": True,
            },
            "E_native_consumer": {
                "status": "PASS",
                "api_reads": api_reads,
                "CF_PIT_legacy_projection_and_budgets_rejected": True,
            },
            "F_publication": {
                "status": "PASS",
                "published_pointers": pointers,
            },
            "source_manifest": acquisition["source_manifest"],
            "assignments": acquisition["assignments"],
            "current_producer_code_sha256": writer.producer_code_sha256(),
            "dagster_runs": runs,
        }
        atomic_json(root / "native-trial-proof.json", report, immutable=True)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_native_checks(args.root)))


if __name__ == "__main__":
    main()
