import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from deltalake import DeltaTable
from test_moex_contract_economics_spark_job import _configure_windows_spark_runtime

from trading_advisor_3000.product_plane.common.data_snapshots import (
    capture_table,
    create_snapshot,
    current_snapshot,
    publish_snapshot,
    save_snapshot,
)
from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import economics_cutover as cutover
from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    moex_economics_store_contract,
)


def _fixture(root):
    columns = dict(moex_economics_store_contract()["raw_moex_contract_securities"]["columns"])
    columns.pop("snapshot_metadata_json")
    columns["enrichment_provenance_json"] = "string"
    payload = {
        "SECID": "TESTU6",
        "BOARDID": "RFUD",
        "TRADEDATE": "2026-06-26",
        "ASSETCODE": "TEST",
        "SETTLEPRICE": 100,
    }
    raw = dict.fromkeys(columns)
    raw.update(
        source_id="legacy_enriched",
        source_url="https://example.invalid/history",
        source_document_id="prepared-old",
        source_document_hash="prepared-hash",
        board="RFUD",
        moex_secid="TESTU6",
        trade_date="2026-06-26",
        assetcode="TEST",
        min_step=99,
        lot_volume=77,
        last_settle_price=100,
        raw_payload_json=json.dumps(payload),
        enrichment_provenance_json=json.dumps(
            {
                "original_source_id": "moex_iss_history_forts_securities_unmodified",
                "original_history_hash": hashlib.sha256(
                    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            }
        ),
    )
    raw_path = root / "raw/economics/raw_moex_contract_securities.delta"
    write_delta_table_rows(table_path=raw_path, columns=columns, rows=[raw])
    prepared_path = root / "provenance/prepared/economics/raw_moex_contract_securities.delta"
    prepared = {**raw, "min_step": 0.25, "lot_volume": 4}
    write_delta_table_rows(table_path=prepared_path, columns=columns, rows=[prepared])
    policy = prepared_path.parent
    (policy / "bounded-parameter-regimes.json").write_text("[]")
    (policy / "dated-official-tc-evidence.json").write_text('{"retained":"original"}')
    economics = root / "canonical/economics/canonical_contract_economics.delta"
    write_delta_table_rows(
        table_path=economics,
        columns={
            "contract_id": "string",
            "economics_session_date": "date",
            "clearing_type": "string",
            "step_price_rub": "double",
            "margin_required_estimate": "double",
            "source_flags_json": "string",
        },
        rows=[
            {
                "contract_id": "TESTU6",
                "economics_session_date": "2026-06-26",
                "clearing_type": "mc",
                "step_price_rub": 2,
                "margin_required_estimate": 200,
                "source_flags_json": '{"legacy":"retained"}',
            }
        ],
    )
    unrelated = root / "canonical/unrelated.delta"
    write_delta_table_rows(table_path=unrelated, columns={"value": "double"}, rows=[{"value": 123}])
    tables = {
        "raw_moex_contract_securities": capture_table(raw_path),
        "prepared_raw_moex_contract_securities": capture_table(prepared_path),
        "canonical_contract_economics": capture_table(economics),
        "unrelated": capture_table(unrelated),
    }
    snapshot_root = root / "canonical/moex/baseline-4y-current/.data_snapshots"
    manifest = create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="fixture",
        run_id="old",
        code_version="old",
        quality_checks={"fixture": "PASS"},
    )
    publish_snapshot(
        snapshot_root, save_snapshot(snapshot_root, manifest), expected_current_id=None
    )
    receipt = root / "accepted.json"
    receipt.write_text(
        json.dumps(
            {
                "status": "ACCEPTED_PRODUCTION_BASELINE",
                "production_tables": {
                    "calculated_parameters": tables["prepared_raw_moex_contract_securities"],
                    "coverage": tables["canonical_contract_economics"],
                },
                "production_publication": {"snapshot_id": manifest["snapshot_id"]},
            }
        )
    )
    plan = cutover.plan_cutover(root, receipt, code_version="test-release")
    return root / "canonical/economics/.production-cutover/plan.json", manifest, plan


def _process(path, root, *, phase="apply", crash=""):
    code = """
import os,sys,json
from pathlib import Path
from trading_advisor_3000.product_plane.data_plane.moex.economics_cutover import (
 apply_cutover,verify_cutover,rollback_cutover,
)
from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session
def event(name):
 if name==sys.argv[3]:os._exit(79)
s=_create_spark_session('native-canonical-cutover','local[2]')
try:
 if sys.argv[2]=='apply':r=apply_cutover(Path(sys.argv[1]),s,on_commit=event)
 elif sys.argv[2]=='rollback':r=rollback_cutover(Path(sys.argv[1]),s)
 else:r=verify_cutover(Path(sys.argv[1]),s)
finally:s.stop()
print('CUTOVER_RESULT='+json.dumps(r))
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(path), phase, crash],
        capture_output=True,
        text=True,
        timeout=240,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    (root / (phase + "-" + (crash or "normal") + ".log")).write_text(
        result.stdout + result.stderr, encoding="utf-8"
    )
    if crash:
        assert result.returncode == 79, result.stderr[-6000:]
        return None
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-8000:]
    return json.loads(
        next(
            line.split("=", 1)[1]
            for line in result.stdout.splitlines()
            if line.startswith("CUTOVER_RESULT=")
        )
    )


def test_native_cutover_recovers_publishes_complete_snapshot_and_rolls_back(tmp_path, monkeypatch):
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv("TA3000_SPARK_DRIVER_MEMORY", "4g")
    path, previous, plan = _fixture(tmp_path)
    (tmp_path / "accepted.json").unlink()
    snapshot_root = tmp_path / "canonical/moex/baseline-4y-current/.data_snapshots"
    _process(path, tmp_path, crash="parameters")
    assert current_snapshot(snapshot_root) == previous
    initial = capture_table(tmp_path / "canonical/economics/canonical_contract_parameters.delta")
    _process(path, tmp_path, crash="raw")
    assert capture_table(Path(initial["table_path"])) == initial
    clean = DeltaTable(plan["sources"]["raw"]["table_path"]).to_pyarrow_table().to_pylist()[0]
    assert clean["min_step"] is None and clean["lot_volume"] is None
    assert clean["source_document_id"] == "RFUD:TESTU6:2026-06-26"
    assert json.loads(clean["raw_payload_json"])["SETTLEPRICE"] == 100
    assert clean["snapshot_metadata_json"] is None
    assert current_snapshot(snapshot_root) == previous
    _process(path, tmp_path, crash="candidate_saved")
    intent_path = path.parent / "candidate-intent.json"
    intent_bytes = intent_path.read_bytes()
    intent_path.unlink()
    with pytest.raises(ValueError, match="lost its issued identity"):
        cutover._issued_candidate(plan, path.parent, snapshot_root)
    intent_path.write_bytes(intent_bytes)
    issued = json.loads((path.parent / "candidate-intent.json").read_text())["snapshot_id"]
    result = _process(path, tmp_path)
    assert result["snapshot_id"] == issued
    assert _process(path, tmp_path, phase="verify")["differences"] == 0
    selected = current_snapshot(snapshot_root)
    assert selected["tables"]["unrelated"] == previous["tables"]["unrelated"]
    assert (
        selected["tables"]["canonical_contract_economics"]
        == previous["tables"]["canonical_contract_economics"]
    )
    assert "prepared_raw_moex_contract_securities" not in selected["tables"]
    descendant = create_snapshot(
        tables=selected["tables"],
        required_tables=list(selected["tables"]),
        dataset_version="fixture",
        run_id="later-owner-publication",
        code_version="test-release",
        quality_checks={"fixture": "PASS-NOOP"},
        parents=[issued],
    )
    publish_snapshot(
        snapshot_root, save_snapshot(snapshot_root, descendant), expected_current_id=issued
    )
    continuity = _process(path, tmp_path, phase="verify")
    assert continuity["snapshot_id"] == issued
    assert continuity["selected_snapshot_id"] == descendant["snapshot_id"]
    publish_snapshot(
        snapshot_root,
        snapshot_root / "manifests" / (issued + ".json"),
        expected_current_id=descendant["snapshot_id"],
    )
    stored = DeltaTable(initial["table_path"]).to_pyarrow_table().to_pylist()[0]
    assert stored["min_step"] == 0.25 and stored["lot_volume"] == 4
    assert stored["source_validation_status"] == "ACCEPTED_PRODUCTION_BASELINE"
    assert stored["execution_admissible"] is True
    assert (
        json.loads(stored["calculation_origin_json"])["source_table"]
        == plan["sources"]["parameters"]
    )
    assert _process(path, tmp_path, phase="rollback")["status"] == "rolled_back"
    assert current_snapshot(snapshot_root) == previous
    restored = capture_table(Path(plan["sources"]["raw"]["table_path"]))
    assert all(restored[k] == v for k, v in plan["sources"]["raw"].items() if k != "delta_version")
    assert _process(path, tmp_path, phase="rollback")["status"] == "rolled_back"
    assert capture_table(Path(initial["table_path"])) == initial


def test_native_cutover_aborts_own_raw_commit_before_publication(tmp_path, monkeypatch):
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    path, previous, plan = _fixture(tmp_path)
    _process(path, tmp_path, crash="raw")
    assert _process(path, tmp_path, phase="rollback")["status"] == "rolled_back"
    snapshot_root = tmp_path / "canonical/moex/baseline-4y-current/.data_snapshots"
    assert current_snapshot(snapshot_root) == previous
    restored = capture_table(Path(plan["sources"]["raw"]["table_path"]))
    assert all(restored[k] == v for k, v in plan["sources"]["raw"].items() if k != "delta_version")


@pytest.mark.parametrize("change", ["hash", "identity", "locator", "nonfinite"])
def test_original_raw_refuses_source_contradictions_before_writes(tmp_path, monkeypatch, change):
    _configure_windows_spark_runtime(tmp_path, monkeypatch)
    from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session

    path, _, plan = _fixture(tmp_path)
    raw_path = Path(plan["sources"]["raw"]["table_path"])
    table = DeltaTable(str(raw_path))
    row = table.to_pyarrow_table().to_pylist()[0]
    if change == "hash":
        row["raw_payload_json"] = row["raw_payload_json"].replace("100", "101")
    elif change == "identity":
        row["moex_secid"] = "WRONG"
    elif change == "locator":
        provenance = json.loads(row["enrichment_provenance_json"])
        provenance["original_source_document_id"] = "WRONG"
        row["enrichment_provenance_json"] = json.dumps(provenance)
    else:
        row["raw_payload_json"] = row["raw_payload_json"].replace("100", "NaN")
    spark = _create_spark_session("source-contradiction", "local[2]")
    try:
        frame = spark.read.format("delta").load(str(raw_path))
        from pyspark.sql import functions as F

        for key in ("raw_payload_json", "moex_secid", "enrichment_provenance_json"):
            frame = frame.withColumn(key, F.lit(row[key]))
        with pytest.raises(ValueError):
            cutover._original_raw(frame)
    finally:
        spark.stop()
    assert capture_table(raw_path) == plan["sources"]["raw"]
    assert not (tmp_path / "canonical/economics/canonical_contract_parameters.delta").exists()


def test_cutover_writer_lock_rejects_a_competing_owner(tmp_path):
    root = tmp_path / "economics"
    with cutover.economics_writer_lock(root):
        with pytest.raises(ValueError, match="another economics writer"):
            with cutover.economics_writer_lock(root):
                pytest.fail("competing writer entered")
