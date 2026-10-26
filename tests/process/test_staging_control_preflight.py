import base64
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


def test_busy_production_never_allows_a_permit(monkeypatch):
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [{"status": "STARTED"}])
    value = {"production_url": "prod", "plan": {"stage": "acceptance"}}
    assert control.production_barrier(value, None, datetime.now(UTC)) is False


def test_explicit_safe_window_accepts_only_idle_production(monkeypatch):
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [])
    value = {
        "production_url": "prod",
        "staging_url": "stage",
        "plan": {
            "stage": "acceptance",
            "execution_window": {
                "start_utc": "2030-01-01T00:00:00Z",
                "end_utc": "2030-01-01T01:00:00Z",
            },
        },
    }
    monkeypatch.setattr(
        control,
        "graphql_schedule",
        lambda *a: {"ticks": [{"status": "SUCCESS", "runIds": ["root"], "timestamp": 1}]},
    )
    assert control.production_barrier(
        value, {"runId": "root"}, datetime(2030, 1, 1, 0, 30, tzinfo=UTC)
    )
    assert value["permit_window_end"] == datetime(2030, 1, 1, 1, tzinfo=UTC)
    assert not control.production_barrier(
        value, {"runId": "root"}, datetime(2030, 1, 1, 2, tzinfo=UTC)
    )


def test_missing_window_cannot_borrow_successful_production_graph(monkeypatch):
    jobs = ("moex_baseline_update_job", "moex_cf_catch_up_job", "research_data_prep_job")

    def runs(url, **kwargs):
        if kwargs.get("job_name"):
            return [{"runId": "root", "pipelineName": jobs[0], "status": "SUCCESS"}]
        parent = (kwargs.get("tags") or {}).get("ta3000/upstream_run_id")
        if parent:
            job, run_id = (jobs[1], "cf") if parent == "root" else (jobs[2], "research")
            return [{"runId": run_id, "pipelineName": job, "status": "SUCCESS"}]
        return []

    monkeypatch.setattr(control, "graphql_runs", runs)
    monkeypatch.setattr(
        control,
        "graphql_schedule",
        lambda p, url: {
            "ticks": [
                {
                    "status": "SUCCESS",
                    "timestamp": 1,
                    "runIds": ["stage-root" if url == "stage" else "root"],
                }
            ]
        },
    )
    value = {"production_url": "prod", "staging_url": "stage", "plan": {"stage": "acceptance"}}
    assert not control.production_barrier(value, {"runId": "stage-root"}, datetime.now(UTC))


def test_unsupported_scenarios_and_invalid_consumer_fail_before_native_work():
    with pytest.raises(ValueError, match="Unsupported scenario"):
        control.source_preflight({"required_scenarios": ["rollover"]}, {}, {})
    with pytest.raises(ValueError, match="Consumer requires"):
        control.source_preflight({"consumer": {}}, {}, {})
    consumer = {
        key: "fixture"
        for key in (
            "dataset_version",
            "indicator_set_version",
            "derived_indicator_set_version",
            "contour_id",
            "timeframe",
        )
    }
    consumer.update(
        {
            key: ["fixture"]
            for key in ("instrument_ids", "price_columns", "indicator_columns", "derived_columns")
        }
    )
    plan = {
        "consumer": consumer,
        "scope": {"timeframes": ["fixture"], "instrument_ids": ["fixture"]},
    }
    with pytest.raises(ValueError, match="Runtime dataset"):
        control.source_preflight(plan, {}, {control.PREFIX + "DATASET_VERSION": "wrong"})


def test_signer_must_match_both_operator_and_protected_gate_keyrings(tmp_path, monkeypatch):
    key = Ed25519PrivateKey.generate()
    private = tmp_path / "host-only.key"
    private.write_bytes(
        key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        )
    )
    public = base64.b64encode(
        key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    ).decode()
    protected = tmp_path / "protected"
    (protected / "configs").mkdir(parents=True)
    operator = tmp_path / "control"
    operator.mkdir()
    (operator / "operator-keyring.json").write_text(json.dumps({"keys": {"fixture": public}}))
    gate = protected / "configs/staging-acceptance-keys.v1.json"
    gate.write_text('{"keys":{}}')
    monkeypatch.setattr(control, "ROOT", protected)
    args = SimpleNamespace(private_key=private, key_id="fixture")
    with pytest.raises(ValueError, match="protected verifier keyring"):
        control.signer_preflight(args, operator)
    gate.write_text(json.dumps({"keys": {"fixture": public}}))
    control.signer_preflight(args, operator)


def test_runtime_identity_change_blocks_resource_admission(monkeypatch):
    value = {
        "production_url": "prod",
        "production_runtime_identity": {"id": "old"},
        "plan": {
            "stage": "branch",
            "daily_execution_window_utc": {"start": "00:00", "end": "02:00"},
        },
    }
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [])
    monkeypatch.setattr(control, "inspect_container", lambda *a: {"State": {"Running": False}})
    with pytest.raises(ValueError, match="runtime identity changed"):
        control.production_barrier(value, None, datetime(2030, 1, 1, 1, tzinfo=UTC))


def test_replaced_production_name_cannot_hide_behind_old_container_id(monkeypatch):
    value = {
        "production_url": "prod",
        "production_runtime_identity": {"id": "old"},
        "plan": {
            "stage": "branch",
            "daily_execution_window_utc": {"start": "00:00", "end": "02:00"},
        },
    }

    def inspect(name):
        assert name == "ta3000-dagster-webserver"
        return {"Id": "replacement", "State": {"Running": True}}

    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [])
    monkeypatch.setattr(control, "inspect_container", inspect)
    monkeypatch.setattr(control, "_production_identity", lambda c: {"id": c["Id"]})
    with pytest.raises(ValueError, match="runtime identity changed"):
        control.production_barrier(value, None, datetime(2030, 1, 1, 1, tzinfo=UTC))
