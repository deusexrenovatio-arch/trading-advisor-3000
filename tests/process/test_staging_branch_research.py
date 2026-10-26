from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from deltalake import write_deltalake

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import collect_staging_acceptance as collector
import launch_moex_baseline_staging_run as transport
import run_staging_control as control

from trading_advisor_3000.product_plane.common import staging_source_scope as sources
from trading_advisor_3000.product_plane.common import staging_test_fixture as fixtures


def branch_plan():
    return {
        "stage": "branch",
        "attempt_kind": "small",
        "require_scheduled": False,
        "root_job": "research_data_prep_job",
        "candidate": {"head_sha": "a" * 40, "executable_sha256": "b" * 64},
        "source": {
            "fixture": {"manifest_sha256": "c" * 64},
            "native_schedule": {
                "manual_run_id": "11111111-1111-1111-1111-111111111111",
                "run_config": {"ops": {"native": {"config": {"bounded": True}}}},
            },
        },
    }


def test_research_branch_launch_uses_exact_captured_recipe_once(monkeypatch):
    plan = branch_plan()
    calls = []

    def post(**kw):
        calls.append(kw)
        return {
            "data": {
                "launchRun": {
                    "__typename": "LaunchRunSuccess",
                    "run": {"runId": plan["source"]["native_schedule"]["manual_run_id"]},
                }
            }
        }

    monkeypatch.setattr(transport, "_post_graphql", post)
    monkeypatch.setattr(control, "runtime_url", lambda *a: "http://127.0.0.1:3011/graphql")
    control.launch_branch_fixture({"plan": plan}, {})
    assert len(calls) == 1
    params = calls[0]["variables"]["executionParams"]
    assert params["selector"]["jobName"] == "research_data_prep_job"
    assert params["runConfigData"] == plan["source"]["native_schedule"]["run_config"]
    assert (
        params["executionMetadata"]["runId"] == plan["source"]["native_schedule"]["manual_run_id"]
    )


def test_research_launch_error_does_not_retry(monkeypatch):
    calls = []
    monkeypatch.setattr(control, "runtime_url", lambda *a: "http://127.0.0.1:3011/graphql")
    monkeypatch.setattr(
        transport, "_post_graphql", lambda **kw: calls.append(kw) or {"errors": ["failed"]}
    )
    with pytest.raises(ValueError, match="no automatic retry"):
        control.launch_branch_fixture({"plan": branch_plan()}, {})
    assert len(calls) == 1


def test_fixture_input_preflight_cannot_claim_consumer_or_main_proof(tmp_path, monkeypatch):
    plan = branch_plan()
    manifest = tmp_path / "files.json"
    manifest.write_text("{}")
    monkeypatch.setattr(collector, "load_plan", lambda *a: plan)
    monkeypatch.setattr(collector, "verify_deployed_files", lambda *a: "b" * 64)
    monkeypatch.setattr(
        fixtures, "validate_branch_fixture", lambda *a: {"manifest_sha256": "c" * 64}
    )
    result = collector.preflight(tmp_path / "plan.json", tmp_path, manifest)
    assert result["status"] == "PREFLIGHT_PASS"
    assert result["native_consumer"] == "CHECKED_AFTER_MATERIALIZATION"
    assert result["proof_kind"] == "synthetic_fixture_inputs_only"
    with pytest.raises(ValueError, match="actual scheduled"):
        collector.observe(tmp_path / "plan.json", tmp_path, manifest)


def test_fixture_sources_cannot_escape_to_large_or_production_data(tmp_path, monkeypatch):
    plan = branch_plan()
    plan.update(data_root=str(tmp_path / "outputs"), scope={"max_source_bytes": 16 * 1024**2})
    monkeypatch.setattr(sources, "_plan", lambda *a: plan)
    monkeypatch.setattr(fixtures, "validate_branch_fixture", lambda *a: {})
    outside = tmp_path / "other-source.json"
    outside.write_text("{}")
    with pytest.raises(sources.StagingExecutionError, match="escapes"):
        sources.require_staging_source_bytes(
            output_path=Path(plan["data_root"]), input_paths=[outside]
        )


def test_fixture_row_cap_checks_physical_rows_before_reading_values(tmp_path, monkeypatch):
    output = tmp_path / "outputs"
    table = output / "input.delta"
    write_deltalake(str(table), pa.table({"value": [1, 2]}))
    plan = branch_plan()
    plan.update(data_root=str(output), scope={"max_source_bytes": 16 * 1024**2})
    monkeypatch.setattr(sources, "_plan", lambda *a: plan)
    monkeypatch.setattr(fixtures, "validate_branch_fixture", lambda *a: {})
    monkeypatch.setattr(fixtures, "MAX_ROWS", 1)
    with pytest.raises(sources.StagingExecutionError, match="row/file cap"):
        sources.require_staging_source_bytes(output_path=output, input_paths=[table])


def test_every_fixture_consumer_instrument_must_have_populated_requested_columns(
    tmp_path, monkeypatch
):
    from contextlib import nullcontext

    import pandas as pd

    from trading_advisor_3000.product_plane.common import data_snapshots
    from trading_advisor_3000.product_plane.research import io

    plan = branch_plan()
    plan.update(
        data_root=str(tmp_path),
        consumer={
            "instrument_ids": ["BR", "Si"],
            "timeframe": "15m",
            "price_columns": ["close"],
            "indicator_columns": [],
            "derived_columns": [],
        },
    )
    frames = [
        SimpleNamespace(frame=pd.DataFrame({"close": [1.0]})),
        SimpleNamespace(frame=pd.DataFrame({"close": [None]})),
    ]
    monkeypatch.setattr(collector, "load_plan", lambda *a: plan)
    monkeypatch.setattr(data_snapshots, "load_snapshot", lambda *a: {"snapshot_id": "test"})
    monkeypatch.setattr(data_snapshots, "snapshot_read_scope", lambda *a: nullcontext())
    monkeypatch.setattr(io, "ResearchSliceRequest", lambda **kw: SimpleNamespace(**kw))
    monkeypatch.setattr(io, "load_backtest_frames", lambda **kw: (frames, None, False))
    with pytest.raises(ValueError, match="every instrument/column"):
        collector.candidate_consumer(tmp_path / "plan", tmp_path / "snapshot", tmp_path)


@pytest.mark.parametrize(
    "missing",
    [
        None,
        "run_continuous_front_spark_job",
        "run_research_bar_views_spark_job",
        "materialize_indicator_frames",
        "materialize_derived_indicator_frames",
    ],
)
def test_branch_report_requires_actual_research_native_claims(missing):
    claims = {
        name: 1
        for name in (
            "run_continuous_front_spark_job",
            "run_research_bar_views_spark_job",
            "materialize_indicator_frames",
            "materialize_derived_indicator_frames",
        )
    }
    if missing is None:
        collector.require_branch_native_claims(claims)
    else:
        claims[missing] = 0
        with pytest.raises(ValueError, match="actual CF/L0/L1/L2"):
            collector.require_branch_native_claims(claims)
