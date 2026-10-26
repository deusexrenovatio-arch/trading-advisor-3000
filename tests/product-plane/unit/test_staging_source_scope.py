from __future__ import annotations

import copy
import json
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from trading_advisor_3000.product_plane.common.staging_contracts import digest
from trading_advisor_3000.product_plane.common.staging_execution import StagingExecutionError
from trading_advisor_3000.product_plane.common.staging_source_scope import (
    require_staging_parameters,
    require_staging_source_bytes,
)


@pytest.fixture
def protected(tmp_path, monkeypatch):
    fixture = Path(__file__).resolve().parents[1] / "fixtures/contracts/staging_acceptance.v1.json"
    plan = copy.deepcopy(json.loads(fixture.read_text("utf-8")))
    data, quota = tmp_path / "data", tmp_path / "quota"
    data.mkdir()
    quota.mkdir()
    plan.update(
        data_root=str(data),
        quota_home=str(quota),
        producer_parameters={"baseline": {"timeframes": ["1d"], "lookback_days": 7}},
    )
    plan["scope"]["max_source_bytes"] = 1
    path = tmp_path / "plan.json"

    def save():
        path.write_text(json.dumps(plan), encoding="utf-8")
        (data / ".ta3000-staging.json").write_text(
            json.dumps(
                {
                    "schema_version": "staging_root.v1",
                    "plan_path": str(path),
                    "plan_digest": digest(plan),
                }
            ),
            encoding="utf-8",
        )

    save()
    monkeypatch.setenv("TA3000_STAGING_PLAN", str(path))
    return plan, data, save


def test_ui_cannot_widen_producer_parameters_or_cutoff(protected):
    _, data, _ = protected
    with pytest.raises(StagingExecutionError, match="parameters"):
        require_staging_parameters(
            stage="baseline",
            output_path=data,
            parameters={"timeframes": ["1m"], "lookback_days": 365},
        )
    with pytest.raises(StagingExecutionError, match="cutoff"):
        require_staging_parameters(
            stage="baseline",
            output_path=data,
            parameters={"timeframes": ["1d"], "lookback_days": 7},
            input_until_utc="2030-01-03T00:00:00Z",
        )


def test_candidate_cannot_renew_oi_pin_using_reference_behavior(protected):
    plan, data, save = protected
    expected = {
        "daily_open_interest_path": (data / "oi.delta").as_posix(),
        "daily_open_interest_version": 0,
    }
    plan["producer_parameters"]["run_moex_baseline_update"] = expected
    save()
    with pytest.raises(StagingExecutionError, match="parameters"):
        require_staging_parameters(
            stage="run_moex_baseline_update",
            output_path=data,
            parameters={**expected, "daily_open_interest_version": 1},
        )


def test_source_byte_limit_is_checked_from_native_delta_metadata(protected, monkeypatch):
    plan, data, save = protected
    table = data / "input.delta"
    write_deltalake(str(table), pa.table({"value": [1, 2, 3]}))
    # Metadata inspection must not read business rows to establish the cap.
    from deltalake import DeltaTable

    monkeypatch.setattr(
        DeltaTable,
        "to_pyarrow_table",
        lambda *args, **kwargs: pytest.fail("source cap scanned rows"),
    )
    with pytest.raises(StagingExecutionError, match="source bytes"):
        require_staging_source_bytes(output_path=data, input_paths=[table])
    plan["scope"]["max_source_bytes"] = 1024 * 1024
    save()
    require_staging_source_bytes(output_path=data, input_paths=[table, table])


def test_source_cap_uses_actual_snapshot_and_explicit_version(protected):
    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.common.data_snapshots import (
        capture_table,
        create_snapshot,
        snapshot_read_scope,
    )

    plan, data, save = protected
    table = data / "versioned.delta"
    write_deltalake(str(table), pa.table({"value": [f"{i:064x}" * 4 for i in range(500)]}))
    old = DeltaTable(str(table))
    old_size = sum(old.get_add_actions().column("size_bytes").to_pylist())
    manifest = create_snapshot(
        tables={"input": capture_table(table)},
        required_tables=["input"],
        dataset_version="test",
        run_id="test",
        code_version="test",
        quality_checks={"test": "PASS"},
    )
    write_deltalake(str(table), pa.table({"value": ["small"]}), mode="overwrite")
    new_size = sum(DeltaTable(str(table)).get_add_actions().column("size_bytes").to_pylist())
    assert old_size > new_size
    plan["scope"]["max_source_bytes"] = (old_size + new_size) // 2
    save()
    require_staging_source_bytes(output_path=data, input_paths=[table])
    with snapshot_read_scope(manifest), pytest.raises(StagingExecutionError, match="source bytes"):
        require_staging_source_bytes(output_path=data, input_paths=[table])
    with pytest.raises(StagingExecutionError, match="source bytes"):
        require_staging_source_bytes(output_path=data, input_paths=[(table, 0)])


def test_raw_in_memory_payload_cannot_bypass_source_cap(protected):
    _, data, _ = protected
    with pytest.raises(StagingExecutionError, match="source bytes"):
        require_staging_source_bytes(
            output_path=data, input_paths=[], input_values=[[{"large": "x" * 1000}]]
        )
