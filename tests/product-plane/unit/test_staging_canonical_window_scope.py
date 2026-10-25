"""All effective canonical windows must pass native scope checks before Spark."""

import json
from copy import deepcopy

import pytest

from trading_advisor_3000.product_plane.common import staging_execution as guard
from trading_advisor_3000.spark_jobs.moex_canonicalization_job import (
    run_moex_canonicalization_spark_delta_job,
)


@pytest.fixture
def protected_windows(tmp_path, monkeypatch):
    plan = {
        "attempt_kind": "acceptance",
        "scope": {
            "instrument_ids": ["BR"],
            "timeframes": ["1d"],
            "start_utc": "2030-01-01T00:00:00Z",
            "end_utc": "2030-01-11T00:00:00Z",
            "max_changed_window_days": 10,
        },
    }
    monkeypatch.setattr(guard, "_plan", lambda *a: plan)
    monkeypatch.setattr(guard, "_require_permit", lambda *a: None)
    monkeypatch.setattr(
        guard,
        "_consume_grant",
        lambda *a: (_ for _ in ()).throw(RuntimeError("native owner boundary")),
    )
    token = guard._ACTIVE.set(None)
    window = {
        "internal_id": "BR",
        "source_timeframe": "1d",
        "window_start_utc": "2030-01-09T00:00:00Z",
        "window_end_utc": "2030-01-10T00:00:00Z",
    }
    path = tmp_path / "effective-windows.jsonl"
    kwargs = dict(
        raw_table_path=tmp_path / "raw.delta",
        changed_windows_path=path,
        selected_source_intervals_path=tmp_path / "selected.jsonl",
        session_intervals_path=None,
        output_dir=tmp_path / "output",
        build_run_id="metadata-fixture",
        built_at_utc="2030-01-10T00:00:00Z",
        spark_session_factory=lambda *a: pytest.fail("Spark started"),
    )
    yield path, window, kwargs
    guard._ACTIVE.reset(token)


@pytest.mark.parametrize(
    "change",
    [
        {"internal_id": "OTHER"},
        {"source_timeframe": "1m"},
        {"window_start_utc": "2029-12-01T00:00:00Z"},
        {"window_end_utc": "2030-01-12T00:00:00Z"},
    ],
)
def test_retained_pending_window_outside_scope_is_rejected_before_native_claim_or_spark(
    protected_windows, change
):
    path, current, kwargs = protected_windows
    pending = {**deepcopy(current), **change}
    path.write_text("\n".join(json.dumps(row) for row in (current, pending)))
    with pytest.raises(guard.StagingExecutionError, match="window exceeds"):
        run_moex_canonicalization_spark_delta_job(**kwargs)
    assert not kwargs["output_dir"].exists()


def test_valid_effective_windows_reach_owner_check_once_without_spark(protected_windows):
    path, current, kwargs = protected_windows
    path.write_text(json.dumps(current) + "\n")
    with pytest.raises(RuntimeError, match="native owner boundary"):
        run_moex_canonicalization_spark_delta_job(**kwargs)
    assert not kwargs["output_dir"].exists()


@pytest.mark.parametrize("value", ['{"broken"', "[]"])
def test_malformed_effective_window_metadata_fails_closed_before_claim(protected_windows, value):
    path, _, kwargs = protected_windows
    path.write_text(value)
    with pytest.raises(ValueError):
        run_moex_canonicalization_spark_delta_job(**kwargs)
    assert not kwargs["output_dir"].exists()


def test_effective_windows_file_byte_cap_precedes_json_parse_and_native_claim(
    protected_windows, monkeypatch
):
    from trading_advisor_3000.product_plane.common import staging_source_scope as source

    path, _, kwargs = protected_windows
    plan = guard._plan(kwargs["output_dir"])
    plan["scope"]["max_source_bytes"] = 1
    monkeypatch.setattr(source, "_plan", lambda *a: plan)
    path.write_text("invalid JSON must never be parsed")
    with pytest.raises(guard.StagingExecutionError, match="source bytes"):
        run_moex_canonicalization_spark_delta_job(**kwargs)
    assert not kwargs["output_dir"].exists()
