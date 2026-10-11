from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from trading_advisor_3000.product_plane.common import stage_timings as timing


@pytest.fixture
def events(caplog):
    logger = logging.getLogger("ta3000.pipeline_timing")
    logger.addHandler(caplog.handler)
    yield lambda: [json.loads(record.message) for record in caplog.records]
    logger.removeHandler(caplog.handler)


def test_nested_steps_preserve_result_and_correlate_events(events):
    result = {"status": "PASS"}

    @timing.timed_step("canonical")
    def child():
        stages = {}
        started = timing.stage_timer("read")
        timing.record_stage_timing(stages, "read", started, row_count=12)
        timing.record_skipped_stage(stages, "write", reason="reuse")
        return result

    @timing.timed_step("pipeline")
    def parent(*, run_id):
        return child()

    assert parent(run_id="test-run") is result
    rows = events()
    assert [row["event"] for row in rows] == [
        "step_started",
        "step_started",
        "stage_started",
        "stage_finished",
        "stage_finished",
        "step_finished",
        "step_finished",
    ]
    assert {row["run_id"] for row in rows} == {"test-run"}
    assert rows[1]["parent_step_id"] == rows[0]["step_id"]
    assert rows[3]["row_count"] == 12
    assert rows[4]["status"] == "SKIPPED"
    assert rows[4]["elapsed_seconds"] == 0.0
    assert all(row.get("elapsed_seconds", 0) >= 0 for row in rows)


def test_failure_closes_active_stage_and_preserves_exception(events):
    error = ValueError("do not include payload in telemetry")

    @timing.timed_step("raw")
    def fail(*, run_id):
        timing.stage_timer("merge")
        raise error

    with pytest.raises(ValueError) as caught:
        fail(run_id="failed-run")
    assert caught.value is error
    rows = events()
    assert rows[-2]["stage"] == "merge"
    assert rows[-2]["status"] == rows[-1]["status"] == "FAIL"
    assert rows[-2]["error_type"] == "ValueError"
    assert "do not include payload" not in json.dumps(rows)


def test_stage_context_records_failure_before_cleanup(events):
    stages = {}
    with pytest.raises(RuntimeError):
        with timing.timed_stage(stages, "compute"):
            raise RuntimeError("failed")
    assert stages["compute"]["status"] == "FAIL"
    assert events()[-1]["error_type"] == "RuntimeError"


def test_report_blocked_status_is_not_logged_as_pass(events):
    @timing.timed_step("publish")
    def publish():
        return {"status": "BLOCKED"}

    assert publish()["status"] == "BLOCKED"
    assert events()[-1]["status"] == "BLOCKED"


def test_started_then_skipped_stage_is_not_left_incomplete(events):
    @timing.timed_step("publish")
    def publish():
        timing.stage_timer("sidecars")
        timing.record_skipped_stage({}, "sidecars", reason="no changes")

    publish()
    finished = [row for row in events() if row["event"] == "stage_finished"]
    assert len(finished) == 1
    assert finished[0]["status"] == "SKIPPED"


def test_logging_failure_does_not_break_calculation(monkeypatch):
    def broken(*args, **kwargs):
        raise OSError("log sink unavailable")

    monkeypatch.setattr(logging.getLogger("ta3000.pipeline_timing"), "log", broken)

    @timing.timed_step("compute")
    def compute():
        with timing.timed_stage({}, "read"):
            return 42

    assert compute() == 42


def test_step_context_does_not_leak_between_calls(events):
    @timing.timed_step("compute")
    def compute(*, run_id):
        return {}

    compute(run_id="one")
    compute(run_id="two")
    starts = [row for row in events() if row["event"] == "step_started"]
    assert [row["run_id"] for row in starts] == ["one", "two"]
    assert all(row["parent_step_id"] is None for row in starts)
    assert starts[0]["step_id"] != starts[1]["step_id"]


def test_concurrent_steps_keep_separate_run_contexts(events):
    barrier = Barrier(2)

    @timing.timed_step("parallel")
    def compute(run_id):
        barrier.wait(timeout=5)
        started = timing.stage_timer("compute")
        timing.record_stage_timing({}, "compute", started, owner=run_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(compute, ["one", "two"]))
    finished = [row for row in events() if row["event"] == "stage_finished"]
    assert len(finished) == 2
    assert all(row["owner"] == row["run_id"] for row in finished)


def test_elapsed_time_uses_monotonic_clock(events, monkeypatch):
    clock = iter([10.0, 12.5])
    monkeypatch.setattr(timing, "perf_counter", lambda: next(clock))
    stages = {}
    started = timing.stage_timer("read")
    timing.record_stage_timing(stages, "read", started)
    assert stages["read"]["elapsed_seconds"] == 2.5
    assert events()[-1]["elapsed_seconds"] == 2.5


@pytest.mark.parametrize("metric_key", ["event", "event_name"])
def test_reserved_metrics_preserve_result_and_canonical_envelope(events, metric_key):
    stages = {}
    result = object()
    metrics = {
        metric_key: "caller-metric",
        "schema_version": "spoofed",
        "timestamp_utc": "spoofed",
        "pid": "spoofed",
        "step": "spoofed",
        "step_id": "spoofed",
        "parent_step_id": "spoofed",
        "run_id": "spoofed",
        "dataset_version": "spoofed",
    }

    @timing.timed_step("compute")
    def compute(*, run_id, dataset_version):
        with timing.timed_stage(stages, "read", **metrics):
            return result

    assert compute(run_id="actual-run", dataset_version="actual-version") is result
    assert stages["read"][metric_key] == "caller-metric"
    rows = events()
    finished = next(row for row in rows if row["event"] == "stage_finished")
    assert finished["schema_version"] == "pipeline_timing.v1"
    assert finished["timestamp_utc"].endswith("+00:00")
    assert isinstance(finished["pid"], int)
    assert finished["step"] == "compute"
    assert finished["step_id"] == rows[0]["step_id"]
    assert finished["parent_step_id"] is None
    assert finished["run_id"] == "actual-run"
    assert finished["dataset_version"] == "actual-version"


@pytest.mark.parametrize("metric_key", ["event", "event_name"])
def test_reserved_metrics_do_not_mask_original_stage_exception(events, metric_key):
    error = ValueError("original error")
    stages = {}
    with pytest.raises(ValueError) as caught:
        with timing.timed_stage(stages, "compute", **{metric_key: "caller-metric"}):
            raise error
    assert caught.value is error
    assert stages["compute"]["status"] == "FAIL"
    assert events()[-1]["event"] == "stage_finished"
    assert events()[-1]["error_type"] == "ValueError"


def test_logging_failure_preserves_original_exception(monkeypatch):
    error = RuntimeError("original error")

    def broken(*args, **kwargs):
        raise OSError("log sink unavailable")

    monkeypatch.setattr(timing.LOGGER, "log", broken)

    @timing.timed_step("compute")
    def compute():
        with timing.timed_stage({}, "read"):
            raise error

    with pytest.raises(RuntimeError) as caught:
        compute()
    assert caught.value is error


def test_runtime_exports_share_common_context_and_legacy_reports(events):
    from trading_advisor_3000.product_plane.runtime import stage_timings as legacy

    assert set(legacy.__all__) == {
        "StageTimings",
        "stage_timer",
        "record_stage_timing",
        "record_skipped_stage",
        "timed_stage",
        "timed_step",
    }
    for name in legacy.__all__:
        assert getattr(legacy, name) is getattr(timing, name)
    stages = {}

    @legacy.timed_step("legacy")
    def compute(*, run_id):
        started = legacy.stage_timer()
        timing.record_stage_timing(stages, "read", started, row_count=12)
        legacy.record_skipped_stage(stages, "write", reason="reuse")
        return stages

    assert compute(run_id="mixed-run") is stages
    assert set(stages["read"]) == {"status", "elapsed_seconds", "row_count"}
    assert stages["write"] == {"status": "SKIPPED", "elapsed_seconds": 0.0, "reason": "reuse"}
    assert {row["run_id"] for row in events()} == {"mixed-run"}


def test_default_ids_override_parent_then_restore_parent_context(events):
    @timing.timed_step("default-child")
    def child(*, run_id="configured-run", dataset_version="configured-version"):
        return 42

    @timing.timed_step("inherited-child")
    def inherited():
        return 42

    @timing.timed_step("parent")
    def parent(*, run_id, dataset_version):
        assert child() == inherited() == 42

    parent(run_id="parent-run", dataset_version="parent-version")
    starts = [row for row in events() if row["event"] == "step_started"]
    assert [(row["run_id"], row["dataset_version"]) for row in starts] == [
        ("parent-run", "parent-version"),
        ("configured-run", "configured-version"),
        ("parent-run", "parent-version"),
    ]
    assert starts[1]["parent_step_id"] == starts[2]["parent_step_id"] == starts[0]["step_id"]
    assert events()[-1]["run_id"] == "parent-run"


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("same_stage", [False, True])
def test_equal_clock_readings_do_not_lose_pending_stages(events, monkeypatch, failed, same_stage):
    monkeypatch.setattr(timing, "perf_counter", lambda: 10.0)
    error = ValueError("original error")
    second_stage = "read" if same_stage else "write"

    @timing.timed_step("compute")
    def compute():
        assert isinstance(timing.stage_timer("read"), float)
        assert isinstance(timing.stage_timer(second_stage), float)
        if failed:
            raise error
        return 42

    if failed:
        with pytest.raises(ValueError) as caught:
            compute()
        assert caught.value is error
    else:
        assert compute() == 42
    stages = [row for row in events() if row["event"] == "stage_finished"]
    assert [row["stage"] for row in stages] == ["read", second_stage]
    assert {row["status"] for row in stages} == ({"FAIL"} if failed else {"INCOMPLETE"})


def test_false_success_report_is_logged_as_failure(events):
    result = {"success": False}

    @timing.timed_step("compute")
    def compute():
        return result

    assert compute() is result
    assert events()[-1]["status"] == "FAIL"


@pytest.mark.parametrize("failed", [False, True])
def test_yielded_reserved_metrics_preserve_stage_and_exception(events, failed):
    stages = {}
    result = object()
    error = ValueError("original error")

    def compute():
        with timing.timed_stage(stages, "compute") as metrics:
            metrics.update(
                event="spoofed", stage="spoofed", started_at="spoofed", stage_timings="spoofed"
            )
            if failed:
                raise error
            return result

    if failed:
        with pytest.raises(ValueError) as caught:
            compute()
        assert caught.value is error
    else:
        assert compute() is result
    assert stages["compute"]["stage"] == "spoofed"
    assert stages["compute"]["started_at"] == "spoofed"
    assert stages["compute"]["stage_timings"] == "spoofed"
    row = events()[-1]
    assert row["event"] == "stage_finished"
    assert row["stage"] == "compute"
    assert row["status"] == ("FAIL" if failed else "PASS")


@pytest.mark.parametrize("finish", ["record", "skip"])
def test_equal_clock_completion_removes_only_matching_stage(events, monkeypatch, finish):
    monkeypatch.setattr(timing, "perf_counter", lambda: 10.0)

    @timing.timed_step("compute")
    def compute():
        started = timing.stage_timer("read")
        timing.stage_timer("write")
        if finish == "record":
            timing.record_stage_timing({}, "read", started)
        else:
            timing.record_skipped_stage({}, "read", reason="reuse")

    compute()
    stages = [row for row in events() if row["event"] == "stage_finished"]
    assert [(row["stage"], row["status"]) for row in stages] == [
        ("read", "PASS" if finish == "record" else "SKIPPED"),
        ("write", "INCOMPLETE"),
    ]


def test_failed_nested_step_restores_parent_and_next_root_context(events):
    error = ValueError("original error")

    @timing.timed_step("child")
    def child(*, run_id="child-run", dataset_version="child-version"):
        raise error

    @timing.timed_step("sibling")
    def sibling():
        return 42

    @timing.timed_step("root")
    def root(*, run_id, dataset_version):
        with pytest.raises(ValueError) as caught:
            child()
        assert caught.value is error
        return sibling()

    assert root(run_id="first", dataset_version="v1") == 42
    assert root(run_id="second", dataset_version="v2") == 42
    starts = [row for row in events() if row["event"] == "step_started"]
    assert [(row["run_id"], row["dataset_version"]) for row in starts] == [
        ("first", "v1"),
        ("child-run", "child-version"),
        ("first", "v1"),
        ("second", "v2"),
        ("child-run", "child-version"),
        ("second", "v2"),
    ]
    assert starts[0]["parent_step_id"] is starts[3]["parent_step_id"] is None
    assert starts[0]["step_id"] != starts[3]["step_id"]
