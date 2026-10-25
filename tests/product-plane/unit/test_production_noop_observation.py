"""Real native metadata fixture witnesses; no producer or fake originRunIds."""

import sys
import time
from datetime import datetime
from pathlib import Path

import pytest
from dagster._core.storage.tags import SCHEDULED_EXECUTION_TIME_TAG

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
from observe_production_noop import CORE, _source_facts, native_noop  # noqa: E402
from test_staging_reference_observation import CF_SENSOR, JOBS  # noqa: E402
from test_staging_reference_observation import recorded_cycle as recorded_cycle  # noqa: E402


def observe(cycle):
    root = cycle.instance.get_run_by_id("root")
    stamp = datetime.fromisoformat(root.tags[SCHEDULED_EXECUTION_TIME_TAG]).timestamp()
    return native_noop(cycle.instance, "root", stamp, "0 0 * * *")


def test_real_global_success_cursor_and_explicit_native_noop_are_required(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root")
    tick = cycle.sensor_tick(
        CF_SENSOR, "root", reason="NOOP: proven_no_changes; admission=" + "a" * 64
    )
    assert not tick.origin_run_ids
    result = observe(cycle)
    assert result["status"] == "PASS-NOOP" and result["compute_attempts"] == 0
    assert result["sensor"]["tick_id"] == str(tick.tick_id)
    assert result["sensor"]["cursor"] == tick.cursor


@pytest.mark.parametrize("damage", ["unrelated", "blocked", "empty", "child"])
def test_wrong_cursor_blocked_or_missing_explicit_noop_never_grants_witness(recorded_cycle, damage):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root")
    target = "root"
    if damage == "unrelated":
        cycle.record(JOBS[0], "another")
        target = "another"
    reason = {
        "blocked": "BLOCKED: missing_accepted_input",
        "empty": "Sensor function returned an empty result",
    }.get(damage, "NOOP: proven_no_changes; admission=" + "a" * 64)
    cycle.sensor_tick(CF_SENSOR, target, reason=reason)
    if damage == "child":
        cycle.record(JOBS[1], "cf", upstream="root")
        assert observe(cycle)["status"] == "NOT_NOOP"
    else:
        with pytest.raises(ValueError):
            observe(cycle)


def test_later_same_cursor_empty_poll_preserves_real_noop_but_blocked_poll_denies(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root")
    original = cycle.sensor_tick(
        CF_SENSOR, "root", reason="NOOP: proven_no_changes; admission=" + "a" * 64
    )
    # Add actual native tick objects; retain the exact cursor, never originRunIds.
    from dagster._core.scheduler.instigation import TickStatus

    later = cycle.instance.create_tick(
        original.tick_data._replace(
            timestamp=original.timestamp + 1,
            status=TickStatus.SKIPPED,
            skip_reason="Sensor function returned an empty result",
        )
    )
    assert observe(cycle)["status"] == "PASS-NOOP"
    cycle.instance.update_tick(
        later.with_status(TickStatus.SKIPPED, skip_reason="BLOCKED: scope changed")
    )
    with pytest.raises(ValueError):
        observe(cycle)


def test_source_fence_covers_complete_main_wal_set_and_byte_identity(tmp_path):
    for name in CORE:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"native-fixture")
    before = _source_facts(tmp_path, list(CORE), time.monotonic() + 10)
    assert before == _source_facts(tmp_path, list(CORE), time.monotonic() + 10)
    (tmp_path / CORE[0]).write_bytes(b"changed-facts!")
    assert before != _source_facts(tmp_path, list(CORE), time.monotonic() + 10)
    (tmp_path / (CORE[0] + "-wal")).write_bytes(b"new-wal")
    with pytest.raises(ValueError, match="family set changed"):
        _source_facts(tmp_path, list(CORE), time.monotonic() + 10)
    with pytest.raises(ValueError):
        _source_facts(tmp_path, ["../outside"], time.monotonic() + 10)


def test_newer_tick_with_changed_origin_cannot_reuse_older_noop(recorded_cycle):
    cycle = recorded_cycle
    cycle.record(JOBS[0], "root")
    old = cycle.sensor_tick(
        CF_SENSOR, "root", reason="NOOP: proven_no_changes; admission=" + "a" * 64
    )
    cycle.instance.create_tick(
        old.tick_data._replace(timestamp=old.timestamp + 1, origin_run_ids=["foreign"])
    )
    with pytest.raises(ValueError, match="origin changed"):
        observe(cycle)
