from __future__ import annotations

import inspect
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_staging_plan as preparation  # noqa: E402


def test_actual_native_asset_parameters_captured_without_entering_producer(tmp_path, monkeypatch):
    from trading_advisor_3000.dagster_defs import moex_historical_assets as assets

    original = assets.run_moex_baseline_update
    entered = []

    def forbidden_producer(**kwargs):
        entered.append(kwargs)
        raise AssertionError("Native producer body must never run during preparation")

    forbidden_producer.__signature__ = inspect.signature(original)
    monkeypatch.setattr(assets, "run_moex_baseline_update", forbidden_producer)
    monkeypatch.setattr(assets, "registered_history_refresh_kwargs", lambda root: {})
    monkeypatch.setattr(assets, "_route_artifact_root", lambda: tmp_path)
    instant = datetime(2026, 10, 24, 23, tzinfo=UTC)  # 02:00 MSK native cron.
    run_config = assets._build_baseline_daily_schedule_run_config(
        SimpleNamespace(scheduled_execution_time=instant)
    )
    config = run_config["ops"]["moex_baseline_update"]["config"]
    parameters, cutoff = preparation.capture_parameters(assets, config)
    assert not entered
    assert assets.run_moex_baseline_update is forbidden_producer
    assert parameters["stability_lag_minutes"] == 0
    assert parameters["refresh_window_days"] == config["refresh_window_days"]
    assert parameters["timeframes"] == sorted(["1m", "5m", "15m", "1h", "4h", "1d", "1w"])
    assert parameters["raw_table_path"] == str(
        (tmp_path / "raw/moex/baseline-4y-current/raw_moex_history.delta").resolve()
    ).replace("\\", "/")
    assert parameters["allow_run_id_retry"] is False
    assert parameters["daily_open_interest_version"] is None
    assert cutoff == "2026-10-24T21:00:00Z"
    assert "run_id" not in parameters and "ingest_till_utc" not in parameters
    # The host receives the full native config, including dynamic fields and any
    # outer Dagster execution settings; Linux paths must survive Windows callers.
    config["raw_table_path"] = "/ta3000-data/moex-historical/raw/moex/raw.delta"
    run_config["execution"] = {"config": {"in_process": {}}}
    expected = json.loads(json.dumps(run_config))
    metadata = preparation.native_schedule_metadata(assets, instant, run_config)
    assert metadata["run_config"] is run_config
    assert metadata["run_config"] == expected
    assert (
        metadata["run_config"]["ops"]["moex_baseline_update"]["config"]["run_id"]
        == config["run_id"]
    )


def test_capture_restores_native_entry_after_configuration_failure(tmp_path, monkeypatch):
    from trading_advisor_3000.dagster_defs import moex_historical_assets as assets

    original = assets.run_moex_baseline_update
    monkeypatch.setattr(assets, "_route_artifact_root", lambda: tmp_path)
    with pytest.raises(RuntimeError, match="requires integer"):
        preparation.capture_parameters(assets, {"refresh_window_days": "bad"})
    assert assets.run_moex_baseline_update is original


@pytest.mark.parametrize(
    "value", ["2026-10-24T23:01:00Z", "2026-10-24T23:00:00", "2050-10-24T23:00:00Z"]
)
def test_schedule_preparation_rejects_arbitrary_or_naive_clock(value):
    assets = SimpleNamespace(
        MOEX_BASELINE_DAILY_CRON="0 2 * * *", MOEX_HISTORICAL_EXECUTION_TIMEZONE="Europe/Moscow"
    )
    with pytest.raises(ValueError):
        preparation.scheduled_instant(assets, value)


def test_schedule_preparation_uses_native_cron_instant():
    assets = SimpleNamespace(
        MOEX_BASELINE_DAILY_CRON="0 2 * * *", MOEX_HISTORICAL_EXECUTION_TIMEZONE="Europe/Moscow"
    )
    assert preparation.scheduled_instant(assets, "2026-10-24T23:00:00Z").hour == 23


def test_next_native_cron_is_only_a_future_metadata_proposal():
    from dagster._utils.schedules import cron_string_iterator

    assets = SimpleNamespace(
        MOEX_BASELINE_DAILY_CRON="0 2 * * *", MOEX_HISTORICAL_EXECUTION_TIMEZONE="Europe/Moscow"
    )
    future = next(cron_string_iterator(datetime.now(UTC).timestamp(), "0 2 * * *", "Europe/Moscow"))
    assert preparation.scheduled_instant(assets, future.isoformat()) == future
    proposal = preparation.native_schedule_metadata(assets, future, {})
    assert proposal["preparation"] == "metadata-only; not execution or acceptance"
