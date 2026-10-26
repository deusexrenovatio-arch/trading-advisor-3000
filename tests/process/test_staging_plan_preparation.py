from __future__ import annotations

import inspect
import json
import sys
from datetime import UTC, datetime, timedelta
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
    assert preparation.scheduled_instant(assets, "2026-10-24T23:00:00Z", stage="branch").hour == 23


def test_next_native_cron_is_only_a_future_metadata_proposal():
    from dagster._utils.schedules import cron_string_iterator

    assets = SimpleNamespace(
        MOEX_BASELINE_DAILY_CRON="0 2 * * *", MOEX_HISTORICAL_EXECUTION_TIMEZONE="Europe/Moscow"
    )
    future = next(cron_string_iterator(datetime.now(UTC).timestamp(), "0 2 * * *", "Europe/Moscow"))
    assert preparation.scheduled_instant(assets, future.isoformat()) == future
    proposal = preparation.native_schedule_metadata(assets, future, {})
    assert proposal["preparation"] == "metadata-only; not execution or acceptance"


def test_branch_plan_uses_registered_small_recipe_and_native_clock_only():
    from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (
        build_moex_baseline_run_config_for_instance,
        load_moex_runtime_instances_registry,
    )

    root = Path(__file__).resolve().parents[2]
    instant = datetime(2026, 10, 24, 23, tzinfo=UTC)
    daily = {
        "ops": {
            "moex_baseline_update": {
                "config": {
                    "run_id": "20261024T230000Z",
                    "ingest_till_utc": "2026-10-24T21:00:00Z",
                    "timeframes": "1m,5m,15m,1h,4h,1d,1w",
                    "refresh_window_days": 7,
                    "stability_lag_minutes": 0,
                }
            }
        }
    }
    assets = SimpleNamespace(_build_baseline_daily_schedule_run_config=lambda context: daily)
    captured = preparation.native_root_run_config(assets, instant, "branch", root)
    config = captured["ops"]["moex_baseline_update"]["config"]
    assert config["timeframes"] == "1d"
    assert config["refresh_window_days"] == 1
    assert config["max_changed_window_days"] == 2
    assert config["expand_contract_chain"] is False
    clock = daily["ops"]["moex_baseline_update"]["config"]
    registered = load_moex_runtime_instances_registry(repo_root=root).instance(
        "moex_branch_staging"
    )
    registered_config = build_moex_baseline_run_config_for_instance(
        registered, run_id=clock["run_id"], ingest_till_utc=clock["ingest_till_utc"]
    )
    expected = registered_config["ops"]["moex_baseline_update"]["config"]
    assert expected["stability_lag_minutes"] == 20  # Existing manual branch defaults stay intact.
    expected["stability_lag_minutes"] = clock["stability_lag_minutes"]
    assert captured == registered_config
    closed_daily_candle = datetime.fromisoformat(clock["ingest_till_utc"].replace("Z", "+00:00"))
    assert closed_daily_candle <= closed_daily_candle - timedelta(
        minutes=config["stability_lag_minutes"]
    )
    assert preparation.native_root_run_config(assets, instant, "acceptance", root) is daily


def test_schedule_next_is_real_native_future_cron_even_at_exact_tick():
    assets = SimpleNamespace(
        MOEX_BASELINE_DAILY_CRON="0 2 * * *", MOEX_HISTORICAL_EXECUTION_TIMEZONE="Europe/Moscow"
    )
    now = datetime(2030, 1, 1, 23, tzinfo=UTC)
    assert preparation.scheduled_instant(assets, "next", now=now).astimezone(
        UTC
    ) == now + timedelta(days=1)


def test_acceptance_rejects_elapsed_cron_while_branch_metadata_preserves_it():
    assets = SimpleNamespace(
        MOEX_BASELINE_DAILY_CRON="0 2 * * *", MOEX_HISTORICAL_EXECUTION_TIMEZONE="Europe/Moscow"
    )
    now = datetime(2030, 1, 1, 12, tzinfo=UTC)
    past = "2029-12-31T23:00:00Z"
    with pytest.raises(ValueError, match="future"):
        preparation.scheduled_instant(assets, past, stage="acceptance", now=now)
    assert preparation.scheduled_instant(assets, past, stage="branch", now=now).year == 2029


@pytest.mark.parametrize(
    "change",
    [
        {"daily_window_end_utc": None},
        {"window_start": "2030-01-01T12:00:00Z", "window_end": "2030-01-01T13:00:00Z"},
    ],
)
def test_window_arguments_reject_incomplete_or_mixed_alternatives(change):
    args = SimpleNamespace(
        window_start=None,
        window_end=None,
        daily_window_start_utc="22:00",
        daily_window_end_utc="02:00",
    )
    vars(args).update(change)
    with pytest.raises(ValueError):
        preparation.plan_windows(args)


def test_plan_cli_defaults_next_and_passes_daily_window_flags(tmp_path, monkeypatch):
    captured = {}
    monkeypatch.setattr(preparation, "prepare", lambda args: captured.update(vars(args)) or {})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare",
            "--candidate",
            str(tmp_path / "candidate.json"),
            "--research-manifest",
            str(tmp_path / "source.json"),
            "--consumer",
            str(tmp_path / "consumer.json"),
            "--data-root",
            str(tmp_path / "data"),
            "--task-id",
            "contract-test",
            "--deadline",
            "2035-01-01T00:00:00Z",
            "--verifier-revision",
            "a" * 40,
            "--max-runtime-seconds",
            "60",
            "--max-source-bytes",
            "1024",
            "--daily-window-start-utc",
            "22:00",
            "--daily-window-end-utc",
            "02:00",
        ],
    )
    assert preparation.main() == 0
    assert captured["scheduled_at"] == "next"
    assert preparation.plan_windows(SimpleNamespace(**captured)) == {
        "daily_execution_window_utc": {"start": "22:00", "end": "02:00"}
    }
