from __future__ import annotations

import hashlib
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


@pytest.fixture
def research_recipe(tmp_path, monkeypatch):
    from trading_advisor_3000.dagster_defs import research_assets as assets
    from trading_advisor_3000.product_plane.research import admission

    fixture = dict(
        manifest_sha256="f" * 64,
        instrument_ids=["BR", "Si"],
        timeframes=["15m", "1h", "4h", "1d"],
        warmup_bars=512,
        start_utc="2024-10-20T21:00:00Z",
        end_utc="2026-03-16T21:00:00Z",
        data_manifest_path=str(tmp_path / "fixture/canonical/.data_snapshots/manifests/data.json"),
        canonical_dir=str(tmp_path / "fixture/canonical"),
        raw_dir=str(tmp_path / "fixture/raw"),
        data_snapshot_id="data-fixture",
        data_manifest_sha256="d" * 64,
        dataset_version="branch-fixture-v1",
    )
    entered, captured = [], {}

    def forbidden(*args, **kwargs):
        entered.append((args, kwargs))
        raise AssertionError("Preparation must never execute a Research producer")

    for name in (*preparation.RESEARCH_STEPS, "materialize_research_dataset"):
        monkeypatch.setattr(assets, name, forbidden, raising=False)
    native_builder = assets._build_moex_cf_catch_up_run_config
    native_admission = admission.configure_admission

    def builder(**kwargs):
        bound = inspect.signature(native_builder).bind(**kwargs)
        bound.apply_defaults()
        captured["builder"] = bound.arguments
        return native_builder(**kwargs)

    def admit(config, **kwargs):
        # Capture native admission's metadata call, without fixture Delta I/O here.
        bound = inspect.signature(native_admission).bind(config, **kwargs)
        bound.apply_defaults()
        captured["admission"] = bound.arguments
        assert kwargs == {"operation": "rebuild"}
        assert config["changed_windows"] == []
        return dict(
            config,
            operation="rebuild",
            force_full_refresh=True,
            refresh_scope_version=1,
            refresh_admission={"fixture_metadata_capture": True},
        )

    monkeypatch.setattr(assets, "_build_moex_cf_catch_up_run_config", builder)
    monkeypatch.setattr(admission, "configure_admission", admit)
    return assets, fixture, captured, entered


def test_research_branch_recipe_uses_native_builders_without_entering_producers(
    tmp_path, research_recipe
):
    assets, fixture, captured, entered = research_recipe
    run_id = "40c6c40f-76b4-49b6-bb96-79178d9ca8ea"
    recipe = preparation.native_research_run_config(assets, fixture, tmp_path / "outputs", run_id)
    assert not entered
    assert captured["builder"]["_admission_probe"] is True
    assert len(captured["builder"]["windows"]) == 8
    assert set(recipe["ops"]) == set(assets._RESEARCH_DATA_PREP_CONFIG_OPS)
    configs = [op["config"] for op in recipe["ops"].values()]
    assert all(config == configs[0] for config in configs)
    config = configs[0]
    assert config["campaign_run_id"] == run_id
    assert config["dataset_instrument_ids"] == fixture["instrument_ids"]
    assert config["timeframes"] == fixture["timeframes"]
    assert config["operation"] == "rebuild" and config["force_full_refresh"] is True
    assert config["canonical_output_dir"] == Path(fixture["canonical_dir"]).resolve().as_posix()
    assert (
        config["data_snapshot_manifest_path"]
        == Path(fixture["data_manifest_path"]).resolve().as_posix()
    )
    assert config["data_snapshot_raw_output_dir"] == Path(fixture["raw_dir"]).resolve().as_posix()
    assert (
        config["volume_profile_raw_1m_table_path"]
        == (Path(fixture["raw_dir"]) / "raw_moex_history.delta").as_posix()
    )
    assert (
        config["canonical_contract_economics_path"]
        == (Path(fixture["canonical_dir"]) / "canonical_contract_economics.delta").as_posix()
    )
    assert Path(config["materialized_output_dir"]).is_relative_to(tmp_path / "outputs/research")
    assert Path(config["results_output_dir"]).is_relative_to(tmp_path / "outputs/research")


@pytest.fixture
def research_plan_args(tmp_path, monkeypatch, research_recipe):
    from trading_advisor_3000.dagster_defs import moex_historical_assets
    from trading_advisor_3000.product_plane.common import staging_test_fixture

    _, fixture, _, _ = research_recipe
    root = tmp_path / "output-data"
    monkeypatch.setattr(moex_historical_assets, "_route_artifact_root", lambda: root)
    manifest = tmp_path / "fixture/manifest.json"
    manifest.parent.mkdir()
    manifest.write_text("{}", encoding="utf-8")
    fixture_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
    fixture["manifest_sha256"] = fixture_sha
    calls = []

    def verify(path, expected_sha256):
        assert path == manifest.parent
        assert expected_sha256 == fixture_sha
        calls.append(path)
        return fixture

    monkeypatch.setattr(staging_test_fixture, "verify_fixture", verify)
    monkeypatch.setattr(
        staging_test_fixture, "prepare_fixture", lambda *a: pytest.fail("fixture generation")
    )
    monkeypatch.setattr(
        preparation, "capture_parameters", lambda *a: pytest.fail("baseline capture")
    )
    monkeypatch.setattr(
        preparation, "native_root_run_config", lambda *a: pytest.fail("baseline recipe")
    )
    consumer = tmp_path / "consumer.json"
    consumer.write_text(
        json.dumps(
            dict(
                dataset_version=fixture["dataset_version"],
                contour_id="pit_active_front",
                indicator_set_version="indicators-v1",
                timeframe="1d",
                instrument_ids=["BR", "Si"],
                price_columns=["close"],
            )
        ),
        encoding="utf-8",
    )
    candidate = tmp_path / "candidate.json"
    candidate.write_text(
        json.dumps(
            dict(
                schema_version="staging_candidate.v1",
                candidate=dict(
                    base_sha="a" * 40,
                    head_sha="b" * 40,
                    tree_sha="c" * 40,
                    branch_heads={"refs/heads/fixture": "b" * 40},
                    executable_sha256="d" * 64,
                    config_sha256="e" * 64,
                    image_digest="sha256:" + "f" * 64,
                ),
            )
        ),
        encoding="utf-8",
    )
    args = SimpleNamespace(
        stage="branch",
        branch_target="research",
        fixture_manifest=manifest,
        research_manifest=None,
        small_attempts=2,
        deployed_root=Path(__file__).resolve().parents[2],
        data_root=root,
        scheduled_at="next",
        deadline="2035-01-01T00:00:00Z",
        consumer=consumer,
        candidate=candidate,
        task_id="fixture-metadata-test",
        quota_home=tmp_path / "quota",
        max_runtime_seconds=600,
        max_source_bytes=64 * 1024**2,
        verifier_revision="a" * 40,
    )
    return args, fixture, calls


def test_research_plan_binds_fixture_single_root_and_one_manual_run_id(
    monkeypatch, research_plan_args, research_recipe
):
    args, fixture, verified = research_plan_args
    _, _, _, entered = research_recipe
    run_id = "40c6c40f-76b4-49b6-bb96-79178d9ca8ea"
    calls = []
    monkeypatch.setattr(preparation, "uuid4", lambda: calls.append(run_id) or run_id)
    plan = preparation.prepare(args)
    assert not entered and len(verified) == 1 and calls == [run_id]
    assert plan["limits"] == preparation.initial_limits("branch", 2)
    assert plan["root_job"] == "research_data_prep_job"
    assert plan["jobs"] == {"research_data_prep_job": {"upstream_job": None, "max_runs": 1}}
    assert plan["native_steps"] == {
        **dict.fromkeys(preparation.RESEARCH_STEPS, 1),
        "materialize_research_dataset": 2,
    }
    assert plan["attempt_kind"] == "small" and plan["require_scheduled"] is False
    assert "producer_parameters" not in plan
    source = plan["source"]
    assert source["fixture"] == {
        "manifest_path": "/staging-control/fixture/manifest.json",
        "manifest_sha256": fixture["manifest_sha256"],
    }
    assert source["data_manifest_path"] == fixture["data_manifest_path"]
    assert source["snapshot_id"] == fixture["data_snapshot_id"]
    assert source["native_schedule"]["manual_run_id"] == run_id
    assert "research_manifest_path" not in source
    for op in source["native_schedule"]["run_config"]["ops"].values():
        assert op["config"]["campaign_run_id"] == run_id
        assert (
            op["config"]["warmup_bars"] == research_recipe[0].DEFAULT_RESEARCH_DATA_PREP_WARMUP_BARS
        )
    assert plan["scope"]["instrument_ids"] == fixture["instrument_ids"]
    assert plan["scope"]["timeframes"] == fixture["timeframes"]


@pytest.mark.parametrize(
    "change",
    [
        {"instrument_ids": []},
        {"instrument_ids": ["OTHER"]},
        {"timeframe": "1m"},
        {"dataset_version": "other"},
        {"price_columns": []},
        {"indicator_set_version": "other"},
    ],
)
def test_research_plan_refuses_consumer_outside_fixture(research_plan_args, change):
    args, _, _ = research_plan_args
    consumer = json.loads(args.consumer.read_text("utf-8"))
    consumer.update(change)
    args.consumer.write_text(json.dumps(consumer), encoding="utf-8")
    with pytest.raises(ValueError, match="[Cc]onsumer"):
        preparation.prepare(args)


@pytest.mark.parametrize(
    "stage,fixture", [("branch", None), ("acceptance", Path("fixture/manifest.json"))]
)
def test_research_target_requires_branch_and_fixture_before_native_metadata(stage, fixture):
    args = SimpleNamespace(
        stage=stage, small_attempts=2, branch_target="research", fixture_manifest=fixture
    )
    with pytest.raises(ValueError, match="requires branch stage"):
        preparation.prepare(args)


def test_research_cli_uses_fixture_without_research_seed_manifest(tmp_path, monkeypatch):
    captured = {}
    monkeypatch.setattr(preparation, "prepare", lambda args: captured.update(vars(args)) or {})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare",
            "--stage",
            "branch",
            "--branch-target",
            "research",
            "--small-attempts",
            "2",
            "--fixture-manifest",
            str(tmp_path / "manifest.json"),
            "--candidate",
            str(tmp_path / "candidate.json"),
            "--consumer",
            str(tmp_path / "consumer.json"),
            "--data-root",
            str(tmp_path / "output"),
            "--task-id",
            "fixture-test",
            "--deadline",
            "2035-01-01T00:00:00Z",
            "--verifier-revision",
            "a" * 40,
            "--max-runtime-seconds",
            "600",
            "--max-source-bytes",
            "1024",
        ],
    )
    assert preparation.main() == 0
    assert captured["research_manifest"] is None and captured["branch_target"] == "research"
    assert captured["small_attempts"] == 2


@pytest.mark.parametrize(
    "change",
    [
        {"instrument_ids": ["BR"]},
        {"warmup_bars": 513},
        {"derived_indicator_set_version": "other"},
        {"contour_id": "native_tradable"},
    ],
)
def test_research_fixture_rejects_partial_or_unsupported_consumer(research_plan_args, change):
    args, _, _ = research_plan_args
    consumer = json.loads(args.consumer.read_text("utf-8"))
    consumer.update(change)
    args.consumer.write_text(json.dumps(consumer), encoding="utf-8")
    with pytest.raises(ValueError, match="[Cc]onsumer"):
        preparation.prepare(args)


def test_manual_fixture_deadline_can_precede_next_daily_cron(monkeypatch, research_plan_args):
    args, _, _ = research_plan_args
    now = datetime(2030, 1, 1, 12, tzinfo=UTC)

    class MetadataClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(preparation, "datetime", MetadataClock)
    monkeypatch.setattr(
        preparation,
        "scheduled_instant",
        lambda *a, **kw: pytest.fail("manual fixture queried cron"),
    )
    args.deadline = (now + timedelta(minutes=10)).isoformat()
    plan = preparation.prepare(args)
    holder = plan["source"]["native_schedule"]
    assert holder["trigger"] == "manual_fixture"
    assert holder["logical_scheduled_at_utc"] == now.isoformat()
    assert "not a scheduled tick" in holder["preparation"]
    assert datetime.fromisoformat(plan["deadline_utc"]) < now.replace(hour=23)
    assert plan["require_scheduled"] is False
    for op in holder["run_config"]["ops"].values():
        assert op["config"]["warmup_bars"] == 300


def test_manual_fixture_refuses_explicit_scheduled_at(research_plan_args):
    args, _, _ = research_plan_args
    args.scheduled_at = "2030-01-01T23:00:00Z"
    with pytest.raises(ValueError, match="Manual Research fixture"):
        preparation.prepare(args)


def test_research_fixture_refuses_too_small_source_cap_before_native_metadata(research_plan_args):
    args, _, _ = research_plan_args
    args.max_source_bytes = 1
    with pytest.raises(ValueError, match="source byte cap"):
        preparation.prepare(args)
