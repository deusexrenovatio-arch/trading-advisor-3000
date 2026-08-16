from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from trading_advisor_3000.dagster_defs import research_assets
from trading_advisor_3000.dagster_defs.research_assets import _resolve_research_output_dirs
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research import campaigns
from trading_advisor_3000.product_plane.research.backtests.results import (
    backtest_store_contract,
    results_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import ContinuousFrontPolicy
from trading_advisor_3000.product_plane.research.registry_store import research_registry_root

ROOT = Path(__file__).resolve().parents[3]


def _campaign_payload(
    tmp_path: Path,
    *,
    target_stage: str = "backtest",
    force_rematerialize: bool = False,
) -> dict[str, object]:
    return {
        "campaign_name": "research-campaign-smoke",
        "target_stage": target_stage,
        "canonical_output_dir": (tmp_path / "canonical").as_posix(),
        "materialized_root": (tmp_path / "materialized").as_posix(),
        "runs_root": (tmp_path / "runs").as_posix(),
        "dataset": {
            "dataset_version": "dataset-v1",
            "dataset_name": "dataset",
            "universe_id": "moex-futures",
            "series_mode": "contract",
            "timeframes": ["15m"],
            "base_timeframe": "15m",
            "start_ts": None,
            "end_ts": None,
            "warmup_bars": 10,
            "split_method": "walk_forward",
            "contract_ids": [],
            "instrument_ids": [],
        },
        "profiles": {
            "indicator_set_version": "indicators-v1",
            "indicator_profile_version": "core_v1",
            "derived_indicator_set_version": "derived-v1",
            "derived_indicator_profile_version": "core_v1",
        },
        "strategy_space": {
            "family_keys": ["ma_cross"],
            "template_ids": [],
            "exclude_template_manifest_hashes": [],
            "max_parameter_combinations": 64,
            "search_space_overrides": {},
        },
        "backtest": {
            "param_batch_size": 10,
            "series_batch_size": 2,
            "backtest_timeframe": "15m",
            "fees_bps": 0.0,
            "slippage_bps": 2.5,
            "allow_short": True,
            "window_count": 1,
        },
        "ranking_policy": {
            "policy_id": "research_screen_strict_v1",
            "metric_order": ["sharpe", "profit_factor", "max_drawdown", "total_return"],
            "require_out_of_sample_pass": True,
            "min_trade_count": 12,
            "min_trade_count_per_fold": 4,
            "min_fold_count": 2,
            "max_drawdown_cap": 0.25,
            "min_positive_fold_ratio": 0.67,
            "stress_slippage_bps": 12.5,
            "min_parameter_stability": 0.55,
            "min_slippage_score": 0.6,
        },
        "projection_policy": {
            "selection_policy": "all_policy_pass",
            "max_candidates_per_partition": 1,
            "min_robust_score": 0.0,
            "decision_lag_bars_max": 4,
        },
        "execution": {
            "force_rematerialize": force_rematerialize,
            "raise_on_error": True,
        },
    }


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_run_campaign_route_is_removed_from_operator_surface() -> None:
    assert not hasattr(campaigns, "run_campaign")


def _seed_reusable_materialization(
    materialized_root: Path, *, materialization_key: str = ""
) -> None:
    for table_name in campaigns.DATA_PREP_TABLES:
        log_dir = materialized_root / f"{table_name}.delta" / "_delta_log"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "00000000000000000000.json").write_text("{}", encoding="utf-8")
    if materialization_key:
        (materialized_root / campaigns.MATERIALIZATION_LOCK_FILENAME).write_text(
            json.dumps({"materialization_key": materialization_key}),
            encoding="utf-8",
        )


def _stub_prepare_strategy_space(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_prepare_strategy_space(**_: object) -> SimpleNamespace:
        return SimpleNamespace(
            strategy_space_id="space-v1",
            family_search_specs=[
                SimpleNamespace(
                    to_dict=lambda: {
                        "search_spec_version": "search-spec-v1",
                        "family_key": "ma_cross",
                        "template_key": "tmpl-ma",
                        "strategy_version_label": "test",
                        "intent": "test strategy",
                        "allowed_clock_profiles": ["regular"],
                        "allowed_market_states": ["any"],
                        "required_price_inputs": ["close"],
                        "required_materialized_indicators": [],
                        "required_materialized_derived": [],
                        "signal_surface_key": "ma_cross",
                        "signal_surface_mode": "entries_exits",
                        "parameter_mode": "product",
                        "parameter_space": {"fast": [3], "slow": [9]},
                    },
                    search_spec=SimpleNamespace(parameter_space={"fast": [3], "slow": [9]}),
                )
            ],
        )

    monkeypatch.setattr(research_assets, "prepare_strategy_space", _fake_prepare_strategy_space)


def test_normalize_campaign_accepts_optuna_strategy_optimizer(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    strategy_space = dict(payload["strategy_space"])  # type: ignore[arg-type]
    strategy_space["optimizer"] = {
        "engine": "optuna",
        "sampler": "tpe",
        "seed": 7,
        "n_trials": 16,
        "objective": "robust_oos_trial_v1",
        "direction": "maximize",
        "top_k": 2,
        "radius": 1,
        "max_neighborhood_trials": 4,
        "ranking_policy": {
            "policy_id": "optimizer_smoke_v1",
            "metric_order": ["sharpe", "profit_factor"],
            "require_out_of_sample_pass": False,
            "min_trade_count": 1,
            "min_trade_count_per_fold": 1,
            "min_fold_count": 1,
            "max_drawdown_cap": 1.0,
            "min_positive_fold_ratio": 0.0,
            "stress_slippage_bps": 0.0,
            "min_parameter_stability": 0.0,
            "min_slippage_score": 0.0,
        },
    }
    payload["strategy_space"] = strategy_space

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)

    assert normalized["strategy_space"]["optimizer"] == strategy_space["optimizer"]


def test_normalize_campaign_forwards_money_backtest_policy(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    backtest = payload["backtest"]
    assert isinstance(backtest, dict)
    backtest.update(
        {
            "sizing_mode": "risk_per_trade",
            "risk_per_trade_pct": 0.02,
            "max_contracts": 7,
            "max_margin_fraction": 0.4,
            "commission_per_contract": 3.5,
            "slippage_ticks": 1.25,
        }
    )

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    common = campaigns._dagster_common_kwargs(  # type: ignore[attr-defined]
        normalized_config=normalized,
        materialized_root=tmp_path / "materialized",
        results_root=tmp_path / "results",
        reuse_existing_materialization=False,
        campaign_id="campaign",
        campaign_run_id="run",
    )

    assert common["sizing_mode"] == "risk_per_trade"
    assert common["risk_per_trade_pct"] == 0.02
    assert common["max_contracts"] == 7
    assert common["max_margin_fraction"] == 0.4
    assert common["commission_per_contract"] == 3.5
    assert common["slippage_ticks"] == 1.25


def test_normalize_campaign_rejects_removed_money_mode(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    backtest = payload["backtest"]
    assert isinstance(backtest, dict)
    backtest["money_mode"] = "legacy_vectorbt"

    with pytest.raises(SchemaValidationError, match="money_mode"):
        campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)


def test_campaign_routes_continuous_front_indicator_execution_modes(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="data_prep")
    execution = dict(payload["execution"])  # type: ignore[arg-type]
    execution["continuous_front_indicator_qc_mode"] = "audit"
    execution["continuous_front_indicator_sidecar_materialization_mode"] = "spark"
    execution["spark_master"] = "local[4]"
    payload["execution"] = execution

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    common = campaigns._dagster_common_kwargs(  # type: ignore[attr-defined]
        normalized_config=normalized,
        materialized_root=tmp_path / "materialized",
        results_root=tmp_path / "runs",
        reuse_existing_materialization=False,
        campaign_id="campaign",
        campaign_run_id="run",
    )

    assert common["continuous_front_indicator_qc_mode"] == "audit"
    assert common["continuous_front_indicator_sidecar_materialization_mode"] == "spark"
    assert common["spark_master"] == "local[4]"


def test_campaign_common_kwargs_match_backtest_and_projection_asset_helpers(
    tmp_path: Path,
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    execution = dict(payload["execution"])  # type: ignore[arg-type]
    execution["continuous_front_indicator_qc_mode"] = "audit"
    execution["continuous_front_indicator_sidecar_materialization_mode"] = "spark"
    execution["spark_master"] = "local[4]"
    payload["execution"] = execution

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    common = campaigns._dagster_common_kwargs(  # type: ignore[attr-defined]
        normalized_config=normalized,
        materialized_root=tmp_path / "materialized",
        results_root=tmp_path / "results",
        reuse_existing_materialization=False,
        campaign_id="campaign",
        campaign_run_id="run",
    )

    for helper in (
        research_assets.materialize_research_backtest_assets,
        research_assets.materialize_research_projection_assets,
    ):
        helper_params = set(inspect.signature(helper).parameters)
        assert sorted(set(common) - helper_params) == []


def test_campaign_rejects_python_continuous_front_sidecar_mode(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="data_prep")
    execution = dict(payload["execution"])  # type: ignore[arg-type]
    execution["continuous_front_indicator_sidecar_materialization_mode"] = "python"
    payload["execution"] = execution

    with pytest.raises(
        ValueError,
        match="execution.continuous_front_indicator_sidecar_materialization_mode",
    ):
        campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)


def test_normalize_campaign_builds_default_nested_validation_plan(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    dataset = dict(payload["dataset"])  # type: ignore[arg-type]
    dataset["start_ts"] = "2021-04-01T00:00:00Z"
    dataset["end_ts"] = "2026-03-31T23:59:59Z"
    dataset["warmup_bars"] = 300
    payload["dataset"] = dataset
    payload["validation"] = {
        "scheme": "nested_walk_forward_v1",
        "leakage_controls": {"purge_bars": 32, "embargo_bars": 16},
    }

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    plan = campaigns._build_validation_plan(normalized)  # type: ignore[attr-defined]

    assert normalized["validation"]["outer_folds"]["train_months"] == 18
    assert plan["outer_fold_count"] == 14
    assert plan["inner_fold_count"] == 42
    assert {
        row["optimizer_visible"] for row in plan["windows"] if row["fold_role"] == "confirmation"
    } == {False}


def _mock_report(
    *, materialized_root: Path, results_root: Path, target_stage: str
) -> dict[str, object]:
    registry_root = research_registry_root(materialized_root=materialized_root)
    output_paths = {
        "continuous_front_bars": (materialized_root / "continuous_front_bars.delta").as_posix(),
        "continuous_front_roll_events": (
            materialized_root / "continuous_front_roll_events.delta"
        ).as_posix(),
        "continuous_front_adjustment_ladder": (
            materialized_root / "continuous_front_adjustment_ladder.delta"
        ).as_posix(),
        "continuous_front_qc_report": (
            materialized_root / "continuous_front_qc_report.delta"
        ).as_posix(),
        "research_datasets": (materialized_root / "research_datasets.delta").as_posix(),
        "research_instrument_tree": (
            materialized_root / "research_instrument_tree.delta"
        ).as_posix(),
        "research_bar_views": (materialized_root / "research_bar_views.delta").as_posix(),
        "research_indicator_frames": (
            materialized_root / "research_indicator_frames.delta"
        ).as_posix(),
        "research_derived_indicator_frames": (
            materialized_root / "research_derived_indicator_frames.delta"
        ).as_posix(),
        "research_strategy_families": (
            registry_root / "research_strategy_families.delta"
        ).as_posix(),
        "research_strategy_templates": (
            registry_root / "research_strategy_templates.delta"
        ).as_posix(),
        "research_strategy_template_modules": (
            registry_root / "research_strategy_template_modules.delta"
        ).as_posix(),
        "research_strategy_search_specs": (
            results_root / "research_strategy_search_specs.delta"
        ).as_posix(),
        "research_vbt_search_runs": (results_root / "research_vbt_search_runs.delta").as_posix(),
        "research_optimizer_studies": (
            results_root / "research_optimizer_studies.delta"
        ).as_posix(),
        "research_optimizer_trials": (results_root / "research_optimizer_trials.delta").as_posix(),
        "research_vbt_param_results": (
            results_root / "research_vbt_param_results.delta"
        ).as_posix(),
        "research_vbt_param_gate_events": (
            results_root / "research_vbt_param_gate_events.delta"
        ).as_posix(),
        "research_vbt_ephemeral_indicator_cache": (
            results_root / "research_vbt_ephemeral_indicator_cache.delta"
        ).as_posix(),
        "research_strategy_promotion_events": (
            results_root / "research_strategy_promotion_events.delta"
        ).as_posix(),
        "research_backtest_batches": (results_root / "research_backtest_batches.delta").as_posix(),
        "research_strategy_rankings": (
            results_root / "research_strategy_rankings.delta"
        ).as_posix(),
        "research_strategy_evaluation_profiles": (
            results_root / "research_strategy_evaluation_profiles.delta"
        ).as_posix(),
        "research_signal_candidates": (
            results_root / "research_signal_candidates.delta"
        ).as_posix(),
    }
    if target_stage == "data_prep":
        selected_assets = list(campaigns.DATA_PREP_TABLES)
        materialized_assets = list(selected_assets)
    elif target_stage == "backtest":
        selected_assets = [
            "research_backtest_batches",
            "research_strategy_rankings",
            "research_strategy_evaluation_profiles",
        ]
        materialized_assets = [
            "continuous_front_bars",
            "continuous_front_roll_events",
            "continuous_front_adjustment_ladder",
            "continuous_front_qc_report",
            "research_datasets",
            "research_instrument_tree",
            "research_bar_views",
            "research_indicator_frames",
            "research_derived_indicator_frames",
            "research_strategy_families",
            "research_strategy_templates",
            "research_strategy_template_modules",
            "research_strategy_search_specs",
            "research_vbt_search_runs",
            "research_optimizer_studies",
            "research_optimizer_trials",
            "research_vbt_param_results",
            "research_vbt_param_gate_events",
            "research_vbt_ephemeral_indicator_cache",
            "research_strategy_promotion_events",
            "research_backtest_batches",
            "research_strategy_rankings",
            "research_strategy_evaluation_profiles",
        ]
    else:
        selected_assets = ["research_signal_candidates"]
        materialized_assets = [
            "continuous_front_bars",
            "continuous_front_roll_events",
            "continuous_front_adjustment_ladder",
            "continuous_front_qc_report",
            "research_datasets",
            "research_instrument_tree",
            "research_bar_views",
            "research_indicator_frames",
            "research_derived_indicator_frames",
            "research_strategy_families",
            "research_strategy_templates",
            "research_strategy_template_modules",
            "research_strategy_search_specs",
            "research_vbt_search_runs",
            "research_optimizer_studies",
            "research_optimizer_trials",
            "research_vbt_param_results",
            "research_vbt_param_gate_events",
            "research_vbt_ephemeral_indicator_cache",
            "research_strategy_promotion_events",
            "research_strategy_rankings",
            "research_strategy_evaluation_profiles",
            "research_signal_candidates",
        ]
    return {
        "success": True,
        "strategy_space_id": "sspace_test",
        "selected_assets": selected_assets,
        "materialized_assets": materialized_assets,
        "output_paths": output_paths,
        "rows_by_table": {name: 1 for name in materialized_assets},
    }


def _passed_contract_validation() -> dict[str, object]:
    return {
        "status": "passed",
        "validated_tables": [],
        "warnings": [],
        "errors": [],
        "row_counts": {},
    }


def test_normalize_campaign_config_sorts_timeframes_and_filters(tmp_path: Path) -> None:
    raw = _campaign_payload(tmp_path)
    raw["dataset"]["timeframes"] = ["1h", "15m", "15m"]  # type: ignore[index]
    raw["dataset"]["contract_ids"] = ["Si-6.26", "BR-6.26", "BR-6.26"]  # type: ignore[index]
    raw["dataset"]["instrument_ids"] = ["Si", "BR", "BR"]  # type: ignore[index]

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw)

    assert normalized["dataset"]["timeframes"] == ["15m", "1h"]
    assert normalized["dataset"]["contract_ids"] == ["BR-6.26", "Si-6.26"]
    assert normalized["dataset"]["instrument_ids"] == ["BR", "Si"]


def test_normalize_campaign_config_keeps_absent_volume_profile_absent(tmp_path: Path) -> None:
    raw = _campaign_payload(tmp_path)

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw)

    assert "volume_profile" not in normalized


def test_materialization_key_is_deterministic_for_semantically_equivalent_configs(
    tmp_path: Path,
) -> None:
    raw_a = _campaign_payload(tmp_path)
    raw_b = _campaign_payload(tmp_path)
    raw_b["dataset"]["timeframes"] = ["15m", "15m"]  # type: ignore[index]
    raw_b["dataset"]["contract_ids"] = ["BR-6.26", "BR-6.26"]  # type: ignore[index]
    raw_a["dataset"]["contract_ids"] = ["BR-6.26"]  # type: ignore[index]

    normalized_a = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_a)
    normalized_b = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_b)

    assert campaigns.build_config_fingerprint(normalized_a) == campaigns.build_config_fingerprint(
        normalized_b
    )
    assert campaigns.build_materialization_key(normalized_a) == campaigns.build_materialization_key(
        normalized_b
    )


def test_materialization_key_ignores_dataset_name_and_universe_id(tmp_path: Path) -> None:
    raw_a = _campaign_payload(tmp_path)
    raw_b = _campaign_payload(tmp_path)
    raw_b["dataset"]["dataset_name"] = "renamed-dataset"  # type: ignore[index]
    raw_b["dataset"]["universe_id"] = "renamed-universe"  # type: ignore[index]

    normalized_a = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_a)
    normalized_b = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_b)

    assert campaigns.build_materialization_key(normalized_a) == campaigns.build_materialization_key(
        normalized_b
    )


def test_materialization_key_changes_when_continuous_front_policy_changes(tmp_path: Path) -> None:
    raw_a = _campaign_payload(tmp_path)
    raw_b = _campaign_payload(tmp_path)
    raw_a["dataset"]["series_mode"] = "continuous_front"  # type: ignore[index]
    raw_b["dataset"]["series_mode"] = "continuous_front"  # type: ignore[index]
    raw_a["dataset"]["continuous_front_policy"] = ContinuousFrontPolicy(
        confirmation_bars=1
    ).to_config_dict()  # type: ignore[index]
    raw_b["dataset"]["continuous_front_policy"] = ContinuousFrontPolicy(
        confirmation_bars=3
    ).to_config_dict()  # type: ignore[index]

    normalized_a = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_a)
    normalized_b = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_b)

    assert campaigns.build_materialization_key(normalized_a) != campaigns.build_materialization_key(
        normalized_b
    )


def test_materialization_key_changes_when_volume_profile_source_changes(tmp_path: Path) -> None:
    raw_a = _campaign_payload(tmp_path)
    raw_b = _campaign_payload(tmp_path)
    raw_a["volume_profile"] = {
        "raw_1m_table_path": (tmp_path / "raw-a" / "raw_moex_history.delta").as_posix(),
        "tick_size_by_instrument": {"BR": 1.0},
    }
    raw_b["volume_profile"] = {
        "raw_1m_table_path": (tmp_path / "raw-b" / "raw_moex_history.delta").as_posix(),
        "tick_size_by_instrument": {"BR": 1.0},
    }

    normalized_a = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_a)
    normalized_b = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_b)

    assert campaigns.build_materialization_key(normalized_a) != campaigns.build_materialization_key(
        normalized_b
    )


def test_volume_profile_tick_sizes_reject_non_finite_values(tmp_path: Path) -> None:
    raw = _campaign_payload(tmp_path)
    raw["volume_profile"] = {
        "raw_1m_table_path": (tmp_path / "raw" / "raw_moex_history.delta").as_posix(),
        "tick_size_by_instrument": {"BR": float("inf")},
    }

    with pytest.raises(
        campaigns.CampaignBlockedError,
        match=re.escape("volume_profile.tick_size_by_instrument values must be finite and > 0"),
    ):
        campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw)


def test_materialization_lock_records_continuous_front_policy(tmp_path: Path) -> None:
    raw = _campaign_payload(tmp_path, target_stage="data_prep")
    raw["dataset"]["series_mode"] = "continuous_front"  # type: ignore[index]
    raw["dataset"]["continuous_front_policy"] = ContinuousFrontPolicy(
        confirmation_bars=3
    ).to_config_dict()  # type: ignore[index]
    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw)
    materialized_root = tmp_path / "materialized-lock"

    campaigns._write_materialization_lock(  # type: ignore[attr-defined]
        materialized_root=materialized_root,
        materialization_key=campaigns.build_materialization_key(normalized),
        normalized_config=normalized,
        campaign_id="camp_lock",
        campaign_run_id="crun_lock",
        report=_mock_report(
            materialized_root=materialized_root,
            results_root=tmp_path / "results-lock",
            target_stage="data_prep",
        ),
    )

    lock = _load_json(materialized_root / campaigns.MATERIALIZATION_LOCK_FILENAME)
    assert lock["continuous_front_policy"]["confirmation_bars"] == 3


def test_campaign_run_config_forwards_volume_profile_config_to_data_prep(
    tmp_path: Path,
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="data_prep")
    raw_1m_path = tmp_path / "raw" / "raw_moex_history.delta"
    payload["volume_profile"] = {
        "raw_1m_table_path": raw_1m_path.as_posix(),
        "tick_size_by_instrument": {"Si": 10.0, "BR": 1.0},
    }
    execution = dict(payload["execution"])  # type: ignore[arg-type]
    execution["spark_master"] = "local[4]"
    payload["execution"] = execution

    run_config = research_assets.build_research_campaign_run_config(
        campaign_config=payload,
        repo_root=ROOT,
        dagster_job_name=research_assets.RESEARCH_DATA_PREP_JOB_NAME,
    )
    context_config = run_config["ops"]["research_campaign_context"]["config"]
    data_prep_config = run_config["ops"]["research_datasets"]["config"]

    assert context_config["campaign_config"]["target_stage"] == "data_prep"
    assert data_prep_config["derived_indicator_profile_version"] == "core_v1"
    assert data_prep_config["spark_master"] == "local[4]"
    assert data_prep_config["volume_profile_raw_1m_table_path"] == raw_1m_path.resolve().as_posix()
    assert data_prep_config["volume_profile_tick_size_by_instrument"] == {"BR": 1.0, "Si": 10.0}


def test_research_output_dirs_fail_closed_for_partial_or_mixed_dir_inputs(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        _resolve_research_output_dirs(materialized_output_dir=tmp_path / "materialized-only")

    with pytest.raises(ValueError):
        _resolve_research_output_dirs(
            research_output_dir=tmp_path / "shared-research-root",
            results_output_dir=tmp_path / "results",
        )


def test_dispatch_campaign_route_is_removed() -> None:
    assert not hasattr(campaigns, "_dispatch_campaign")


@pytest.mark.parametrize(
    ("force_rematerialize", "expected_reuse"),
    (
        (False, True),
        (True, False),
    ),
)
def test_campaign_run_config_reuse_decision_respects_force_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    force_rematerialize: bool,
    expected_reuse: bool,
) -> None:
    _stub_prepare_strategy_space(monkeypatch)
    payload = _campaign_payload(
        tmp_path, target_stage="backtest", force_rematerialize=force_rematerialize
    )
    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    materialization_key = campaigns.build_materialization_key(normalized)
    materialized_root = Path(str(normalized["materialized_root"]))
    _seed_reusable_materialization(materialized_root, materialization_key=materialization_key)

    run_config = research_assets.build_research_campaign_run_config(
        campaign_config=payload,
        repo_root=ROOT,
        dagster_job_name=research_assets.RESEARCH_BACKTEST_JOB_NAME,
    )
    backtest_config = run_config["ops"]["research_backtest_batches"]["config"]

    assert backtest_config["reuse_existing_materialization"] is expected_reuse
    assert backtest_config["min_fold_count"] == 2
    assert backtest_config["materialized_output_dir"] == materialized_root.as_posix()


def test_campaign_run_config_rejects_invalid_config(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    del payload["dataset"]["base_timeframe"]  # type: ignore[index]

    with pytest.raises(SchemaValidationError):
        research_assets.build_research_campaign_run_config(
            campaign_config=payload,
            repo_root=ROOT,
            dagster_job_name=research_assets.RESEARCH_BACKTEST_JOB_NAME,
        )


def test_campaign_run_config_does_not_write_artifacts_before_dagster_run(
    tmp_path: Path,
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="data_prep")
    run_config = research_assets.build_research_campaign_run_config(
        campaign_config=payload,
        repo_root=ROOT,
        dagster_job_name=research_assets.RESEARCH_DATA_PREP_JOB_NAME,
    )

    assert "research_campaign_context" in run_config["ops"]
    assert not (tmp_path / "runs").exists()


def test_duration_metrics_use_backtest_batch_duration_for_backtest_throughput(
    tmp_path: Path,
) -> None:
    results_root = tmp_path / "results"
    write_delta_table_rows(
        table_path=results_root / "research_backtest_batches.delta",
        columns=backtest_store_contract()["research_backtest_batches"]["columns"],
        rows=[
            {
                "backtest_batch_id": "BTB-1",
                "campaign_run_id": "CRUN",
                "strategy_space_id": "SSPACE",
                "dataset_version": "dataset-v1",
                "indicator_set_version": "indicators-v1",
                "derived_indicator_set_version": "derived-v1",
                "engine_name": "vectorbt.Portfolio.from_signals",
                "param_batch_size": 32,
                "series_batch_size": 1,
                "combination_count": 64,
                "series_count": 4,
                "cache_id": "",
                "cache_hit": 0,
                "duration_seconds": 10.0,
                "evaluations_per_second": 6.4,
                "run_rows_per_second": 12.0,
                "trade_rows_per_second": 1200.0,
                "created_at": "2026-04-30T00:00:00Z",
            }
        ],
    )

    metrics = campaigns._build_duration_metrics(  # type: ignore[attr-defined]
        total_seconds=100.0,
        rows_by_table={
            "research_optimizer_trials": 640,
            "research_backtest_runs": 120,
            "research_strategy_stats": 120,
            "research_trade_records": 12_000,
            "research_order_records": 24_000,
            "research_strategy_rankings": 30,
        },
        results_root=results_root,
    )

    assert metrics["total_seconds"] == 100.0
    assert metrics["backtest_duration_seconds"] == 10.0
    assert metrics["backtest_runs_per_second"] == 12.0
    assert metrics["trade_records_per_second"] == 1200.0
    assert metrics["campaign_backtest_runs_per_second"] == 1.2
    assert metrics["ranking_rows_per_campaign_second"] == 0.3


def test_result_digest_includes_ranking_policy_subscores(tmp_path: Path) -> None:
    results_root = tmp_path / "results"
    contract = results_store_contract()["research_strategy_rankings"]["columns"]
    write_delta_table_rows(
        table_path=results_root / "research_strategy_rankings.delta",
        columns=contract,
        rows=[
            {
                "ranking_id": "RANK-BLOCKED",
                "campaign_run_id": "CRUN",
                "backtest_run_id": "RUN-BLOCKED",
                "strategy_instance_id": "SINST-BLOCKED",
                "strategy_template_id": "STPL",
                "family_id": "SFAM",
                "family_key": "trend_mtf_pullback_v1",
                "strategy_version_label": "trend-mtf-pullback-v1",
                "dataset_version": "dataset-v1",
                "indicator_set_version": "indicators-v1",
                "derived_indicator_set_version": "derived-v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "rank": 1,
                "family_rank": 1,
                "selected_rank": 1,
                "objective_score": 0.81,
                "score_total": 0.95,
                "policy_metric_score": 0.81,
                "fold_consistency_score": 1.0,
                "parameter_stability_score": 0.99,
                "parameter_stability_source": "one_step_neighbor_return_distance",
                "slippage_sensitivity_score": 1.0,
                "preferred_metric_score": 0.85,
                "ranking_policy_id": "robust_oos_v1",
                "ranking_policy_json": {},
                "rank_reason_json": {},
                "qualifies_for_projection": False,
                "out_of_sample_pass": 0,
                "policy_pass": 0,
                "policy_failure_reasons_json": ["min_fold_count"],
                "params_hash": "PARAM-BLOCKED",
                "mean_total_return": 0.12,
                "trade_count_total": 20,
                "worst_max_drawdown": 0.03,
                "representative_backtest_run_id": "RUN-BLOCKED",
                "window_ids_json": ["wf-01"],
                "created_at": "2026-04-29T00:00:00Z",
            },
            {
                "ranking_id": "RANK-1",
                "campaign_run_id": "CRUN",
                "backtest_run_id": "RUN",
                "strategy_instance_id": "SINST",
                "strategy_template_id": "STPL",
                "family_id": "SFAM",
                "family_key": "trend_mtf_pullback_v1",
                "strategy_version_label": "trend-mtf-pullback-v1",
                "dataset_version": "dataset-v1",
                "indicator_set_version": "indicators-v1",
                "derived_indicator_set_version": "derived-v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "rank": 1,
                "family_rank": 1,
                "selected_rank": 1,
                "objective_score": 0.75,
                "score_total": 0.9,
                "policy_metric_score": 0.75,
                "fold_consistency_score": 1.0,
                "parameter_stability_score": 0.88,
                "parameter_stability_source": "one_step_neighbor_return_distance",
                "slippage_sensitivity_score": 0.91,
                "preferred_metric_score": 0.8,
                "ranking_policy_id": "robust_oos_v1",
                "ranking_policy_json": {},
                "rank_reason_json": {},
                "qualifies_for_projection": True,
                "out_of_sample_pass": 1,
                "policy_pass": 1,
                "policy_failure_reasons_json": [],
                "params_hash": "PARAM",
                "mean_total_return": 0.1,
                "trade_count_total": 20,
                "worst_max_drawdown": 0.03,
                "representative_backtest_run_id": "RUN",
                "window_ids_json": ["wf-01"],
                "created_at": "2026-04-29T00:00:00Z",
            },
        ],
    )
    write_delta_table_rows(
        table_path=results_root / "research_strategy_evaluation_profiles.delta",
        columns=results_store_contract()["research_strategy_evaluation_profiles"]["columns"],
        rows=[
            {
                "evaluation_profile_id": "SEP-BLOCKED",
                "profile_version": "strategy-evaluation-profile.v1",
                "approved_universe_profile": "approved-universe-v1",
                "campaign_run_id": "CRUN",
                "ranking_id": "RANK-BLOCKED",
                "backtest_run_id": "RUN-BLOCKED",
                "strategy_instance_id": "SINST-BLOCKED",
                "strategy_template_id": "STPL",
                "family_id": "SFAM",
                "family_key": "trend_mtf_pullback_v1",
                "strategy_version_label": "trend-mtf-pullback-v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "params_hash": "PARAM-BLOCKED",
                "ranking_policy_id": "robust_oos_v1",
                "policy_pass": False,
                "qualifies_for_projection": False,
                "paper_signal_ready": False,
                "paper_trade_ready": False,
                "live_candidate_ready": False,
                "verdict": "reject",
                "promotion_state": "rejected",
                "blocker_reasons_json": [
                    "research_ranking_policy_failed",
                    "ranking_policy:min_fold_count",
                ],
                "missing_data_json": [],
                "evidence_snapshot_json": {"active_instrument_count": 1},
                "created_at": "2026-04-29T00:00:00Z",
            },
            {
                "evaluation_profile_id": "SEP-1",
                "profile_version": "strategy-evaluation-profile.v1",
                "approved_universe_profile": "approved-universe-v1",
                "campaign_run_id": "CRUN",
                "ranking_id": "RANK-1",
                "backtest_run_id": "RUN",
                "strategy_instance_id": "SINST",
                "strategy_template_id": "STPL",
                "family_id": "SFAM",
                "family_key": "trend_mtf_pullback_v1",
                "strategy_version_label": "trend-mtf-pullback-v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "params_hash": "PARAM",
                "ranking_policy_id": "robust_oos_v1",
                "policy_pass": True,
                "qualifies_for_projection": True,
                "paper_signal_ready": True,
                "paper_trade_ready": False,
                "live_candidate_ready": False,
                "verdict": "paper-signal",
                "promotion_state": "paper_signal_ready",
                "blocker_reasons_json": ["missing_risk_per_position"],
                "missing_data_json": ["risk_per_position"],
                "evidence_snapshot_json": {"active_instrument_count": 1},
                "created_at": "2026-04-29T00:00:00Z",
            },
        ],
    )

    digest = campaigns._build_result_digest(target_stage="backtest", results_root=results_root)

    assert digest is not None
    top = digest["ranking_top_rows"][0]
    assert top["strategy_instance_id"] == "SINST-BLOCKED"
    assert top["qualifies_for_projection"] is False
    assert digest["best_overall_rows"][0] == top
    eligible_top = digest["projection_eligible_top_rows"][0]
    assert eligible_top["strategy_instance_id"] == "SINST"
    assert eligible_top["parameter_stability_score"] == 0.88
    assert eligible_top["slippage_sensitivity_score"] == 0.91
    assert digest["strategy_evaluation_count"] == 2
    assert digest["strategy_evaluations_by_verdict"] == {"paper-signal": 1, "reject": 1}
    assert digest["paper_signal_ready_count"] == 1
    assert digest["paper_trade_ready_count"] == 0
    assert digest["live_candidate_ready_count"] == 0
    assert digest["strategy_evaluation_top_rows"][1]["promotion_state"] == "paper_signal_ready"


def test_result_digest_records_forced_data_prep_proof(tmp_path: Path) -> None:
    materialized_root = tmp_path / "forced-proof"
    output_paths = {}
    rows_by_table = {}
    for index, table_name in enumerate(campaigns.DATA_PREP_TABLES, start=1):
        table_path = materialized_root / f"{table_name}.delta"
        (table_path / "_delta_log").mkdir(parents=True, exist_ok=True)
        output_paths[table_name] = table_path.as_posix()
        rows_by_table[table_name] = index * 10

    digest = campaigns._build_result_digest(
        target_stage="data_prep",
        results_root=tmp_path / "results",
        rows_by_table=rows_by_table,
        materialized_root=materialized_root,
        output_paths=output_paths,
        executed_steps=("research_data_prep",),
        reused_steps=(),
        force_rematerialize=True,
    )

    assert digest is not None
    proof = digest["data_prep_proof"]
    assert proof["mode"] == "forced_refresh"
    assert proof["force_rematerialize"] is True
    assert proof["materialized_root"] == materialized_root.as_posix()
    assert proof["tables"]["research_bar_views"]["row_count"] == rows_by_table["research_bar_views"]
    assert proof["tables"]["research_bar_views"]["has_delta_log"] is True


def test_dagster_failure_summary_writer_persists_failed_campaign_artifacts(
    tmp_path: Path,
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    _seed_reusable_materialization(
        Path(str(normalized["materialized_root"])),
        materialization_key=campaigns.build_materialization_key(normalized),
    )
    run_config = research_assets.build_research_campaign_run_config(
        campaign_config=payload,
        repo_root=ROOT,
        dagster_job_name=research_assets.RESEARCH_BACKTEST_JOB_NAME,
        run_id="failed-dagster-run",
    )

    wrote = research_assets._write_failed_research_campaign_summary(  # type: ignore[attr-defined]
        run_config=run_config,
        dagster_job_name=research_assets.RESEARCH_BACKTEST_JOB_NAME,
        dagster_run_id="dagster-run-id",
    )
    context_config = run_config["ops"]["research_campaign_context"]["config"]
    campaign_context = research_assets._build_research_campaign_context_payload(  # type: ignore[attr-defined]
        context_config=context_config,
        dagster_job_name=research_assets.RESEARCH_BACKTEST_JOB_NAME,
        dagster_run_id="dagster-run-id",
        write_start_artifacts=False,
    )
    run_root = Path(str(campaign_context["run_root"]))
    persisted_summary = _load_json(run_root / "run-summary.json")

    assert wrote is True
    assert persisted_summary["status"] == "failed"
    assert persisted_summary["error"]["type"] == "DagsterRunFailure"
    assert persisted_summary["dagster_selected_assets"][0] == "research_campaign_context"
    assert _load_json(run_root / "status.json")["status"] == "failed"


def test_continuous_front_sidecar_failure_marks_campaign_status_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="data_prep")
    dataset = dict(payload["dataset"])  # type: ignore[arg-type]
    dataset["series_mode"] = "continuous_front"
    payload["dataset"] = dataset
    run_config = research_assets.build_research_campaign_run_config(
        campaign_config=payload,
        repo_root=ROOT,
        dagster_job_name=research_assets.RESEARCH_DATA_PREP_JOB_NAME,
        run_id="failed-data-prep-run",
    )
    op_config = run_config["ops"]["continuous_front_indicator_acceptance_report"]["config"]
    dagster_run = SimpleNamespace(
        run_config=run_config,
        job_name=research_assets.RESEARCH_DATA_PREP_JOB_NAME,
        run_id="dagster-run-id",
    )
    context = SimpleNamespace(
        op_execution_context=SimpleNamespace(
            op_config=op_config,
            run_config=run_config,
            dagster_run=dagster_run,
            job_name=research_assets.RESEARCH_DATA_PREP_JOB_NAME,
            run_id="dagster-run-id",
        ),
        dagster_run=dagster_run,
        job_name=research_assets.RESEARCH_DATA_PREP_JOB_NAME,
        run_id="dagster-run-id",
        log=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
    )

    monkeypatch.setattr(
        research_assets,
        "_existing_research_dataset_context",
        lambda _config: {
            "materialized_output_dir": (tmp_path / "materialized").as_posix(),
            "dataset_manifest": {"series_mode": "continuous_front"},
        },
    )

    def _fail_sidecar(_research_datasets: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("sidecar failed")

    monkeypatch.setattr(
        research_assets,
        "_run_continuous_front_indicator_sidecar",
        _fail_sidecar,
    )

    with pytest.raises(RuntimeError, match="sidecar failed"):
        research_assets.continuous_front_indicator_acceptance_report.op.compute_fn.decorated_fn(  # type: ignore[attr-defined]
            context
        )

    context_config = run_config["ops"]["research_campaign_context"]["config"]
    campaign_context = research_assets._build_research_campaign_context_payload(  # type: ignore[attr-defined]
        context_config=context_config,
        dagster_job_name=research_assets.RESEARCH_DATA_PREP_JOB_NAME,
        dagster_run_id="dagster-run-id",
        write_start_artifacts=False,
    )
    run_root = Path(str(campaign_context["run_root"]))

    assert _load_json(run_root / "status.json")["status"] == "failed"
    assert _load_json(run_root / "run-summary.json")["status"] == "failed"


def test_dagster_failure_summary_writer_ignores_non_campaign_runs() -> None:
    wrote = research_assets._write_failed_research_campaign_summary(  # type: ignore[attr-defined]
        run_config={"ops": {}},
        dagster_job_name=research_assets.RESEARCH_BACKTEST_JOB_NAME,
        dagster_run_id="dagster-run-id",
    )

    assert wrote is False
