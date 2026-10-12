from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from trading_advisor_3000.dagster_defs import (
    RESEARCH_BACKTEST_JOB_NAME,
    RESEARCH_DATA_PREP_JOB_NAME,
    build_product_plane_definitions,
    build_research_campaign_run_config,
    research_assets,
)
from trading_advisor_3000.product_plane.research import campaigns

ROOT = Path(__file__).resolve().parents[3]
RUNBOOK_ROUTE = ROOT / "docs" / "runbooks" / "app" / "research-campaign-route.md"
RUNBOOK_OPERATIONS = ROOT / "docs" / "runbooks" / "app" / "research-plane-operations.md"


def _campaign_payload(
    tmp_path: Path,
    *,
    campaign_name: str,
    target_stage: str,
) -> dict[str, object]:
    return {
        "campaign_name": campaign_name,
        "target_stage": target_stage,
        "canonical_output_dir": (tmp_path / "canonical").as_posix(),
        "materialized_root": (tmp_path / "materialized").as_posix(),
        "runs_root": (tmp_path / "runs").as_posix(),
        "dataset": {
            "dataset_version": "campaign-dataset-v1",
            "dataset_name": "campaign-dataset",
            "universe_id": "moex-futures",
            "series_mode": "contract",
            "timeframes": ["15m"],
            "base_timeframe": "15m",
            "start_ts": None,
            "end_ts": None,
            "warmup_bars": 0,
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
            "param_batch_size": 25,
            "series_batch_size": 4,
            "backtest_timeframe": "15m",
            "fees_bps": 0.0,
            "slippage_bps": 0.0,
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
            "force_rematerialize": False,
            "raise_on_error": True,
        },
    }


def _seed_reusable_materialization(payload: dict[str, object]) -> None:
    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    materialized_root = Path(str(normalized["materialized_root"]))
    materialized_root.mkdir(parents=True, exist_ok=True)
    for table_name in campaigns.DATA_PREP_TABLES:
        log_dir = materialized_root / f"{table_name}.delta" / "_delta_log"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "00000000000000000000.json").write_text("{}", encoding="utf-8")
    (materialized_root / campaigns.MATERIALIZATION_LOCK_FILENAME).write_text(
        json.dumps({"materialization_key": campaigns.build_materialization_key(normalized)}),
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


def test_run_campaign_module_is_removed_operator_route() -> None:
    assert (
        importlib.util.find_spec("trading_advisor_3000.product_plane.research.jobs.run_campaign")
        is None
    )


def test_dagster_campaign_run_config_targets_existing_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_prepare_strategy_space(monkeypatch)
    repository = build_product_plane_definitions().get_repository_def()
    data_prep_nodes = set(repository.get_job(RESEARCH_DATA_PREP_JOB_NAME).graph.node_dict)
    backtest_nodes = set(repository.get_job(RESEARCH_BACKTEST_JOB_NAME).graph.node_dict)
    payload = _campaign_payload(
        tmp_path,
        campaign_name="dagster-config",
        target_stage="backtest",
    )
    _seed_reusable_materialization(payload)

    run_config = build_research_campaign_run_config(
        campaign_config=payload,
        repo_root=ROOT,
        dagster_job_name=RESEARCH_BACKTEST_JOB_NAME,
    )

    assert "research_campaign_context" in data_prep_nodes
    assert "research_campaign_context" in backtest_nodes
    assert "research_backtest_batches" in run_config["ops"]
    assert "research_datasets" not in run_config["ops"]
    assert "research_indicator_frames" not in run_config["ops"]


def test_backtest_campaign_run_config_fails_closed_without_existing_gold(
    tmp_path: Path,
) -> None:
    payload = _campaign_payload(
        tmp_path,
        campaign_name="missing-gold",
        target_stage="backtest",
    )

    with pytest.raises(ValueError, match="existing research gold layer"):
        build_research_campaign_run_config(
            campaign_config=payload,
            repo_root=ROOT,
            dagster_job_name=RESEARCH_BACKTEST_JOB_NAME,
        )


def test_research_runbooks_publish_dagster_job_route() -> None:
    old_command = "python -m trading_advisor_3000.product_plane.research.jobs.run_campaign"
    route_text = RUNBOOK_ROUTE.read_text(encoding="utf-8")
    operations_text = RUNBOOK_OPERATIONS.read_text(encoding="utf-8")

    assert old_command not in route_text
    assert old_command not in operations_text
    assert "research_campaign_context" in route_text
    assert RESEARCH_DATA_PREP_JOB_NAME in route_text
    assert RESEARCH_BACKTEST_JOB_NAME in route_text
