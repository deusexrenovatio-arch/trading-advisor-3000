from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from trading_advisor_3000.dagster_defs.research_assets import _resolve_research_output_dirs
from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research import campaigns
from trading_advisor_3000.product_plane.research.backtests.results import backtest_store_contract, results_store_contract


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
            "split_method": "holdout",
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
            "policy_id": "robust_oos_v1",
            "metric_order": ["total_return", "profit_factor", "max_drawdown"],
            "require_out_of_sample_pass": True,
            "min_trade_count": 4,
            "min_fold_count": 2,
            "max_drawdown_cap": 0.35,
            "min_positive_fold_ratio": 0.5,
            "stress_slippage_bps": 7.5,
            "min_parameter_stability": 0.35,
            "min_slippage_score": 0.45,
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


def _write_campaign(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _seed_reusable_materialization(materialized_root: Path, *, materialization_key: str = "") -> None:
    for table_name in campaigns.DATA_PREP_TABLES:
        log_dir = materialized_root / f"{table_name}.delta" / "_delta_log"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "00000000000000000000.json").write_text("{}", encoding="utf-8")
    if materialization_key:
        (materialized_root / campaigns.MATERIALIZATION_LOCK_FILENAME).write_text(
            json.dumps({"materialization_key": materialization_key}),
            encoding="utf-8",
        )


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
    }
    payload["strategy_space"] = strategy_space

    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)

    assert normalized["strategy_space"]["optimizer"] == strategy_space["optimizer"]


def _mock_report(*, materialized_root: Path, results_root: Path, target_stage: str) -> dict[str, object]:
    registry_root = campaigns.research_registry_root(materialized_root=materialized_root)
    output_paths = {
        "research_datasets": (materialized_root / "research_datasets.delta").as_posix(),
        "research_instrument_tree": (materialized_root / "research_instrument_tree.delta").as_posix(),
        "research_bar_views": (materialized_root / "research_bar_views.delta").as_posix(),
        "research_indicator_frames": (materialized_root / "research_indicator_frames.delta").as_posix(),
        "research_derived_indicator_frames": (materialized_root / "research_derived_indicator_frames.delta").as_posix(),
        "research_strategy_families": (registry_root / "research_strategy_families.delta").as_posix(),
        "research_strategy_templates": (registry_root / "research_strategy_templates.delta").as_posix(),
        "research_strategy_template_modules": (registry_root / "research_strategy_template_modules.delta").as_posix(),
        "research_strategy_search_specs": (results_root / "research_strategy_search_specs.delta").as_posix(),
        "research_vbt_search_runs": (results_root / "research_vbt_search_runs.delta").as_posix(),
        "research_optimizer_studies": (results_root / "research_optimizer_studies.delta").as_posix(),
        "research_optimizer_trials": (results_root / "research_optimizer_trials.delta").as_posix(),
        "research_vbt_param_results": (results_root / "research_vbt_param_results.delta").as_posix(),
        "research_vbt_param_gate_events": (results_root / "research_vbt_param_gate_events.delta").as_posix(),
        "research_vbt_ephemeral_indicator_cache": (results_root / "research_vbt_ephemeral_indicator_cache.delta").as_posix(),
        "research_strategy_promotion_events": (results_root / "research_strategy_promotion_events.delta").as_posix(),
        "research_backtest_batches": (results_root / "research_backtest_batches.delta").as_posix(),
        "research_strategy_rankings": (results_root / "research_strategy_rankings.delta").as_posix(),
        "research_signal_candidates": (results_root / "research_signal_candidates.delta").as_posix(),
    }
    if target_stage == "data_prep":
        selected_assets = [
            "research_datasets",
            "research_instrument_tree",
            "research_bar_views",
            "research_indicator_frames",
            "research_derived_indicator_frames",
        ]
        materialized_assets = list(selected_assets)
    elif target_stage == "backtest":
        selected_assets = ["research_backtest_batches", "research_strategy_rankings"]
        materialized_assets = [
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
        ]
    else:
        selected_assets = ["research_signal_candidates"]
        materialized_assets = [
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


def test_materialization_key_is_deterministic_for_semantically_equivalent_configs(tmp_path: Path) -> None:
    raw_a = _campaign_payload(tmp_path)
    raw_b = _campaign_payload(tmp_path)
    raw_b["dataset"]["timeframes"] = ["15m", "15m"]  # type: ignore[index]
    raw_b["dataset"]["contract_ids"] = ["BR-6.26", "BR-6.26"]  # type: ignore[index]
    raw_a["dataset"]["contract_ids"] = ["BR-6.26"]  # type: ignore[index]

    normalized_a = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_a)
    normalized_b = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_b)

    assert campaigns.build_config_fingerprint(normalized_a) == campaigns.build_config_fingerprint(normalized_b)
    assert campaigns.build_materialization_key(normalized_a) == campaigns.build_materialization_key(normalized_b)


def test_materialization_key_ignores_dataset_name_and_universe_id(tmp_path: Path) -> None:
    raw_a = _campaign_payload(tmp_path)
    raw_b = _campaign_payload(tmp_path)
    raw_b["dataset"]["dataset_name"] = "renamed-dataset"  # type: ignore[index]
    raw_b["dataset"]["universe_id"] = "renamed-universe"  # type: ignore[index]

    normalized_a = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_a)
    normalized_b = campaigns.normalize_campaign_config(repo_root=ROOT, raw=raw_b)

    assert campaigns.build_materialization_key(normalized_a) == campaigns.build_materialization_key(normalized_b)


def test_research_output_dirs_fail_closed_for_partial_or_mixed_dir_inputs(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        _resolve_research_output_dirs(materialized_output_dir=tmp_path / "materialized-only")

    with pytest.raises(ValueError):
        _resolve_research_output_dirs(
            research_output_dir=tmp_path / "shared-research-root",
            results_output_dir=tmp_path / "results",
        )


@pytest.mark.parametrize(
    ("target_stage", "expected_helper"),
    (
        ("data_prep", "data_prep"),
        ("backtest", "backtest"),
        ("projection", "projection"),
    ),
)
def test_dispatch_campaign_selects_route_by_target_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_stage: str,
    expected_helper: str,
) -> None:
    calls: list[str] = []

    def _data_prep(**_: object) -> dict[str, object]:
        calls.append("data_prep")
        return {"route": "data_prep"}

    def _backtest(**_: object) -> dict[str, object]:
        calls.append("backtest")
        return {"route": "backtest"}

    def _projection(**_: object) -> dict[str, object]:
        calls.append("projection")
        return {"route": "projection"}

    monkeypatch.setattr(campaigns, "materialize_research_data_prep_assets", _data_prep)
    monkeypatch.setattr(campaigns, "materialize_research_backtest_assets", _backtest)
    monkeypatch.setattr(campaigns, "materialize_research_projection_assets", _projection)

    normalized = campaigns.normalize_campaign_config(
        repo_root=ROOT,
        raw=_campaign_payload(tmp_path, target_stage=target_stage),
    )
    report = campaigns._dispatch_campaign(  # type: ignore[attr-defined]
        normalized_config=normalized,
        materialized_root=tmp_path / "materialized-root",
        results_root=tmp_path / "results-root",
        reuse_existing_materialization=False,
        campaign_id="camp_test",
        campaign_run_id="crun_test",
    )

    assert report["route"] == expected_helper
    assert calls == [expected_helper]


@pytest.mark.parametrize(
    ("force_rematerialize", "expected_reuse"),
    (
        (False, True),
        (True, False),
    ),
)
def test_run_campaign_reuse_decision_respects_force_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    force_rematerialize: bool,
    expected_reuse: bool,
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest", force_rematerialize=force_rematerialize)
    normalized = campaigns.normalize_campaign_config(repo_root=ROOT, raw=payload)
    materialization_key = campaigns.build_materialization_key(normalized)
    materialized_root = Path(str(normalized["materialized_root"]))
    _seed_reusable_materialization(materialized_root, materialization_key=materialization_key)

    config_path = tmp_path / "campaign.yaml"
    _write_campaign(config_path, payload)

    captured: dict[str, object] = {}

    def _backtest(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return _mock_report(
            materialized_root=Path(str(kwargs["materialized_output_dir"])),
            results_root=Path(str(kwargs["results_output_dir"])),
            target_stage="backtest",
        )

    monkeypatch.setattr(campaigns, "materialize_research_backtest_assets", _backtest)
    monkeypatch.setattr(campaigns, "validate_research_contracts", lambda **_: _passed_contract_validation())

    summary = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)

    assert captured["reuse_existing_materialization"] is expected_reuse
    assert captured["min_fold_count"] == 2
    assert summary["materialized_root"] == materialized_root.as_posix()
    assert summary["reused_steps"] == (["research_data_prep"] if expected_reuse else [])


def test_run_campaign_persists_blocked_evidence_for_invalid_config(tmp_path: Path) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    del payload["dataset"]["base_timeframe"]  # type: ignore[index]
    config_path = tmp_path / "invalid-campaign.yaml"
    _write_campaign(config_path, payload)

    summary = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)
    run_root = Path(str(summary["run_root"]))
    campaign_lock = _load_json(run_root / "campaign.lock.json")

    assert summary["status"] == "blocked"
    assert (run_root / "campaign.lock.json").exists()
    assert (run_root / "run-summary.json").exists()
    assert _load_json(run_root / "status.json")["status"] == "blocked"
    assert campaign_lock["validation_status"] == "blocked"
    assert campaign_lock["error"]["type"] == "SchemaValidationError"


def test_run_campaign_writes_run_folder_layout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _campaign_payload(tmp_path, target_stage="data_prep")
    config_path = tmp_path / "campaign.yaml"
    _write_campaign(config_path, payload)

    def _data_prep(**kwargs: object) -> dict[str, object]:
        return _mock_report(
            materialized_root=Path(str(kwargs["materialized_output_dir"])),
            results_root=Path(str(kwargs["results_output_dir"])),
            target_stage="data_prep",
        )

    monkeypatch.setattr(campaigns, "materialize_research_data_prep_assets", _data_prep)
    monkeypatch.setattr(campaigns, "validate_research_contracts", lambda **_: _passed_contract_validation())

    summary = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)
    run_root = Path(str(summary["run_root"]))

    assert summary["status"] == "success"
    assert (run_root / "campaign.lock.json").exists()
    assert (run_root / "status.json").exists()
    assert (run_root / "run-summary.json").exists()
    assert (run_root / "artifacts-index.json").exists()
    assert (run_root / "logs" / "stdout.log").exists()
    assert (run_root / "logs" / "stderr.log").exists()
    assert (run_root / "results" / "publish-commit.json").exists()
    assert _load_json(run_root / "status.json")["status"] == "success"
    assert "backtest_runs_per_second" in summary["durations"]
    assert "research_strategy_families" not in summary["output_paths"]
    assert "research_campaigns" in summary["output_paths"]
    materialization_lock = _load_json(Path(str(summary["materialized_root"])) / campaigns.MATERIALIZATION_LOCK_FILENAME)
    assert set(materialization_lock["output_paths"]) == set(campaigns.DATA_PREP_TABLES)


def test_duration_metrics_use_backtest_batch_duration_for_backtest_throughput(tmp_path: Path) -> None:
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
            }
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


def test_run_campaign_emits_failure_summary_when_dispatch_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="backtest")
    config_path = tmp_path / "campaign.yaml"
    _write_campaign(config_path, payload)

    def _backtest(**_: object) -> dict[str, object]:
        raise RuntimeError("simulated dispatch failure")

    monkeypatch.setattr(campaigns, "materialize_research_backtest_assets", _backtest)

    summary = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)
    run_root = Path(str(summary["run_root"]))
    persisted_summary = _load_json(run_root / "run-summary.json")

    assert summary["status"] == "failed"
    assert persisted_summary["status"] == "failed"
    assert persisted_summary["error"] == {
        "type": "RuntimeError",
        "message": "simulated dispatch failure",
    }
    assert _load_json(run_root / "status.json")["status"] == "failed"


def test_run_campaign_quarantines_uncommitted_publish_when_registry_publish_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _campaign_payload(tmp_path, target_stage="data_prep")
    config_path = tmp_path / "campaign.yaml"
    _write_campaign(config_path, payload)

    def _data_prep(**kwargs: object) -> dict[str, object]:
        results_root = Path(str(kwargs["results_output_dir"]))
        staged_table = results_root / "research_backtest_batches.delta"
        (staged_table / "_delta_log").mkdir(parents=True, exist_ok=True)
        return {
            **_mock_report(
                materialized_root=Path(str(kwargs["materialized_output_dir"])),
                results_root=results_root,
                target_stage="data_prep",
            ),
            "output_paths": {"research_backtest_batches": staged_table.as_posix()},
        }

    original_write_campaign_run = campaigns.write_campaign_run

    def _write_campaign_run(**kwargs: object) -> object:
        if kwargs.get("status") == "publishing":
            raise RuntimeError("simulated registry publish failure")
        return original_write_campaign_run(**kwargs)

    monkeypatch.setattr(campaigns, "materialize_research_data_prep_assets", _data_prep)
    monkeypatch.setattr(campaigns, "validate_research_contracts", lambda **_: _passed_contract_validation())
    monkeypatch.setattr(campaigns, "write_campaign_run", _write_campaign_run)

    summary = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)
    run_root = Path(str(summary["run_root"]))

    assert summary["status"] == "failed"
    assert not (run_root / "results" / "publish-commit.json").exists()
    assert (run_root / "results-quarantine" / "research_backtest_batches.delta" / "_delta_log").exists()

