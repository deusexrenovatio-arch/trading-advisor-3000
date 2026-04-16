from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from trading_advisor_3000.product_plane.data_plane import run_sample_backfill
from trading_advisor_3000.product_plane.research import campaigns


ROOT = Path(__file__).resolve().parents[3]
RAW_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"
RUNBOOK_ROUTE = ROOT / "docs" / "runbooks" / "app" / "research-campaign-route.md"
RUNBOOK_OPERATIONS = ROOT / "docs" / "runbooks" / "app" / "research-plane-operations.md"


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_campaign(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _seed_canonical(canonical_dir: Path) -> None:
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )


def _campaign_payload(
    canonical_dir: Path,
    materialized_root: Path,
    runs_root: Path,
    *,
    campaign_name: str,
    target_stage: str,
    feature_profile_version: str = "core_v1",
) -> dict[str, object]:
    return {
        "campaign_name": campaign_name,
        "target_stage": target_stage,
        "canonical_output_dir": canonical_dir.as_posix(),
        "materialized_root": materialized_root.as_posix(),
        "runs_root": runs_root.as_posix(),
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
            "split_method": "holdout",
            "contract_ids": [],
            "instrument_ids": [],
        },
        "profiles": {
            "indicator_set_version": "indicators-v1",
            "indicator_profile_version": "core_v1",
            "feature_set_version": "features-v1",
            "feature_profile_version": feature_profile_version,
        },
        "backtest": {
            "strategy_versions": ["ma-cross-v1"],
            "combination_count": 1,
            "param_batch_size": 25,
            "series_batch_size": 4,
            "backtest_timeframe": "15m",
            "fees_bps": 0.0,
            "slippage_bps": 0.0,
            "allow_short": True,
            "window_count": 1,
        },
        "ranking_policy": {
            "policy_id": "robust_oos_v1",
            "metric_order": ["total_return", "profit_factor", "max_drawdown"],
            "require_out_of_sample_pass": True,
            "min_trade_count": 4,
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
            "force_rematerialize": False,
            "raise_on_error": True,
        },
    }


def test_bootstrap_campaign_module_executes_and_writes_summary(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical"
    _seed_canonical(canonical_dir)

    config_path = tmp_path / "campaign.yaml"
    _write_campaign(
        config_path,
        _campaign_payload(
            canonical_dir,
            tmp_path / "materialized",
            tmp_path / "runs",
            campaign_name="bootstrap-route",
            target_stage="bootstrap",
        ),
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "trading_advisor_3000.product_plane.research.jobs.run_campaign",
            "--config",
            config_path.as_posix(),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip())
    run_root = Path(str(payload["run_root"]))

    assert payload["status"] == "success"
    assert (run_root / "run-summary.json").exists()
    assert _load_json(run_root / "status.json")["status"] == "success"


def test_bootstrap_campaign_dispatches_through_dagster_helper_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_dir = tmp_path / "canonical"
    _seed_canonical(canonical_dir)

    payload = _campaign_payload(
        canonical_dir,
        tmp_path / "materialized",
        tmp_path / "runs",
        campaign_name="bootstrap-dispatch",
        target_stage="bootstrap",
    )
    config_path = tmp_path / "campaign.yaml"
    _write_campaign(config_path, payload)

    calls: list[dict[str, object]] = []

    def _bootstrap(**kwargs: object) -> dict[str, object]:
        calls.append(dict(kwargs))
        materialized_root = Path(str(kwargs["materialized_output_dir"]))
        results_root = Path(str(kwargs["results_output_dir"]))
        return {
            "success": True,
            "selected_assets": ["research_datasets"],
            "materialized_assets": ["research_datasets"],
            "output_paths": {
                "research_datasets": (materialized_root / "research_datasets.delta").as_posix(),
                "research_backtest_batches": (results_root / "research_backtest_batches.delta").as_posix(),
            },
            "rows_by_table": {"research_datasets": 1},
        }

    monkeypatch.setattr(campaigns, "materialize_phase2b_bootstrap_assets", _bootstrap)
    monkeypatch.setattr(
        campaigns,
        "validate_phase2b_contracts",
        lambda **_: {
            "status": "passed",
            "validated_tables": [],
            "warnings": [],
            "errors": [],
            "row_counts": {},
        },
    )

    summary = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)

    assert summary["status"] == "success"
    assert len(calls) == 1
    assert calls[0]["dataset_version"] == "campaign-dataset-v1"
    assert Path(str(summary["run_root"])).exists()


def test_backtest_campaign_reuses_existing_compatible_materialized_layer(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical"
    materialized_root = tmp_path / "materialized"
    runs_root = tmp_path / "runs"
    _seed_canonical(canonical_dir)

    config_path = tmp_path / "campaign-backtest.yaml"
    _write_campaign(
        config_path,
        _campaign_payload(
            canonical_dir,
            materialized_root,
            runs_root,
            campaign_name="reuse-check",
            target_stage="backtest",
        ),
    )

    first = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)
    second = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)

    assert first["status"] == "success"
    assert second["status"] == "success"
    assert first["materialized_root"] == second["materialized_root"]
    assert first["run_root"] != second["run_root"]
    assert first["reused_steps"] == []
    assert second["reused_steps"] == ["bootstrap"]


def test_changed_profile_version_forces_new_materialization_root(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical"
    materialized_root = tmp_path / "materialized"
    runs_root = tmp_path / "runs"
    _seed_canonical(canonical_dir)

    first_path = tmp_path / "campaign-first.yaml"
    second_path = tmp_path / "campaign-second.yaml"
    _write_campaign(
        first_path,
        _campaign_payload(
            canonical_dir,
            materialized_root,
            runs_root,
            campaign_name="profile-v1",
            target_stage="backtest",
            feature_profile_version="core_v1",
        ),
    )
    _write_campaign(
        second_path,
        _campaign_payload(
            canonical_dir,
            materialized_root,
            runs_root,
            campaign_name="profile-v2",
            target_stage="backtest",
            feature_profile_version="core_intraday_v1",
        ),
    )

    first = campaigns.run_campaign(config_path=first_path, repo_root=ROOT)
    second = campaigns.run_campaign(config_path=second_path, repo_root=ROOT)

    assert first["status"] == "success"
    assert second["status"] == "success"
    assert first["materialization_key"] != second["materialization_key"]
    assert first["materialized_root"] != second["materialized_root"]
    assert second["reused_steps"] == []


def test_projection_campaign_emits_ranking_and_candidate_summaries(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical"
    _seed_canonical(canonical_dir)

    config_path = tmp_path / "campaign-projection.yaml"
    _write_campaign(
        config_path,
        _campaign_payload(
            canonical_dir,
            tmp_path / "materialized",
            tmp_path / "runs",
            campaign_name="projection-digest",
            target_stage="projection",
        ),
    )

    summary = campaigns.run_campaign(config_path=config_path, repo_root=ROOT)
    digest = dict(summary["result_digest"])

    assert summary["status"] == "success"
    assert "ranking_top_rows" in digest
    assert "policy_pass_count" in digest
    assert "positive_return_count" in digest
    assert "candidate_count" in digest
    assert "candidate_rows_by_strategy" in digest
    assert "candidate_rows_by_timeframe" in digest


def test_official_research_runbooks_only_support_campaign_runner_route() -> None:
    expected_command = "python -m trading_advisor_3000.product_plane.research.jobs.run_campaign"
    internal_marker = "internal/debug-only"

    route_text = RUNBOOK_ROUTE.read_text(encoding="utf-8")
    operations_text = RUNBOOK_OPERATIONS.read_text(encoding="utf-8")

    assert expected_command in route_text
    assert expected_command in operations_text
    assert internal_marker in route_text.lower()
    assert internal_marker in operations_text.lower()

    route_supported_section = route_text.split("## Internal / Debug Only", maxsplit=1)[0]
    operations_supported_section = operations_text.split("## Internal / Debug Only", maxsplit=1)[0]
    for legacy_route in (
        "trading_advisor_3000.product_plane.research.jobs.bootstrap",
        "trading_advisor_3000.product_plane.research.jobs.backtest",
        "trading_advisor_3000.product_plane.research.jobs.project_candidates",
    ):
        assert legacy_route not in route_supported_section
        assert legacy_route not in operations_supported_section
