from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.dagster_defs import (
    materialize_phase2b_backtest_assets,
    materialize_phase2b_bootstrap_assets,
    materialize_phase2b_projection_assets,
)
from trading_advisor_3000.product_plane.data_plane import run_sample_backfill
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.research.datasets import (
    ResearchDatasetManifest,
    load_materialized_research_dataset,
    materialize_research_dataset,
)
from trading_advisor_3000.product_plane.research.features import materialize_feature_frames, reload_feature_frames
from trading_advisor_3000.product_plane.research.indicators import materialize_indicator_frames, reload_indicator_frames
from trading_advisor_3000.product_plane.contracts import CanonicalBar


ROOT = Path(__file__).resolve().parents[3]
RAW_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"


def _load_canonical_context(output_dir: Path) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    bars = [CanonicalBar.from_dict(row) for row in read_delta_table_rows(output_dir / "canonical_bars.delta")]
    session_calendar = [
        SessionCalendarEntry(
            instrument_id=str(row["instrument_id"]),
            timeframe=str(row["timeframe"]),
            session_date=str(row["session_date"]),
            session_open_ts=str(row["session_open_ts"]),
            session_close_ts=str(row["session_close_ts"]),
        )
        for row in read_delta_table_rows(output_dir / "canonical_session_calendar.delta")
    ]
    roll_map = [
        RollMapEntry(
            instrument_id=str(row["instrument_id"]),
            session_date=str(row["session_date"]),
            active_contract_id=str(row["active_contract_id"]),
            reason=str(row["reason"]),
        )
        for row in read_delta_table_rows(output_dir / "canonical_roll_map.delta")
    ]
    return bars, session_calendar, roll_map


def test_phase2b_dagster_bootstrap_materializes_dataset_and_indicator_layers(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    dagster_dir = tmp_path / "dagster-phase2b"
    dagster_report = materialize_phase2b_bootstrap_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )
    assert dagster_report["success"] is True
    assert set(dagster_report["selected_assets"]) == {
        "research_datasets",
        "research_bar_views",
        "research_indicator_frames",
        "research_feature_frames",
    }
    assert set(dagster_report["materialized_assets"]) == {
        "research_datasets",
        "research_bar_views",
        "research_indicator_frames",
        "research_feature_frames",
    }

    loaded_dataset = load_materialized_research_dataset(output_dir=dagster_dir, dataset_version="dagster-dataset-v1")
    loaded_indicators = reload_indicator_frames(
        indicator_output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        indicator_set_version="indicators-v1",
    )
    loaded_features = reload_feature_frames(
        feature_output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
    )
    assert loaded_dataset["dataset_manifest"]["dataset_version"] == "dagster-dataset-v1"
    assert len(loaded_dataset["bar_views"]) == 2
    assert len(loaded_indicators) == 2
    assert len(loaded_features) == 2
    assert all(row.profile_version == "core_v1" for row in loaded_indicators)
    assert all(row.profile_version == "core_v1" for row in loaded_features)


def test_phase2b_dagster_bootstrap_matches_direct_materialization(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical-direct"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )
    bars, session_calendar, roll_map = _load_canonical_context(canonical_dir)

    direct_dir = tmp_path / "direct"
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version="same-dataset-v1",
            dataset_name="direct",
            universe_id="moex-futures",
            timeframes=("15m",),
            base_timeframe="15m",
            start_ts="2026-03-16T10:00:00Z",
            end_ts="2026-03-16T10:00:00Z",
            warmup_bars=0,
            split_method="holdout",
            code_version="test",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=direct_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=direct_dir,
        indicator_output_dir=direct_dir,
        dataset_version="same-dataset-v1",
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
    )
    materialize_feature_frames(
        dataset_output_dir=direct_dir,
        indicator_output_dir=direct_dir,
        feature_output_dir=direct_dir,
        dataset_version="same-dataset-v1",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        profile_version="core_v1",
    )

    dagster_dir = tmp_path / "dagster"
    materialize_phase2b_bootstrap_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="same-dataset-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )

    direct_dataset = load_materialized_research_dataset(output_dir=direct_dir, dataset_version="same-dataset-v1")
    dagster_dataset = load_materialized_research_dataset(output_dir=dagster_dir, dataset_version="same-dataset-v1")
    direct_indicator_rows = [row.to_dict() for row in reload_indicator_frames(indicator_output_dir=direct_dir, dataset_version="same-dataset-v1", indicator_set_version="indicators-v1")]
    dagster_indicator_rows = [row.to_dict() for row in reload_indicator_frames(indicator_output_dir=dagster_dir, dataset_version="same-dataset-v1", indicator_set_version="indicators-v1")]
    direct_feature_rows = [
        row.to_dict()
        for row in reload_feature_frames(
            feature_output_dir=direct_dir,
            dataset_version="same-dataset-v1",
            indicator_set_version="indicators-v1",
            feature_set_version="features-v1",
        )
    ]
    dagster_feature_rows = [
        row.to_dict()
        for row in reload_feature_frames(
            feature_output_dir=dagster_dir,
            dataset_version="same-dataset-v1",
            indicator_set_version="indicators-v1",
            feature_set_version="features-v1",
        )
    ]

    assert direct_dataset["dataset_manifest"]["bars_hash"] == dagster_dataset["dataset_manifest"]["bars_hash"]
    assert [row.to_dict() for row in direct_dataset["bar_views"]] == [row.to_dict() for row in dagster_dataset["bar_views"]]
    assert len(direct_indicator_rows) == len(dagster_indicator_rows)
    assert [row["ts"] for row in direct_indicator_rows] == [row["ts"] for row in dagster_indicator_rows]
    assert [row["profile_version"] for row in direct_indicator_rows] == [row["profile_version"] for row in dagster_indicator_rows]
    assert len(direct_feature_rows) == len(dagster_feature_rows)
    assert [row["ts"] for row in direct_feature_rows] == [row["ts"] for row in dagster_feature_rows]
    assert [row["profile_version"] for row in direct_feature_rows] == [row["profile_version"] for row in dagster_feature_rows]


def test_phase2b_dagster_backtest_and_projection_jobs_materialize_research_flow(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical-stage7"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    dagster_dir = tmp_path / "dagster-stage7"
    backtest_report = materialize_phase2b_backtest_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-stage7-v1",
        timeframes=("15m",),
        strategy_versions=("ma-cross-v1",),
        combination_count=1,
        backtest_timeframe="15m",
    )
    assert backtest_report["success"] is True
    assert set(backtest_report["selected_assets"]) == {
        "research_backtest_batches",
        "research_backtest_runs",
        "research_strategy_stats",
        "research_trade_records",
        "research_order_records",
        "research_drawdown_records",
        "research_strategy_rankings",
    }
    assert "research_datasets" in backtest_report["materialized_assets"]
    assert "research_strategy_rankings" in backtest_report["materialized_assets"]
    assert (Path(backtest_report["output_paths"]["research_backtest_batches"]) / "_delta_log").exists()
    assert (Path(backtest_report["output_paths"]["research_strategy_rankings"]) / "_delta_log").exists()
    assert read_delta_table_rows(Path(backtest_report["output_paths"]["research_backtest_runs"]))
    assert read_delta_table_rows(Path(backtest_report["output_paths"]["research_strategy_rankings"]))

    projection_report = materialize_phase2b_projection_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-stage7-v1",
        timeframes=("15m",),
        strategy_versions=("ma-cross-v1",),
        combination_count=1,
        backtest_timeframe="15m",
        selection_policy="all_policy_pass",
        min_robust_score=0.0,
        decision_lag_bars_max=4,
    )
    assert projection_report["success"] is True
    assert set(projection_report["selected_assets"]) == {"research_signal_candidates"}
    assert "research_signal_candidates" in projection_report["materialized_assets"]
    assert (Path(projection_report["output_paths"]["research_signal_candidates"]) / "_delta_log").exists()
    candidate_rows = read_delta_table_rows(Path(projection_report["output_paths"]["research_signal_candidates"]))
    assert isinstance(candidate_rows, list)
