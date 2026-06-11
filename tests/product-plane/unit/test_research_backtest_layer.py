from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    backtest_store_contract,
    build_ephemeral_strategy_space,
    default_ranking_policy,
)
from trading_advisor_3000.product_plane.research.backtests.engine import (
    BacktestEngineConfig,
    _has_native_field,
    _optuna_ranking_policy,
    build_input_bundle,
    project_family_candidate,
    strategy_spec_to_search_spec,
)
from trading_advisor_3000.product_plane.research.backtests.input_requirements import (
    loader_columns_for_search_specs,
)
from trading_advisor_3000.product_plane.research.datasets import research_dataset_store_contract
from trading_advisor_3000.product_plane.research.derived_indicators import (
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import indicator_store_contract
from trading_advisor_3000.product_plane.research.io import loaders
from trading_advisor_3000.product_plane.research.io.loaders import (
    ResearchSeriesFrame,
    ResearchSliceRequest,
)
from trading_advisor_3000.product_plane.research.strategies import build_strategy_registry
from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)


def test_strategy_registry_exposes_stage5_parameter_combinations_and_execution_modes() -> None:
    registry = build_strategy_registry()
    assert registry.catalog_version() == "research-strategy-catalog-v1"

    ma_cross = registry.get("ma-cross-v1")
    assert ma_cross.execution_mode == "signals"
    assert {"close", "ema_10", "ema_20", "ema_50", "atr_14"} <= set(ma_cross.required_columns)
    assert len(registry.parameter_combinations("ma-cross-v1")) == 4

    squeeze = registry.get("volatility-squeeze-release-v1")
    assert squeeze.execution_mode == "signals"
    assert {
        "close",
        "bb_position_20_2",
        "kc_position_20_1_5",
        "bb_width_20_2",
        "atr_14",
    } <= set(squeeze.required_columns)
    assert "mtf_4h_to_15m_bb_position_20_2" not in squeeze.required_columns
    assert set(squeeze.required_columns_for_role("decision")) >= {
        "bb_width_20_2",
        "bb_position_20_2",
        "kc_position_20_1_5",
    }
    assert len(registry.parameter_combinations("volatility-squeeze-release-v1")) == 1944


def test_backtest_batch_request_id_is_deterministic_with_batch_sizes() -> None:
    request = BacktestBatchRequest(
        campaign_run_id="crun_test",
        strategy_space_id="sspace_test",
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        search_specs=build_ephemeral_strategy_space(
            strategy_version_labels=("ma-cross-v1", "volatility-squeeze-release-v1"),
            instances_per_strategy=2,
        ).search_specs,
        combination_count=3,
        param_batch_size=2,
        series_batch_size=1,
        timeframe="15m",
    )
    assert request.batch_id() == request.batch_id()


def test_loader_columns_for_search_specs_include_native_clock_inputs() -> None:
    search_specs = build_ephemeral_strategy_space(
        strategy_version_labels=("ma-cross-v1",),
        instances_per_strategy=2,
    ).search_specs

    columns = loader_columns_for_search_specs(search_specs)

    assert "close" in columns.price_columns
    assert {"ema_10", "ema_20", "ema_50", "atr_14"} <= set(columns.indicator_columns)


def test_vectorbt_bundle_accepts_numeric_string_volume_profile_inputs() -> None:
    frame = pd.DataFrame(
        {
            "ts": ["2026-04-01T10:00:00Z", "2026-04-01T10:15:00Z"],
            "close": [100.0, 101.0],
            "vp_poc_price": ["100.0", "101.0"],
        }
    )
    frame.index = pd.to_datetime(frame["ts"], utc=True)

    bundle = build_input_bundle(
        (
            ResearchSeriesFrame(
                contract_id="BR-6.26",
                instrument_id="FUT_BR",
                timeframe="15m",
                frame=frame,
            ),
        ),
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        clock_profile={"execution_tf": "15m"},
    )

    assert bundle.fields["vp_poc_price"]["FUT_BR"].tolist() == [100.0, 101.0]


def test_vectorbt_bundle_rejects_non_numeric_volume_profile_values() -> None:
    frame = pd.DataFrame(
        {
            "ts": ["2026-04-01T10:00:00Z", "2026-04-01T10:15:00Z"],
            "close": [100.0, 101.0],
            "vp_poc_price": ["100.0", "bad"],
        }
    )
    frame.index = pd.to_datetime(frame["ts"], utc=True)

    with pytest.raises(ValueError, match="contains non-numeric values"):
        build_input_bundle(
            (
                ResearchSeriesFrame(
                    contract_id="BR-6.26",
                    instrument_id="FUT_BR",
                    timeframe="15m",
                    frame=frame,
                ),
            ),
            dataset_version="dataset-v1",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            clock_profile={"execution_tf": "15m"},
        )


def test_has_native_field_does_not_leak_execution_fields_to_signal_layer() -> None:
    execution_frame = pd.DataFrame(
        {
            "ts": ["2026-04-01T10:00:00Z", "2026-04-01T10:15:00Z"],
            "close": [100.0, 101.0],
            "bb_width_20_2": [1.0, 1.1],
        }
    )
    signal_frame = pd.DataFrame(
        {
            "ts": ["2026-04-01T10:00:00Z"],
            "close": [100.0],
        }
    )
    execution_frame.index = pd.to_datetime(execution_frame["ts"], utc=True)
    signal_frame.index = pd.to_datetime(signal_frame["ts"], utc=True)

    bundle = build_input_bundle(
        (
            ResearchSeriesFrame(
                contract_id="BR-6.26",
                instrument_id="FUT_BR",
                timeframe="15m",
                frame=execution_frame,
            ),
            ResearchSeriesFrame(
                contract_id="BR-6.26",
                instrument_id="FUT_BR",
                timeframe="1h",
                frame=signal_frame,
            ),
        ),
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        clock_profile={"execution_tf": "15m", "signal_tf": "1h"},
    )

    assert _has_native_field(bundle, "bb_width_20_2", "signal") is False
    assert _has_native_field(bundle, "bb_width_20_2", "execution") is True


def test_project_family_candidate_rejects_same_bar_long_short_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame(
        {
            "ts": ["2026-04-01T10:00:00Z", "2026-04-01T10:15:00Z"],
            "close": [100.0, 101.0],
        }
    )
    frame.index = pd.to_datetime(frame["ts"], utc=True)
    series = ResearchSeriesFrame(
        contract_id="BR-6.26",
        instrument_id="FUT_BR",
        timeframe="15m",
        frame=frame,
    )
    search_spec = build_ephemeral_strategy_space(
        strategy_version_labels=("ma-cross-v1",),
        instances_per_strategy=1,
    ).search_specs[0]

    def _same_bar_conflict_surface(**_: object) -> object:
        return type(
            "Surface",
            (),
            {
                "entries": pd.DataFrame([False, True]),
                "short_entries": pd.DataFrame([False, True]),
                "sl_stop": pd.DataFrame([0.02, 0.02]),
                "tp_stop": pd.DataFrame([0.04, 0.04]),
            },
        )()

    monkeypatch.setattr(
        "trading_advisor_3000.product_plane.research.backtests.engine.build_signal_surface",
        _same_bar_conflict_surface,
    )

    assert (
        project_family_candidate(
            series=series,
            search_spec=search_spec,
            params={},
            config=BacktestEngineConfig(),
            dataset_version="dataset-v1",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
        )
        is None
    )


def test_vectorbt_bundle_uses_execution_frame_for_execution_metadata() -> None:
    frame = pd.DataFrame(
        {
            "ts": ["2026-04-01T10:00:00Z", "2026-04-01T10:15:00Z"],
            "close": [100.0, 101.0],
            "price_space": ["frame-space", "frame-space"],
            "active_contract_id": ["FRAME", "FRAME"],
        }
    )
    frame.index = pd.to_datetime(frame["ts"], utc=True)
    signal_frame = frame.copy()
    signal_frame["price_space"] = "signal-space"
    signal_frame["active_contract_id"] = "SIGNAL"
    execution_frame = frame.copy()
    execution_frame["price_space"] = "execution-space"
    execution_frame["active_contract_id"] = "EXEC"
    execution_frame["series_mode"] = "continuous_front"
    execution_frame["series_id"] = "BR-continuous"

    bundle = build_input_bundle(
        (
            ResearchSeriesFrame(
                contract_id="BR-6.26",
                instrument_id="BR",
                timeframe="15m",
                frame=frame,
                signal_frame=signal_frame,
                execution_frame=execution_frame,
            ),
        ),
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        clock_profile={"execution_tf": "15m"},
    )

    metadata = bundle.metadata["execution_metadata"]["BR"]
    assert metadata["active_contract_id"].tolist() == ["EXEC", "EXEC"]
    assert bundle.metadata["signal_price_spaces"]["BR"] == "signal-space"
    assert bundle.metadata["series_modes"]["BR"] == "continuous_front"
    assert bundle.metadata["series_ids"]["BR"] == "BR-continuous"


def test_legacy_search_spec_without_clock_requirements_keeps_legacy_input_validation() -> None:
    strategy = StrategySpec(
        version="legacy-inputs-v1",
        family="legacy",
        description="legacy input contract",
        required_columns=("close", "atr_14"),
        parameter_grid=(StrategyParameter("threshold", (1.0,)),),
        signal_builder_key="legacy",
        risk_policy=StrategyRiskPolicy(
            stop_atr_multiple=1.0,
            target_atr_multiple=2.0,
        ),
        ranking_metadata=StrategyRankingMetadata(tags=("test",)),
    )

    spec = strategy_spec_to_search_spec(strategy)

    assert spec.required_inputs_by_clock == {}


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("min_trade_count", 0, "min_trade_count must be positive"),
        ("min_trade_count_per_fold", 0, "min_trade_count_per_fold must be positive"),
        ("min_fold_count", 0, "min_fold_count must be positive"),
        ("max_drawdown_cap", 0.0, "max_drawdown_cap must be positive"),
    ),
)
def test_optuna_ranking_policy_preserves_zero_values_for_validation(
    field: str, value: object, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _optuna_ranking_policy({"ranking_policy": {field: value}})


def test_optuna_ranking_policy_defaults_null_parameter_stability() -> None:
    policy = _optuna_ranking_policy({"ranking_policy": {"min_parameter_stability": None}})

    assert policy.min_parameter_stability == default_ranking_policy().min_parameter_stability


def test_backtest_store_contract_contains_stage5_artifacts() -> None:
    contract = backtest_store_contract()
    assert {
        "research_strategy_search_specs",
        "research_vbt_search_runs",
        "research_optimizer_studies",
        "research_optimizer_trials",
        "research_vbt_param_results",
        "research_vbt_param_gate_events",
        "research_vbt_ephemeral_indicator_cache",
        "research_strategy_promotion_events",
        "research_validation_folds",
        "research_optimizer_selections",
        "research_backtest_batches",
        "research_backtest_runs",
        "research_strategy_stats",
        "research_trade_records",
        "research_order_records",
        "research_drawdown_records",
    } == set(contract)
    stats_columns = set(contract["research_strategy_stats"]["columns"])
    assert {
        "total_return",
        "annualized_return",
        "sharpe",
        "sortino",
        "calmar",
        "max_drawdown",
        "win_rate",
        "profit_factor",
        "expectancy",
        "trade_count",
        "exposure",
        "avg_holding_bars",
        "commission_total",
        "slippage_total",
    } <= stats_columns
    assert {"order_id", "side", "fill_qty"} <= set(contract["research_order_records"]["columns"])
    assert {"ts", "drawdown_pct", "status"} <= set(contract["research_drawdown_records"]["columns"])
    assert {
        "optimizer_study_id",
        "sampler",
        "n_trials_requested",
        "best_param_hash",
        "study_config_json",
    } <= set(contract["research_optimizer_studies"]["columns"])
    assert {
        "optimizer_trial_id",
        "trial_number",
        "trial_kind",
        "status",
        "objective_components_json",
    } <= set(contract["research_optimizer_trials"]["columns"])
    assert {
        "duration_seconds",
        "evaluations_per_second",
        "run_rows_per_second",
        "trade_rows_per_second",
    } <= set(contract["research_backtest_batches"]["columns"])


def test_backtest_loader_uses_native_delta_projection_without_python_row_reload(
    tmp_path: Path,
) -> None:
    materialized = tmp_path / "materialized"
    dataset_contract = research_dataset_store_contract()
    indicator_contract = indicator_store_contract()
    derived_contract = research_derived_indicator_store_contract()
    write_delta_table_rows(
        table_path=materialized / "research_datasets.delta",
        columns=dataset_contract["research_datasets"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "dataset_name": "dataset",
                "source_table": "canonical_bars",
                "series_mode": "contract",
                "universe_id": "universe-v1",
                "timeframes_json": ["15m"],
                "base_timeframe": "15m",
                "start_ts": "2026-04-01T10:00:00Z",
                "end_ts": "2026-04-01T10:15:00Z",
                "warmup_bars": 0,
                "split_method": "holdout",
                "split_params_json": {},
                "bars_hash": "BARS",
                "created_at": "2026-04-01T10:00:00Z",
                "code_version": "test",
                "notes_json": {},
                "source_tables": ["canonical_bars"],
                "continuous_front_policy": {},
                "lineage_key": "dataset-v1",
            }
        ],
    )
    write_delta_table_rows(
        table_path=materialized / "research_bar_views.delta",
        columns=dataset_contract["research_bar_views"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:00:00Z",
                "open": 99.0,
                "high": 101.0,
                "low": 98.5,
                "close": 100.0,
                "volume": 10_000,
                "open_interest": 20_000,
                "session_date": "2026-04-01",
                "session_open_ts": "2026-04-01T07:00:00Z",
                "session_close_ts": "2026-04-01T21:00:00Z",
                "active_contract_id": "BR-6.26",
                "bar_index": 0,
                "slice_role": "analysis",
            },
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:15:00Z",
                "open": 100.0,
                "high": 102.0,
                "low": 99.5,
                "close": 101.0,
                "volume": 11_000,
                "open_interest": 20_100,
                "session_date": "2026-04-01",
                "session_open_ts": "2026-04-01T07:00:00Z",
                "session_close_ts": "2026-04-01T21:00:00Z",
                "active_contract_id": "BR-6.26",
                "bar_index": 1,
                "slice_role": "warmup",
            },
            {
                "dataset_version": "dataset-v1",
                "contour_id": "pit_active_front",
                "contract_id": "BR-7.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:00:00Z",
                "open": 200.0,
                "high": 201.0,
                "low": 199.0,
                "close": 200.5,
                "volume": 21_000,
                "open_interest": 30_100,
                "session_date": "2026-04-01",
                "session_open_ts": "2026-04-01T07:00:00Z",
                "session_close_ts": "2026-04-01T21:00:00Z",
                "active_contract_id": "BR-7.26",
                "series_id": "FUT_BR",
                "series_mode": "continuous_front",
                "bar_index": 0,
                "slice_role": "analysis",
            },
        ],
    )
    write_delta_table_rows(
        table_path=materialized / "research_indicator_frames.delta",
        columns=indicator_contract["research_indicator_frames"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "series_mode": "contract",
                "series_id": "BR-6.26",
                "indicator_set_version": "indicators-v1",
                "profile_version": "core_v1",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:00:00Z",
                "rsi_14": 42.0,
                "atr_14": 1.5,
                "ema_10": 100.0,
                "source_bars_hash": "SRC",
                "row_count": 2,
                "warmup_span": 0,
                "null_warmup_span": 0,
                "created_at": "2026-04-01T10:00:00Z",
            }
        ],
    )
    write_delta_table_rows(
        table_path=materialized / "research_derived_indicator_frames.delta",
        columns=derived_contract["research_derived_indicator_frames"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contour_id": "native_tradable",
                "series_mode": "contract",
                "series_id": "BR-6.26",
                "indicator_set_version": "indicators-v1",
                "derived_indicator_set_version": "derived-v1",
                "profile_version": "core_v1",
                "contract_id": "BR-6.26",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-01T10:00:00Z",
                "distance_to_session_vwap": -0.25,
                "rolling_high_20": 102.0,
                "source_bars_hash": "SRC",
                "source_indicators_hash": "IND",
                "row_count": 2,
                "warmup_span": 0,
                "null_warmup_span": 0,
                "created_at": "2026-04-01T10:00:00Z",
            }
        ],
    )

    frames, _, cache_hit = loaders.load_backtest_frames(
        dataset_output_dir=materialized,
        indicator_output_dir=materialized,
        derived_indicator_output_dir=materialized,
        request=ResearchSliceRequest(
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            timeframe="15m",
            instrument_ids=("FUT_BR",),
            price_columns=("close",),
            indicator_columns=("rsi_14",),
            derived_columns=("distance_to_session_vwap",),
        ),
    )

    assert cache_hit is False
    assert len(frames) == 1
    assert frames[0].series_mode == "contract"
    frame = frames[0].frame
    assert len(frame) == 1
    assert frame["close"].tolist() == [100.0]
    assert {"close", "rsi_14", "distance_to_session_vwap"} <= set(frame.columns)
    assert {
        "open",
        "high",
        "low",
        "volume",
        "slice_role",
        "atr_14",
        "ema_10",
        "rolling_high_20",
    }.isdisjoint(frame.columns)
