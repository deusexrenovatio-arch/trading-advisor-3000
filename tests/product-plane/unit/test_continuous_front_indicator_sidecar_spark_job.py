from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research.continuous_front_indicators import (
    continuous_front_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import research_dataset_store_contract
from trading_advisor_3000.product_plane.research.derived_indicators import (
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import indicator_store_contract
from trading_advisor_3000.spark_jobs.continuous_front_indicator_sidecar_job import (
    BASE_SOURCE_RESERVED_COLUMNS,
    DERIVED_SOURCE_RESERVED_COLUMNS,
    SIDECAR_JOIN_KEY_COLUMNS,
    _build_derived_sidecar_frame,
    run_continuous_front_indicator_sidecar_spark_job,
)


def _write_empty_sidecar_sources(
    root: Path,
    *,
    indicator_columns: dict[str, str],
    derived_columns: dict[str, str] | None = None,
    include_ladder_policy_columns: bool = True,
) -> None:
    indicator_contract_columns = indicator_store_contract()["research_indicator_frames"]["columns"]
    derived_contract_columns = research_derived_indicator_store_contract()[
        "research_derived_indicator_frames"
    ]["columns"]
    write_delta_table_rows(
        table_path=root / "research_bar_views.delta",
        rows=[],
        columns=research_dataset_store_contract()["research_bar_views"]["columns"],
    )
    ladder_columns = {
        "dataset_version": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "roll_event_id": "string",
        "roll_sequence": "int",
        "effective_ts": "timestamp",
        "additive_gap": "double",
        "cumulative_offset_before": "double",
        "cumulative_offset_after": "double",
        "ratio_gap": "double",
        "ratio_factor_before": "double",
        "ratio_factor_after": "double",
        "created_at": "timestamp",
    }
    if include_ladder_policy_columns:
        ladder_columns = {
            "dataset_version": "string",
            "roll_policy_version": "string",
            "adjustment_policy_version": "string",
            **{key: value for key, value in ladder_columns.items() if key != "dataset_version"},
        }
    write_delta_table_rows(
        table_path=root / "continuous_front_adjustment_ladder.delta",
        rows=[],
        columns=ladder_columns,
    )
    write_delta_table_rows(
        table_path=root / "research_indicator_frames.delta",
        rows=[],
        columns={
            **{
                column: indicator_contract_columns[column]
                for column in BASE_SOURCE_RESERVED_COLUMNS
            },
            **indicator_columns,
        },
    )
    write_delta_table_rows(
        table_path=root / "research_derived_indicator_frames.delta",
        rows=[],
        columns={
            **{
                column: derived_contract_columns[column]
                for column in DERIVED_SOURCE_RESERVED_COLUMNS
            },
            **(derived_columns or {}),
        },
    )


def test_spark_sidecar_job_rejects_native_contour_before_spark(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="pit_active_front"):
        run_continuous_front_indicator_sidecar_spark_job(
            materialized_output_dir=tmp_path,
            output_dir=tmp_path,
            dataset_version="cf-dataset-v1",
            contour_id="native_tradable",
            source_canonical_version="continuous_front_bars",
            roll_policy_version="front_liquidity_oi_v1",
            adjustment_policy_version="backward_current_anchor_additive_v1",
            indicator_set_version="indicators-v1",
            derived_set_version="derived-v1",
            rule_set_version="continuous_front_indicators_v1",
            adapter_hash="ADAPTER",
            indicator_value_columns=("sma_20",),
            derived_value_columns=("session_vwap",),
            max_base_cross_contract_window_bars=20,
            max_derived_cross_contract_window_bars=20,
            created_at_utc="2026-06-16T00:00:00Z",
            contract=continuous_front_indicator_store_contract(),
            include_derived=True,
            spark_session_factory=lambda _app, _master: pytest.fail("Spark should not start"),
        )


def test_spark_sidecar_job_rejects_missing_base_indicator_columns_before_spark(
    tmp_path: Path,
) -> None:
    _write_empty_sidecar_sources(
        tmp_path,
        indicator_columns={"sma_20": "double"},
        derived_columns={"session_vwap": "double"},
    )

    with pytest.raises(ValueError, match="base indicator columns missing: rsi_14"):
        run_continuous_front_indicator_sidecar_spark_job(
            materialized_output_dir=tmp_path,
            output_dir=tmp_path,
            dataset_version="cf-dataset-v1",
            contour_id="pit_active_front",
            source_canonical_version="continuous_front_bars",
            roll_policy_version="front_liquidity_oi_v1",
            adjustment_policy_version="backward_current_anchor_additive_v1",
            indicator_set_version="indicators-v1",
            derived_set_version="derived-v1",
            rule_set_version="continuous_front_indicators_v1",
            adapter_hash="ADAPTER",
            indicator_value_columns=("sma_20", "rsi_14"),
            derived_value_columns=("session_vwap",),
            max_base_cross_contract_window_bars=20,
            max_derived_cross_contract_window_bars=20,
            created_at_utc="2026-06-16T00:00:00Z",
            contract=continuous_front_indicator_store_contract(),
            include_derived=True,
            spark_session_factory=lambda _app, _master: pytest.fail("Spark should not start"),
        )


def test_spark_sidecar_job_rejects_ladder_without_policy_columns_before_spark(
    tmp_path: Path,
) -> None:
    _write_empty_sidecar_sources(
        tmp_path,
        indicator_columns={"sma_20": "double", "rsi_14": "double"},
        derived_columns={"session_vwap": "double"},
        include_ladder_policy_columns=False,
    )

    with pytest.raises(
        ValueError,
        match=("adjustment ladder missing: adjustment_policy_version, roll_policy_version"),
    ):
        run_continuous_front_indicator_sidecar_spark_job(
            materialized_output_dir=tmp_path,
            output_dir=tmp_path,
            dataset_version="cf-dataset-v1",
            contour_id="pit_active_front",
            source_canonical_version="continuous_front_bars",
            roll_policy_version="front_liquidity_oi_v1",
            adjustment_policy_version="backward_current_anchor_additive_v1",
            indicator_set_version="indicators-v1",
            derived_set_version="derived-v1",
            rule_set_version="continuous_front_indicators_v1",
            adapter_hash="ADAPTER",
            indicator_value_columns=("sma_20", "rsi_14"),
            derived_value_columns=("session_vwap",),
            max_base_cross_contract_window_bars=20,
            max_derived_cross_contract_window_bars=20,
            created_at_utc="2026-06-16T00:00:00Z",
            contract=continuous_front_indicator_store_contract(),
            include_derived=True,
            spark_session_factory=lambda _app, _master: pytest.fail("Spark should not start"),
        )


def test_spark_sidecar_job_rejects_missing_derived_columns_before_spark(
    tmp_path: Path,
) -> None:
    _write_empty_sidecar_sources(
        tmp_path,
        indicator_columns={"sma_20": "double", "rsi_14": "double"},
        derived_columns={"session_vwap": "double"},
    )

    with pytest.raises(
        ValueError,
        match="derived indicator columns missing: rolling_high_20",
    ):
        run_continuous_front_indicator_sidecar_spark_job(
            materialized_output_dir=tmp_path,
            output_dir=tmp_path,
            dataset_version="cf-dataset-v1",
            contour_id="pit_active_front",
            source_canonical_version="continuous_front_bars",
            roll_policy_version="front_liquidity_oi_v1",
            adjustment_policy_version="backward_current_anchor_additive_v1",
            indicator_set_version="indicators-v1",
            derived_set_version="derived-v1",
            rule_set_version="continuous_front_indicators_v1",
            adapter_hash="ADAPTER",
            indicator_value_columns=("sma_20", "rsi_14"),
            derived_value_columns=("session_vwap", "rolling_high_20"),
            max_base_cross_contract_window_bars=20,
            max_derived_cross_contract_window_bars=20,
            created_at_utc="2026-06-16T00:00:00Z",
            contract=continuous_front_indicator_store_contract(),
            include_derived=True,
            spark_session_factory=lambda _app, _master: pytest.fail("Spark should not start"),
        )


def test_derived_sidecar_join_projects_unique_key_columns_before_hash_join() -> None:
    import inspect

    source = inspect.getsource(_build_derived_sidecar_frame)

    assert SIDECAR_JOIN_KEY_COLUMNS == ("instrument_id", "timeframe", "ts")
    assert 'F.col("derived.instrument_id").alias("instrument_id")' in source
    assert 'F.col("input.ts_close").alias("ts_close")' in source
    assert source.index('F.col("derived.instrument_id").alias("instrument_id")') < (
        source.index('with_input.alias("joined").join(')
    )


def test_full_sidecar_scopes_input_to_derived_source_before_base_join() -> None:
    import inspect

    source = inspect.getsource(run_continuous_front_indicator_sidecar_spark_job)

    assert "_derived_sidecar_scope_keys(" in source
    assert "_filter_to_sidecar_key_scope(" in source
    assert source.index("_filter_to_sidecar_key_scope(") < source.index(
        "base_frame = _build_base_sidecar_frame("
    )
