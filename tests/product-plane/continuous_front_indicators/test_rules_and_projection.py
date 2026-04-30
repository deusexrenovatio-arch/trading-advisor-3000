from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.continuous_front import continuous_front_store_contract
from trading_advisor_3000.product_plane.research.continuous_front_indicators import (
    CF_INDICATOR_TABLES,
    build_cf_indicator_input_rows,
    continuous_front_indicator_store_contract,
    default_indicator_roll_rules,
    run_continuous_front_indicator_pandas_job,
)
from trading_advisor_3000.product_plane.research.continuous_front_indicators.pandas_job import (
    _cross_contract_window_any,
    _verify_lineage,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView, research_dataset_store_contract
from trading_advisor_3000.product_plane.research.derived_indicators import (
    build_derived_indicator_frames,
    current_derived_indicator_profile,
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import (
    IndicatorFrameRow,
    IndicatorParameter,
    IndicatorProfile,
    IndicatorSpec,
    build_indicator_frames,
    default_indicator_profile,
)


def _view(
    *,
    ts: str,
    close: float,
    roll_epoch: int,
    active_contract_id: str,
    is_roll_bar: bool = False,
    volume: float = 1000,
    open_interest: float = 2000,
    bars_since_roll: int | None = None,
) -> ResearchBarView:
    return ResearchBarView(
        dataset_version="cf-dataset-v1",
        contract_id=active_contract_id,
        instrument_id="FUT_BR",
        timeframe="15m",
        ts=ts,
        open=close - 0.5,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=volume,
        open_interest=open_interest,
        session_date=ts[:10],
        session_open_ts=f"{ts[:10]}T09:00:00Z",
        session_close_ts=f"{ts[:10]}T23:45:00Z",
        active_contract_id=active_contract_id,
        ret_1=None,
        log_ret_1=None,
        true_range=2.0,
        hl_range=2.0,
        oc_range=0.5,
        bar_index=0,
        slice_role="analysis",
        series_id="FUT_BR|15m|continuous_front",
        series_mode="continuous_front",
        roll_epoch=roll_epoch,
        is_roll_bar=is_roll_bar,
        is_first_bar_after_roll=is_roll_bar,
        bars_since_roll=0 if is_roll_bar else (bars_since_roll if bars_since_roll is not None else roll_epoch),
        price_space="continuous_backward_current_anchor_additive",
        native_open=close - 0.5,
        native_high=close + 1.0,
        native_low=close - 1.0,
        native_close=close,
        continuous_open=close - 0.5,
        continuous_high=close + 1.0,
        continuous_low=close - 1.0,
        continuous_close=close,
        execution_open=close - 0.5,
        execution_high=close + 1.0,
        execution_low=close - 1.0,
        execution_close=close,
        previous_contract_id="BRK2@MOEX" if is_roll_bar else None,
        candidate_contract_id=active_contract_id,
        adjustment_mode="additive",
        cumulative_additive_offset=0.0,
    )


def _indicator_row(
    view: ResearchBarView,
    *,
    sma_20: float = 100.0,
    ema_20: float = 100.0,
    extra_values: dict[str, float | None] | None = None,
) -> IndicatorFrameRow:
    values = {
        "atr_14": 2.0,
        "sma_20": sma_20,
        "ema_20": ema_20,
        "rsi_14": 50.0,
        "volume_z_20": 0.0,
        "rvol_20": 1.0,
        "donchian_high_20": sma_20 + 2.0,
        "donchian_low_20": sma_20 - 2.0,
    }
    values.update(extra_values or {})
    return IndicatorFrameRow(
        dataset_version=view.dataset_version,
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
        contract_id=view.contract_id,
        instrument_id=view.instrument_id,
        timeframe=view.timeframe,
        ts=view.ts,
        values=values,
        source_bars_hash="SRC",
        row_count=3,
        warmup_span=20,
        null_warmup_span=0,
        created_at="2026-04-29T00:00:00Z",
        output_columns_hash="OUT",
    )


def test_roll_rule_catalog_covers_every_base_and_derived_output() -> None:
    rules = default_indicator_roll_rules()
    columns = {rule.output_column for rule in rules}
    expected_base = set(default_indicator_profile().expected_output_columns())
    expected_derived = set(research_derived_indicator_store_contract()["research_derived_indicator_frames"]["columns"])
    expected_derived -= {
        "dataset_version",
        "indicator_set_version",
        "derived_indicator_set_version",
        "profile_version",
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "source_bars_hash",
        "source_dataset_bars_hash",
        "source_indicators_hash",
        "source_indicator_profile_version",
        "source_indicator_output_columns_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
        "output_columns_hash",
    }

    assert expected_base <= columns
    assert expected_derived <= columns
    assert next(rule for rule in rules if rule.output_column == "ema_20").calculation_group_id == "price_level_post_transform"
    assert next(rule for rule in rules if rule.output_column == "roc_10").calculation_group_id == "anchor_sensitive_roll_aware"
    assert next(rule for rule in rules if rule.output_column == "bb_width_20_2").calculation_group_id == "anchor_sensitive_roll_aware"
    assert next(rule for rule in rules if rule.output_column == "donchian_width_20").calculation_group_id == "price_range_on_p0"
    assert next(rule for rule in rules if rule.output_column == "rvol_20").calculation_group_id == "native_volume_oi_roll_aware"
    assert next(rule for rule in rules if rule.output_column == "session_vwap").calculation_group_id == "pandas_window_derived_level"
    assert next(rule for rule in rules if rule.output_column == "session_vwap").group.adapter_id == "pandas_window_adapter"
    for column in ("volume_change_1", "price_volume_corr_20", "price_oi_corr_20", "volume_oi_corr_20"):
        rule = next(rule for rule in rules if rule.output_column == column)
        assert rule.calculation_group_id == "native_volume_oi_roll_aware"
        assert not rule.group.allow_cross_contract_window


def test_input_projection_materializes_causal_zero_anchor_prices_from_ladder() -> None:
    rows = build_cf_indicator_input_rows(
        bar_views=[
            _view(ts="2026-03-16T09:00:00Z", close=100.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
            _view(
                ts="2026-03-16T09:15:00Z",
                close=112.0,
                roll_epoch=1,
                active_contract_id="BRM2@MOEX",
                is_roll_bar=True,
            ),
        ],
        adjustment_ladder_rows=(
            {
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:15:00Z",
                "additive_gap": 10.0,
            },
        ),
        dataset_version="cf-dataset-v1",
        roll_policy_version="front_liquidity_oi_v1",
        adjustment_policy_version="backward_current_anchor_additive_v1",
        created_at_utc="2026-04-29T00:00:00Z",
    )

    assert rows[0]["close0"] == pytest.approx(100.0)
    assert rows[0]["cumulative_additive_offset"] == pytest.approx(0.0)
    assert rows[0]["price_space_normalized"] == "causal_zero_anchor"
    assert rows[0]["ts_close"] == "2026-03-16T09:15:00Z"
    assert rows[0]["causality_watermark_ts"] == "2026-03-16T09:15:00Z"
    assert rows[1]["close0"] == pytest.approx(102.0)
    assert rows[1]["ts_close"] == "2026-03-16T09:30:00Z"
    assert rows[1]["cumulative_additive_offset"] == pytest.approx(10.0)
    assert rows[1]["true_range0"] == pytest.approx(3.0)
    assert rows[1]["input_front_row_hash"]


def test_input_projection_does_not_anchor_past_rows_to_future_rolls() -> None:
    rows = build_cf_indicator_input_rows(
        bar_views=[
            _view(ts="2026-03-16T09:00:00Z", close=100.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
            _view(
                ts="2026-03-16T09:15:00Z",
                close=112.0,
                roll_epoch=1,
                active_contract_id="BRM2@MOEX",
                is_roll_bar=True,
            ),
            _view(
                ts="2026-03-16T09:30:00Z",
                close=120.0,
                roll_epoch=2,
                active_contract_id="BRN2@MOEX",
                is_roll_bar=True,
            ),
        ],
        adjustment_ladder_rows=(
            {
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:15:00Z",
                "additive_gap": 10.0,
            },
            {
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_sequence": 2,
                "effective_ts": "2026-03-16T09:30:00Z",
                "additive_gap": 5.0,
            },
        ),
        dataset_version="cf-dataset-v1",
        created_at_utc="2026-04-29T00:00:00Z",
    )

    assert [row["cumulative_additive_offset"] for row in rows] == pytest.approx([0.0, 10.0, 15.0])
    assert [row["close0"] for row in rows] == pytest.approx([100.0, 102.0, 105.0])


def test_base_indicators_use_their_declared_roll_calculation_group() -> None:
    profile = IndicatorProfile(
        version="roll_group_test",
        description="Small profile that proves group-specific continuous-front calculations.",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_2",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter(name="length", value=2),),
                required_input_columns=("close",),
                output_columns=("sma_2",),
                warmup_bars=2,
            ),
            IndicatorSpec(
                indicator_id="mom_2",
                category="momentum",
                operation_key="mom",
                parameters=(IndicatorParameter(name="length", value=2),),
                required_input_columns=("close",),
                output_columns=("mom_2",),
                warmup_bars=2,
            ),
            IndicatorSpec(
                indicator_id="roc_2",
                category="momentum",
                operation_key="roc",
                parameters=(IndicatorParameter(name="length", value=2),),
                required_input_columns=("close",),
                output_columns=("roc_2",),
                warmup_bars=2,
            ),
        ),
    )
    bars = [
        _view(ts="2026-03-16T09:00:00Z", close=100.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
        _view(ts="2026-03-16T09:15:00Z", close=101.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
        _view(
            ts="2026-03-16T09:30:00Z",
            close=112.0,
            roll_epoch=1,
            active_contract_id="BRM2@MOEX",
            is_roll_bar=True,
        ),
        _view(ts="2026-03-16T09:45:00Z", close=113.0, roll_epoch=1, active_contract_id="BRM2@MOEX"),
    ]

    rows = build_indicator_frames(
        dataset_version="cf-dataset-v1",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="continuous_front",
        profile=profile,
        adjustment_ladder_rows=(
            {
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:30:00Z",
                "additive_gap": 10.0,
            },
        ),
    )

    values_by_ts = {row.ts: row.values for row in rows}
    assert values_by_ts["2026-03-16T09:15:00Z"]["sma_2"] == pytest.approx(100.5)
    assert values_by_ts["2026-03-16T09:30:00Z"]["sma_2"] == pytest.approx(111.5)
    assert values_by_ts["2026-03-16T09:45:00Z"]["sma_2"] == pytest.approx(112.5)
    assert values_by_ts["2026-03-16T09:45:00Z"]["mom_2"] == pytest.approx(2.0)
    assert values_by_ts["2026-03-16T09:45:00Z"]["roc_2"] == pytest.approx(((113.0 / 111.0) - 1.0) * 100.0)


def test_one_bar_native_oi_change_is_null_on_roll_boundary() -> None:
    profile = IndicatorProfile(
        version="oi_roll_boundary_test",
        description="Small profile proving one-bar OI formulas compare the previous roll epoch.",
        indicators=(
            IndicatorSpec(
                indicator_id="oi_change_1",
                category="open_interest",
                operation_key="oi_change",
                parameters=(IndicatorParameter(name="length", value=1),),
                required_input_columns=("open_interest",),
                output_columns=("oi_change_1",),
                warmup_bars=1,
            ),
        ),
    )
    rows = build_indicator_frames(
        dataset_version="cf-dataset-v1",
        indicator_set_version="indicators-v1",
        bar_views=[
            _view(
                ts="2026-03-16T09:00:00Z",
                close=100.0,
                roll_epoch=0,
                active_contract_id="BRK2@MOEX",
                open_interest=5,
            ),
            _view(
                ts="2026-03-16T09:15:00Z",
                close=112.0,
                roll_epoch=1,
                active_contract_id="BRM2@MOEX",
                is_roll_bar=True,
                open_interest=1000,
            ),
        ],
        series_mode="continuous_front",
        profile=profile,
        adjustment_ladder_rows=(
            {
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:15:00Z",
                "additive_gap": 10.0,
            },
        ),
    )

    assert rows[-1].values["oi_change_1"] is None


def test_derived_levels_use_zero_anchor_then_target_anchor() -> None:
    bars = [
        _view(ts="2026-03-16T09:00:00Z", close=100.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
        _view(ts="2026-03-16T09:15:00Z", close=101.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
        _view(
            ts="2026-03-16T09:30:00Z",
            close=112.0,
            roll_epoch=1,
            active_contract_id="BRM2@MOEX",
            is_roll_bar=True,
        ),
    ]
    indicators = [
        _indicator_row(bars[0], sma_20=100.0, ema_20=100.0),
        _indicator_row(bars[1], sma_20=100.5, ema_20=100.5),
        _indicator_row(bars[2], sma_20=111.5, ema_20=111.5),
    ]

    rows = build_derived_indicator_frames(
        dataset_version="cf-dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="continuous_front",
        adjustment_ladder_rows=(
            {
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:30:00Z",
                "additive_gap": 10.0,
            },
        ),
    )

    tail = rows[-1]
    assert tail.values["session_high"] == pytest.approx(113.0)
    assert tail.values["close_change_1"] == pytest.approx(1.0)
    assert tail.values["sma_20_slope_5"] is None


def test_derived_divergence_over_reset_state_is_null_while_window_crosses_roll() -> None:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    bars: list[ResearchBarView] = []
    indicators: list[IndicatorFrameRow] = []
    for index in range(41):
        roll_epoch = 0 if index < 20 else 1
        is_roll_bar = index == 20
        close = 100.0 + index + (10.0 if roll_epoch else 0.0)
        ts = (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")
        bar = _view(
            ts=ts,
            close=close,
            roll_epoch=roll_epoch,
            active_contract_id="BRK2@MOEX" if roll_epoch == 0 else "BRM2@MOEX",
            is_roll_bar=is_roll_bar,
            bars_since_roll=0 if is_roll_bar else (index if roll_epoch == 0 else index - 20),
        )
        bars.append(bar)
        indicators.append(
            _indicator_row(
                bar,
                sma_20=close,
                ema_20=close,
                extra_values={
                    "obv": float(index * 5 if roll_epoch == 0 else (index - 20) * 7),
                    "oi_change_1": 1.0 if index > 0 and not is_roll_bar else None,
                },
            )
        )

    rows = build_derived_indicator_frames(
        dataset_version="cf-dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="continuous_front",
        adjustment_ladder_rows=(
            {
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": bars[20].ts,
                "additive_gap": 10.0,
            },
        ),
    )

    assert rows[35].values["divergence_price_obv_score"] is None
    assert rows[40].values["divergence_price_obv_score"] is not None


def test_cross_contract_metadata_tracks_active_calculation_window() -> None:
    assert _cross_contract_window_any({"roll_seq": 1, "bars_since_roll": 0}, max_window_bars=20)
    assert _cross_contract_window_any({"roll_seq": 1, "bars_since_roll": 19}, max_window_bars=20)
    assert not _cross_contract_window_any({"roll_seq": 1, "bars_since_roll": 20}, max_window_bars=20)


def test_continuous_front_indicator_job_writes_governed_sidecar_tables(tmp_path: Path) -> None:
    materialized_dir = tmp_path / "materialized"
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    views = []
    roll_index = 220
    for index in range(260):
        roll_epoch = 0 if index < roll_index else 1
        is_roll_bar = index == roll_index
        close = 100.0 + index * 0.25 + (8.0 if roll_epoch else 0.0)
        views.append(
            _view(
                ts=(start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z"),
                close=close,
                roll_epoch=roll_epoch,
                active_contract_id="BRK2@MOEX" if roll_epoch == 0 else "BRM2@MOEX",
                is_roll_bar=is_roll_bar,
                volume=1000 + index * 3 + (index % 5) * 17,
                open_interest=2000 + index * 7,
                bars_since_roll=index if roll_epoch == 0 else index - roll_index,
            )
        )
    dataset_contract = research_dataset_store_contract()
    write_delta_table_rows(
        table_path=materialized_dir / "research_datasets.delta",
        rows=[
            {
                "dataset_version": "cf-dataset-v1",
                "dataset_name": "cf",
                "source_table": "continuous_front_bars",
                "series_mode": "continuous_front",
                "universe_id": "moex-futures",
                "timeframes_json": ["15m"],
                "base_timeframe": "15m",
                "start_ts": "2026-03-16T09:00:00Z",
                "end_ts": views[-1].ts,
                "warmup_bars": 0,
                "split_method": "full",
                "split_params_json": {},
                "bars_hash": "BARS",
                "created_at": "2026-04-29T00:00:00Z",
                "code_version": "test",
                "notes_json": {},
                "source_tables": ["continuous_front_bars"],
                "continuous_front_policy": {
                    "roll_policy_version": "front_liquidity_oi_v1",
                    "adjustment_policy_version": "backward_current_anchor_additive_v1",
                },
                "lineage_key": "LINEAGE",
            }
        ],
        columns=dict(dataset_contract["research_datasets"]["columns"]),
    )
    write_delta_table_rows(
        table_path=materialized_dir / "research_bar_views.delta",
        rows=[view.to_dict() for view in views],
        columns=dict(dataset_contract["research_bar_views"]["columns"]),
    )
    write_delta_table_rows(
        table_path=materialized_dir / "continuous_front_adjustment_ladder.delta",
        rows=[
            {
                "dataset_version": "cf-dataset-v1",
                "roll_policy_version": "front_liquidity_oi_v1",
                "adjustment_policy_version": "backward_current_anchor_additive_v1",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_event_id": "roll-1",
                "roll_sequence": 1,
                "effective_ts": views[roll_index].ts,
                "additive_gap": 8.0,
                "cumulative_offset_before": 8.0,
                "cumulative_offset_after": 0.0,
                "ratio_gap": 1.0,
                "ratio_factor_before": 1.0,
                "ratio_factor_after": 1.0,
                "created_at": "2026-04-29T00:00:00Z",
            }
        ],
        columns=dict(continuous_front_store_contract()["continuous_front_adjustment_ladder"]["columns"]),
    )
    report = run_continuous_front_indicator_pandas_job(
        materialized_output_dir=materialized_dir,
        dataset_version="cf-dataset-v1",
        indicator_set_version="indicators-v1",
        derived_set_version="derived-v1",
        run_id="cf-indicator-test",
        calculation_app_id="spark-test-continuous-front-indicators",
        event_log_path="file:///tmp/spark-events/cf-indicator-test",
    )

    assert report["publish_status"] == "accepted"
    for table_name in CF_INDICATOR_TABLES:
        assert has_delta_log(materialized_dir / f"{table_name}.delta")
    assert read_delta_table_rows(materialized_dir / "indicator_roll_rules.delta")
    acceptance = read_delta_table_rows(materialized_dir / "continuous_front_indicator_acceptance_report.delta")[0]
    assert acceptance["publish_status"] == "accepted"
    assert acceptance["prefix_invariance_fail_count"] == 0
    assert acceptance["formula_sample_fail_count"] == 0
    assert acceptance["pandas_ta_parity_fail_count"] == 0
    assert acceptance["lineage_fail_count"] == 0
    manifest = read_delta_table_rows(materialized_dir / "continuous_front_indicator_run_manifest.delta")[0]
    assert manifest["created_by_pipeline"] == "spark_delta_governed"
    assert manifest["spark_app_id"]
    assert manifest["spark_event_log_path"]
    assert manifest["dependency_lock_hash"]
    assert manifest["output_delta_versions_hash"]
    formula_qc = next(row for row in report["qc_rows"] if row["check_group"] == "formula_sample")
    observed_formula_value = ast.literal_eval(str(formula_qc["observed_value"]))
    required_formula_count = (
        len(default_indicator_profile().expected_output_columns())
        + len(current_derived_indicator_profile().output_columns)
    )
    assert observed_formula_value["checked_columns_count"] == required_formula_count
    assert observed_formula_value["required_columns_count"] == required_formula_count
    assert observed_formula_value["failures"] == 0
    checked_formula_columns = set(observed_formula_value["checked_columns"])
    for token in (
        "base:sma_10",
        "base:ad",
        "base:volume_oi_ratio",
        "derived:session_vwap",
        "derived:volume_oi_corr_20",
        "derived:mtf_1d_to_4h_rsi_14",
    ):
        assert token in checked_formula_columns
    qc_groups = {
        row["check_group"]
        for row in read_delta_table_rows(materialized_dir / "continuous_front_indicator_qc_observations.delta")
    }
    assert {
        "prefix_invariance",
        "formula_sample",
        "pandas_ta_parity",
        "lineage",
        "anti_bypass",
    } <= qc_groups
    assert set(continuous_front_indicator_store_contract()) == set(CF_INDICATOR_TABLES)


def test_lineage_gate_fails_without_runtime_evidence() -> None:
    qc = _verify_lineage(
        run_id="runtime-evidence-missing",
        source_versions_digest="INPUTS",
        output_versions_digest="OUTPUTS",
        runtime_evidence={"created_by_pipeline": "spark_delta_governed"},
        input_rows=[],
        base_rows=[],
        derived_rows=[],
        indicator_value_columns=(),
        derived_value_columns=(),
    )

    assert qc["status"] == "fail"
    failures = qc["sample_rows_json"]
    missing_fields = {row["field"] for row in failures if row.get("failure") == "missing_runtime_evidence_field"}
    assert {"spark_app_id", "spark_event_log_path", "dependency_lock_hash"} <= missing_fields
