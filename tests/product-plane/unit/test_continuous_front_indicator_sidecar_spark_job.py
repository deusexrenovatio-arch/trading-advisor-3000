from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.continuous_front import (
    continuous_front_store_contract,
)
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


def _configure_local_spark(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pytest.importorskip("pyspark.sql")
    pytest.importorskip("delta")
    if os.name == "nt":
        candidates = (
            Path("D:/CodexHome/runtime/hadoop-winutils-3.3.6"),
            Path("D:/CodexHome/runtime/hadoop-winutils"),
            Path.cwd() / ".tmp" / "hadoop-winutils",
        )
        hadoop_home = next(
            (path for path in candidates if (path / "bin" / "hadoop.dll").exists()),
            None,
        )
        if hadoop_home is None:
            pytest.skip("local Windows Spark execution requires Hadoop native DLLs")
        monkeypatch.setenv("HADOOP_HOME", hadoop_home.as_posix())
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", sys.executable)
    monkeypatch.setenv("SPARK_LOCAL_IP", "127.0.0.1")
    monkeypatch.setenv("SPARK_LOCAL_HOSTNAME", "localhost")
    monkeypatch.setenv("TA3000_SPARK_SQL_SHUFFLE_PARTITIONS", "1")
    python_path = os.pathsep.join(
        [
            (Path.cwd() / "src").as_posix(),
            str(os.environ.get("PYTHONPATH") or ""),
        ]
    )
    monkeypatch.setenv("PYTHONPATH", python_path)
    monkeypatch.setenv("TA3000_SPARK_RUNTIME_ROOT", (tmp_path / "spark-runtime").as_posix())


def _bar_row(*, ts: str, close: float, contract_id: str, roll_epoch: int) -> dict[str, object]:
    return {
        "dataset_version": "cf-sidecar-v1",
        "contour_id": "pit_active_front",
        "contract_id": contract_id,
        "instrument_id": "FUT_BR",
        "timeframe": "15m",
        "ts": ts,
        "open": close - 0.5,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": 1000,
        "open_interest": 2000,
        "session_date": "2026-03-16",
        "session_open_ts": "2026-03-16T09:00:00Z",
        "session_close_ts": "2026-03-16T23:45:00Z",
        "active_contract_id": contract_id,
        "series_id": "FUT_BR",
        "series_mode": "continuous_front",
        "roll_epoch": roll_epoch,
        "roll_event_id": "roll-1" if roll_epoch else None,
        "is_roll_bar": roll_epoch == 1,
        "is_first_bar_after_roll": roll_epoch == 1,
        "bars_since_roll": 0 if roll_epoch else 1,
        "price_space": "continuous_backward_current_anchor_additive",
        "native_open": close - 0.5,
        "native_high": close + 1.0,
        "native_low": close - 1.0,
        "native_close": close,
        "cumulative_additive_offset": 8.0 if roll_epoch else 0.0,
        "adjustment_mode": "additive",
        "bar_index": roll_epoch,
        "slice_role": "analysis",
    }


def _indicator_row(ts: str, value: float, contract_id: str) -> dict[str, object]:
    return {
        "dataset_version": "cf-sidecar-v1",
        "contour_id": "pit_active_front",
        "series_mode": "continuous_front",
        "series_id": "FUT_BR",
        "indicator_set_version": "indicators-v1",
        "profile_version": "micro-profile",
        "contract_id": contract_id,
        "instrument_id": "FUT_BR",
        "timeframe": "15m",
        "ts": ts,
        "sma_20": value,
        "source_bars_hash": "bars-hash",
        "source_dataset_bars_hash": "dataset-bars-hash",
        "row_count": 3,
        "warmup_span": 0,
        "null_warmup_span": 0,
        "created_at": "2026-04-29T00:00:00Z",
        "output_columns_hash": "indicator-columns-hash",
    }


def _derived_row(ts: str, value: float, contract_id: str) -> dict[str, object]:
    return {
        "dataset_version": "cf-sidecar-v1",
        "contour_id": "pit_active_front",
        "series_mode": "continuous_front",
        "series_id": "FUT_BR",
        "indicator_set_version": "indicators-v1",
        "derived_indicator_set_version": "derived-v1",
        "profile_version": "micro-derived-profile",
        "contract_id": contract_id,
        "instrument_id": "FUT_BR",
        "timeframe": "15m",
        "ts": ts,
        "session_vwap": value,
        "source_bars_hash": "bars-hash",
        "source_dataset_bars_hash": "dataset-bars-hash",
        "source_indicators_hash": "indicators-hash",
        "source_indicator_profile_version": "micro-profile",
        "source_indicator_output_columns_hash": "indicator-columns-hash",
        "row_count": 2,
        "warmup_span": 0,
        "null_warmup_span": 0,
        "created_at": "2026-04-29T00:00:00Z",
        "output_columns_hash": "derived-columns-hash",
    }


def _write_sidecar_micro_sources(root: Path) -> None:
    write_delta_table_rows(
        table_path=root / "research_bar_views.delta",
        rows=[
            _bar_row(
                ts="2026-03-16T09:00:00Z",
                close=100.0,
                contract_id="BRK2@MOEX",
                roll_epoch=0,
            ),
            _bar_row(
                ts="2026-03-16T09:15:00Z",
                close=101.0,
                contract_id="BRK2@MOEX",
                roll_epoch=0,
            ),
            _bar_row(
                ts="2026-03-16T09:30:00Z",
                close=110.0,
                contract_id="BRM2@MOEX",
                roll_epoch=1,
            ),
        ],
        columns=research_dataset_store_contract()["research_bar_views"]["columns"],
    )
    write_delta_table_rows(
        table_path=root / "continuous_front_adjustment_ladder.delta",
        rows=[
            {
                "dataset_version": "cf-sidecar-v1",
                "roll_policy_version": "front_liquidity_oi_v1",
                "adjustment_policy_version": "backward_current_anchor_additive_v1",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "roll_event_id": "roll-1",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:30:00Z",
                "additive_gap": 8.0,
                "cumulative_offset_before": 8.0,
                "cumulative_offset_after": 0.0,
                "ratio_gap": 1.0,
                "ratio_factor_before": 1.0,
                "ratio_factor_after": 1.0,
                "created_at": "2026-04-29T00:00:00Z",
            }
        ],
        columns=continuous_front_store_contract()["continuous_front_adjustment_ladder"]["columns"],
    )
    write_delta_table_rows(
        table_path=root / "research_indicator_frames.delta",
        rows=[
            _indicator_row("2026-03-16T09:00:00Z", 20.0, "BRK2@MOEX"),
            _indicator_row("2026-03-16T09:15:00Z", 21.0, "BRK2@MOEX"),
            _indicator_row("2026-03-16T09:30:00Z", 22.0, "BRM2@MOEX"),
        ],
        columns=indicator_store_contract()["research_indicator_frames"]["columns"],
    )
    write_delta_table_rows(
        table_path=root / "research_derived_indicator_frames.delta",
        rows=[
            _derived_row("2026-03-16T09:15:00Z", 100.5, "BRK2@MOEX"),
            _derived_row("2026-03-16T09:30:00Z", 102.0, "BRM2@MOEX"),
        ],
        columns=research_derived_indicator_store_contract()["research_derived_indicator_frames"][
            "columns"
        ],
    )


def test_public_spark_sidecar_job_writes_scoped_rows_and_readable_delta(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_spark(monkeypatch, tmp_path)
    materialized_dir = tmp_path / "materialized"
    output_dir = tmp_path / "sidecar"
    _write_sidecar_micro_sources(materialized_dir)

    report = run_continuous_front_indicator_sidecar_spark_job(
        materialized_output_dir=materialized_dir,
        output_dir=output_dir,
        dataset_version="cf-sidecar-v1",
        contour_id="pit_active_front",
        source_canonical_version="continuous_front_bars",
        roll_policy_version="front_liquidity_oi_v1",
        adjustment_policy_version="backward_current_anchor_additive_v1",
        indicator_set_version="indicators-v1",
        derived_set_version="derived-v1",
        rule_set_version="rules-v1",
        adapter_hash="adapter-test",
        indicator_value_columns=("sma_20",),
        derived_value_columns=("session_vwap",),
        max_base_cross_contract_window_bars=2,
        max_derived_cross_contract_window_bars=2,
        created_at_utc="2026-04-29T00:00:00Z",
        contract=continuous_front_indicator_store_contract(),
        include_derived=True,
        rule_count=2,
        spark_master="local[1]",
    )

    assert report["status"] == "PASS"
    assert report["rows_by_table"] == {
        "cf_indicator_input_frame": 2,
        "indicator_roll_rules": 2,
        "continuous_front_indicator_frames": 2,
        "continuous_front_derived_indicator_frames": 2,
    }
    assert "scope_input_frame_to_derived_source" in report["stage_timings"]

    input_rows = read_delta_table_rows(
        output_dir / "cf_indicator_input_frame.delta",
        filters=[
            ("dataset_version", "=", "cf-sidecar-v1"),
            ("roll_policy_version", "=", "front_liquidity_oi_v1"),
        ],
    )
    base_rows = read_delta_table_rows(
        output_dir / "continuous_front_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", "cf-sidecar-v1"),
            ("indicator_set_version", "=", "indicators-v1"),
        ],
    )
    derived_rows = read_delta_table_rows(
        output_dir / "continuous_front_derived_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", "cf-sidecar-v1"),
            ("indicator_set_version", "=", "indicators-v1"),
            ("derived_set_version", "=", "derived-v1"),
        ],
    )
    expected_ts = {"2026-03-16T09:15:00Z", "2026-03-16T09:30:00Z"}
    assert {row["ts"] for row in input_rows} == expected_ts
    assert {row["ts"] for row in base_rows} == expected_ts
    assert {row["ts"] for row in derived_rows} == expected_ts

    rolled_input = next(row for row in input_rows if row["ts"] == "2026-03-16T09:30:00Z")
    assert rolled_input["cumulative_additive_offset"] == 8.0
    assert rolled_input["close0"] == 102.0

    base_hash_by_ts = {row["ts"]: row["indicator_row_hash"] for row in base_rows}
    assert {row["sma_20"] for row in base_rows} == {21.0, 22.0}
    assert all(row["indicator_row_hash_version"] for row in base_rows)
    assert {row["session_vwap"] for row in derived_rows} == {100.5, 102.0}
    assert all(
        row["source_base_indicator_row_hash"] == base_hash_by_ts[row["ts"]] for row in derived_rows
    )
    assert all(row["derived_row_hash_version"] for row in derived_rows)


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
