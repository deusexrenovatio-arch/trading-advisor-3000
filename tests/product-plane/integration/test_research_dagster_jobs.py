from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import DagsterInstance

from trading_advisor_3000.dagster_defs import (
    MOEX_CF_REBUILD_ASSETS,
    MOEX_CF_REBUILD_JOB_NAME,
    MOEX_DATA_REBUILD_OP_NAME,
    MOEX_DERIVED_INDICATOR_REBUILD_ASSETS,
    MOEX_DERIVED_INDICATOR_REBUILD_JOB_NAME,
    MOEX_HISTORICAL_DATA_REBUILD_RESEARCH_PREP_SENSOR_NAME,
    MOEX_INDICATOR_REBUILD_ASSETS,
    MOEX_INDICATOR_REBUILD_JOB_NAME,
    MOEX_RESEARCH_BAR_REBUILD_ASSETS,
    MOEX_RESEARCH_BAR_REBUILD_JOB_NAME,
    MOEX_RESEARCH_INDICATOR_SIDECAR_ASSETS,
    MOEX_RESEARCH_INDICATOR_SIDECAR_JOB_NAME,
    RESEARCH_BACKTEST_AFTER_STRATEGY_REGISTRY_SENSOR_NAME,
    RESEARCH_BACKTEST_JOB_NAME,
    RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME,
    RESEARCH_DATA_PREP_ASSETS,
    RESEARCH_DATA_PREP_JOB_NAME,
    RESEARCH_PROJECTION_AFTER_BACKTEST_SENSOR_NAME,
    RESEARCH_PROJECTION_JOB_NAME,
    RESEARCH_STRATEGY_REGISTRY_AFTER_DATA_PREP_SENSOR_NAME,
    STRATEGY_REGISTRY_REFRESH_ASSETS,
    STRATEGY_REGISTRY_REFRESH_JOB_NAME,
    build_research_data_prep_run_config,
    build_research_definitions,
    materialize_research_backtest_assets,
    materialize_research_data_prep_assets,
    materialize_research_projection_assets,
    materialize_strategy_registry_refresh_assets,
    research_assets,
)
from trading_advisor_3000.product_plane.data_plane import run_sample_backfill
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    iter_delta_table_row_batches,
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.schemas import (
    historical_data_delta_schema_manifest,
)
from trading_advisor_3000.product_plane.research.continuous_front import (
    continuous_front_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    load_materialized_research_dataset,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import (
    materialize as dataset_materialize_module,
)
from trading_advisor_3000.product_plane.research.derived_indicators import (
    load_derived_indicator_frames,
)
from trading_advisor_3000.product_plane.research.indicators import load_indicator_frames
from trading_advisor_3000.product_plane.research.registry_store import research_registry_root
from trading_advisor_3000.product_plane.research.strategies.compiler_bridge import (
    REQUIRED_STG02_ADAPTER_KEYS,
)
from trading_advisor_3000.spark_jobs.research_bar_views_job import (
    run_research_bar_views_spark_job,
)

ROOT = Path(__file__).resolve().parents[3]
RAW_FIXTURE = (
    ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"
)


def _windows_hadoop_nativeio_unavailable() -> bool:
    if os.name != "nt":
        return False
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        return True
    return not (Path(hadoop_home) / "bin" / "hadoop.dll").exists()


requires_spark_delta_runtime = pytest.mark.skipif(
    _windows_hadoop_nativeio_unavailable(),
    reason=(
        "local Windows Spark/Delta requires Hadoop NativeIO; "
        "Docker/Linux staging proof runs this route"
    ),
)


def _read_batched_delta_rows(table_path: Path) -> list[dict[str, object]]:
    return [row for batch in iter_delta_table_row_batches(table_path) for row in batch]


def _load_materialized_dataset_rows(
    output_dir: Path, *, dataset_version: str, contour_id: str = "native_tradable"
) -> dict[str, object]:
    filters = [("dataset_version", "=", dataset_version), ("contour_id", "=", contour_id)]
    manifest_rows = read_delta_table_rows(output_dir / "research_datasets.delta", filters=filters)
    if not manifest_rows:
        raise AssertionError(
            f"missing research dataset manifest for {dataset_version}/{contour_id}"
        )
    return {
        "dataset_manifest": dict(manifest_rows[0]),
        "instrument_tree": read_delta_table_rows(
            output_dir / "research_instrument_tree.delta", filters=filters
        ),
        "bar_views": sorted(
            read_delta_table_rows(output_dir / "research_bar_views.delta", filters=filters),
            key=lambda row: (
                str(row["contour_id"]),
                str(row["series_id"]),
                str(row["contract_id"]),
                str(row["timeframe"]),
                str(row["ts"]),
            ),
        ),
    }


def _write_rich_stage7_canonical_context(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    schema_manifest = historical_data_delta_schema_manifest()
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    bars: list[dict[str, object]] = []
    provenance: list[dict[str, object]] = []
    intervals: list[dict[str, object]] = []
    calendar: list[dict[str, object]] = []
    roll_map: list[dict[str, object]] = []

    for contract_id, instrument_id, base_close, step in (
        ("BR-6.26", "BR", 82.0, 0.22),
        ("Si-6.26", "Si", 91_800.0, 55.0),
    ):
        interval_id = f"{instrument_id}-2026-03-16-regular-1"
        intervals.append(
            {
                "instrument_id": instrument_id,
                "session_date": "2026-03-16",
                "interval_id": interval_id,
                "interval_seq": 1,
                "expected_open_ts": "2026-03-16T09:00:00Z",
                "expected_close_ts": "2026-03-16T23:45:00Z",
                "session_class": "regular",
                "interval_type": "regular_trading",
                "policy_id": "stage7-rich-fixture-session-v1",
                "source_id": "stage7-rich-fixture-session",
                "source_document_hash": "sha256:stage7-rich-fixture",
            }
        )
        for index in range(60):
            bar_start = start + timedelta(minutes=15 * index)
            bar_end = bar_start + timedelta(minutes=15)
            ts = bar_start.isoformat().replace("+00:00", "Z")
            bar_end_ts = bar_end.isoformat().replace("+00:00", "Z")
            if index < 20:
                close = base_close + (index * step)
            elif index < 40:
                close = base_close + (20 * step) - ((index - 20) * step * 1.2)
            else:
                close = base_close + (20 * step) - (20 * step * 1.2) + ((index - 40) * step * 1.5)
            open_price = close - (0.35 * step)
            high = max(open_price, close) + (0.75 * step)
            low = min(open_price, close) - (0.85 * step)
            bars.append(
                {
                    "contract_id": contract_id,
                    "instrument_id": instrument_id,
                    "timeframe": "15m",
                    "ts": ts,
                    "open": round(open_price, 6),
                    "high": round(high, 6),
                    "low": round(low, 6),
                    "close": round(close, 6),
                    "volume": int(1_000 + index * 20 + (120 if index % 7 == 0 else 0)),
                    "open_interest": 20_000 + index,
                }
            )
            provenance.append(
                {
                    "contract_id": contract_id,
                    "instrument_id": instrument_id,
                    "timeframe": "15m",
                    "ts": ts,
                    "bar_start_ts": ts,
                    "bar_end_ts": bar_end_ts,
                    "session_interval_id": interval_id,
                    "source_provider": "stage7-rich-fixture",
                    "source_timeframe": "15m",
                    "source_interval": 15,
                    "source_run_id": "stage7-rich-fixture",
                    "source_ingest_run_id": "stage7-rich-fixture",
                    "source_row_count": 1,
                    "source_ts_open_first": ts,
                    "source_ts_close_last": bar_end_ts,
                    "open_interest_imputed": False,
                    "build_run_id": "stage7-rich-fixture",
                    "built_at_utc": "2026-03-16T00:00:00Z",
                }
            )
        calendar.append(
            {
                "instrument_id": instrument_id,
                "timeframe": "15m",
                "session_date": "2026-03-16",
                "session_open_ts": "2026-03-16T09:00:00Z",
                "session_close_ts": "2026-03-16T23:45:00Z",
                "session_class": "regular",
            }
        )
        roll_map.append(
            {
                "instrument_id": instrument_id,
                "session_date": "2026-03-16",
                "active_contract_id": contract_id,
                "reason": "stage7-rich-fixture",
            }
        )

    write_delta_table_rows(
        table_path=output_dir / "canonical_bars.delta",
        rows=bars,
        columns=schema_manifest["canonical_bars"]["columns"],
    )
    write_delta_table_rows(
        table_path=output_dir / "canonical_bar_provenance.delta",
        rows=provenance,
        columns=schema_manifest["canonical_bar_provenance"]["columns"],
    )
    write_delta_table_rows(
        table_path=output_dir / "canonical_session_intervals.delta",
        rows=intervals,
        columns=schema_manifest["canonical_session_intervals"]["columns"],
    )
    write_delta_table_rows(
        table_path=output_dir / "canonical_session_calendar.delta",
        rows=calendar,
        columns=schema_manifest["canonical_session_calendar"]["columns"],
    )
    write_delta_table_rows(
        table_path=output_dir / "canonical_roll_map.delta",
        rows=roll_map,
        columns=schema_manifest["canonical_roll_map"]["columns"],
    )


def _write_spark_front_outputs_from_canonical(
    *,
    canonical_dir: Path,
    output_dir: Path,
    dataset_version: str,
    run_id: str,
    policy: ContinuousFrontPolicy,
    timeframes: tuple[str, ...],
) -> dict[str, object]:
    contract = continuous_front_store_contract()
    filters = (
        [("timeframe", "in", [*timeframes])]
        if timeframes
        else [("instrument_id", "in", ["BR", "Si"])]
    )
    canonical_rows = [
        row
        for batch in iter_delta_table_row_batches(
            canonical_dir / "canonical_bars.delta", filters=filters
        )
        for row in batch
    ]
    created_at = "2026-04-29T00:00:00Z"
    front_rows = [
        {
            "dataset_version": dataset_version,
            "roll_policy_version": policy.roll_policy_version,
            "adjustment_policy_version": policy.adjustment_policy_version,
            "instrument_id": str(row["instrument_id"]),
            "timeframe": str(row["timeframe"]),
            "ts": str(row["ts"]),
            "active_contract_id": str(row["contract_id"]),
            "previous_contract_id": None,
            "candidate_contract_id": str(row["contract_id"]),
            "roll_epoch": 0,
            "roll_event_id": None,
            "is_roll_bar": False,
            "is_first_bar_after_roll": False,
            "bars_since_roll": index,
            "native_open": float(row["open"]),
            "native_high": float(row["high"]),
            "native_low": float(row["low"]),
            "native_close": float(row["close"]),
            "native_volume": int(row["volume"]),
            "native_open_interest": int(row["open_interest"]),
            "continuous_open": float(row["open"]),
            "continuous_high": float(row["high"]),
            "continuous_low": float(row["low"]),
            "continuous_close": float(row["close"]),
            "adjustment_mode": policy.adjustment_mode,
            "cumulative_additive_offset": 0.0,
            "ratio_factor": None,
            "price_space": policy.price_space,
            "causality_watermark_ts": str(row["ts"]),
            "input_row_count": len(canonical_rows),
            "created_at": created_at,
        }
        for index, row in enumerate(canonical_rows)
    ]
    groups = sorted({(str(row["instrument_id"]), str(row["timeframe"])) for row in front_rows})
    qc_rows = [
        {
            "dataset_version": dataset_version,
            "roll_policy_version": policy.roll_policy_version,
            "adjustment_policy_version": policy.adjustment_policy_version,
            "instrument_id": instrument_id,
            "timeframe": timeframe,
            "run_id": run_id,
            "started_at": created_at,
            "completed_at": created_at,
            "input_row_count": len(
                [
                    row
                    for row in front_rows
                    if row["instrument_id"] == instrument_id and row["timeframe"] == timeframe
                ]
            ),
            "output_row_count": len(
                [
                    row
                    for row in front_rows
                    if row["instrument_id"] == instrument_id and row["timeframe"] == timeframe
                ]
            ),
            "roll_event_count": 0,
            "missing_active_bar_count": 0,
            "duplicate_key_count": 0,
            "timeline_error_count": 0,
            "ohlc_error_count": 0,
            "negative_volume_oi_count": 0,
            "missing_reference_price_count": 0,
            "future_causality_violation_count": 0,
            "gap_abs_max": 0.0,
            "gap_abs_mean": 0.0,
            "blocked_reason": None,
            "status": "PASS",
        }
        for instrument_id, timeframe in groups
    ]
    table_rows = {
        "continuous_front_bars": front_rows,
        "continuous_front_roll_events": [],
        "continuous_front_adjustment_ladder": [],
        "continuous_front_qc_report": qc_rows,
    }
    output_paths: dict[str, str] = {}
    for table_name, rows in table_rows.items():
        table_path = output_dir / f"{table_name}.delta"
        write_delta_table_rows(
            table_path=table_path,
            rows=rows,
            columns=dict(contract[table_name]["columns"]),
        )
        output_paths[table_name] = table_path.as_posix()
    return {
        "success": True,
        "status": "PASS",
        "run_id": run_id,
        "dataset_version": dataset_version,
        "policy": policy.to_config_dict(),
        "output_paths": output_paths,
        "staged_output_paths": {},
        "rows_by_table": {table_name: len(rows) for table_name, rows in table_rows.items()},
        "qc_rows": qc_rows,
        "contract_check_errors": [],
        "delta_manifest": contract,
        "spark_profile": {
            "causal_roll_engine": "test-spark-entrypoint",
        },
    }


def _write_research_bar_outputs_from_front(
    *,
    continuous_front_bars_path: Path,
    output_dir: Path,
    dataset_version: str,
    dataset_name: str,
    universe_id: str,
    run_id: str,
    timeframes: tuple[str, ...],
) -> dict[str, object]:
    contract = research_dataset_store_contract()
    front_rows = read_delta_table_rows(
        continuous_front_bars_path,
        filters=[("dataset_version", "=", dataset_version)],
    )
    if timeframes:
        allowed_timeframes = set(timeframes)
        front_rows = [row for row in front_rows if str(row["timeframe"]) in allowed_timeframes]
    created_at = "2026-04-29T00:00:00Z"
    contour_id = "pit_active_front"
    instruments = sorted({str(row["instrument_id"]) for row in front_rows})
    dataset_row = {
        "dataset_version": dataset_version,
        "contour_id": contour_id,
        "dataset_name": dataset_name,
        "source_table": "continuous_front_bars",
        "series_mode": "continuous_front",
        "universe_id": universe_id,
        "timeframes_json": list(timeframes),
        "base_timeframe": timeframes[0] if timeframes else "15m",
        "start_ts": min(str(row["ts"]) for row in front_rows),
        "end_ts": max(str(row["ts"]) for row in front_rows),
        "warmup_bars": 0,
        "split_method": "holdout",
        "split_params_json": {"series_mode": "continuous_front"},
        "bars_hash": "sha256:isolated-rebuild-bars",
        "run_id": run_id,
        "as_of_ts": created_at,
        "source_delta_versions_json": {},
        "source_delta_hashes_json": {},
        "created_at": created_at,
        "code_version": "isolated-rebuild-test",
        "notes_json": {"lineage_key": "isolated-rebuild-test"},
        "source_tables": ["continuous_front_bars"],
        "continuous_front_policy": {"roll_policy_mode": "calendar_expiry_v1"},
        "lineage_key": "isolated-rebuild-test",
    }
    instrument_rows = []
    for instrument_id in instruments:
        instrument_front_rows = [
            row for row in front_rows if str(row["instrument_id"]) == instrument_id
        ]
        instrument_rows.append(
            {
                "dataset_version": dataset_version,
                "contour_id": contour_id,
                "universe_id": universe_id,
                "asset_class": "future",
                "asset_group": "unknown",
                "internal_id": f"FUT_{instrument_id.upper()}",
                "instrument_id": instrument_id,
                "source_instrument_id": instrument_id,
                "contract_ids_json": sorted(
                    {str(row["active_contract_id"]) for row in instrument_front_rows}
                ),
                "active_contract_ids_json": sorted(
                    {str(row["active_contract_id"]) for row in instrument_front_rows}
                ),
                "timeframes_json": sorted({str(row["timeframe"]) for row in instrument_front_rows}),
                "row_count": len(instrument_front_rows),
                "first_ts": min(str(row["ts"]) for row in instrument_front_rows),
                "last_ts": max(str(row["ts"]) for row in instrument_front_rows),
                "source_bars_hash": "sha256:isolated-rebuild-bars",
                "lineage_key": "isolated-rebuild-test",
                "created_at": created_at,
            }
        )
    bar_rows = []
    for index, row in enumerate(front_rows):
        ts = str(row["ts"])
        bar_rows.append(
            {
                "dataset_version": dataset_version,
                "contour_id": contour_id,
                "contract_id": str(row["active_contract_id"]),
                "instrument_id": str(row["instrument_id"]),
                "timeframe": str(row["timeframe"]),
                "ts": ts,
                "open": float(row["continuous_open"]),
                "high": float(row["continuous_high"]),
                "low": float(row["continuous_low"]),
                "close": float(row["continuous_close"]),
                "volume": int(row["native_volume"]),
                "open_interest": int(row["native_open_interest"]),
                "session_date": ts[:10],
                "session_open_ts": f"{ts[:10]}T09:00:00Z",
                "session_close_ts": f"{ts[:10]}T23:45:00Z",
                "active_contract_id": str(row["active_contract_id"]),
                "series_id": f"{row['instrument_id']}:continuous_front:{row['timeframe']}",
                "series_mode": "continuous_front",
                "roll_epoch": int(row["roll_epoch"]),
                "roll_event_id": row.get("roll_event_id"),
                "is_roll_bar": bool(row["is_roll_bar"]),
                "is_first_bar_after_roll": bool(row["is_first_bar_after_roll"]),
                "bars_since_roll": int(row["bars_since_roll"]),
                "price_space": str(row["price_space"]),
                "native_open": float(row["native_open"]),
                "native_high": float(row["native_high"]),
                "native_low": float(row["native_low"]),
                "native_close": float(row["native_close"]),
                "continuous_open": float(row["continuous_open"]),
                "continuous_high": float(row["continuous_high"]),
                "continuous_low": float(row["continuous_low"]),
                "continuous_close": float(row["continuous_close"]),
                "execution_open": float(row["continuous_open"]),
                "execution_high": float(row["continuous_high"]),
                "execution_low": float(row["continuous_low"]),
                "execution_close": float(row["continuous_close"]),
                "previous_contract_id": row.get("previous_contract_id"),
                "candidate_contract_id": row.get("candidate_contract_id"),
                "adjustment_mode": str(row["adjustment_mode"]),
                "cumulative_additive_offset": float(row["cumulative_additive_offset"]),
                "ratio_factor": row.get("ratio_factor"),
                "ret_1": None,
                "log_ret_1": None,
                "true_range": float(row["continuous_high"]) - float(row["continuous_low"]),
                "hl_range": float(row["continuous_high"]) - float(row["continuous_low"]),
                "oc_range": abs(float(row["continuous_open"]) - float(row["continuous_close"])),
                "bar_index": index,
                "slice_role": "train",
            }
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        "research_datasets": (output_dir / "research_datasets.delta").as_posix(),
        "research_instrument_tree": (output_dir / "research_instrument_tree.delta").as_posix(),
        "research_bar_views": (output_dir / "research_bar_views.delta").as_posix(),
    }
    write_delta_table_rows(
        table_path=Path(output_paths["research_datasets"]),
        rows=[dataset_row],
        columns=contract["research_datasets"]["columns"],
    )
    write_delta_table_rows(
        table_path=Path(output_paths["research_instrument_tree"]),
        rows=instrument_rows,
        columns=contract["research_instrument_tree"]["columns"],
    )
    write_delta_table_rows(
        table_path=Path(output_paths["research_bar_views"]),
        rows=bar_rows,
        columns=contract["research_bar_views"]["columns"],
    )
    return {
        "success": True,
        "dataset_manifest": dataset_row,
        "instrument_tree_count": len(instrument_rows),
        "bar_view_count": len(bar_rows),
        "output_paths": output_paths,
        "delta_manifest": contract,
    }


@requires_spark_delta_runtime
def test_research_data_prep_materializes_dataset_indicator_and_derived_layers_only(
    tmp_path: Path,
) -> None:
    canonical_dir = tmp_path / "canonical"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    dagster_dir = tmp_path / "dagster-research-data-prep"
    dagster_report = materialize_research_data_prep_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )
    assert dagster_report["success"] is True
    assert set(dagster_report["selected_assets"]) == set(RESEARCH_DATA_PREP_ASSETS)
    assert set(dagster_report["materialized_assets"]) == set(RESEARCH_DATA_PREP_ASSETS)
    assert "research_strategy_families" not in dagster_report["rows_by_table"]

    loaded_dataset = _load_materialized_dataset_rows(
        dagster_dir, dataset_version="dagster-dataset-v1"
    )
    loaded_indicators = load_indicator_frames(
        output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        indicator_set_version="indicators-v1",
    )
    loaded_derived = load_derived_indicator_frames(
        output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )
    assert loaded_dataset["dataset_manifest"]["dataset_version"] == "dagster-dataset-v1"
    assert len(loaded_dataset["instrument_tree"]) == 2
    assert {row["internal_id"] for row in loaded_dataset["instrument_tree"]} == {
        "FUT_BR",
        "FUT_SI",
    }
    assert {
        row["instrument_id"]: row["asset_group"] for row in loaded_dataset["instrument_tree"]
    } == {
        "BR": "commodity",
        "Si": "unknown",
    }
    assert len(loaded_dataset["bar_views"]) == 2
    assert len(loaded_indicators) == 2
    assert len(loaded_derived) == 2
    assert all(row.profile_version == "core_v1" for row in loaded_indicators)
    assert all(row.profile_version == "core_v1" for row in loaded_derived)


@requires_spark_delta_runtime
def test_research_data_prep_can_source_indicators_from_continuous_front(
    tmp_path: Path, monkeypatch
) -> None:
    canonical_dir = tmp_path / "canonical-continuous"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    spark_calls: list[dict[str, object]] = []

    def _spark_entrypoint(**kwargs: object) -> dict[str, object]:
        spark_calls.append(dict(kwargs))
        return _write_spark_front_outputs_from_canonical(
            canonical_dir=Path(str(kwargs["canonical_bars_path"])).parent,
            output_dir=Path(str(kwargs["output_dir"])),
            dataset_version=str(kwargs["dataset_version"]),
            run_id=str(kwargs["run_id"]),
            policy=kwargs["policy"],  # type: ignore[arg-type]
            timeframes=tuple(str(item) for item in kwargs["timeframes"]),  # type: ignore[index]
        )

    monkeypatch.setattr(research_assets, "run_continuous_front_spark_job", _spark_entrypoint)

    dagster_dir = tmp_path / "dagster-continuous"
    dagster_report = materialize_research_data_prep_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-continuous-v1",
        timeframes=("15m",),
        series_mode="continuous_front",
        continuous_front_policy=ContinuousFrontPolicy().to_config_dict(),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )

    assert dagster_report["success"] is True
    assert spark_calls
    assert dagster_report["rows_by_table"]["continuous_front_bars"] > 0
    assert dagster_report["rows_by_table"]["continuous_front_qc_report"] > 0
    assert (
        dagster_report["rows_by_table"]["research_bar_views"]
        == dagster_report["rows_by_table"]["continuous_front_bars"]
    )
    assert (
        dagster_report["total_rows_by_table"]["research_bar_views"]
        >= dagster_report["rows_by_table"]["research_bar_views"]
    )
    loaded_dataset = load_materialized_research_dataset(
        output_dir=dagster_dir,
        dataset_version="dagster-continuous-v1",
        contour_id="pit_active_front",
    )
    assert loaded_dataset["dataset_manifest"]["source_table"] == "continuous_front_bars"
    assert (
        loaded_dataset["dataset_manifest"]["continuous_front_policy"]["roll_policy_mode"]
        == "calendar_expiry_v1"
    )
    assert (
        len(loaded_dataset["bar_views"]) == dagster_report["rows_by_table"]["continuous_front_bars"]
    )
    assert all(row.contour_id == "pit_active_front" for row in loaded_dataset["bar_views"])
    native_dataset = load_materialized_research_dataset(
        output_dir=dagster_dir,
        dataset_version="dagster-continuous-v1",
        contour_id="native_tradable",
    )
    assert native_dataset["dataset_manifest"]["source_table"] == "canonical_bars"
    assert len(native_dataset["bar_views"]) > 0


@requires_spark_delta_runtime
def test_isolated_moex_data_layer_rebuild_jobs_update_only_their_layer(
    tmp_path: Path, monkeypatch
) -> None:
    canonical_dir = tmp_path / "canonical-isolated"
    _write_rich_stage7_canonical_context(canonical_dir)
    materialized_dir = tmp_path / "research-current"
    results_dir = tmp_path / "research-runs"
    dataset_version = "isolated-data-layer-v1"

    def _spark_entrypoint(**kwargs: object) -> dict[str, object]:
        return _write_spark_front_outputs_from_canonical(
            canonical_dir=Path(str(kwargs["canonical_bars_path"])).parent,
            output_dir=Path(str(kwargs["output_dir"])),
            dataset_version=str(kwargs["dataset_version"]),
            run_id=str(kwargs["run_id"]),
            policy=kwargs["policy"],  # type: ignore[arg-type]
            timeframes=tuple(str(item) for item in kwargs["timeframes"]),  # type: ignore[index]
        )

    monkeypatch.setattr(research_assets, "run_continuous_front_spark_job", _spark_entrypoint)

    def _bar_views_entrypoint(**kwargs: object) -> dict[str, object]:
        return _write_research_bar_outputs_from_front(
            continuous_front_bars_path=Path(str(kwargs["continuous_front_bars_path"])),
            output_dir=Path(str(kwargs["output_dir"])),
            dataset_version=str(kwargs["dataset_version"]),
            dataset_name=str(kwargs["dataset_name"]),
            universe_id=str(kwargs["universe_id"]),
            run_id=str(kwargs["run_id"]),
            timeframes=tuple(str(item) for item in kwargs["timeframes"]),  # type: ignore[index]
        )

    monkeypatch.setattr(research_assets, "run_research_bar_views_spark_job", _bar_views_entrypoint)

    run_config = build_research_data_prep_run_config(
        canonical_output_dir=canonical_dir,
        materialized_output_dir=materialized_dir,
        results_output_dir=results_dir,
        dataset_version=dataset_version,
        timeframes=("15m",),
        series_mode="continuous_front",
        continuous_front_policy=ContinuousFrontPolicy().to_config_dict(),
        campaign_run_id="isolated-data-layer-run",
    )
    repository = build_research_definitions().get_repository_def()
    instance = DagsterInstance.ephemeral()

    def _run_config_for_ops(*op_names: str) -> dict[str, object]:
        ops = dict(run_config["ops"])
        return {"ops": {op_name: ops[op_name] for op_name in op_names if op_name in ops}}

    cf_result = repository.get_job(MOEX_CF_REBUILD_JOB_NAME).execute_in_process(
        run_config=_run_config_for_ops("continuous_front_bars"),
        instance=instance,
        raise_on_error=True,
    )
    assert cf_result.success
    assert read_delta_table_rows(
        materialized_dir / "continuous_front_bars.delta",
        filters=[("dataset_version", "=", dataset_version)],
    )
    assert not (materialized_dir / "research_datasets.delta" / "_delta_log").exists()

    bar_result = repository.get_job(MOEX_RESEARCH_BAR_REBUILD_JOB_NAME).execute_in_process(
        run_config=_run_config_for_ops("research_datasets"),
        instance=instance,
        raise_on_error=True,
    )
    assert bar_result.success
    assert read_delta_table_rows(
        materialized_dir / "research_bar_views.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
        ],
    )
    assert not (materialized_dir / "research_indicator_frames.delta" / "_delta_log").exists()

    indicator_result = repository.get_job(MOEX_INDICATOR_REBUILD_JOB_NAME).execute_in_process(
        run_config=_run_config_for_ops("research_indicator_frames"),
        instance=instance,
        raise_on_error=True,
    )
    assert indicator_result.success
    assert read_delta_table_rows(
        materialized_dir / "research_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
        ],
    )
    assert not (
        materialized_dir / "research_derived_indicator_frames.delta" / "_delta_log"
    ).exists()

    derived_result = repository.get_job(MOEX_DERIVED_INDICATOR_REBUILD_JOB_NAME).execute_in_process(
        run_config=_run_config_for_ops("research_derived_indicator_frames"),
        instance=instance,
        raise_on_error=True,
    )
    assert derived_result.success
    assert read_delta_table_rows(
        materialized_dir / "research_derived_indicator_frames.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", "pit_active_front"),
            ("indicator_set_version", "=", "indicators-v1"),
            ("derived_indicator_set_version", "=", "derived-v1"),
        ],
    )


@requires_spark_delta_runtime
def test_research_data_prep_reuse_does_not_reload_full_frames(tmp_path: Path, monkeypatch) -> None:
    canonical_dir = tmp_path / "canonical"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    dagster_dir = tmp_path / "dagster-reuse"
    materialize_research_data_prep_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-reuse-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )

    original_dataset_reader = dataset_materialize_module.read_filtered_delta_table_rows

    def _blocked_dataset_reader(table_path: Path, *args, **kwargs):
        if Path(table_path).name == "research_bar_views.delta" and not kwargs.get("filters"):
            raise AssertionError("reuse path must not reload unfiltered research_bar_views")
        return original_dataset_reader(table_path, *args, **kwargs)

    def _blocked_frame_reload(*args, **kwargs):
        raise AssertionError("reuse path must validate existing frames without full reload")

    monkeypatch.setattr(
        dataset_materialize_module, "read_filtered_delta_table_rows", _blocked_dataset_reader
    )
    monkeypatch.setattr(
        research_assets, "reload_indicator_frames", _blocked_frame_reload, raising=False
    )
    monkeypatch.setattr(
        research_assets, "reload_derived_indicator_frames", _blocked_frame_reload, raising=False
    )

    reuse_report = materialize_research_data_prep_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-reuse-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
        reuse_existing_materialization=True,
    )

    assert reuse_report["success"] is True
    assert reuse_report["rows_by_table"]["research_bar_views"] == 2
    assert reuse_report["rows_by_table"]["research_indicator_frames"] == 2
    assert reuse_report["rows_by_table"]["research_derived_indicator_frames"] == 2


@requires_spark_delta_runtime
def test_strategy_registry_refresh_is_separate_from_research_data_prep(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical-strategy"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    dagster_dir = tmp_path / "dagster-strategy"
    materialize_research_data_prep_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-strategy-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )
    registry_report = materialize_strategy_registry_refresh_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-strategy-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )

    assert registry_report["success"] is True
    assert set(registry_report["selected_assets"]) == set(STRATEGY_REGISTRY_REFRESH_ASSETS)
    assert set(STRATEGY_REGISTRY_REFRESH_ASSETS).issubset(
        set(registry_report["materialized_assets"])
    )
    assert "research_feature_frames" not in registry_report["materialized_assets"]
    assert registry_report["rows_by_table"]["research_strategy_families"] == len(
        REQUIRED_STG02_ADAPTER_KEYS
    )
    assert registry_report["rows_by_table"]["research_strategy_templates"] == len(
        REQUIRED_STG02_ADAPTER_KEYS
    )
    assert registry_report["rows_by_table"]["research_strategy_template_modules"] >= len(
        REQUIRED_STG02_ADAPTER_KEYS
    )
    registry_root = research_registry_root(canonical_output_dir=canonical_dir)
    family_rows = read_delta_table_rows(registry_root / "research_strategy_families.delta")
    assert {str(row["family_key"]) for row in family_rows} == set(REQUIRED_STG02_ADAPTER_KEYS)


def test_research_definitions_expose_isolated_data_layer_jobs_without_moex_success_sensor(
    tmp_path: Path,
) -> None:
    definitions = build_research_definitions()
    repository = definitions.get_repository_def()
    cf_job = repository.get_job(MOEX_CF_REBUILD_JOB_NAME)
    research_bar_job = repository.get_job(MOEX_RESEARCH_BAR_REBUILD_JOB_NAME)
    indicator_job = repository.get_job(MOEX_INDICATOR_REBUILD_JOB_NAME)
    derived_job = repository.get_job(MOEX_DERIVED_INDICATOR_REBUILD_JOB_NAME)
    sidecar_job = repository.get_job(MOEX_RESEARCH_INDICATOR_SIDECAR_JOB_NAME)
    strategy_job = repository.get_job(STRATEGY_REGISTRY_REFRESH_JOB_NAME)

    job_names = {job.name for job in repository.get_all_jobs()}
    assert RESEARCH_DATA_PREP_JOB_NAME not in job_names
    assert set(cf_job.graph.node_dict) == set(MOEX_CF_REBUILD_ASSETS)
    assert set(research_bar_job.graph.node_dict) == set(MOEX_RESEARCH_BAR_REBUILD_ASSETS)
    assert set(indicator_job.graph.node_dict) == set(MOEX_INDICATOR_REBUILD_ASSETS)
    assert set(derived_job.graph.node_dict) == set(MOEX_DERIVED_INDICATOR_REBUILD_ASSETS)
    assert set(sidecar_job.graph.node_dict) == set(MOEX_RESEARCH_INDICATOR_SIDECAR_ASSETS)
    assert set(strategy_job.graph.node_dict) == {
        "research_datasets",
        *STRATEGY_REGISTRY_REFRESH_ASSETS,
    }
    isolated_sensor_names = {sensor.name for sensor in repository.sensor_defs}
    assert RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME not in isolated_sensor_names
    assert MOEX_HISTORICAL_DATA_REBUILD_RESEARCH_PREP_SENSOR_NAME not in isolated_sensor_names
    assert RESEARCH_STRATEGY_REGISTRY_AFTER_DATA_PREP_SENSOR_NAME not in isolated_sensor_names
    assert RESEARCH_BACKTEST_AFTER_STRATEGY_REGISTRY_SENSOR_NAME not in isolated_sensor_names
    assert RESEARCH_PROJECTION_AFTER_BACKTEST_SENSOR_NAME not in isolated_sensor_names

    run_config = build_research_data_prep_run_config(
        canonical_output_dir=tmp_path / "canonical",
        materialized_output_dir=tmp_path / "materialized",
        results_output_dir=tmp_path / "results",
        dataset_version="sensor-data-v1",
        timeframes=("15m",),
    )
    op_config = run_config["ops"]["research_datasets"]["config"]
    assert op_config["dataset_version"] == "sensor-data-v1"
    assert op_config["timeframes"] == ["15m"]
    assert op_config["derived_indicator_set_version"] == "derived-v1"
    assert op_config["derived_indicator_profile_version"] == "core_v1"
    assert (
        Path(str(op_config["volume_profile_raw_1m_table_path"]))
        .as_posix()
        .endswith("canonical/moex/baseline-4y-current/canonical_bars.delta")
    )
    assert op_config["volume_profile_tick_size_by_instrument"]["FUT_BR"] == 0.01
    assert run_config["ops"]["continuous_front_bars"]["config"]["series_mode"] == "continuous_front"
    assert op_config["continuous_front_policy"]["roll_policy_mode"] == "calendar_expiry_v1"
    assert op_config["continuous_front_policy"]["session_start_time"] == "09:00"
    assert op_config["continuous_front_policy"]["session_end_time"] == "23:50"
    assert op_config["continuous_front_policy"]["expected_timeline_mode"] == "active_contract_bars"
    assert (
        run_config["ops"]["continuous_front_indicator_acceptance_report"]["config"]
        == op_config
    )


def _single_sensor_run_request(sensor, context):
    result = sensor._run_status_sensor_fn(context)
    if hasattr(result, "__iter__") and not hasattr(result, "run_key"):
        requests = list(result)
        assert len(requests) == 1
        return requests[0]
    return result


def test_research_downstream_cascade_sensors_forward_dataset_context(tmp_path: Path) -> None:
    prep_run_config = build_research_data_prep_run_config(
        canonical_output_dir=tmp_path / "canonical",
        materialized_output_dir=tmp_path / "materialized",
        results_output_dir=tmp_path / "results",
        dataset_version="cascade-data-v1",
        timeframes=("15m",),
        series_mode="continuous_front",
    )
    prep_context = SimpleNamespace(
        dagster_run=SimpleNamespace(
            run_id="prep-run-1",
            run_config=prep_run_config,
            tags={},
        )
    )

    registry_request = _single_sensor_run_request(
        research_assets.strategy_registry_refresh_after_research_data_prep_sensor,
        prep_context,
    )

    assert registry_request.run_key == f"{STRATEGY_REGISTRY_REFRESH_JOB_NAME}:prep-run-1"
    assert registry_request.tags["ta3000/upstream_job"] == RESEARCH_DATA_PREP_JOB_NAME
    assert registry_request.tags["ta3000/dataset_version"] == "cascade-data-v1"
    assert registry_request.tags["ta3000/series_mode"] == "continuous_front"
    registry_config = registry_request.run_config["ops"]["research_datasets"]["config"]
    assert registry_config["dataset_version"] == "cascade-data-v1"
    assert registry_config["campaign_run_id"] == "strategy_registry_after_prep-run-1"
    assert registry_config["code_version"] == "strategy-registry-after-research-data-prep"
    assert registry_config["prepare_strategy_space"] is True

    prepared_registry_config = research_assets._prepare_strategy_space_run_config(
        registry_config,
        campaign_id="test_regular_research_cascade",
        campaign_run_id=str(registry_config["campaign_run_id"]),
    )
    assert str(prepared_registry_config["strategy_space_id"]).strip()
    assert prepared_registry_config["search_specs"]
    assert prepared_registry_config["combination_count"] > 0

    registry_context = SimpleNamespace(
        dagster_run=SimpleNamespace(
            run_id="registry-run-1",
            run_config=registry_request.run_config,
            tags=registry_request.tags,
        )
    )
    backtest_request = _single_sensor_run_request(
        research_assets.research_backtest_after_strategy_registry_sensor,
        registry_context,
    )

    assert backtest_request.run_key == f"{RESEARCH_BACKTEST_JOB_NAME}:registry-run-1"
    assert backtest_request.run_config == {}
    assert backtest_request.tags["ta3000/upstream_job"] == STRATEGY_REGISTRY_REFRESH_JOB_NAME
    assert backtest_request.tags["ta3000/dataset_version"] == "cascade-data-v1"
    assert backtest_request.tags["ta3000/series_mode"] == "continuous_front"

    backtest_context = SimpleNamespace(
        dagster_run=SimpleNamespace(
            run_id="backtest-run-1",
            run_config=backtest_request.run_config,
            tags=backtest_request.tags,
        )
    )
    projection_request = _single_sensor_run_request(
        research_assets.research_projection_after_backtest_sensor,
        backtest_context,
    )

    assert projection_request.run_key == f"{RESEARCH_PROJECTION_JOB_NAME}:backtest-run-1"
    assert projection_request.run_config == {}
    assert projection_request.tags["ta3000/upstream_job"] == RESEARCH_BACKTEST_JOB_NAME
    assert projection_request.tags["ta3000/dataset_version"] == "cascade-data-v1"


def test_moex_data_rebuild_research_refresh_sensor_ignores_preview_and_research_only_profiles() -> None:
    promoted_canonical_run_config = {
        "ops": {
            MOEX_DATA_REBUILD_OP_NAME: {
                "config": {
                    "profile_name": "canonical_from_existing_raw",
                    "publish_mode": "promote",
                }
            }
        }
    }
    staging_canonical_run_config = {
        "ops": {
            MOEX_DATA_REBUILD_OP_NAME: {
                "config": {
                    "profile_name": "full_raw_to_canonical",
                    "publish_mode": "staging_only",
                }
            }
        }
    }
    research_only_run_config = {
        "ops": {
            MOEX_DATA_REBUILD_OP_NAME: {
                "config": {
                    "profile_name": "indicator_rebuild",
                    "publish_mode": "promote",
                }
            }
        }
    }

    assert research_assets._should_start_research_after_moex_data_rebuild(
        promoted_canonical_run_config
    )
    assert not research_assets._should_start_research_after_moex_data_rebuild(
        staging_canonical_run_config
    )
    assert not research_assets._should_start_research_after_moex_data_rebuild(
        research_only_run_config
    )


def test_scheduled_research_refresh_uses_calendar_expiry_policy(monkeypatch) -> None:
    monkeypatch.delenv(
        research_assets.RESEARCH_DATA_PREP_CONTINUOUS_FRONT_POLICY_ENV, raising=False
    )

    policy = research_assets.scheduled_continuous_front_policy_config()

    assert policy["roll_policy_mode"] == "calendar_expiry_v1"
    assert policy["roll_policy_version"] == "front_calendar_expiry_t2_session_0900_2350_v1"
    assert policy["switch_timing"] == "first_active_bar_on_or_after_roll_session"
    assert policy["reference_price_policy"] == "last_old_active_close_to_first_new_active_close"


def test_scheduled_research_refresh_policy_can_be_overridden_from_env(monkeypatch) -> None:
    monkeypatch.setenv(
        research_assets.RESEARCH_DATA_PREP_CONTINUOUS_FRONT_POLICY_ENV,
        json.dumps({"roll_policy_mode": "liquidity_volume_oi_v1", "confirmation_bars": 3}),
    )

    policy = research_assets.scheduled_continuous_front_policy_config()

    assert policy["roll_policy_mode"] == "liquidity_volume_oi_v1"
    assert policy["confirmation_bars"] == 3


def test_scheduled_research_refresh_policy_rejects_unknown_env_keys(monkeypatch) -> None:
    monkeypatch.setenv(
        research_assets.RESEARCH_DATA_PREP_CONTINUOUS_FRONT_POLICY_ENV,
        json.dumps({"roll_policy_mode": "calendar_expiry_v1", "roll_policy_mod": "typo"}),
    )

    with pytest.raises(RuntimeError, match="roll_policy_mod"):
        research_assets.scheduled_continuous_front_policy_config()


def test_research_data_prep_defaults_follow_moex_historical_data_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TA3000_MOEX_HISTORICAL_DATA_ROOT", tmp_path.as_posix())
    monkeypatch.delenv("TA3000_RESEARCH_DATA_PREP_CANONICAL_OUTPUT_DIR", raising=False)
    monkeypatch.delenv("TA3000_RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR", raising=False)
    monkeypatch.delenv("TA3000_RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR", raising=False)

    run_config = build_research_data_prep_run_config(
        dataset_version="defaults-data-v1",
        volume_profile_raw_1m_table_path="   ",
    )
    op_config = run_config["ops"]["research_datasets"]["config"]

    assert (
        Path(str(op_config["canonical_output_dir"]))
        == (tmp_path / "canonical" / "moex" / "baseline-4y-current").resolve()
    )
    assert (
        Path(str(op_config["materialized_output_dir"]))
        == (tmp_path / "research" / "gold" / "current").resolve()
    )
    assert (
        Path(str(op_config["results_output_dir"]))
        == (tmp_path / "research" / "runs" / "data-prep").resolve()
    )
    assert op_config["timeframes"] == ["15m", "1h", "4h", "1d"]
    assert op_config["warmup_bars"] == 300
    assert op_config["derived_indicator_set_version"] == "derived-v1"
    assert op_config["derived_indicator_profile_version"] == "core_v1"
    assert (
        Path(str(op_config["volume_profile_raw_1m_table_path"]))
        == (
            tmp_path / "canonical" / "moex" / "baseline-4y-current" / "canonical_bars.delta"
        ).resolve()
    )
    assert op_config["volume_profile_tick_size_by_instrument"]["FUT_BR"] == 0.01


def test_research_data_prep_rejects_invalid_volume_profile_tick_size(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="invalid volume profile tick_size for FUT_BR"):
        build_research_data_prep_run_config(
            canonical_output_dir=tmp_path / "canonical",
            materialized_output_dir=tmp_path / "materialized",
            results_output_dir=tmp_path / "results",
            dataset_version="invalid-volume-profile-tick",
            volume_profile_tick_size_by_instrument={"FUT_BR": 0.0},
        )


@requires_spark_delta_runtime
def test_research_data_prep_uses_spark_l0_materialization(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical-spark-l0"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    dagster_dir = tmp_path / "dagster"
    report = materialize_research_data_prep_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="same-dataset-v1",
        timeframes=("15m",),
        indicator_set_version="indicators-v1",
        indicator_profile_version="core_v1",
    )

    dagster_dataset = _load_materialized_dataset_rows(
        dagster_dir, dataset_version="same-dataset-v1"
    )
    dagster_indicator_rows = [
        row.to_dict()
        for row in load_indicator_frames(
            output_dir=dagster_dir,
            dataset_version="same-dataset-v1",
            indicator_set_version="indicators-v1",
        )
    ]
    dagster_derived_rows = [
        row.to_dict()
        for row in load_derived_indicator_frames(
            output_dir=dagster_dir,
            dataset_version="same-dataset-v1",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
        )
    ]

    assert report["success"] is True
    assert dagster_dataset["dataset_manifest"]["code_version"] == "research-bar-views-spark"
    assert dagster_dataset["dataset_manifest"]["source_table"] == "canonical_bars"
    assert dagster_dataset["dataset_manifest"]["contour_id"] == "native_tradable"
    assert len(dagster_dataset["bar_views"]) == 2
    assert len(dagster_indicator_rows) == 2
    assert len(dagster_derived_rows) == 2
    assert {row["contour_id"] for row in dagster_indicator_rows} == {"native_tradable"}
    assert {row["contour_id"] for row in dagster_derived_rows} == {"native_tradable"}


@requires_spark_delta_runtime
def test_weekly_bar_usage_counts_daily_sessions_by_contract(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical-weekly-contract-gap"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    schema_manifest = historical_data_delta_schema_manifest()
    session_dates = [f"2026-02-0{day}" for day in range(2, 7)]
    daily_rows = [
        {
            "contract_id": "BR-6.26",
            "instrument_id": "BR",
            "timeframe": "1d",
            "ts": f"{session_date}T00:00:00Z",
            "open": 80.0,
            "high": 81.0,
            "low": 79.0,
            "close": 80.5,
            "volume": 1_000,
            "open_interest": 10_000,
        }
        for session_date in session_dates
    ]
    weekly_row = {
        "contract_id": "BR-7.26",
        "instrument_id": "BR",
        "timeframe": "1w",
        "ts": "2026-02-02T00:00:00Z",
        "open": 90.0,
        "high": 92.0,
        "low": 89.0,
        "close": 91.0,
        "volume": 2_000,
        "open_interest": 20_000,
    }
    write_delta_table_rows(
        table_path=canonical_dir / "canonical_bars.delta",
        rows=[*daily_rows, weekly_row],
        columns=schema_manifest["canonical_bars"]["columns"],
    )
    write_delta_table_rows(
        table_path=canonical_dir / "canonical_bar_provenance.delta",
        rows=[
            {
                "contract_id": "BR-7.26",
                "instrument_id": "BR",
                "timeframe": "1w",
                "ts": "2026-02-02T00:00:00Z",
                "bar_start_ts": "2026-02-02T10:00:00Z",
                "bar_end_ts": "2026-02-06T18:45:00Z",
                "session_interval_id": None,
                "source_provider": "test",
                "source_timeframe": "1d",
                "source_interval": 1440,
                "source_run_id": "weekly-contract-gap",
                "source_ingest_run_id": "weekly-contract-gap",
                "source_row_count": 5,
                "source_ts_open_first": "2026-02-02T10:00:00Z",
                "source_ts_close_last": "2026-02-06T18:45:00Z",
                "open_interest_imputed": False,
                "build_run_id": "weekly-contract-gap",
                "built_at_utc": "2026-02-07T00:00:00Z",
            }
        ],
        columns=schema_manifest["canonical_bar_provenance"]["columns"],
    )
    write_delta_table_rows(
        table_path=canonical_dir / "canonical_session_intervals.delta",
        rows=[
            {
                "instrument_id": "BR",
                "session_date": session_date,
                "interval_id": f"BR-{session_date}-regular-1",
                "interval_seq": 1,
                "expected_open_ts": f"{session_date}T10:00:00Z",
                "expected_close_ts": f"{session_date}T18:45:00Z",
                "session_class": "regular",
                "interval_type": "regular_trading",
                "policy_id": "test-official-session-v1",
                "source_id": "test-official-session",
                "source_document_hash": "sha256:test",
            }
            for session_date in session_dates
        ],
        columns=schema_manifest["canonical_session_intervals"]["columns"],
    )
    write_delta_table_rows(
        table_path=canonical_dir / "canonical_session_calendar.delta",
        rows=[
            {
                "instrument_id": "BR",
                "timeframe": "1w",
                "session_date": "2026-02-02",
                "session_open_ts": "2026-02-02T10:00:00Z",
                "session_close_ts": "2026-02-06T18:45:00Z",
                "session_class": "regular",
            }
        ],
        columns=schema_manifest["canonical_session_calendar"]["columns"],
    )
    write_delta_table_rows(
        table_path=canonical_dir / "canonical_roll_map.delta",
        rows=[
            {
                "instrument_id": "BR",
                "session_date": "2026-02-02",
                "active_contract_id": "BR-7.26",
                "reason": "weekly-contract-gap",
            }
        ],
        columns=schema_manifest["canonical_roll_map"]["columns"],
    )

    output_dir = tmp_path / "research-weekly-contract-gap"
    run_research_bar_views_spark_job(
        canonical_bars_path=canonical_dir / "canonical_bars.delta",
        canonical_bar_provenance_path=canonical_dir / "canonical_bar_provenance.delta",
        canonical_session_intervals_path=canonical_dir / "canonical_session_intervals.delta",
        canonical_session_calendar_path=canonical_dir / "canonical_session_calendar.delta",
        canonical_roll_map_path=canonical_dir / "canonical_roll_map.delta",
        continuous_front_bars_path=canonical_dir / "continuous_front_bars.delta",
        output_dir=output_dir,
        dataset_version="weekly-contract-gap-v1",
        run_id="weekly-contract-gap",
        timeframes=("1w",),
        contours=("native_tradable",),
    )

    rows = [
        row
        for batch in iter_delta_table_row_batches(
            output_dir / "research_bar_views.delta",
            filters=[
                ("dataset_version", "=", "weekly-contract-gap-v1"),
                ("contour_id", "=", "native_tradable"),
            ],
        )
        for row in batch
    ]
    assert len(rows) == 1
    assert rows[0]["contract_id"] == "BR-7.26"
    assert rows[0]["session_class"] == "partial_or_gap"
    assert rows[0]["bar_usage_profile"] == "incomplete"


@requires_spark_delta_runtime
def test_research_backtest_and_projection_jobs_materialize_research_flow(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical-stage7"
    _write_rich_stage7_canonical_context(canonical_dir)

    dagster_dir = tmp_path / "dagster-stage7"
    backtest_report = materialize_research_backtest_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-stage7-v1",
        timeframes=("15m",),
        strategy_space={
            "family_keys": ["ma_cross"],
            "template_ids": [],
            "exclude_template_manifest_hashes": [],
            "max_parameter_combinations": 64,
            "search_space_overrides": {},
            "optimizer": {
                "engine": "optuna",
                "sampler": "tpe",
                "seed": 11,
                "n_trials": 3,
                "objective": "robust_oos_trial_v1",
                "direction": "maximize",
                "top_k": 1,
                "radius": 1,
                "max_neighborhood_trials": 2,
            },
        },
        combination_count=4,
        param_batch_size=2,
        series_batch_size=1,
        backtest_timeframe="15m",
        require_out_of_sample_pass=False,
        min_trade_count=1,
        max_drawdown_cap=1.0,
        min_positive_fold_ratio=0.0,
        min_parameter_stability=0.0,
        min_slippage_score=0.0,
    )
    assert backtest_report["success"] is True
    assert set(backtest_report["selected_assets"]) == {
        "research_strategy_search_specs",
        "research_vbt_search_runs",
        "research_optimizer_studies",
        "research_optimizer_trials",
        "research_vbt_param_results",
        "research_vbt_param_gate_events",
        "research_vbt_ephemeral_indicator_cache",
        "research_strategy_promotion_events",
        "research_optimizer_studies",
        "research_optimizer_trials",
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
    assert backtest_report["rows_by_table"]["research_trade_records"] > 0
    assert backtest_report["rows_by_table"]["research_order_records"] > 0
    assert backtest_report["rows_by_table"]["research_strategy_search_specs"] > 0
    assert backtest_report["rows_by_table"]["research_vbt_search_runs"] > 0
    assert backtest_report["rows_by_table"]["research_optimizer_studies"] > 0
    assert backtest_report["rows_by_table"]["research_optimizer_trials"] > 0
    assert backtest_report["rows_by_table"]["research_vbt_param_results"] > 0
    assert backtest_report["rows_by_table"]["research_vbt_param_gate_events"] > 0
    assert "research_drawdown_records" in backtest_report["rows_by_table"]
    assert backtest_report["rows_by_table"]["research_strategy_rankings"] > 0
    assert (
        Path(backtest_report["output_paths"]["research_backtest_batches"]) / "_delta_log"
    ).exists()
    assert (
        Path(backtest_report["output_paths"]["research_optimizer_studies"]) / "_delta_log"
    ).exists()
    assert (
        Path(backtest_report["output_paths"]["research_optimizer_trials"]) / "_delta_log"
    ).exists()
    assert (
        Path(backtest_report["output_paths"]["research_strategy_rankings"]) / "_delta_log"
    ).exists()
    assert read_delta_table_rows(Path(backtest_report["output_paths"]["research_backtest_runs"]))
    optimizer_trials = read_delta_table_rows(
        Path(backtest_report["output_paths"]["research_optimizer_trials"])
    )
    assert {row["trial_kind"] for row in optimizer_trials} >= {"optuna_trial"}
    assert "neighborhood_probe" not in {row["trial_kind"] for row in optimizer_trials}
    assert all(row["param_hash"] for row in optimizer_trials)
    optimizer_components = [
        json.loads(row["objective_components_json"])
        if isinstance(row["objective_components_json"], str)
        else row["objective_components_json"]
        for row in optimizer_trials
    ]
    assert all(
        row["signal_generator"] == "vectorbt.SignalFactory.from_choice_func"
        for row in optimizer_components
    )
    assert read_delta_table_rows(
        Path(backtest_report["output_paths"]["research_strategy_rankings"])
    )

    projection_report = materialize_research_projection_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-stage7-v1",
        timeframes=("15m",),
        strategy_space={
            "family_keys": ["ma_cross"],
            "template_ids": [],
            "exclude_template_manifest_hashes": [],
            "max_parameter_combinations": 64,
            "search_space_overrides": {},
        },
        combination_count=4,
        param_batch_size=2,
        series_batch_size=1,
        backtest_timeframe="15m",
        selection_policy="all_policy_pass",
        min_robust_score=0.0,
        decision_lag_bars_max=25,
        require_out_of_sample_pass=False,
        min_trade_count=1,
        max_drawdown_cap=1.0,
        min_positive_fold_ratio=0.0,
        min_parameter_stability=0.0,
        min_slippage_score=0.0,
    )
    assert projection_report["success"] is True
    assert set(projection_report["selected_assets"]) == {"research_signal_candidates"}
    assert "research_signal_candidates" in projection_report["materialized_assets"]
    assert projection_report["rows_by_table"]["research_signal_candidates"] > 0
    assert (
        Path(projection_report["output_paths"]["research_signal_candidates"]) / "_delta_log"
    ).exists()
    candidate_rows = read_delta_table_rows(
        Path(projection_report["output_paths"]["research_signal_candidates"])
    )
    assert isinstance(candidate_rows, list)
    assert candidate_rows
    assert all(float(row["score"]) > 0.0 for row in candidate_rows)
    assert all(str(row["campaign_run_id"]).startswith("crun_") for row in candidate_rows)
