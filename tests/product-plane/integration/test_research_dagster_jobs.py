from __future__ import annotations

from pathlib import Path
from datetime import UTC, datetime, timedelta

from trading_advisor_3000.dagster_defs import (
    RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME,
    RESEARCH_DATA_PREP_ASSETS,
    RESEARCH_DATA_PREP_JOB_NAME,
    STRATEGY_REGISTRY_REFRESH_ASSETS,
    STRATEGY_REGISTRY_REFRESH_JOB_NAME,
    build_research_definitions,
    build_research_data_prep_run_config,
    materialize_research_backtest_assets,
    materialize_research_data_prep_assets,
    materialize_research_projection_assets,
    materialize_strategy_registry_refresh_assets,
)
from trading_advisor_3000.dagster_defs import research_assets
from trading_advisor_3000.product_plane.data_plane import run_sample_backfill
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.schemas import historical_data_delta_schema_manifest
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    ResearchDatasetManifest,
    load_materialized_research_dataset,
    materialize_research_dataset,
)
from trading_advisor_3000.product_plane.research.continuous_front import continuous_front_store_contract
import trading_advisor_3000.product_plane.research.datasets.materialize as dataset_materialize_module
from trading_advisor_3000.product_plane.research.derived_indicators import (
    materialize_derived_indicator_frames,
    reload_derived_indicator_frames,
)
from trading_advisor_3000.product_plane.research.indicators import materialize_indicator_frames, reload_indicator_frames
from trading_advisor_3000.product_plane.research.registry_store import research_registry_root
from trading_advisor_3000.product_plane.research.strategies.compiler_bridge import REQUIRED_STG02_ADAPTER_KEYS
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


def _write_rich_stage7_canonical_context(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    schema_manifest = historical_data_delta_schema_manifest()
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    bars: list[dict[str, object]] = []
    calendar: list[dict[str, object]] = []
    roll_map: list[dict[str, object]] = []

    for contract_id, instrument_id, base_close, step in (
        ("BR-6.26", "BR", 82.0, 0.22),
        ("Si-6.26", "Si", 91_800.0, 55.0),
    ):
        for index in range(60):
            ts = (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")
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
        calendar.append(
            {
                "instrument_id": instrument_id,
                "timeframe": "15m",
                "session_date": "2026-03-16",
                "session_open_ts": "2026-03-16T09:00:00Z",
                "session_close_ts": "2026-03-16T23:45:00Z",
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
    canonical_rows = [
        row
        for row in read_delta_table_rows(canonical_dir / "canonical_bars.delta")
        if not timeframes or str(row.get("timeframe")) in timeframes
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


def test_research_data_prep_materializes_dataset_indicator_and_derived_layers_only(tmp_path: Path) -> None:
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

    loaded_dataset = load_materialized_research_dataset(output_dir=dagster_dir, dataset_version="dagster-dataset-v1")
    loaded_indicators = reload_indicator_frames(
        indicator_output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        indicator_set_version="indicators-v1",
    )
    loaded_derived = reload_derived_indicator_frames(
        derived_indicator_output_dir=dagster_dir,
        dataset_version="dagster-dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )
    assert loaded_dataset["dataset_manifest"]["dataset_version"] == "dagster-dataset-v1"
    assert len(loaded_dataset["instrument_tree"]) == 2
    assert {row["internal_id"] for row in loaded_dataset["instrument_tree"]} == {"FUT_BR", "FUT_SI"}
    assert len(loaded_dataset["bar_views"]) == 2
    assert len(loaded_indicators) == 2
    assert len(loaded_derived) == 2
    assert all(row.profile_version == "core_v1" for row in loaded_indicators)
    assert all(row.profile_version == "core_v1" for row in loaded_derived)


def test_research_data_prep_can_source_indicators_from_continuous_front(tmp_path: Path, monkeypatch) -> None:
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
    loaded_dataset = load_materialized_research_dataset(output_dir=dagster_dir, dataset_version="dagster-continuous-v1")
    assert loaded_dataset["dataset_manifest"]["source_table"] == "continuous_front_bars"
    assert loaded_dataset["dataset_manifest"]["continuous_front_policy"]["roll_policy_mode"] == "liquidity_oi_v1"
    assert len(loaded_dataset["bar_views"]) == dagster_report["rows_by_table"]["research_bar_views"]


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

    original_dataset_reader = dataset_materialize_module.read_delta_table_rows

    def _blocked_dataset_reader(table_path: Path, *args, **kwargs):
        if Path(table_path).name == "research_bar_views.delta":
            raise AssertionError("reuse path must not reload full research_bar_views")
        return original_dataset_reader(table_path, *args, **kwargs)

    def _blocked_frame_reload(*args, **kwargs):
        raise AssertionError("reuse path must validate existing frames without full reload")

    monkeypatch.setattr(dataset_materialize_module, "read_delta_table_rows", _blocked_dataset_reader)
    monkeypatch.setattr(research_assets, "reload_indicator_frames", _blocked_frame_reload, raising=False)
    monkeypatch.setattr(research_assets, "reload_derived_indicator_frames", _blocked_frame_reload, raising=False)

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
    assert set(STRATEGY_REGISTRY_REFRESH_ASSETS).issubset(set(registry_report["materialized_assets"]))
    assert "research_feature_frames" not in registry_report["materialized_assets"]
    assert registry_report["rows_by_table"]["research_strategy_families"] == len(REQUIRED_STG02_ADAPTER_KEYS)
    assert registry_report["rows_by_table"]["research_strategy_templates"] == len(REQUIRED_STG02_ADAPTER_KEYS)
    assert registry_report["rows_by_table"]["research_strategy_template_modules"] >= len(REQUIRED_STG02_ADAPTER_KEYS)
    registry_root = research_registry_root(canonical_output_dir=canonical_dir)
    family_rows = read_delta_table_rows(registry_root / "research_strategy_families.delta")
    assert {str(row["family_key"]) for row in family_rows} == set(REQUIRED_STG02_ADAPTER_KEYS)


def test_research_definitions_expose_product_jobs_and_moex_success_sensor(tmp_path: Path) -> None:
    definitions = build_research_definitions()
    repository = definitions.get_repository_def()
    data_prep_job = repository.get_job(RESEARCH_DATA_PREP_JOB_NAME)
    strategy_job = repository.get_job(STRATEGY_REGISTRY_REFRESH_JOB_NAME)

    assert set(data_prep_job.graph.node_dict) == set(RESEARCH_DATA_PREP_ASSETS)
    assert set(strategy_job.graph.node_dict) == {"research_datasets", *STRATEGY_REGISTRY_REFRESH_ASSETS}
    assert RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME in {sensor.name for sensor in repository.sensor_defs}

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
    assert run_config["ops"]["continuous_front_bars"]["config"]["series_mode"] == "continuous_front"
    assert op_config["continuous_front_policy"]["roll_policy_mode"] == "liquidity_oi_v1"


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
    )
    op_config = run_config["ops"]["research_datasets"]["config"]

    assert Path(str(op_config["canonical_output_dir"])) == (
        tmp_path / "canonical" / "moex" / "baseline-4y-current"
    ).resolve()
    assert Path(str(op_config["materialized_output_dir"])) == (
        tmp_path / "research" / "gold" / "current"
    ).resolve()
    assert Path(str(op_config["results_output_dir"])) == (
        tmp_path / "research" / "runs" / "data-prep"
    ).resolve()
    assert op_config["timeframes"] == ["15m", "1h", "4h", "1d"]
    assert op_config["warmup_bars"] == 300
    assert op_config["derived_indicator_set_version"] == "derived-v1"


def test_research_data_prep_matches_direct_materialization(tmp_path: Path) -> None:
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
    materialize_derived_indicator_frames(
        dataset_output_dir=direct_dir,
        indicator_output_dir=direct_dir,
        derived_indicator_output_dir=direct_dir,
        dataset_version="same-dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        profile_version="core_v1",
    )

    dagster_dir = tmp_path / "dagster"
    materialize_research_data_prep_assets(
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
    direct_derived_rows = [
        row.to_dict()
        for row in reload_derived_indicator_frames(
            derived_indicator_output_dir=direct_dir,
            dataset_version="same-dataset-v1",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
        )
    ]
    dagster_derived_rows = [
        row.to_dict()
        for row in reload_derived_indicator_frames(
            derived_indicator_output_dir=dagster_dir,
            dataset_version="same-dataset-v1",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
        )
    ]

    assert direct_dataset["dataset_manifest"]["bars_hash"] == dagster_dataset["dataset_manifest"]["bars_hash"]
    assert [row.to_dict() for row in direct_dataset["bar_views"]] == [row.to_dict() for row in dagster_dataset["bar_views"]]
    assert len(direct_indicator_rows) == len(dagster_indicator_rows)
    assert [row["ts"] for row in direct_indicator_rows] == [row["ts"] for row in dagster_indicator_rows]
    assert [row["profile_version"] for row in direct_indicator_rows] == [row["profile_version"] for row in dagster_indicator_rows]
    assert len(direct_derived_rows) == len(dagster_derived_rows)
    assert [row["ts"] for row in direct_derived_rows] == [row["ts"] for row in dagster_derived_rows]
    assert [row["profile_version"] for row in direct_derived_rows] == [row["profile_version"] for row in dagster_derived_rows]


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
            "include_instance_ids": [],
            "exclude_manifest_hashes": [],
            "materialize_instances": True,
            "max_instance_count": 64,
            "search_space_overrides": {},
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
    assert "research_drawdown_records" in backtest_report["rows_by_table"]
    assert backtest_report["rows_by_table"]["research_strategy_rankings"] > 0
    assert (Path(backtest_report["output_paths"]["research_backtest_batches"]) / "_delta_log").exists()
    assert (Path(backtest_report["output_paths"]["research_strategy_rankings"]) / "_delta_log").exists()
    assert read_delta_table_rows(Path(backtest_report["output_paths"]["research_backtest_runs"]))
    assert read_delta_table_rows(Path(backtest_report["output_paths"]["research_strategy_rankings"]))

    projection_report = materialize_research_projection_assets(
        canonical_output_dir=canonical_dir,
        research_output_dir=dagster_dir,
        dataset_version="dagster-stage7-v1",
        timeframes=("15m",),
        strategy_space={
            "family_keys": ["ma_cross"],
            "template_ids": [],
            "include_instance_ids": [],
            "exclude_manifest_hashes": [],
            "materialize_instances": True,
            "max_instance_count": 64,
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
    assert (Path(projection_report["output_paths"]["research_signal_candidates"]) / "_delta_log").exists()
    candidate_rows = read_delta_table_rows(Path(projection_report["output_paths"]["research_signal_candidates"]))
    assert isinstance(candidate_rows, list)
    assert candidate_rows
    assert all(float(row["score"]) > 0.0 for row in candidate_rows)
    assert all(str(row["campaign_run_id"]).startswith("crun_") for row in candidate_rows)

