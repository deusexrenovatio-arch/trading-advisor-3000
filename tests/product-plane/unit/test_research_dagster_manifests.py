from __future__ import annotations

from trading_advisor_3000.dagster_defs import research_asset_specs
from trading_advisor_3000.dagster_defs import research_assets
from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.research.backtests.results import backtest_store_contract
from trading_advisor_3000.product_plane.research.datasets import (
    ResearchDatasetManifest,
    materialize_research_dataset,
    research_dataset_store_contract,
)
from trading_advisor_3000.product_plane.research.datasets import materialize as dataset_materialize_module
from trading_advisor_3000.product_plane.research.derived_indicators import research_derived_indicator_store_contract
from trading_advisor_3000.product_plane.research.derived_indicators import store as derived_store_module
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    DerivedIndicatorFramePartitionKey,
    DerivedIndicatorFrameRow,
    write_derived_indicator_frames,
)
from trading_advisor_3000.product_plane.research.ids import candidate_id
from trading_advisor_3000.product_plane.research.indicators import build_indicator_profile_registry, indicator_store_contract
from trading_advisor_3000.product_plane.research.indicators import store as indicator_store_module
from trading_advisor_3000.product_plane.research.indicators.store import (
    IndicatorFramePartitionKey,
    IndicatorFrameRow,
    write_indicator_frames,
)
from trading_advisor_3000.product_plane.research.strategies.families import phase_stg02_family_adapters


def test_research_dagster_asset_specs_declared() -> None:
    specs = {spec.key: spec for spec in research_asset_specs()}
    keys = set(specs)
    assert {
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
            "research_backtest_runs",
            "research_strategy_stats",
        "research_trade_records",
        "research_order_records",
        "research_drawdown_records",
        "research_strategy_rankings",
        "research_signal_candidates",
    } == keys
    assert set(specs["research_datasets"].inputs) == {
        "canonical_bars_delta",
        "canonical_session_calendar_delta",
        "canonical_roll_map_delta",
    }
    assert set(specs["research_instrument_tree"].inputs) == {"research_datasets_delta"}
    assert set(specs["research_indicator_frames"].inputs) == {
        "research_datasets_delta",
        "research_bar_views_delta",
    }
    assert set(specs["research_derived_indicator_frames"].inputs) == {
        "research_datasets_delta",
        "research_bar_views_delta",
        "research_indicator_frames_delta",
    }
    assert set(specs["research_strategy_families"].inputs) == {"research_datasets_delta"}
    assert set(specs["research_strategy_templates"].inputs) == {"research_strategy_families_delta"}
    assert set(specs["research_strategy_template_modules"].inputs) == {"research_strategy_templates_delta"}
    assert set(specs["research_backtest_batches"].inputs) == {
        "research_datasets_delta",
        "research_indicator_frames_delta",
        "research_derived_indicator_frames_delta",
        "research_strategy_template_modules_delta",
    }
    assert set(specs["research_strategy_search_specs"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_vbt_search_runs"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_optimizer_studies"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_optimizer_trials"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_vbt_param_results"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_vbt_param_gate_events"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_vbt_ephemeral_indicator_cache"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_strategy_promotion_events"].inputs) == {"research_backtest_batches_delta"}
    assert set(specs["research_strategy_rankings"].inputs) == {
        "research_backtest_runs_delta",
        "research_strategy_stats_delta",
        "research_trade_records_delta",
    }
    assert set(specs["research_signal_candidates"].inputs) == {
        "research_datasets_delta",
        "research_derived_indicator_frames_delta",
        "research_strategy_rankings_delta",
    }


def test_backtest_result_manifest_fanout_does_not_reload_large_delta_tables(tmp_path, monkeypatch) -> None:
    table_path = tmp_path / "research_order_records.delta"
    (table_path / "_delta_log").mkdir(parents=True, exist_ok=True)
    (table_path / "_delta_log" / "00000000000000000000.json").write_text("{}", encoding="utf-8")

    def _forbidden_row_reload(*_: object, **__: object) -> None:
        raise AssertionError("manifest fanout must not reload large Delta rows")

    monkeypatch.setattr(research_assets, "read_delta_table_rows", _forbidden_row_reload)

    manifest = research_assets._backtest_result_table_manifest(  # type: ignore[attr-defined]
        {
            "output_paths": {"research_order_records": table_path.as_posix()},
            "row_counts": {"research_order_records": 885_565},
            "results_output_dir": tmp_path.as_posix(),
        },
        "research_order_records",
    )

    assert manifest == {
        "table_name": "research_order_records",
        "table_path": table_path.as_posix(),
        "row_count": 885_565,
        "has_delta_log": True,
    }


def test_backtest_ranking_inputs_use_projected_delta_columns_without_row_reload(tmp_path, monkeypatch) -> None:
    table_path = tmp_path / "research_trade_records.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns=backtest_store_contract()["research_trade_records"]["columns"],
        rows=[
            {
                "backtest_run_id": "RUN",
                "campaign_run_id": "CRUN",
                "strategy_instance_id": "SINST",
                "strategy_template_id": "STPL",
                "family_id": "SFAM",
                "family_key": "trend_mtf_pullback_v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "window_id": "wf-01",
                "param_hash": "PARAM",
                "trade_id": "TRADE",
                "side": "long",
                "status": "closed",
                "entry_ts": "2026-04-30T10:00:00Z",
                "exit_ts": "2026-04-30T11:00:00Z",
                "entry_price": 100.0,
                "exit_price": 101.0,
                "qty": 1.0,
                "gross_pnl": 1.0,
                "net_pnl": 0.8,
                "commission": 0.1,
                "slippage": 0.1,
                "holding_bars": 4,
                "stop_ref": 99.0,
                "target_ref": 102.0,
            }
        ],
    )

    def _forbidden_row_reload(*_: object, **__: object) -> None:
        raise AssertionError("ranking inputs must use projected Delta reads")

    monkeypatch.setattr(research_assets, "read_delta_table_rows", _forbidden_row_reload)

    rows = research_assets._backtest_result_rows(  # type: ignore[attr-defined]
        {
            "output_paths": {"research_trade_records": table_path.as_posix()},
            "results_output_dir": tmp_path.as_posix(),
        },
        "research_trade_records",
        columns=research_assets.RANKING_TRADE_COLUMNS,
    )

    assert rows == [
        {
            "backtest_run_id": "RUN",
            "campaign_run_id": "CRUN",
            "contract_id": "BRQ2@MOEX",
            "instrument_id": "FUT_BR",
            "timeframe": "15m",
            "window_id": "wf-01",
            "entry_price": 100.0,
            "exit_price": 101.0,
            "qty": 1.0,
            "gross_pnl": 1.0,
            "net_pnl": 0.8,
            "commission": 0.1,
        }
    ]


def test_dagster_backtest_handoff_passes_delta_manifests_into_ranking(tmp_path, monkeypatch) -> None:
    results_dir = tmp_path / "results"
    ranking_path = results_dir / "research_strategy_rankings.delta"
    for table_name in (
        "research_backtest_runs",
        "research_strategy_stats",
        "research_trade_records",
        "research_strategy_rankings",
    ):
        (results_dir / f"{table_name}.delta" / "_delta_log").mkdir(parents=True, exist_ok=True)

    batch_manifest = {
        "output_paths": {
            "research_backtest_runs": (results_dir / "research_backtest_runs.delta").as_posix(),
            "research_strategy_stats": (results_dir / "research_strategy_stats.delta").as_posix(),
            "research_trade_records": (results_dir / "research_trade_records.delta").as_posix(),
        },
        "row_counts": {
            "research_backtest_runs": 6_135,
            "research_strategy_stats": 6_135,
            "research_trade_records": 443_028,
        },
        "results_output_dir": results_dir.as_posix(),
    }
    run_manifest = research_assets.research_backtest_runs(batch_manifest)
    stat_manifest = research_assets.research_strategy_stats(batch_manifest)
    trade_manifest = research_assets.research_trade_records(batch_manifest)
    assert run_manifest["table_path"].endswith("research_backtest_runs.delta")
    assert stat_manifest["row_count"] == 6_135
    assert trade_manifest["row_count"] == 443_028

    captured: dict[str, object] = {}

    def _fake_rank_backtest_results(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "ranking_rows": [{"ranking_id": "RANK"}],
            "finding_rows": [],
            "output_paths": {"research_strategy_rankings": ranking_path.as_posix()},
        }

    monkeypatch.setattr(research_assets, "rank_backtest_results", _fake_rank_backtest_results)
    ranking_manifest = research_assets.research_strategy_rankings(
        {
            "results_output_dir": results_dir.as_posix(),
            "ranking_policy": {
                "policy_id": "robust_oos_v1",
                "metric_order": ["total_return", "profit_factor", "max_drawdown"],
                "require_out_of_sample_pass": True,
                "min_trade_count": 12,
                "min_fold_count": 2,
                "max_drawdown_cap": 0.35,
                "min_positive_fold_ratio": 0.5,
                "stress_slippage_bps": 7.5,
                "min_parameter_stability": 0.35,
                "min_slippage_score": 0.45,
            },
        },
        run_manifest,
        stat_manifest,
        trade_manifest,
    )

    assert captured["backtest_output_dir"] == results_dir
    assert "run_rows" not in captured
    assert "stat_rows" not in captured
    assert "trade_rows" not in captured
    assert ranking_manifest == {
        "table_name": "research_strategy_rankings",
        "table_path": ranking_path.as_posix(),
        "row_count": 1,
        "has_delta_log": True,
    }


def test_reused_data_prep_validation_does_not_reload_materialized_research_rows(tmp_path, monkeypatch) -> None:
    dataset_contract = research_dataset_store_contract()
    indicator_contract = indicator_store_contract()
    derived_contract = research_derived_indicator_store_contract()
    write_delta_table_rows(
        table_path=tmp_path / "research_datasets.delta",
        columns=dataset_contract["research_datasets"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "dataset_name": "dataset",
                "source_table": "canonical_bars",
                "universe_id": "universe-v1",
                "timeframes_json": ["15m"],
                "base_timeframe": "15m",
                "series_mode": "contract",
                "split_method": "holdout",
                "warmup_bars": 10,
                "row_count": 1,
                "created_at": "2026-04-30T00:00:00Z",
            }
        ],
    )
    write_delta_table_rows(
        table_path=tmp_path / "research_instrument_tree.delta",
        columns=dataset_contract["research_instrument_tree"]["columns"],
        rows=[{"dataset_version": "dataset-v1", "instrument_id": "FUT_BR", "row_count": 1}],
    )
    write_delta_table_rows(
        table_path=tmp_path / "research_bar_views.delta",
        columns=dataset_contract["research_bar_views"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-30T10:00:00Z",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1,
                "open_interest": 1,
                "bar_index": 0,
                "slice_role": "train",
            }
        ],
    )
    write_delta_table_rows(
        table_path=tmp_path / "research_indicator_frames.delta",
        columns=indicator_contract["research_indicator_frames"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "indicator_set_version": "indicators-v1",
                "profile_version": "core_v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-30T10:00:00Z",
            }
        ],
    )
    write_delta_table_rows(
        table_path=tmp_path / "research_derived_indicator_frames.delta",
        columns=derived_contract["research_derived_indicator_frames"]["columns"],
        rows=[
            {
                "dataset_version": "dataset-v1",
                "indicator_set_version": "indicators-v1",
                "derived_indicator_set_version": "derived-v1",
                "profile_version": "core_v1",
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-30T10:00:00Z",
            }
        ],
    )

    row_load_paths: list[str] = []
    original_read_rows = research_assets.read_delta_table_rows

    def _tracked_row_read(table_path, *args, **kwargs):
        row_load_paths.append(table_path.name)
        return original_read_rows(table_path, *args, **kwargs)

    monkeypatch.setattr(research_assets, "read_delta_table_rows", _tracked_row_read)

    research_assets._require_existing_data_prep(  # type: ignore[attr-defined]
        materialized_output_dir=tmp_path,
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )
    manifest = research_assets._dataset_manifest_row(  # type: ignore[attr-defined]
        materialized_output_dir=tmp_path,
        dataset_version="dataset-v1",
    )

    assert manifest["dataset_version"] == "dataset-v1"
    assert row_load_paths == ["research_datasets.delta"]


def test_research_dataset_materialization_replaces_delta_version_without_existing_row_reload(tmp_path, monkeypatch) -> None:
    bars = [
        CanonicalBar.from_dict(
            {
                "contract_id": "BRQ2@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "ts": "2026-04-30T10:00:00Z",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10,
                "open_interest": 100,
            }
        )
    ]
    calendar = [SessionCalendarEntry("FUT_BR", "15m", "2026-04-30", "2026-04-30T10:00:00Z", "2026-04-30T10:00:00Z")]
    roll_map = [RollMapEntry("FUT_BR", "2026-04-30", "BRQ2@MOEX", "test")]
    manifest = ResearchDatasetManifest(
        dataset_version="native-dataset-v1",
        dataset_name="native dataset",
        universe_id="moex-futures",
        timeframes=("15m",),
        base_timeframe="15m",
        start_ts="2026-04-30T10:00:00Z",
        end_ts="2026-04-30T10:00:00Z",
        warmup_bars=0,
        split_method="full",
        code_version="test",
    )
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars,
        session_calendar=calendar,
        roll_map=roll_map,
        output_dir=tmp_path,
    )

    def _forbidden_existing_reload(*_: object, **__: object) -> None:
        raise AssertionError("dataset materialization must not reload existing Delta rows before replacement")

    monkeypatch.setattr(dataset_materialize_module, "read_delta_table_rows", _forbidden_existing_reload)
    materialize_research_dataset(
        manifest_seed=manifest,
        bars=bars,
        session_calendar=calendar,
        roll_map=roll_map,
        output_dir=tmp_path,
    )

    rows = read_delta_table_rows(tmp_path / "research_bar_views.delta", filters=[("dataset_version", "=", "native-dataset-v1")])
    assert len(rows) == 1
    assert rows[0]["contract_id"] == "BRQ2@MOEX"


def test_indicator_store_replaces_partition_with_delta_delete_append_without_row_reload(tmp_path, monkeypatch) -> None:
    path = tmp_path / "research_indicator_frames.delta"
    columns = indicator_store_contract()["research_indicator_frames"]["columns"]
    partition = IndicatorFramePartitionKey(
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        instrument_id="FUT_BR",
        contract_id="BRQ2@MOEX",
        timeframe="15m",
    )
    existing = IndicatorFrameRow(
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
        contract_id="BRQ2@MOEX",
        instrument_id="FUT_BR",
        timeframe="15m",
        ts="2026-04-30T10:00:00Z",
        values={"ema_20": 1.0},
        source_bars_hash="OLD",
        row_count=1,
        warmup_span=0,
        null_warmup_span=0,
        created_at="2026-04-30T10:00:00Z",
    )
    replacement = IndicatorFrameRow(
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
        contract_id="BRQ2@MOEX",
        instrument_id="FUT_BR",
        timeframe="15m",
        ts="2026-04-30T10:00:00Z",
        values={"ema_20": 2.0},
        source_bars_hash="NEW",
        row_count=1,
        warmup_span=0,
        null_warmup_span=0,
        created_at="2026-04-30T10:01:00Z",
    )
    write_delta_table_rows(table_path=path, columns=columns, rows=[existing.to_dict()])

    def _forbidden_row_reload(*_: object, **__: object) -> None:
        raise AssertionError("indicator partition replacement must not reload existing rows")

    monkeypatch.setattr(indicator_store_module, "read_delta_table_rows", _forbidden_row_reload)
    write_indicator_frames(output_dir=tmp_path, rows=[replacement], replace_partitions=(partition,))

    rows = read_delta_table_rows(path, filters=[("dataset_version", "=", "dataset-v1")])
    assert len(rows) == 1
    assert rows[0]["ema_20"] == 2.0
    assert rows[0]["source_bars_hash"] == "NEW"


def test_derived_store_replaces_partition_with_delta_delete_append_without_row_reload(tmp_path, monkeypatch) -> None:
    path = tmp_path / "research_derived_indicator_frames.delta"
    columns = research_derived_indicator_store_contract()["research_derived_indicator_frames"]["columns"]
    partition = DerivedIndicatorFramePartitionKey(
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        instrument_id="FUT_BR",
        contract_id="BRQ2@MOEX",
        timeframe="15m",
    )
    existing = DerivedIndicatorFrameRow(
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        profile_version="core_v1",
        contract_id="BRQ2@MOEX",
        instrument_id="FUT_BR",
        timeframe="15m",
        ts="2026-04-30T10:00:00Z",
        values={"distance_to_ema_20_atr": 1.0},
        source_bars_hash="OLD",
        source_indicators_hash="OLD",
        row_count=1,
        warmup_span=0,
        null_warmup_span=0,
        created_at="2026-04-30T10:00:00Z",
    )
    replacement = DerivedIndicatorFrameRow(
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        profile_version="core_v1",
        contract_id="BRQ2@MOEX",
        instrument_id="FUT_BR",
        timeframe="15m",
        ts="2026-04-30T10:00:00Z",
        values={"distance_to_ema_20_atr": 2.0},
        source_bars_hash="NEW",
        source_indicators_hash="NEW",
        row_count=1,
        warmup_span=0,
        null_warmup_span=0,
        created_at="2026-04-30T10:01:00Z",
    )
    write_delta_table_rows(table_path=path, columns=columns, rows=[existing.to_dict()])

    def _forbidden_row_reload(*_: object, **__: object) -> None:
        raise AssertionError("derived partition replacement must not reload existing rows")

    monkeypatch.setattr(derived_store_module, "read_delta_table_rows", _forbidden_row_reload)
    write_derived_indicator_frames(output_dir=tmp_path, rows=[replacement], replace_partitions=(partition,))

    rows = read_delta_table_rows(path, filters=[("dataset_version", "=", "dataset-v1")])
    assert len(rows) == 1
    assert rows[0]["distance_to_ema_20_atr"] == 2.0
    assert rows[0]["source_bars_hash"] == "NEW"


def test_research_contract_lineage_is_consistent_across_dataset_indicator_and_derived_layers() -> None:
    dataset_manifest = research_dataset_store_contract()
    indicator_manifest = indicator_store_contract()
    derived_manifest = research_derived_indicator_store_contract()

    assert {"research_datasets", "research_instrument_tree", "research_bar_views"} == set(dataset_manifest)
    assert "research_indicator_frames" in indicator_manifest
    assert "research_derived_indicator_frames" in derived_manifest
    indicator_columns = set(indicator_manifest["research_indicator_frames"]["columns"])
    derived_columns = set(derived_manifest["research_derived_indicator_frames"]["columns"])
    assert {
        "dataset_version",
        "indicator_set_version",
        "profile_version",
        "source_bars_hash",
        "source_dataset_bars_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "output_columns_hash",
    } <= indicator_columns

    registry = build_indicator_profile_registry()
    assert registry.versions() == ("core_v1", "core_intraday_v1", "core_swing_v1")
    assert set(registry.get("core_v1").expected_output_columns()) <= indicator_columns
    assert {
        "dataset_version",
        "indicator_set_version",
        "derived_indicator_set_version",
        "profile_version",
        "source_bars_hash",
        "source_dataset_bars_hash",
        "source_indicators_hash",
        "source_indicator_profile_version",
        "source_indicator_output_columns_hash",
        "output_columns_hash",
        "distance_to_ema_20_atr",
        "donchian_position_20",
        "divergence_price_rsi_14_score",
        "mtf_1h_to_15m_ema_20",
        "mtf_1h_to_15m_ema_50",
    } <= derived_columns
    assert {
        "volatility_regime_code",
        "oscillator_pressure_code",
        "breakout_ready_flag",
        "reversion_ready_flag",
        "atr_stop_ref_1x",
        "atr_target_ref_2x",
    }.isdisjoint(derived_columns)


def test_research_candidate_id_formula_is_stable() -> None:
    value = candidate_id(
        strategy_instance_id="sinst_trend_follow",
        contract_id="BR-6.26",
        timeframe="15m",
        ts_signal="2026-03-16T10:15:00Z",
    )
    assert value == candidate_id(
        strategy_instance_id="sinst_trend_follow",
        contract_id="BR-6.26",
        timeframe="15m",
        ts_signal="2026-03-16T10:15:00Z",
    )
    assert value.startswith("CAND-")


def test_research_default_strategy_space_follows_frozen_stg02_adapter_inventory() -> None:
    default_strategy_space = research_assets._default_strategy_space()
    assert tuple(default_strategy_space["family_keys"]) == tuple(
        adapter.family_manifest.family_key for adapter in phase_stg02_family_adapters()
    )
    assert default_strategy_space["max_parameter_combinations"] == 250000
    assert "materialize_instances" not in default_strategy_space

