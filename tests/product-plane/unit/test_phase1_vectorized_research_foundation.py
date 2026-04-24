from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.research import MissingResearchDependencyError, ensure_research_dependencies
from trading_advisor_3000.product_plane.research.backtests import BacktestBatchRequest, build_ephemeral_strategy_space
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    HoldoutSplitConfig,
    ResearchDatasetManifest,
    ResearchDatasetPartitionKey,
    WalkForwardSplitConfig,
    build_holdout_window,
    build_walk_forward_windows,
)
from trading_advisor_3000.product_plane.research.dependencies import PANDAS_TA_REQUIREMENT, resolve_research_dependency
from trading_advisor_3000.product_plane.research.features import build_feature_profile_registry, phase1_feature_profile
from trading_advisor_3000.product_plane.research.indicators import (
    build_indicator_profile_registry,
    indicator_column_name,
    phase1_indicator_profile,
)
from trading_advisor_3000.product_plane.research.strategies import build_phase1_strategy_registry, phase1_strategy_catalog


ROOT = Path(__file__).resolve().parents[3]


def test_research_base_dependencies_declare_vectorized_stack() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]
    assert "vectorbt>=0.28.5,<0.29" in dependencies
    assert "pandas-ta-classic>=0.4.47,<0.5" in dependencies


def test_indicator_naming_normalizes_decimal_parameters() -> None:
    assert indicator_column_name("kc_upper", 20, 1.5) == "kc_upper_20_1_5"
    assert indicator_column_name("macd_signal", 12, 26, 9) == "macd_signal_12_26_9"


def test_phase1_indicator_profile_covers_core_indicator_groups() -> None:
    profile = phase1_indicator_profile()
    assert profile.version == "core_v1"
    grouped = profile.by_category()
    assert set(grouped) == {"momentum", "oscillator", "trend", "volatility", "volume"}

    all_columns = {column for spec in profile.indicators for column in spec.output_columns}
    assert {
        "ema_10",
        "rsi_14",
        "macd_12_26_9",
        "atr_14",
        "obv",
        "kc_upper_20_1_5",
    } <= all_columns

    registry = build_indicator_profile_registry()
    assert registry.versions() == ("core_v1", "core_intraday_v1", "core_swing_v1")


def test_phase1_feature_profile_declares_cross_layer_feature_groups() -> None:
    profile = phase1_feature_profile()
    assert profile.version == "core_v1"
    grouped = profile.by_category()
    assert {"trend", "levels", "volatility", "volume", "regime", "labels", "references", "mtf"} == set(grouped)

    output_columns = {column for spec in profile.features for column in spec.output_columns}
    assert {
        "trend_state_fast_slow_code",
        "trend_strength",
        "ma_stack_state_code",
        "rolling_high_20",
        "session_vwap",
        "squeeze_on_code",
        "breakout_ready_state_code",
        "breakout_ready_flag",
        "rvol_20",
        "volume_zscore_20",
        "regime_state_code",
        "reversion_ready_flag",
        "atr_stop_ref_1x",
        "atr_target_ref_2x",
        "htf_trend_state_code",
    } <= output_columns

    registry = build_feature_profile_registry()
    assert registry.versions() == ("core_v1", "core_intraday_v1", "core_swing_v1")


def test_phase1_strategy_catalog_declares_required_families_and_modes() -> None:
    catalog = phase1_strategy_catalog()
    assert catalog.version == "research-strategy-catalog-v1"

    strategies = {spec.family: spec for spec in catalog.strategies}
    assert set(strategies) == {
        "breakout",
        "ma_cross",
        "mean_reversion",
        "mtf_pullback",
        "squeeze_release",
    }
    assert strategies["squeeze_release"].execution_mode == "order_func"
    assert strategies["ma_cross"].execution_mode == "signals"

    registry = build_phase1_strategy_registry()
    assert registry.get("ma-cross-v1").family == "ma_cross"
    assert registry.strategy_versions() == tuple(spec.version for spec in catalog.strategies)


def test_dataset_and_backtest_keys_are_deterministic() -> None:
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-v1",
        universe_id="moex-futures",
        timeframes=("15m", "1h"),
        series_mode="continuous_front",
        split_method="walk_forward",
        continuous_front_policy=ContinuousFrontPolicy(),
    )
    assert manifest.lineage_key() == manifest.lineage_key()

    partition = ResearchDatasetPartitionKey(
        dataset_version="dataset-v1",
        timeframe="15m",
        instrument_id="BR",
        contract_id=None,
    )
    assert partition.partition_path().endswith("contract_id=continuous-front/timeframe=15m")

    strategy_space = build_ephemeral_strategy_space(
        strategy_registry=build_phase1_strategy_registry(),
        strategy_version_labels=("ma-cross-v1", "breakout-v1"),
        instances_per_strategy=24,
    )
    batch = BacktestBatchRequest(
        campaign_run_id="crun_foundation_test",
        strategy_space_id=strategy_space.strategy_space_id,
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        strategy_instances=strategy_space.strategy_instances,
        combination_count=len(strategy_space.strategy_instances),
    )
    assert batch.batch_id() == batch.batch_id()


def test_splitter_helpers_produce_predictable_windows() -> None:
    holdout = build_holdout_window(100, HoldoutSplitConfig(holdout_ratio=0.25))
    assert holdout.train_stop == 75
    assert holdout.test_stop == 100

    windows = build_walk_forward_windows(
        120,
        WalkForwardSplitConfig(train_size=40, test_size=20, step_size=20),
    )
    assert tuple(window.window_id for window in windows) == ("wf-01", "wf-02", "wf-03", "wf-04")


def test_required_dependency_error_names_base_environment_requirement(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import(name: str) -> object:
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(
        "trading_advisor_3000.product_plane.research.dependencies.import_module",
        fake_import,
    )

    with pytest.raises(MissingResearchDependencyError) as exc_info:
        resolve_research_dependency(PANDAS_TA_REQUIREMENT)

    message = str(exc_info.value)
    assert "mandatory research contour" in message
    assert "base environment" in message
    assert "pandas-ta-classic" in message


def test_research_dependencies_import_in_real_environment() -> None:
    resolved = ensure_research_dependencies()
    assert resolved["vectorbt"].import_name == "vectorbt"
    assert resolved["pandas-ta"].import_name == "pandas_ta_classic"


def test_research_namespace_import_remains_stable_with_mandatory_stack() -> None:
    env = {"PYTHONPATH": str(ROOT / "src")}
    snippet = r"""
import importlib
importlib.import_module("trading_advisor_3000.product_plane.research")
importlib.import_module("trading_advisor_3000.product_plane.research.strategies")
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
