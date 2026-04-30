from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter
from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyIndicatorRequirement,
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)
from trading_advisor_3000.product_plane.research.strategies.template_compiler import build_strategy_family_adapter


_DIVERGENCE_COLUMNS = (
    "divergence_price_rsi_14_score",
    "divergence_price_stoch_k_14_3_3_score",
    "divergence_price_cci_20_score",
    "divergence_price_willr_14_score",
    "divergence_price_macd_hist_12_26_9_score",
    "divergence_price_ppo_hist_12_26_9_score",
    "divergence_price_tsi_25_13_score",
    "divergence_price_mfi_14_score",
    "divergence_price_cmf_20_score",
    "divergence_price_obv_score",
    "divergence_price_oi_change_1_score",
)


def divergence_reversal_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="divergence-reversal-v1",
        family="divergence_reversal_v1",
        description="Reversal search where price extends but oscillator, volume, or OI divergence weakens the move.",
        required_columns=(
            "close",
            "rsi_14",
            "macd_hist_12_26_9",
            "ppo_hist_12_26_9",
            "tsi_25_13",
            "mfi_14",
            "cmf_20",
            "atr_14",
            "rolling_position_20",
            "bb_position_20_2",
            "close_slope_20",
            *_DIVERGENCE_COLUMNS,
        ),
        parameter_grid=(
            StrategyParameter(
                "divergence_source",
                ("rsi", "macd_hist", "ppo_hist", "tsi", "mfi", "cmf", "obv", "oi_change"),
                role="decision",
                timeframe="1h",
            ),
            StrategyParameter("divergence_min_abs_score", (0.3, 0.5, 0.7), role="decision", timeframe="1h"),
            StrategyParameter("position_extreme_long_max", (0.05, 0.10, 0.20), role="trigger", timeframe="15m"),
            StrategyParameter("position_extreme_short_min", (0.80, 0.90, 0.95), role="trigger", timeframe="15m"),
            StrategyParameter(
                "confirmation_mode",
                ("slope_turn", "cross_back_inside", "oscillator_reclaim"),
                role="trigger",
                timeframe="15m",
            ),
            StrategyParameter("stop_atr_mult", (1.5, 2.0, 2.5), role="risk", timeframe="15m"),
            StrategyParameter("max_holding_bars", (12, 24, 48), role="risk", timeframe="15m"),
        ),
        signal_builder_key="divergence_reversal",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=2.0, target_atr_multiple=2.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("profit_factor", "calmar", "max_drawdown"),
            tags=("range_chop", "late_trend", "reversal"),
        ),
        intent="Find reversal setups after price extremes when momentum, volume, or OI divergence is already visible.",
        allowed_clock_profiles=("short_swing_1h_v1",),
        market_regimes=("range_chop", "late_trend", "panic_high_vol"),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("rsi_14", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("macd_hist_12_26_9", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("ppo_hist_12_26_9", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("tsi_25_13", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("mfi_14", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("cmf_20", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
            *(
                StrategyIndicatorRequirement(column, "decision", "1h", "derived")
                for column in _DIVERGENCE_COLUMNS
            ),
            StrategyIndicatorRequirement("rolling_position_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("bb_position_20_2", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("close_slope_20", "trigger", "15m", "derived"),
        ),
        entry_logic=(
            "Use native 1h divergence scores as the weakening-move decision surface.",
            "Use native 15m extreme position and confirmation state for actual entry timing.",
        ),
        exit_logic=("Use ATR stop/target and max-holding risk parameters on the 15m execution index.",),
        verification_questions=(
            "Can each divergence_source select a concrete derived divergence score?",
            "Does trigger confirmation remain native to 15m rather than a projected higher-timeframe alias?",
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="divergence_reversal_v1",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families."
            "divergence_reversal.divergence_reversal_family_adapter"
        ),
        hypothesis_family="divergence_reversal",
        strategy_spec=strategy_spec,
        template_key="divergence_reversal",
        template_title="Divergence Reversal",
        regime_module_key="native.divergence_context",
        signal_tf="1h",
        trigger_tf="15m",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def divergence_reversal_strategy_spec() -> StrategySpec:
    return divergence_reversal_family_adapter().strategy_spec
