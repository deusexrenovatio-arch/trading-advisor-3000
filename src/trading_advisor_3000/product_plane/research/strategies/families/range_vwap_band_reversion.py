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


def range_vwap_band_reversion_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="range-vwap-band-reversion-v1",
        family="range_vwap_band_reversion_v1",
        description=(
            "Range/chop mean-reversion search from VWAP, Bollinger/Keltner, and rolling-range extremes "
            "back toward the middle of the range."
        ),
        required_columns=(
            "close",
            "rsi_14",
            "stoch_k_14_3_3",
            "stoch_d_14_3_3",
            "cci_20",
            "willr_14",
            "atr_14",
            "adx_14",
            "chop_14",
            "bb_upper_20_2",
            "bb_lower_20_2",
            "bb_mid_20_2",
            "kc_upper_20_1_5",
            "kc_lower_20_1_5",
            "session_vwap",
            "distance_to_session_vwap",
            "bb_position_20_2",
            "kc_position_20_1_5",
            "rolling_position_20",
            "session_position",
            "distance_to_bb_upper_20_2_atr",
            "distance_to_bb_lower_20_2_atr",
            "distance_to_kc_upper_20_1_5_atr",
            "distance_to_kc_lower_20_1_5_atr",
            "close_slope_20",
        ),
        parameter_grid=(
            StrategyParameter("adx_max", (16, 20, 24), role="decision", timeframe="1h"),
            StrategyParameter("chop_min", (50, 55, 60), role="decision", timeframe="1h"),
            StrategyParameter("entry_band_position_long_max", (0.05, 0.10, 0.20), role="trigger", timeframe="15m"),
            StrategyParameter("entry_band_position_short_min", (0.80, 0.90, 0.95), role="trigger", timeframe="15m"),
            StrategyParameter("rsi_oversold", (25, 30, 35), role="trigger", timeframe="15m"),
            StrategyParameter("rsi_overbought", (65, 70, 75), role="trigger", timeframe="15m"),
            StrategyParameter("exit_mode", ("vwap_touch", "bb_mid_touch", "position_mid"), role="exit", timeframe="15m"),
            StrategyParameter("stop_atr_mult", (1.5, 2.0, 2.5), role="risk", timeframe="15m"),
            StrategyParameter("max_holding_bars", (12, 24, 48), role="risk", timeframe="15m"),
        ),
        signal_builder_key="range_vwap_band_reversion",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=2.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("profit_factor", "calmar", "max_drawdown"),
            tags=("range_chop", "mean_reversion"),
        ),
        intent="Find mean reversion in range/chop regimes from VWAP, band, and rolling-range extremes.",
        allowed_clock_profiles=("short_swing_1h_v1",),
        market_regimes=("range_chop", "mixed_unknown"),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("adx_14", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("chop_14", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("bb_upper_20_2", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("bb_lower_20_2", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("bb_mid_20_2", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("kc_upper_20_1_5", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("kc_lower_20_1_5", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("rsi_14", "trigger", "15m", "indicator"),
            StrategyIndicatorRequirement("stoch_k_14_3_3", "trigger", "15m", "indicator"),
            StrategyIndicatorRequirement("stoch_d_14_3_3", "trigger", "15m", "indicator"),
            StrategyIndicatorRequirement("cci_20", "trigger", "15m", "indicator"),
            StrategyIndicatorRequirement("willr_14", "trigger", "15m", "indicator"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
            StrategyIndicatorRequirement("session_vwap", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_session_vwap", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("bb_position_20_2", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("kc_position_20_1_5", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("rolling_position_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("session_position", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_bb_upper_20_2_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_bb_lower_20_2_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_kc_upper_20_1_5_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_kc_lower_20_1_5_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("close_slope_20", "trigger", "15m", "derived"),
        ),
        entry_logic=(
            "Use native 1h ADX/CHOP and band context as the range eligibility mask.",
            "Use native 15m oscillator and band-position extremes as mean-reversion triggers.",
        ),
        exit_logic=("Use VWAP, band middle, or position-mid state plus ATR stop on the 15m execution index.",),
        verification_questions=(
            "Does range/chop eligibility come from native signal-clock indicators?",
            "Do 15m trigger fields remain separate from the 1h regime/decision context?",
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="range_vwap_band_reversion_v1",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families."
            "range_vwap_band_reversion.range_vwap_band_reversion_family_adapter"
        ),
        hypothesis_family="mean_reversion",
        strategy_spec=strategy_spec,
        template_key="range_vwap_band_reversion",
        template_title="Range VWAP Band Reversion",
        regime_module_key="native.range_chop_context",
        signal_tf="1h",
        trigger_tf="15m",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def range_vwap_band_reversion_strategy_spec() -> StrategySpec:
    return range_vwap_band_reversion_family_adapter().strategy_spec
