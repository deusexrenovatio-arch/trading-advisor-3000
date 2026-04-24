from __future__ import annotations

from typing import Any

from .derived import (
    FeatureParameter,
    FeatureProfile,
    FeatureProfileRegistry,
    FeatureSpec,
    build_feature_profile_registry,
    core_intraday_v1_feature_profile,
    core_swing_v1_feature_profile,
    core_v1_feature_profile,
    phase1_feature_profile,
)
from .engine import (
    build_feature_snapshots,
    build_feature_snapshots_from_indicators,
    build_indicator_snapshots,
    medium_term_timeframes,
    run_indicator_feature_quality_gates,
)
from .labels import LabelFeatureSpec, phase1_label_specs
from .levels import LevelFeatureSpec, phase1_level_specs
from .regime import RegimeFeatureSpec, phase1_regime_specs
from .snapshot import (
    GOLD_FEATURE_NUMERIC_COLUMNS,
    GOLD_FEATURE_STRING_COLUMNS,
    GOLD_FEATURE_TYPED_COLUMNS,
    FeatureSnapshot,
    TechnicalIndicatorSnapshot,
)
from .trend import TrendFeatureSpec, phase1_trend_specs

_LAZY_EXPORTS = {
    "build_feature_frames",
    "materialize_feature_frames",
    "reload_feature_frames",
    "FeatureFramePartitionKey",
    "FeatureFrameRow",
    "research_feature_store_contract",
}


def __getattr__(name: str) -> Any:
    if name in {"build_feature_frames", "materialize_feature_frames", "reload_feature_frames"}:
        from . import materialize as _materialize

        return getattr(_materialize, name)
    if name in {"FeatureFramePartitionKey", "FeatureFrameRow", "research_feature_store_contract"}:
        from . import store as _store

        return getattr(_store, name)
    raise AttributeError(name)


__all__ = [
    "FeatureFramePartitionKey",
    "FeatureFrameRow",
    "FeatureParameter",
    "FeatureProfile",
    "FeatureProfileRegistry",
    "FeatureSnapshot",
    "FeatureSpec",
    "GOLD_FEATURE_NUMERIC_COLUMNS",
    "GOLD_FEATURE_STRING_COLUMNS",
    "GOLD_FEATURE_TYPED_COLUMNS",
    "LabelFeatureSpec",
    "LevelFeatureSpec",
    "RegimeFeatureSpec",
    "TechnicalIndicatorSnapshot",
    "TrendFeatureSpec",
    "build_feature_frames",
    "build_feature_profile_registry",
    "build_feature_snapshots",
    "build_feature_snapshots_from_indicators",
    "build_indicator_snapshots",
    "core_intraday_v1_feature_profile",
    "core_swing_v1_feature_profile",
    "core_v1_feature_profile",
    "materialize_feature_frames",
    "medium_term_timeframes",
    "phase1_feature_profile",
    "phase1_label_specs",
    "phase1_level_specs",
    "phase1_regime_specs",
    "phase1_trend_specs",
    "research_feature_store_contract",
    "reload_feature_frames",
    "run_indicator_feature_quality_gates",
]
