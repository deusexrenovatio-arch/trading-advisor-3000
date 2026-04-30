from __future__ import annotations

from typing import Any

from .store import DEFAULT_DERIVED_INDICATOR_SET_VERSION


def __getattr__(name: str) -> Any:
    if name in {
        "DerivedIndicatorProfile",
        "DerivedIndicatorProfileRegistry",
        "WIDE_TECHNICAL_GOLD_V2_DERIVED_COLUMNS",
        "build_derived_indicator_profile_registry",
        "core_v1_derived_indicator_profile",
        "current_derived_indicator_profile",
    }:
        from . import registry as _registry

        return getattr(_registry, name)
    if name in {
        "build_derived_indicator_frames",
        "materialize_derived_indicator_frames",
    }:
        from . import materialize as _materialize

        return getattr(_materialize, name)
    if name in {
        "DerivedIndicatorFramePartitionKey",
        "DerivedIndicatorFrameRow",
        "load_derived_indicator_frames",
        "research_derived_indicator_store_contract",
    }:
        from . import store as _store

        return getattr(_store, name)
    raise AttributeError(name)

__all__ = [
    "DEFAULT_DERIVED_INDICATOR_SET_VERSION",
    "DerivedIndicatorProfile",
    "DerivedIndicatorFramePartitionKey",
    "DerivedIndicatorFrameRow",
    "DerivedIndicatorProfileRegistry",
    "WIDE_TECHNICAL_GOLD_V2_DERIVED_COLUMNS",
    "build_derived_indicator_profile_registry",
    "build_derived_indicator_frames",
    "core_v1_derived_indicator_profile",
    "current_derived_indicator_profile",
    "load_derived_indicator_frames",
    "materialize_derived_indicator_frames",
    "research_derived_indicator_store_contract",
]
