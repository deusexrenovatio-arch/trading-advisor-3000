from __future__ import annotations

from .adapter_contracts import StrategyFamilyAdapter
from .catalog import StrategyCatalog, default_strategy_catalog
from .manifests import (
    StrategyFamilyManifest,
    StrategyInstanceIdentity,
    StrategyInstanceManifest,
    StrategyResolvedModule,
    StrategyTemplateIdentity,
    StrategyTemplateManifest,
    StrategyTemplateModule,
    build_strategy_family_id,
    build_strategy_instance_identity,
    build_strategy_template_identity,
    canonical_manifest_json,
)
from .registry import StrategyRegistry, build_strategy_registry
from .reference import evaluate_strategy, sample_strategy_ids, strategy_family_of, supported_strategy_ids
from .spec import StrategyParameter, StrategyRankingMetadata, StrategyRiskPolicy, StrategySpec

__all__ = [
    "StrategyCatalog",
    "StrategyFamilyAdapter",
    "StrategyFamilyManifest",
    "StrategyInstanceIdentity",
    "StrategyInstanceManifest",
    "StrategyParameter",
    "StrategyRankingMetadata",
    "StrategyRegistry",
    "StrategyRiskPolicy",
    "StrategySpec",
    "StrategyResolvedModule",
    "StrategyTemplateIdentity",
    "StrategyTemplateManifest",
    "StrategyTemplateModule",
    "build_strategy_family_id",
    "build_strategy_registry",
    "build_strategy_instance_identity",
    "build_strategy_template_identity",
    "canonical_manifest_json",
    "evaluate_strategy",
    "default_strategy_catalog",
    "sample_strategy_ids",
    "strategy_family_of",
    "supported_strategy_ids",
]
