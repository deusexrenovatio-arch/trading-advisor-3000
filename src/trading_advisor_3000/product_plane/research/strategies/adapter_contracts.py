from __future__ import annotations

from dataclasses import dataclass

from .manifests import StrategyFamilyManifest, StrategyTemplateManifest
from .spec import StrategySpec


def _require_non_empty(value: str, *, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must be non-empty")
    return normalized


def _require_adapter_source_ref(value: str) -> str:
    normalized = _require_non_empty(value, field="source_ref")
    if not normalized.startswith("python_adapter:"):
        raise ValueError("source_ref must start with 'python_adapter:'")
    module_ref = normalized.removeprefix("python_adapter:").strip()
    if not module_ref:
        raise ValueError("source_ref must include adapter import path after 'python_adapter:'")
    if "/" in module_ref or "\\" in module_ref:
        raise ValueError("source_ref must use import-path notation, not filesystem paths")
    if ".py" in module_ref:
        raise ValueError("source_ref must not include filesystem extension")
    return f"python_adapter:{module_ref}"


@dataclass(frozen=True)
class StrategyFamilyAdapter:
    adapter_key: str
    adapter_version: str
    source_ref: str
    strategy_spec: StrategySpec
    family_manifest: StrategyFamilyManifest
    template_manifest: StrategyTemplateManifest

    def __post_init__(self) -> None:
        object.__setattr__(self, "adapter_key", _require_non_empty(self.adapter_key, field="adapter_key"))
        object.__setattr__(self, "adapter_version", _require_non_empty(self.adapter_version, field="adapter_version"))
        object.__setattr__(self, "source_ref", _require_adapter_source_ref(self.source_ref))
        if self.strategy_spec.family != self.adapter_key:
            raise ValueError("strategy_spec.family must match adapter_key")
        if self.family_manifest.family_key != self.adapter_key:
            raise ValueError("family_manifest.family_key must match adapter_key")
        if self.template_manifest.family_key != self.adapter_key:
            raise ValueError("template_manifest.family_key must match adapter_key")
        if self.template_manifest.execution_mode != self.strategy_spec.execution_mode:
            raise ValueError("template_manifest.execution_mode must match strategy_spec.execution_mode")
        if self.template_manifest.source_ref != self.source_ref:
            raise ValueError("template_manifest.source_ref must match source_ref")
