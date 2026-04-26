from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping


def _require_non_empty(value: str, *, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must be non-empty")
    return normalized


def _require_non_empty_items(values: tuple[str, ...], *, field: str) -> tuple[str, ...]:
    if not values:
        raise ValueError(f"{field} must not be empty")
    normalized = tuple(item.strip() for item in values if item.strip())
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


@dataclass(frozen=True)
class StrategyFamilyManifest:
    family_key: str
    family_version: str
    hypothesis_family: str
    description: str
    default_direction_mode: str
    allowed_execution_modes: tuple[str, ...]
    allowed_markets: tuple[str, ...]
    allowed_instrument_types: tuple[str, ...]
    allowed_signal_tfs: tuple[str, ...]
    module_builder_key: str | None = None
    source_ref: str | None = None
    status: str = "active"

    def __post_init__(self) -> None:
        object.__setattr__(self, "family_key", _require_non_empty(self.family_key, field="family_key"))
        object.__setattr__(self, "family_version", _require_non_empty(self.family_version, field="family_version"))
        object.__setattr__(self, "hypothesis_family", _require_non_empty(self.hypothesis_family, field="hypothesis_family"))
        object.__setattr__(self, "description", _require_non_empty(self.description, field="description"))
        object.__setattr__(
            self,
            "default_direction_mode",
            _require_non_empty(self.default_direction_mode, field="default_direction_mode"),
        )
        object.__setattr__(
            self,
            "allowed_execution_modes",
            _require_non_empty_items(self.allowed_execution_modes, field="allowed_execution_modes"),
        )
        object.__setattr__(self, "allowed_markets", _require_non_empty_items(self.allowed_markets, field="allowed_markets"))
        object.__setattr__(
            self,
            "allowed_instrument_types",
            _require_non_empty_items(self.allowed_instrument_types, field="allowed_instrument_types"),
        )
        object.__setattr__(
            self,
            "allowed_signal_tfs",
            _require_non_empty_items(self.allowed_signal_tfs, field="allowed_signal_tfs"),
        )
        object.__setattr__(self, "status", _require_non_empty(self.status, field="status"))
        if self.module_builder_key is not None:
            object.__setattr__(self, "module_builder_key", _require_non_empty(self.module_builder_key, field="module_builder_key"))
        if self.source_ref is not None:
            object.__setattr__(self, "source_ref", _require_non_empty(self.source_ref, field="source_ref"))

    def to_manifest_dict(self) -> dict[str, object]:
        return {
            "family_key": self.family_key,
            "family_version": self.family_version,
            "hypothesis_family": self.hypothesis_family,
            "description": self.description,
            "default_direction_mode": self.default_direction_mode,
            "allowed_execution_modes": list(self.allowed_execution_modes),
            "allowed_markets": list(self.allowed_markets),
            "allowed_instrument_types": list(self.allowed_instrument_types),
            "allowed_signal_tfs": list(self.allowed_signal_tfs),
            "module_builder_key": self.module_builder_key,
            "source_ref": self.source_ref,
            "status": self.status,
        }


@dataclass(frozen=True)
class StrategyTemplateModule:
    role: str
    alias: str
    module_key: str
    module_version: str
    params: Mapping[str, object]
    search_space: Mapping[str, object] | None = None
    timeframe_scope: str | None = None
    order_index: int = 0
    enabled: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "role", _require_non_empty(self.role, field="role"))
        object.__setattr__(self, "alias", _require_non_empty(self.alias, field="alias"))
        object.__setattr__(self, "module_key", _require_non_empty(self.module_key, field="module_key"))
        object.__setattr__(self, "module_version", _require_non_empty(self.module_version, field="module_version"))
        if self.timeframe_scope is not None:
            object.__setattr__(self, "timeframe_scope", _require_non_empty(self.timeframe_scope, field="timeframe_scope"))
        if self.order_index < 0:
            raise ValueError("order_index must be >= 0")

    def to_manifest_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "alias": self.alias,
            "module_key": self.module_key,
            "module_version": self.module_version,
            "params": dict(self.params),
            "search_space": None if self.search_space is None else dict(self.search_space),
            "timeframe_scope": self.timeframe_scope,
            "order_index": self.order_index,
            "enabled": self.enabled,
        }


@dataclass(frozen=True)
class StrategyResolvedModule:
    role: str
    alias: str
    module_key: str
    module_version: str
    resolved_params: Mapping[str, object]
    timeframe_scope: str | None = None
    derived_indicator_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "role", _require_non_empty(self.role, field="role"))
        object.__setattr__(self, "alias", _require_non_empty(self.alias, field="alias"))
        object.__setattr__(self, "module_key", _require_non_empty(self.module_key, field="module_key"))
        object.__setattr__(self, "module_version", _require_non_empty(self.module_version, field="module_version"))
        if self.timeframe_scope is not None:
            object.__setattr__(self, "timeframe_scope", _require_non_empty(self.timeframe_scope, field="timeframe_scope"))
        if self.derived_indicator_refs:
            object.__setattr__(
                self,
                "derived_indicator_refs",
                tuple(item for item in self.derived_indicator_refs if item and item.strip()),
            )

    def to_manifest_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "alias": self.alias,
            "module_key": self.module_key,
            "module_version": self.module_version,
            "params": dict(self.resolved_params),
            "timeframe_scope": self.timeframe_scope,
            "derived_indicator_refs": list(self.derived_indicator_refs),
        }


@dataclass(frozen=True)
class StrategyTemplateManifest:
    family_key: str
    hypothesis_family: str
    family_version: str
    template_key: str
    template_version: str
    title: str
    description: str
    market: str
    instrument_type: str
    universe_id: str
    direction_mode: str
    regime_tf: str
    signal_tf: str
    trigger_tf: str
    execution_tf: str
    bar_type: str
    closed_bars_only: bool
    execution_mode: str
    modules: tuple[StrategyTemplateModule, ...]
    risk_policy: Mapping[str, object]
    validation_policy: Mapping[str, object]
    required_indicator_columns: tuple[str, ...]
    venue: str | None = None
    search_space: Mapping[str, object] | None = None
    source_ref: str | None = None
    status: str = "active"
    author_source: str = "python_adapter"
    schema_version: str = "strategy-template.v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "family_key", _require_non_empty(self.family_key, field="family_key"))
        object.__setattr__(self, "hypothesis_family", _require_non_empty(self.hypothesis_family, field="hypothesis_family"))
        object.__setattr__(self, "family_version", _require_non_empty(self.family_version, field="family_version"))
        object.__setattr__(self, "template_key", _require_non_empty(self.template_key, field="template_key"))
        object.__setattr__(self, "template_version", _require_non_empty(self.template_version, field="template_version"))
        object.__setattr__(self, "title", _require_non_empty(self.title, field="title"))
        object.__setattr__(self, "description", _require_non_empty(self.description, field="description"))
        object.__setattr__(self, "market", _require_non_empty(self.market, field="market"))
        object.__setattr__(self, "instrument_type", _require_non_empty(self.instrument_type, field="instrument_type"))
        object.__setattr__(self, "universe_id", _require_non_empty(self.universe_id, field="universe_id"))
        object.__setattr__(self, "direction_mode", _require_non_empty(self.direction_mode, field="direction_mode"))
        object.__setattr__(self, "regime_tf", _require_non_empty(self.regime_tf, field="regime_tf"))
        object.__setattr__(self, "signal_tf", _require_non_empty(self.signal_tf, field="signal_tf"))
        object.__setattr__(self, "trigger_tf", _require_non_empty(self.trigger_tf, field="trigger_tf"))
        object.__setattr__(self, "execution_tf", _require_non_empty(self.execution_tf, field="execution_tf"))
        object.__setattr__(self, "bar_type", _require_non_empty(self.bar_type, field="bar_type"))
        object.__setattr__(self, "execution_mode", _require_non_empty(self.execution_mode, field="execution_mode"))
        object.__setattr__(self, "required_indicator_columns", _require_non_empty_items(self.required_indicator_columns, field="required_indicator_columns"))
        object.__setattr__(self, "schema_version", _require_non_empty(self.schema_version, field="schema_version"))
        object.__setattr__(self, "author_source", _require_non_empty(self.author_source, field="author_source"))
        object.__setattr__(self, "status", _require_non_empty(self.status, field="status"))
        if self.venue is not None:
            object.__setattr__(self, "venue", _require_non_empty(self.venue, field="venue"))
        if self.source_ref is not None:
            object.__setattr__(self, "source_ref", _require_non_empty(self.source_ref, field="source_ref"))
        if not self.modules:
            raise ValueError("modules must not be empty")

    def to_manifest_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "family": {
                "family_key": self.family_key,
                "hypothesis_family": self.hypothesis_family,
                "family_version": self.family_version,
            },
            "template": {
                "template_key": self.template_key,
                "template_version": self.template_version,
                "title": self.title,
                "description": self.description,
            },
            "market_scope": {
                "market": self.market,
                "venue": self.venue,
                "instrument_type": self.instrument_type,
                "universe_id": self.universe_id,
                "direction_mode": self.direction_mode,
            },
            "clock_profile": {
                "regime_tf": self.regime_tf,
                "signal_tf": self.signal_tf,
                "trigger_tf": self.trigger_tf,
                "execution_tf": self.execution_tf,
                "bar_type": self.bar_type,
                "closed_bars_only": self.closed_bars_only,
            },
            "modules": [module.to_manifest_dict() for module in self.modules],
            "risk_policy": dict(self.risk_policy),
            "execution_policy": {
                "execution_mode": self.execution_mode,
            },
            "validation_policy": dict(self.validation_policy),
            "required_indicator_columns": list(self.required_indicator_columns),
            "search_space": None if self.search_space is None else dict(self.search_space),
            "source_ref": self.source_ref,
            "author_source": self.author_source,
            "status": self.status,
        }


@dataclass(frozen=True)
class StrategyInstanceManifest:
    strategy_template_id: str
    family_id: str
    family_key: str
    template_key: str
    template_version: str
    market: str
    instrument_type: str
    universe_id: str
    direction_mode: str
    regime_tf: str
    signal_tf: str
    trigger_tf: str
    execution_tf: str
    bar_type: str
    closed_bars_only: bool
    execution_mode: str
    parameter_values: Mapping[str, object]
    resolved_modules: tuple[StrategyResolvedModule, ...]
    risk_policy: Mapping[str, object]
    required_indicator_columns: tuple[str, ...]
    venue: str | None = None
    generated_by_campaign_id: str | None = None
    generated_by_campaign_run_id: str | None = None
    status: str = "active"
    schema_version: str = "strategy-instance.v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_template_id", _require_non_empty(self.strategy_template_id, field="strategy_template_id"))
        object.__setattr__(self, "family_id", _require_non_empty(self.family_id, field="family_id"))
        object.__setattr__(self, "family_key", _require_non_empty(self.family_key, field="family_key"))
        object.__setattr__(self, "template_key", _require_non_empty(self.template_key, field="template_key"))
        object.__setattr__(self, "template_version", _require_non_empty(self.template_version, field="template_version"))
        object.__setattr__(self, "market", _require_non_empty(self.market, field="market"))
        object.__setattr__(self, "instrument_type", _require_non_empty(self.instrument_type, field="instrument_type"))
        object.__setattr__(self, "universe_id", _require_non_empty(self.universe_id, field="universe_id"))
        object.__setattr__(self, "direction_mode", _require_non_empty(self.direction_mode, field="direction_mode"))
        object.__setattr__(self, "regime_tf", _require_non_empty(self.regime_tf, field="regime_tf"))
        object.__setattr__(self, "signal_tf", _require_non_empty(self.signal_tf, field="signal_tf"))
        object.__setattr__(self, "trigger_tf", _require_non_empty(self.trigger_tf, field="trigger_tf"))
        object.__setattr__(self, "execution_tf", _require_non_empty(self.execution_tf, field="execution_tf"))
        object.__setattr__(self, "bar_type", _require_non_empty(self.bar_type, field="bar_type"))
        object.__setattr__(self, "execution_mode", _require_non_empty(self.execution_mode, field="execution_mode"))
        object.__setattr__(self, "required_indicator_columns", _require_non_empty_items(self.required_indicator_columns, field="required_indicator_columns"))
        object.__setattr__(self, "schema_version", _require_non_empty(self.schema_version, field="schema_version"))
        object.__setattr__(self, "status", _require_non_empty(self.status, field="status"))
        if self.venue is not None:
            object.__setattr__(self, "venue", _require_non_empty(self.venue, field="venue"))
        if self.generated_by_campaign_id is not None:
            object.__setattr__(
                self,
                "generated_by_campaign_id",
                _require_non_empty(self.generated_by_campaign_id, field="generated_by_campaign_id"),
            )
        if self.generated_by_campaign_run_id is not None:
            object.__setattr__(
                self,
                "generated_by_campaign_run_id",
                _require_non_empty(self.generated_by_campaign_run_id, field="generated_by_campaign_run_id"),
            )
        if not self.resolved_modules:
            raise ValueError("resolved_modules must not be empty")

    def to_manifest_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "template_id": self.strategy_template_id,
            "family_id": self.family_id,
            "family_key": self.family_key,
            "template_key": self.template_key,
            "template_version": self.template_version,
            "market_scope": {
                "market": self.market,
                "venue": self.venue,
                "instrument_type": self.instrument_type,
                "universe_id": self.universe_id,
                "direction_mode": self.direction_mode,
            },
            "clock_profile": {
                "regime_tf": self.regime_tf,
                "signal_tf": self.signal_tf,
                "trigger_tf": self.trigger_tf,
                "execution_tf": self.execution_tf,
                "bar_type": self.bar_type,
                "closed_bars_only": self.closed_bars_only,
            },
            "execution_policy": {
                "execution_mode": self.execution_mode,
            },
            "parameter_values": dict(self.parameter_values),
            "resolved_modules": [module.to_manifest_dict() for module in self.resolved_modules],
            "risk_policy": dict(self.risk_policy),
            "required_indicator_columns": list(self.required_indicator_columns),
            "generated_by_campaign_id": self.generated_by_campaign_id,
            "generated_by_campaign_run_id": self.generated_by_campaign_run_id,
            "status": self.status,
        }


@dataclass(frozen=True)
class StrategyTemplateIdentity:
    strategy_template_id: str
    template_manifest_hash: str
    canonical_manifest_json: str


@dataclass(frozen=True)
class StrategyInstanceIdentity:
    strategy_instance_id: str
    manifest_hash: str
    canonical_manifest_json: str


def _normalize_decimal(value: Decimal) -> int | float:
    if not value.is_finite():
        raise ValueError("manifest numeric values must be finite")
    integral = value.to_integral_value()
    if value == integral:
        return int(integral)
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        text = "0"
    return float(text)


def _canonicalize_scalar(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("manifest numeric values must be finite")
        return _normalize_decimal(Decimal(str(value)))
    if isinstance(value, Decimal):
        return _normalize_decimal(value)
    return value


def _canonicalize_payload(value: object) -> object:
    value = _canonicalize_scalar(value)
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize_payload(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_canonicalize_payload(item) for item in value]
    if isinstance(value, set):
        canonical_items = [_canonicalize_payload(item) for item in value]
        return sorted(canonical_items, key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))
    return value


def canonical_manifest_json(payload: Mapping[str, object]) -> str:
    canonical_payload = _canonicalize_payload(payload)
    if not isinstance(canonical_payload, dict):
        raise TypeError("canonical manifest payload must be an object")
    return json.dumps(
        canonical_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_digest(canonical_json: str) -> str:
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def build_strategy_family_id(*, family_key: str, family_version: str, hypothesis_family: str) -> str:
    payload = {
        "family_key": _require_non_empty(family_key, field="family_key"),
        "family_version": _require_non_empty(family_version, field="family_version"),
        "hypothesis_family": _require_non_empty(hypothesis_family, field="hypothesis_family"),
    }
    digest = _sha256_digest(canonical_manifest_json(payload))
    return f"sfam_{digest}"


def build_strategy_template_identity(manifest: StrategyTemplateManifest) -> StrategyTemplateIdentity:
    manifest_json = canonical_manifest_json(manifest.to_manifest_dict())
    digest = _sha256_digest(manifest_json)
    return StrategyTemplateIdentity(
        strategy_template_id=f"stpl_{digest}",
        template_manifest_hash=f"sha256:{digest}",
        canonical_manifest_json=manifest_json,
    )


def build_strategy_instance_identity(manifest: StrategyInstanceManifest) -> StrategyInstanceIdentity:
    manifest_json = canonical_manifest_json(manifest.to_manifest_dict())
    digest = _sha256_digest(manifest_json)
    return StrategyInstanceIdentity(
        strategy_instance_id=f"sinst_{digest}",
        manifest_hash=f"sha256:{digest}",
        canonical_manifest_json=manifest_json,
    )
