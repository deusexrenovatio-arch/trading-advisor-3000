from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from .adapter_contracts import StrategyFamilyAdapter
from .manifests import StrategyFamilyManifest, StrategyTemplateManifest, StrategyTemplateModule
from .spec import StrategyClockProfileSpec, StrategySpec


def _parameter_defaults(strategy_spec: StrategySpec) -> dict[str, object]:
    return {
        parameter.name: parameter.values[0]
        for parameter in strategy_spec.parameter_grid
    }


def _parameter_search_space(strategy_spec: StrategySpec) -> dict[str, list[object]]:
    return {
        parameter.name: list(parameter.values)
        for parameter in strategy_spec.parameter_grid
    }


def build_strategy_family_adapter(
    *,
    adapter_key: str,
    adapter_version: str,
    source_ref: str,
    hypothesis_family: str,
    strategy_spec: StrategySpec,
    template_key: str,
    template_title: str,
    regime_module_key: str,
    family_version: str | None = None,
    template_version: str = "v1",
    market: str = "futures",
    venue: str = "moex",
    instrument_type: str = "continuous_future",
    universe_id: str = "liq_futures_core",
    regime_tf: str = "1d",
    signal_tf: str = "4h",
    trigger_tf: str = "1h",
    execution_tf: str = "15m",
    bar_type: str = "time",
    closed_bars_only: bool = True,
    allowed_markets: tuple[str, ...] = ("futures",),
    allowed_instrument_types: tuple[str, ...] = ("continuous_future",),
    allowed_signal_tfs: tuple[str, ...] = ("15m", "1h", "4h"),
    module_versions: Mapping[str, str] | None = None,
) -> StrategyFamilyAdapter:
    resolved_family_version = family_version or f"{adapter_key}-family-{adapter_version}"
    clock_profile = StrategyClockProfileSpec(
        name=strategy_spec.allowed_clock_profiles[0],
        regime_tf=regime_tf,
        signal_tf=signal_tf,
        trigger_tf=trigger_tf,
        execution_tf=execution_tf,
        bar_type=bar_type,
        closed_bars_only=closed_bars_only,
    )
    resolved_strategy_spec = replace(strategy_spec, clock_profile=clock_profile)
    search_space = _parameter_search_space(resolved_strategy_spec)
    parameter_defaults = _parameter_defaults(resolved_strategy_spec)
    module_versions = dict(module_versions or {})

    modules = (
        StrategyTemplateModule(
            role="regime_filter",
            alias="regime_guard",
            module_key=regime_module_key,
            module_version=str(module_versions.get("regime_filter", "v1")),
            params={},
            timeframe_scope=regime_tf,
            order_index=0,
        ),
        StrategyTemplateModule(
            role="entry",
            alias=f"{resolved_strategy_spec.signal_builder_key}_entry",
            module_key=f"signal.{resolved_strategy_spec.signal_builder_key}",
            module_version=str(module_versions.get("entry", "v1")),
            params=parameter_defaults,
            search_space=search_space,
            timeframe_scope=signal_tf,
            order_index=1,
        ),
        StrategyTemplateModule(
            role="risk_exit",
            alias="atr_bracket",
            module_key="risk.atr_bracket",
            module_version=str(module_versions.get("risk_exit", "v1")),
            params={
                "stop_atr_multiple": resolved_strategy_spec.risk_policy.stop_atr_multiple,
                "target_atr_multiple": resolved_strategy_spec.risk_policy.target_atr_multiple,
            },
            timeframe_scope=execution_tf,
            order_index=2,
        ),
    )

    family_manifest = StrategyFamilyManifest(
        family_key=adapter_key,
        family_version=resolved_family_version,
        hypothesis_family=hypothesis_family,
        description=resolved_strategy_spec.description,
        default_direction_mode=resolved_strategy_spec.direction_mode,
        allowed_execution_modes=(resolved_strategy_spec.execution_mode,),
        allowed_markets=allowed_markets,
        allowed_instrument_types=allowed_instrument_types,
        allowed_signal_tfs=allowed_signal_tfs,
        module_builder_key=resolved_strategy_spec.signal_builder_key,
        source_ref=source_ref,
        status="active",
    )

    template_manifest = StrategyTemplateManifest(
        family_key=adapter_key,
        hypothesis_family=hypothesis_family,
        family_version=resolved_family_version,
        template_key=template_key,
        template_version=template_version,
        title=template_title,
        description=resolved_strategy_spec.description,
        market=market,
        venue=venue,
        instrument_type=instrument_type,
        universe_id=universe_id,
        direction_mode=resolved_strategy_spec.direction_mode,
        regime_tf=regime_tf,
        signal_tf=signal_tf,
        trigger_tf=trigger_tf,
        execution_tf=execution_tf,
        bar_type=bar_type,
        closed_bars_only=closed_bars_only,
        execution_mode=resolved_strategy_spec.execution_mode,
        modules=modules,
        risk_policy={
            "stop_model": "atr",
            "stop_atr_multiple": resolved_strategy_spec.risk_policy.stop_atr_multiple,
            "target_model": "atr",
            "target_atr_multiple": resolved_strategy_spec.risk_policy.target_atr_multiple,
        },
        validation_policy={
            "split_method": "walk_forward",
            "preferred_metrics": list(resolved_strategy_spec.ranking_metadata.preferred_metrics),
            "ranking_tags": list(resolved_strategy_spec.ranking_metadata.tags),
            "intent": resolved_strategy_spec.intent or resolved_strategy_spec.description,
            "market_regimes": list(resolved_strategy_spec.market_regimes or resolved_strategy_spec.ranking_metadata.tags),
            "clock_profile": clock_profile.to_dict(),
            "parameter_clock_map": list(resolved_strategy_spec.parameter_clock_map()),
            "parameter_constraints": list(resolved_strategy_spec.parameter_constraints),
            "indicator_requirements": [requirement.to_dict() for requirement in resolved_strategy_spec.indicator_requirements],
            "optional_indicator_plan": [dict(item) for item in resolved_strategy_spec.optional_indicator_plan],
            "entry_logic": list(resolved_strategy_spec.entry_logic),
            "exit_logic": list(resolved_strategy_spec.exit_logic),
            "verification_questions": list(resolved_strategy_spec.verification_questions),
        },
        required_indicator_columns=resolved_strategy_spec.required_columns,
        search_space=search_space,
        source_ref=source_ref,
        status="active",
        author_source="python_adapter",
    )

    return StrategyFamilyAdapter(
        adapter_key=adapter_key,
        adapter_version=adapter_version,
        source_ref=source_ref,
        strategy_spec=resolved_strategy_spec,
        family_manifest=family_manifest,
        template_manifest=template_manifest,
    )
