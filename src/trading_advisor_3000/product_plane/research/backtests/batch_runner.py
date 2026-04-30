from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.product_plane.research.io.cache import ResearchFrameCache
from trading_advisor_3000.product_plane.research.io.loaders import ResearchSliceRequest, load_backtest_frames
from trading_advisor_3000.product_plane.research.strategies import StrategyRegistry, build_strategy_registry

from .engine import (
    BacktestEngineConfig,
    StrategyFamilySearchSpec,
    run_vectorbt_family_search,
    search_spec_id,
    strategy_spec_to_search_spec,
)
from .input_requirements import loader_columns_for_search_specs
from .results import backtest_store_contract, write_backtest_artifacts


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _manifest_split_windows(dataset_manifest: dict[str, object]) -> tuple[dict[str, object], ...]:
    raw = dataset_manifest.get("split_params_json")
    if isinstance(raw, str) and raw.strip():
        payload = json.loads(raw)
    elif isinstance(raw, dict):
        payload = raw
    else:
        payload = {}
    windows = payload.get("windows", []) if isinstance(payload, dict) else []
    return tuple(item for item in windows if isinstance(item, dict))


def _load_dataset_manifest(*, output_dir: Path, dataset_version: str) -> dict[str, object]:
    rows = read_delta_table_rows(
        output_dir / "research_datasets.delta",
        filters=[("dataset_version", "=", dataset_version)],
    )
    if not rows:
        raise ValueError(f"materialized research dataset `{dataset_version}` is missing")
    if len(rows) > 1:
        raise ValueError(f"materialized research dataset `{dataset_version}` is not unique")
    return dict(rows[0])


def _spec_execution_timeframe(spec: StrategyFamilySearchSpec, fallback: str = "15m") -> str:
    value = spec.clock_profile.get("execution_tf") if spec.clock_profile else None
    resolved = str(value).strip() if value is not None else ""
    return resolved or fallback


def _spec_required_timeframes(spec: StrategyFamilySearchSpec, fallback: str = "15m") -> tuple[str, ...]:
    if spec.required_inputs_by_clock:
        timeframes: list[str] = []
        for payload in spec.required_inputs_by_clock.values():
            if not isinstance(payload, dict):
                continue
            has_inputs = any(payload.get(key) for key in ("price_inputs", "materialized_indicators", "materialized_derived"))
            value = str(payload.get("timeframe", "")).strip()
            if has_inputs and value and value not in timeframes:
                timeframes.append(value)
        if timeframes:
            return tuple(timeframes)
        return (fallback,)
    if not spec.clock_profile:
        return (fallback,)
    timeframes: list[str] = []
    for key in ("regime_tf", "signal_tf", "trigger_tf", "execution_tf"):
        value = str(spec.clock_profile.get(key, "")).strip()
        if value and value not in timeframes:
            timeframes.append(value)
    return tuple(timeframes) or (fallback,)


def _loader_timeframe_for_specs(search_specs: tuple[StrategyFamilySearchSpec, ...], requested_timeframe: str) -> str:
    fallback = requested_timeframe or "15m"
    required = {
        timeframe
        for spec in search_specs
        for timeframe in _spec_required_timeframes(spec, fallback=fallback)
    }
    if len(required) <= 1:
        return next(iter(required)) if required else requested_timeframe
    return ""


@dataclass(frozen=True)
class EphemeralStrategySpace:
    strategy_space_id: str
    search_specs: tuple[StrategyFamilySearchSpec, ...]

    def __post_init__(self) -> None:
        if not self.strategy_space_id.strip():
            raise ValueError("strategy_space_id must not be empty")
        if not self.search_specs:
            raise ValueError("search_specs must not be empty")


@dataclass(frozen=True)
class BacktestBatchRequest:
    campaign_run_id: str
    strategy_space_id: str
    dataset_version: str
    indicator_set_version: str
    search_specs: tuple[StrategyFamilySearchSpec, ...]
    combination_count: int
    derived_indicator_set_version: str = "derived-v1"
    param_batch_size: int = 500
    series_batch_size: int = 8
    timeframe: str = ""
    contract_ids: tuple[str, ...] = ()
    instrument_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.campaign_run_id.strip():
            raise ValueError("campaign_run_id must not be empty")
        if not self.strategy_space_id.strip():
            raise ValueError("strategy_space_id must not be empty")
        if not self.search_specs:
            raise ValueError("search_specs must not be empty")
        if self.combination_count <= 0:
            raise ValueError("combination_count must be positive")
        if self.param_batch_size <= 0:
            raise ValueError("param_batch_size must be positive")
        if self.series_batch_size <= 0:
            raise ValueError("series_batch_size must be positive")

    def batch_id(self) -> str:
        payload = "|".join(
            (
                self.campaign_run_id,
                self.strategy_space_id,
                self.dataset_version,
                self.indicator_set_version,
                self.derived_indicator_set_version,
                *[search_spec_id(spec) for spec in self.search_specs],
                str(self.combination_count),
                str(self.param_batch_size),
                str(self.series_batch_size),
                self.timeframe,
                *self.contract_ids,
                *self.instrument_ids,
            )
        )
        return "BTBATCH-" + _stable_hash(payload)


def build_ephemeral_strategy_space(
    *,
    strategy_version_labels: tuple[str, ...],
    instances_per_strategy: int,
    strategy_registry: StrategyRegistry | None = None,
) -> EphemeralStrategySpace:
    registry = strategy_registry or build_strategy_registry()
    if not strategy_version_labels:
        raise ValueError("strategy_version_labels must not be empty")
    if instances_per_strategy <= 0:
        raise ValueError("instances_per_strategy must be positive")

    search_specs: list[StrategyFamilySearchSpec] = []
    for strategy_version_label in strategy_version_labels:
        strategy_spec = registry.get(strategy_version_label)
        base_spec = strategy_spec_to_search_spec(
            strategy_spec,
            max_parameter_combinations=instances_per_strategy,
        )
        rows = strategy_spec.parameter_combinations()[:instances_per_strategy]
        search_specs.append(
            StrategyFamilySearchSpec(
                search_spec_version=base_spec.search_spec_version,
                family_key=base_spec.family_key,
                template_key=base_spec.template_key,
                strategy_version_label=base_spec.strategy_version_label,
                intent=base_spec.intent,
                allowed_clock_profiles=base_spec.allowed_clock_profiles,
                allowed_market_states=base_spec.allowed_market_states,
                required_price_inputs=base_spec.required_price_inputs,
                required_materialized_indicators=base_spec.required_materialized_indicators,
                required_materialized_derived=base_spec.required_materialized_derived,
                signal_surface_key=base_spec.signal_surface_key,
                signal_surface_mode=base_spec.signal_surface_mode,
                parameter_mode="table",
                parameter_space={"rows": tuple(rows)},
                parameter_constraints=base_spec.parameter_constraints,
                clock_profile=base_spec.clock_profile,
                required_inputs_by_clock=base_spec.required_inputs_by_clock,
                parameter_space_by_role=base_spec.parameter_space_by_role,
                parameter_clock_map=base_spec.parameter_clock_map,
                optional_indicator_plan=base_spec.optional_indicator_plan,
                exit_parameter_space=base_spec.exit_parameter_space,
                risk_parameter_space=base_spec.risk_parameter_space,
                execution_assumptions=base_spec.execution_assumptions,
                max_parameter_combinations=instances_per_strategy,
                chunking_policy=base_spec.chunking_policy,
                selection_policy=base_spec.selection_policy,
            )
        )
    strategy_space_id = "sspace_" + _stable_hash(
        "|".join(search_spec_id(spec) for spec in tuple(search_specs))
    )
    return EphemeralStrategySpace(
        strategy_space_id=strategy_space_id,
        search_specs=tuple(search_specs),
    )


def run_backtest_batch(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    derived_indicator_output_dir: Path,
    output_dir: Path,
    request: BacktestBatchRequest,
    engine_config: BacktestEngineConfig | None = None,
    strategy_registry: StrategyRegistry | None = None,
    cache: ResearchFrameCache | None = None,
) -> dict[str, object]:
    del strategy_registry
    engine_config = engine_config or BacktestEngineConfig()
    dataset_manifest = _load_dataset_manifest(
        output_dir=dataset_output_dir,
        dataset_version=request.dataset_version,
    )
    split_windows = _manifest_split_windows(dataset_manifest)
    input_columns = loader_columns_for_search_specs(request.search_specs)
    series_frames, cache_id, cache_hit = load_backtest_frames(
        dataset_output_dir=dataset_output_dir,
        indicator_output_dir=indicator_output_dir,
        derived_indicator_output_dir=derived_indicator_output_dir,
        request=ResearchSliceRequest(
            dataset_version=request.dataset_version,
            indicator_set_version=request.indicator_set_version,
            derived_indicator_set_version=request.derived_indicator_set_version,
            timeframe=_loader_timeframe_for_specs(request.search_specs, request.timeframe),
            contract_ids=request.contract_ids,
            instrument_ids=request.instrument_ids,
            price_columns=input_columns.price_columns,
            indicator_columns=input_columns.indicator_columns,
            derived_columns=input_columns.derived_columns,
        ),
        cache=cache,
    )
    selected_series = list(series_frames)
    if not selected_series:
        raise ValueError("no materialized research series matched the backtest request")

    batch_id = request.batch_id()
    all_search_spec_rows = [_search_spec_row(spec) for spec in request.search_specs]
    all_search_run_rows: list[dict[str, object]] = []
    all_param_result_rows: list[dict[str, object]] = []
    all_gate_rows: list[dict[str, object]] = []
    all_run_rows: list[dict[str, object]] = []
    all_stat_rows: list[dict[str, object]] = []
    all_trade_rows: list[dict[str, object]] = []
    all_order_rows: list[dict[str, object]] = []
    all_drawdown_rows: list[dict[str, object]] = []

    for search_spec in request.search_specs:
        for series_chunk in _chunked_series_for_spec(selected_series, request.series_batch_size, search_spec, request.timeframe):
            report = run_vectorbt_family_search(
                series_frames=series_chunk,
                search_spec=search_spec,
                config=engine_config,
                backtest_batch_id=batch_id,
                campaign_run_id=request.campaign_run_id,
                strategy_space_id=request.strategy_space_id,
                dataset_version=request.dataset_version,
                indicator_set_version=request.indicator_set_version,
                derived_indicator_set_version=request.derived_indicator_set_version,
                split_windows=split_windows,
                param_batch_size=request.param_batch_size,
            )
            all_search_run_rows.extend(report["search_run_rows"])
            all_param_result_rows.extend(report["param_result_rows"])
            all_gate_rows.extend(report["gate_rows"])
            all_run_rows.extend(report["run_rows"])
            all_stat_rows.extend(report["stat_rows"])
            all_trade_rows.extend(report["trade_rows"])
            all_order_rows.extend(report["order_rows"])
            all_drawdown_rows.extend(report["drawdown_rows"])

    batch_row = {
        "backtest_batch_id": batch_id,
        "campaign_run_id": request.campaign_run_id,
        "strategy_space_id": request.strategy_space_id,
        "dataset_version": request.dataset_version,
        "indicator_set_version": request.indicator_set_version,
        "derived_indicator_set_version": request.derived_indicator_set_version,
        "engine_name": engine_config.engine_name,
        "param_batch_size": request.param_batch_size,
        "series_batch_size": request.series_batch_size,
        "combination_count": request.combination_count,
        "series_count": len(selected_series),
        "cache_id": cache_id,
        "cache_hit": 1 if cache_hit else 0,
        "created_at": all_stat_rows[0]["created_at"] if all_stat_rows else "1970-01-01T00:00:00Z",
    }
    output_paths = write_backtest_artifacts(
        output_dir=output_dir,
        batch_rows=[batch_row],
        search_spec_rows=all_search_spec_rows,
        search_run_rows=all_search_run_rows,
        param_result_rows=all_param_result_rows,
        gate_event_rows=all_gate_rows,
        ephemeral_indicator_rows=[],
        promotion_event_rows=[],
        run_rows=all_run_rows,
        stat_rows=all_stat_rows,
        trade_rows=all_trade_rows,
        order_rows=all_order_rows,
        drawdown_rows=all_drawdown_rows,
    )
    return {
        "backtest_batch": batch_row,
        "search_spec_rows": all_search_spec_rows,
        "search_run_rows": all_search_run_rows,
        "param_result_rows": all_param_result_rows,
        "gate_event_rows": all_gate_rows,
        "ephemeral_indicator_rows": [],
        "promotion_event_rows": [],
        "run_rows": all_run_rows,
        "stat_rows": all_stat_rows,
        "trade_rows": all_trade_rows,
        "order_rows": all_order_rows,
        "drawdown_rows": all_drawdown_rows,
        "cache_id": cache_id,
        "cache_hit": cache_hit,
        "delta_manifest": backtest_store_contract(),
        "output_paths": output_paths,
    }


def _search_spec_row(spec: StrategyFamilySearchSpec) -> dict[str, object]:
    payload = spec.to_dict()
    return {
        "search_spec_id": search_spec_id(spec),
        "family_key": spec.family_key,
        "template_key": spec.template_key,
        "search_spec_version": spec.search_spec_version,
        "intent": spec.intent,
        "clock_profiles": list(spec.allowed_clock_profiles),
        "market_states": list(spec.allowed_market_states),
        "required_price_inputs_json": list(spec.required_price_inputs),
        "required_materialized_indicators_json": list(spec.required_materialized_indicators),
        "required_materialized_derived_json": list(spec.required_materialized_derived),
        "optional_indicator_plan_json": [item.to_dict() for item in spec.optional_indicator_plan],
        "signal_surface_key": spec.signal_surface_key,
        "signal_surface_mode": spec.signal_surface_mode,
        "parameter_mode": spec.parameter_mode,
        "parameter_space_json": payload["parameter_space"],
        "parameter_constraints_json": list(spec.parameter_constraints),
        "clock_profile_json": payload["clock_profile"],
        "required_inputs_by_clock_json": payload["required_inputs_by_clock"],
        "parameter_space_by_role_json": payload["parameter_space_by_role"],
        "parameter_clock_map_json": payload["parameter_clock_map"],
        "exit_parameter_space_json": payload["exit_parameter_space"],
        "risk_parameter_space_json": payload["risk_parameter_space"],
        "execution_assumptions_json": payload["execution_assumptions"],
        "created_at": "2026-04-27T00:00:00Z",
    }


def _chunked_series(items: list[object], chunk_size: int) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(items[index : index + chunk_size])
        for index in range(0, len(items), chunk_size)
    ) or (tuple(),)


def _chunked_series_for_spec(
    items: list[object],
    chunk_size: int,
    spec: StrategyFamilySearchSpec,
    fallback_timeframe: str,
) -> tuple[tuple[object, ...], ...]:
    series_items = [item for item in items if hasattr(item, "instrument_id") and hasattr(item, "timeframe")]
    execution_tf = _spec_execution_timeframe(spec, fallback=fallback_timeframe or "15m")
    required_timeframes = set(_spec_required_timeframes(spec, fallback=execution_tf))
    execution_instruments = [
        str(item.instrument_id)
        for item in series_items
        if str(item.timeframe) == execution_tf
    ]
    if not execution_instruments:
        return _chunked_series(items, chunk_size)
    chunks: list[tuple[object, ...]] = []
    for offset in range(0, len(execution_instruments), max(1, chunk_size)):
        instrument_set = set(execution_instruments[offset : offset + max(1, chunk_size)])
        chunk = tuple(
            item
            for item in series_items
            if str(item.instrument_id) in instrument_set and str(item.timeframe) in required_timeframes
        )
        if chunk:
            chunks.append(chunk)
    return tuple(chunks) or (tuple(),)
