from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import product
from time import perf_counter
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import vectorbt as vbt
from numba import njit
from vectorbt.portfolio import enums

from trading_advisor_3000.product_plane.research.io.loaders import ResearchSeriesFrame
from trading_advisor_3000.product_plane.research.strategies import StrategySpec

from .ranking import RankingPolicy, default_ranking_policy, score_optimizer_trial


PRICE_INPUTS = ("open", "high", "low", "close")
INDICATOR_SOURCE_PRIORITY = (
    "materialized_delta",
    "vbt_builtin",
    "vbt_indicator_factory_numba",
    "vbt_indicator_factory_talib",
    "vbt_indicator_factory_pandas_ta",
    "pandas_ta_direct_precompute",
)
METADATA_COLUMNS = {
    "dataset_version",
    "indicator_set_version",
    "derived_indicator_set_version",
    "profile_version",
    "contract_id",
    "instrument_id",
    "timeframe",
    "ts",
    "session_date",
    "session_open_ts",
    "session_close_ts",
    "active_contract_id",
    "slice_role",
    "bar_index",
    "created_at",
    "source_bars_hash",
    "source_indicators_hash",
    "row_count",
    "warmup_span",
    "null_warmup_span",
}
_OPTUNA_CONSTRAINTS_ATTR = "ta3000_constraint_values"
_OPTUNA_CONSTRAINT_NAMES_ATTR = "ta3000_constraint_names"


class InputPlanValidationError(ValueError):
    def __init__(self, failure_code: str, message: str, *, missing: Sequence[str] = ()) -> None:
        super().__init__(message)
        self.failure_code = failure_code
        self.missing = tuple(missing)


def _input_alias(value: object) -> str:
    if isinstance(value, Mapping):
        for key in ("alias", "column", "name", "indicator_key"):
            alias = value.get(key)
            if alias is not None and str(alias).strip():
                return str(alias).strip()
    return str(value).strip()


def _input_aliases(values: Iterable[object]) -> tuple[str, ...]:
    aliases: list[str] = []
    for value in values:
        alias = _input_alias(value)
        if alias and alias not in aliases:
            aliases.append(alias)
    return tuple(aliases)


@dataclass(frozen=True)
class IndicatorInputPlan:
    alias: str
    source: str = "materialized_delta"
    missing_policy: str = "materialized_required"
    params: Mapping[str, object] = field(default_factory=dict)
    provider: str | None = None

    def __post_init__(self) -> None:
        if not self.alias.strip():
            raise ValueError("indicator input alias must be non-empty")
        if self.source not in INDICATOR_SOURCE_PRIORITY:
            raise ValueError(f"unsupported indicator input source: {self.source}")
        if self.missing_policy not in {
            "materialized_required",
            "compute_ephemeral_allowed",
            "compute_ephemeral_then_cache",
            "fail_fast",
            "not_applicable",
        }:
            raise ValueError(f"unsupported indicator missing_policy: {self.missing_policy}")

    @classmethod
    def from_payload(cls, payload: object) -> "IndicatorInputPlan":
        if isinstance(payload, IndicatorInputPlan):
            return payload
        if isinstance(payload, Mapping):
            return cls(
                alias=_input_alias(payload),
                source=str(payload.get("source", "materialized_delta")),
                missing_policy=str(payload.get("missing_policy", "materialized_required")),
                params=dict(payload.get("params", {})) if isinstance(payload.get("params", {}), Mapping) else {},
                provider=str(payload["provider"]) if payload.get("provider") is not None else None,
            )
        return cls(alias=_input_alias(payload))

    def to_dict(self) -> dict[str, object]:
        return {
            "alias": self.alias,
            "source": self.source,
            "missing_policy": self.missing_policy,
            "params": dict(self.params),
            "provider": self.provider,
        }


@dataclass(frozen=True)
class VectorBTIndicatorPlan:
    required_price_inputs: tuple[str, ...]
    materialized_indicators: tuple[str, ...]
    materialized_derived: tuple[str, ...]
    optional_indicators: tuple[IndicatorInputPlan, ...] = ()
    source_priority: tuple[str, ...] = INDICATOR_SOURCE_PRIORITY
    inputs_by_clock: Mapping[str, Mapping[str, object]] = field(default_factory=dict)

    @property
    def plan_hash(self) -> str:
        return _stable_hash(_canonical_json(self.to_dict()))

    def to_dict(self) -> dict[str, object]:
        return {
            "required_price_inputs": list(self.required_price_inputs),
            "materialized_indicators": list(self.materialized_indicators),
            "materialized_derived": list(self.materialized_derived),
            "optional_indicators": [item.to_dict() for item in self.optional_indicators],
            "source_priority": list(self.source_priority),
            "inputs_by_clock": {
                str(layer): dict(payload)
                for layer, payload in self.inputs_by_clock.items()
            },
        }


@dataclass(frozen=True)
class BacktestEngineConfig:
    engine_name: str = "vectorbt_family_search"
    initial_cash: float = 100_000.0
    position_size: float = 1.0
    fees_bps: float = 0.0
    slippage_bps: float = 0.0
    allow_short: bool = True
    session_hours_utc: tuple[int, int] | None = None
    window_count: int = 1
    signal_shift_bars: int = 1

    def __post_init__(self) -> None:
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        if self.position_size <= 0:
            raise ValueError("position_size must be positive")
        if self.fees_bps < 0:
            raise ValueError("fees_bps must be non-negative")
        if self.slippage_bps < 0:
            raise ValueError("slippage_bps must be non-negative")
        if self.window_count <= 0:
            raise ValueError("window_count must be positive")
        if self.signal_shift_bars < 0:
            raise ValueError("signal_shift_bars must be non-negative")


@dataclass(frozen=True)
class StrategyFamilySearchSpec:
    search_spec_version: str
    family_key: str
    template_key: str
    strategy_version_label: str
    intent: str
    allowed_clock_profiles: tuple[str, ...]
    allowed_market_states: tuple[str, ...]
    required_price_inputs: tuple[str, ...]
    required_materialized_indicators: tuple[str, ...]
    required_materialized_derived: tuple[str, ...]
    signal_surface_key: str
    signal_surface_mode: str
    parameter_mode: str
    parameter_space: Mapping[str, tuple[object, ...]]
    parameter_constraints: tuple[str, ...] = ()
    clock_profile: Mapping[str, object] = field(default_factory=dict)
    required_inputs_by_clock: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    parameter_space_by_role: Mapping[str, Mapping[str, tuple[object, ...]]] = field(default_factory=dict)
    parameter_clock_map: tuple[Mapping[str, object], ...] = ()
    optional_indicator_plan: tuple[IndicatorInputPlan, ...] = ()
    exit_parameter_space: Mapping[str, tuple[object, ...]] = field(default_factory=dict)
    risk_parameter_space: Mapping[str, tuple[object, ...]] = field(default_factory=dict)
    execution_assumptions: Mapping[str, object] = field(default_factory=dict)
    max_parameter_combinations: int = 250_000
    chunking_policy: Mapping[str, object] = field(default_factory=dict)
    selection_policy: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in (
            "search_spec_version",
            "family_key",
            "template_key",
            "strategy_version_label",
            "intent",
            "signal_surface_key",
            "signal_surface_mode",
            "parameter_mode",
        ):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} must be non-empty")
        if self.parameter_mode not in {"product", "table"}:
            raise ValueError("parameter_mode must be `product` or `table`")
        if self.max_parameter_combinations <= 0:
            raise ValueError("max_parameter_combinations must be positive")
        object.__setattr__(self, "required_price_inputs", _input_aliases(self.required_price_inputs))
        object.__setattr__(self, "required_materialized_indicators", _input_aliases(self.required_materialized_indicators))
        object.__setattr__(self, "required_materialized_derived", _input_aliases(self.required_materialized_derived))
        object.__setattr__(
            self,
            "optional_indicator_plan",
            tuple(IndicatorInputPlan.from_payload(item) for item in self.optional_indicator_plan),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "search_spec_version": self.search_spec_version,
            "family_key": self.family_key,
            "template_key": self.template_key,
            "strategy_version_label": self.strategy_version_label,
            "intent": self.intent,
            "allowed_clock_profiles": list(self.allowed_clock_profiles),
            "allowed_market_states": list(self.allowed_market_states),
            "required_price_inputs": list(self.required_price_inputs),
            "required_materialized_indicators": list(self.required_materialized_indicators),
            "required_materialized_derived": list(self.required_materialized_derived),
            "optional_indicator_plan": [item.to_dict() for item in self.optional_indicator_plan],
            "signal_surface_key": self.signal_surface_key,
            "signal_surface_mode": self.signal_surface_mode,
            "parameter_mode": self.parameter_mode,
            "parameter_space": {key: list(value) for key, value in self.parameter_space.items()},
            "parameter_constraints": list(self.parameter_constraints),
            "clock_profile": dict(self.clock_profile),
            "required_inputs_by_clock": {
                str(layer): dict(payload)
                for layer, payload in self.required_inputs_by_clock.items()
            },
            "parameter_space_by_role": {
                str(role): {str(key): list(value) for key, value in space.items()}
                for role, space in self.parameter_space_by_role.items()
            },
            "parameter_clock_map": [dict(item) for item in self.parameter_clock_map],
            "exit_parameter_space": {key: list(value) for key, value in self.exit_parameter_space.items()},
            "risk_parameter_space": {key: list(value) for key, value in self.risk_parameter_space.items()},
            "execution_assumptions": dict(self.execution_assumptions),
            "max_parameter_combinations": self.max_parameter_combinations,
            "chunking_policy": dict(self.chunking_policy),
            "selection_policy": dict(self.selection_policy),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "StrategyFamilySearchSpec":
        def _tuple_value(name: str) -> tuple[str, ...]:
            value = payload.get(name, ())
            if isinstance(value, str):
                return (value,)
            if isinstance(value, Iterable):
                return tuple(str(item) for item in value)
            return tuple()

        def _space(name: str) -> dict[str, tuple[object, ...]]:
            value = payload.get(name, {})
            if not isinstance(value, Mapping):
                return {}
            resolved: dict[str, tuple[object, ...]] = {}
            for key, item in value.items():
                if isinstance(item, list | tuple):
                    resolved[str(key)] = tuple(item)
                else:
                    resolved[str(key)] = (item,)
            return resolved

        def _space_by_role(name: str) -> dict[str, dict[str, tuple[object, ...]]]:
            value = payload.get(name, {})
            if not isinstance(value, Mapping):
                return {}
            resolved: dict[str, dict[str, tuple[object, ...]]] = {}
            for role, raw_space in value.items():
                if not isinstance(raw_space, Mapping):
                    continue
                role_space: dict[str, tuple[object, ...]] = {}
                for key, item in raw_space.items():
                    if isinstance(item, list | tuple):
                        role_space[str(key)] = tuple(item)
                    else:
                        role_space[str(key)] = (item,)
                resolved[str(role)] = role_space
            return resolved

        return cls(
            search_spec_version=str(payload["search_spec_version"]),
            family_key=str(payload["family_key"]),
            template_key=str(payload["template_key"]),
            strategy_version_label=str(payload["strategy_version_label"]),
            intent=str(payload["intent"]),
            allowed_clock_profiles=_tuple_value("allowed_clock_profiles"),
            allowed_market_states=_tuple_value("allowed_market_states"),
            required_price_inputs=_tuple_value("required_price_inputs"),
            required_materialized_indicators=_tuple_value("required_materialized_indicators"),
            required_materialized_derived=_tuple_value("required_materialized_derived"),
            signal_surface_key=str(payload["signal_surface_key"]),
            signal_surface_mode=str(payload["signal_surface_mode"]),
            parameter_mode=str(payload["parameter_mode"]),
            parameter_space=_space("parameter_space"),
            parameter_constraints=_tuple_value("parameter_constraints"),
            clock_profile=dict(payload.get("clock_profile", {})) if isinstance(payload.get("clock_profile"), Mapping) else {},
            required_inputs_by_clock={
                str(key): dict(value)
                for key, value in payload.get("required_inputs_by_clock", {}).items()
                if isinstance(value, Mapping)
            }
            if isinstance(payload.get("required_inputs_by_clock"), Mapping)
            else {},
            parameter_space_by_role=_space_by_role("parameter_space_by_role"),
            parameter_clock_map=tuple(
                dict(item)
                for item in payload.get("parameter_clock_map", ())
                if isinstance(item, Mapping)
            )
            if isinstance(payload.get("parameter_clock_map", ()), Iterable)
            and not isinstance(payload.get("parameter_clock_map", ()), (str, bytes))
            else tuple(),
            optional_indicator_plan=tuple(
                IndicatorInputPlan.from_payload(item)
                for item in payload.get("optional_indicator_plan", ())
                if item is not None
            )
            if isinstance(payload.get("optional_indicator_plan", ()), Iterable)
            and not isinstance(payload.get("optional_indicator_plan", ()), (str, bytes))
            else tuple(),
            exit_parameter_space=_space("exit_parameter_space"),
            risk_parameter_space=_space("risk_parameter_space"),
            execution_assumptions=dict(payload.get("execution_assumptions", {}))
            if isinstance(payload.get("execution_assumptions"), Mapping)
            else {},
            max_parameter_combinations=int(payload.get("max_parameter_combinations", 250_000)),
            chunking_policy=dict(payload.get("chunking_policy", {})) if isinstance(payload.get("chunking_policy"), Mapping) else {},
            selection_policy=dict(payload.get("selection_policy", {})) if isinstance(payload.get("selection_policy"), Mapping) else {},
        )


@dataclass(frozen=True)
class VectorBTInputBundle:
    index: pd.Index
    instruments: tuple[str, ...]
    price: Mapping[str, pd.DataFrame]
    fields: Mapping[str, pd.DataFrame]
    metadata: Mapping[str, object]
    timeframe_price: Mapping[str, Mapping[str, pd.DataFrame]] = field(default_factory=dict)
    timeframe_fields: Mapping[str, Mapping[str, pd.DataFrame]] = field(default_factory=dict)

    def field(self, name: str) -> pd.DataFrame:
        if name in self.price:
            return self.price[name]
        if name in self.fields:
            return self.fields[name]
        raise KeyError(name)

    def field_at(self, name: str, timeframe: str, *, align_to_execution: bool = False) -> pd.DataFrame:
        native = self.timeframe_price.get(timeframe, {}).get(name)
        if native is None:
            native = self.timeframe_fields.get(timeframe, {}).get(name)
        if native is None:
            if timeframe == str(self.metadata.get("execution_tf", "")):
                return self.field(name)
            raise KeyError(f"{timeframe}:{name}")
        if not align_to_execution:
            return native
        return native.reindex(self.index, method="ffill")


@dataclass(frozen=True)
class VectorBTSignalSurfaceResult:
    surface_id: str
    search_run_id: str
    family_key: str
    surface_key: str
    template_key: str
    columns: pd.MultiIndex
    param_rows: tuple[dict[str, object], ...]
    parameter_index: pd.DataFrame
    indicator_plan: VectorBTIndicatorPlan
    entries: pd.DataFrame
    exits: pd.DataFrame
    short_entries: pd.DataFrame
    short_exits: pd.DataFrame
    sl_stop: pd.DataFrame
    tp_stop: pd.DataFrame
    diagnostics: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MTFInputResolverResult:
    indicator_plan: VectorBTIndicatorPlan
    input_names: tuple[str, ...]
    role_timeframes: Mapping[str, str]


def _stable_hash(text: str, *, length: int = 12) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length].upper()


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _timeframe_freq(timeframe: str) -> str:
    if timeframe.endswith("m"):
        return f"{int(timeframe[:-1])}min"
    if timeframe.endswith("h"):
        return f"{int(timeframe[:-1])}h"
    if timeframe.endswith("d"):
        return f"{int(timeframe[:-1])}D"
    if timeframe.endswith("w"):
        return f"{int(timeframe[:-1])}W"
    raise ValueError(f"unsupported timeframe token: {timeframe}")


def _timeframe_sort_key(timeframe: str) -> int:
    if timeframe.endswith("m"):
        return int(timeframe[:-1])
    if timeframe.endswith("h"):
        return int(timeframe[:-1]) * 60
    if timeframe.endswith("d"):
        return int(timeframe[:-1]) * 24 * 60
    if timeframe.endswith("w"):
        return int(timeframe[:-1]) * 7 * 24 * 60
    return 10**9


def _bars_per_year(timeframe: str) -> float:
    if timeframe.endswith("m"):
        return (365.0 * 24.0 * 60.0) / float(int(timeframe[:-1]))
    if timeframe.endswith("h"):
        return (365.0 * 24.0) / float(int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return 365.0 / float(int(timeframe[:-1]))
    if timeframe.endswith("w"):
        return 52.0 / float(int(timeframe[:-1]))
    return 0.0


def _annualized_return(total_return: float, *, periods: int, timeframe: str) -> float:
    if periods <= 0:
        return 0.0
    bars_year = _bars_per_year(timeframe)
    if bars_year <= 0.0:
        return 0.0
    return (1.0 + total_return) ** (bars_year / periods) - 1.0


def _numeric_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.apply(pd.to_numeric, errors="coerce")


def _session_entry_mask(index: pd.Index, session_hours_utc: tuple[int, int] | None) -> np.ndarray:
    if session_hours_utc is None:
        return np.ones(len(index), dtype=bool)
    timestamps = pd.to_datetime(index, utc=True)
    hours = timestamps.hour
    start_hour, end_hour = session_hours_utc
    if start_hour <= end_hour:
        return np.asarray((hours >= start_hour) & (hours < end_hour), dtype=bool)
    return np.asarray((hours >= start_hour) | (hours < end_hour), dtype=bool)


def _window_frames(
    frame: pd.DataFrame,
    *,
    window_count: int,
    split_windows: tuple[dict[str, object], ...] | None = None,
) -> tuple[tuple[str, pd.DataFrame], ...]:
    if split_windows:
        resolved: list[tuple[str, pd.DataFrame]] = []
        for index, window in enumerate(split_windows, start=1):
            window_id = str(window.get("window_id", f"wf-{index:02d}"))
            subset = pd.DataFrame()
            if window.get("test_start_ts") and window.get("test_end_ts"):
                start_ts = str(window["test_start_ts"])
                end_ts = str(window["test_end_ts"])
                subset = frame[(frame["ts"] >= start_ts) & (frame["ts"] <= end_ts)].copy()
            if subset.empty:
                test_start = window.get("test_start")
                test_stop = window.get("test_stop")
                if isinstance(test_start, int) and isinstance(test_stop, int):
                    start = max(0, min(len(frame), test_start))
                    stop = max(start, min(len(frame), test_stop))
                    subset = frame.iloc[start:stop].copy()
            if subset.empty and window.get("analysis_start_ts") and window.get("analysis_end_ts"):
                start_ts = str(window["analysis_start_ts"])
                end_ts = str(window["analysis_end_ts"])
                subset = frame[(frame["ts"] >= start_ts) & (frame["ts"] <= end_ts)].copy()
            if len(subset) >= 2:
                resolved.append((window_id, subset))
        if resolved:
            return tuple(resolved)

    if window_count <= 1 or len(frame) < 4:
        return (("wf-01", frame.copy()),)
    base_size = max(2, len(frame) // window_count)
    windows: list[tuple[str, pd.DataFrame]] = []
    start = 0
    for index in range(window_count):
        stop = len(frame) if index == window_count - 1 else min(len(frame), start + base_size)
        if stop - start < 2:
            continue
        windows.append((f"wf-{index + 1:02d}", frame.iloc[start:stop].copy()))
        start = stop
    return tuple(windows) or (("wf-01", frame.copy()),)


def _parameter_rows(spec: StrategyFamilySearchSpec) -> tuple[dict[str, object], ...]:
    if spec.parameter_mode == "table" and "rows" in spec.parameter_space:
        raw_rows = spec.parameter_space["rows"]
        rows = tuple(dict(row) for row in raw_rows if isinstance(row, Mapping))
    else:
        ordered = sorted(spec.parameter_space.items(), key=lambda item: item[0])
        if not ordered:
            rows = ({},)
        else:
            names = [name for name, _ in ordered]
            values = [tuple(items) for _, items in ordered]
            rows = tuple(
                {name: value for name, value in zip(names, combo, strict=True)}
                for combo in product(*values)
            )
    filtered = tuple(row for row in rows if _constraints_pass(row, spec.parameter_constraints))
    if not filtered:
        raise ValueError(f"search spec `{spec.family_key}` resolved to 0 valid parameter rows")
    if len(filtered) > spec.max_parameter_combinations:
        raise ValueError(
            f"search spec `{spec.family_key}` exceeds max_parameter_combinations: "
            f"{len(filtered)} > {spec.max_parameter_combinations}"
        )
    return filtered


def resolve_parameter_rows(spec: StrategyFamilySearchSpec) -> tuple[dict[str, object], ...]:
    return _parameter_rows(spec)


def _constraints_pass(row: Mapping[str, object], constraints: Sequence[str]) -> bool:
    for constraint in constraints:
        if not _constraint_pass(row, constraint):
            return False
    return True


def _constraint_pass(row: Mapping[str, object], constraint: str) -> bool:
    normalized = constraint.strip()
    for operator in ("<=", ">=", "!=", "==", "<", ">"):
        if operator not in normalized:
            continue
        left, right = (part.strip() for part in normalized.split(operator, 1))
        left_value = row.get(left)
        right_value = row.get(right)
        if right_value is None:
            try:
                right_value = float(right)
            except ValueError:
                right_value = right.strip("'\"")
        try:
            if operator == "<=":
                return float(left_value) <= float(right_value)
            if operator == ">=":
                return float(left_value) >= float(right_value)
            if operator == "<":
                return float(left_value) < float(right_value)
            if operator == ">":
                return float(left_value) > float(right_value)
        except (TypeError, ValueError):
            return False
        if operator == "==":
            return left_value == right_value
        if operator == "!=":
            return left_value != right_value
    raise ValueError(f"unsupported parameter constraint: {constraint}")


def param_hash(spec: StrategyFamilySearchSpec, params: Mapping[str, object]) -> str:
    return _stable_hash(
        _canonical_json(
            {
                "family_key": spec.family_key,
                "template_key": spec.template_key,
                "strategy_version_label": spec.strategy_version_label,
                "signal_surface_key": spec.signal_surface_key,
                "params": dict(params),
                "clock_profile": dict(spec.clock_profile),
                "required_inputs_by_clock": {
                    str(layer): dict(payload)
                    for layer, payload in spec.required_inputs_by_clock.items()
                },
                "required_materialized_indicators": list(spec.required_materialized_indicators),
                "required_materialized_derived": list(spec.required_materialized_derived),
                "optional_indicator_plan": [item.to_dict() for item in spec.optional_indicator_plan],
            }
        )
    )


def search_spec_id(spec: StrategyFamilySearchSpec) -> str:
    return "SSPEC-" + _stable_hash(_canonical_json(spec.to_dict()))


def _clock_profile_payload(strategy_spec: StrategySpec) -> dict[str, object]:
    raw_profile = getattr(strategy_spec, "clock_profile", None)
    if raw_profile is not None and hasattr(raw_profile, "to_dict"):
        return dict(raw_profile.to_dict())
    name = strategy_spec.allowed_clock_profiles[0] if strategy_spec.allowed_clock_profiles else "research_clock_v1"
    return {
        "name": name,
        "regime_tf": "1d",
        "signal_tf": "15m",
        "trigger_tf": "15m",
        "execution_tf": "15m",
        "bar_type": "time",
        "closed_bars_only": True,
        "layer_alignment": "native_then_event_to_execution",
    }


def _execution_timeframe_from_profile(clock_profile: Mapping[str, object], fallback: str = "15m") -> str:
    value = clock_profile.get("execution_tf") if isinstance(clock_profile, Mapping) else None
    resolved = str(value).strip() if value is not None else ""
    return resolved or fallback


def _clock_timeframes_from_profile(clock_profile: Mapping[str, object], fallback: str = "15m") -> tuple[str, ...]:
    keys = ("regime_tf", "signal_tf", "trigger_tf", "execution_tf")
    values: list[str] = []
    for key in keys:
        raw = clock_profile.get(key) if isinstance(clock_profile, Mapping) else None
        value = str(raw).strip() if raw is not None else ""
        if value and value not in values:
            values.append(value)
    if not values:
        values.append(fallback)
    return tuple(values)


def _clock_layer_for_role(role: str) -> str:
    if role == "decision":
        return "signal"
    if role in {"entry", "exit", "risk"}:
        return "execution"
    return role


def _required_inputs_by_clock(strategy_spec: StrategySpec, clock_profile: Mapping[str, object]) -> dict[str, dict[str, object]]:
    layer_defaults = {
        "regime": str(clock_profile.get("regime_tf", "1d")),
        "signal": str(clock_profile.get("signal_tf", "15m")),
        "trigger": str(clock_profile.get("trigger_tf", "15m")),
        "execution": str(clock_profile.get("execution_tf", "15m")),
    }
    layers: dict[str, dict[str, object]] = {
        layer: {
            "timeframe": timeframe,
            "price_inputs": [],
            "materialized_indicators": [],
            "materialized_derived": [],
            "requirements": [],
        }
        for layer, timeframe in layer_defaults.items()
    }

    def _append_unique(items: list[str], value: str) -> None:
        if value not in items:
            items.append(value)

    for requirement in strategy_spec.indicator_requirements:
        layer = _clock_layer_for_role(str(requirement.role))
        payload = layers.setdefault(
            layer,
            {
                "timeframe": requirement.timeframe,
                "price_inputs": [],
                "materialized_indicators": [],
                "materialized_derived": [],
                "requirements": [],
            },
        )
        payload["timeframe"] = requirement.timeframe
        payload_requirements = payload["requirements"]
        if isinstance(payload_requirements, list):
            payload_requirements.append(requirement.to_dict())
        if requirement.column in PRICE_INPUTS or requirement.source == "price":
            price_inputs = payload["price_inputs"]
            if isinstance(price_inputs, list):
                _append_unique(price_inputs, requirement.column)
        elif requirement.source == "indicator":
            indicators = payload["materialized_indicators"]
            if isinstance(indicators, list):
                _append_unique(indicators, requirement.column)
        elif requirement.source == "derived":
            derived = payload["materialized_derived"]
            if isinstance(derived, list):
                _append_unique(derived, requirement.column)
    return layers


def strategy_spec_to_search_spec(
    strategy_spec: StrategySpec,
    *,
    template_key: str | None = None,
    max_parameter_combinations: int = 250_000,
) -> StrategyFamilySearchSpec:
    clock_profile = _clock_profile_payload(strategy_spec)
    required_inputs_by_clock = _required_inputs_by_clock(strategy_spec, clock_profile)
    required_indicators: list[str] = []
    required_derived: list[str] = []

    def _append_unique(values: list[str], column: str) -> None:
        if column not in values:
            values.append(column)

    if strategy_spec.indicator_requirements:
        for requirement in strategy_spec.indicator_requirements:
            if requirement.column in PRICE_INPUTS or requirement.source == "price":
                continue
            if requirement.source == "indicator":
                _append_unique(required_indicators, requirement.column)
            elif requirement.source == "derived":
                _append_unique(required_derived, requirement.column)
    else:
        for column in strategy_spec.required_columns:
            if column in PRICE_INPUTS:
                continue
            if column.startswith(("distance_", "cross_", "mtf_", "rolling_", "session_", "bb_position", "kc_position", "donchian_position", "close_slope", "roc_", "mom_")):
                _append_unique(required_derived, column)
            else:
                _append_unique(required_indicators, column)
    return StrategyFamilySearchSpec(
        search_spec_version="vbt-family-search-v1",
        family_key=strategy_spec.family,
        template_key=template_key or strategy_spec.signal_builder_key,
        strategy_version_label=strategy_spec.version,
        intent=strategy_spec.intent or strategy_spec.description,
        allowed_clock_profiles=strategy_spec.allowed_clock_profiles,
        allowed_market_states=tuple(strategy_spec.market_regimes or strategy_spec.ranking_metadata.tags) or ("mixed_unknown",),
        required_price_inputs=tuple(column for column in PRICE_INPUTS if column in strategy_spec.required_columns or column == "close"),
        required_materialized_indicators=tuple(required_indicators),
        required_materialized_derived=tuple(required_derived),
        signal_surface_key=f"vbt_surface.{strategy_spec.signal_builder_key}_v1",
        signal_surface_mode="indicator_factory" if strategy_spec.execution_mode == "signals" else "signal_factory",
        parameter_mode="product",
        parameter_space={parameter.name: tuple(parameter.values) for parameter in strategy_spec.parameter_grid},
        parameter_constraints=strategy_spec.parameter_constraints,
        clock_profile=clock_profile,
        required_inputs_by_clock=required_inputs_by_clock,
        parameter_space_by_role=strategy_spec.parameter_space_by_role()
        if hasattr(strategy_spec, "parameter_space_by_role")
        else {},
        parameter_clock_map=strategy_spec.parameter_clock_map()
        if hasattr(strategy_spec, "parameter_clock_map")
        else tuple(),
        optional_indicator_plan=strategy_spec.optional_indicator_plan,
        risk_parameter_space={
            "stop_atr_mult": (strategy_spec.risk_policy.stop_atr_multiple,),
            "target_atr_mult": (strategy_spec.risk_policy.target_atr_multiple,),
        },
        execution_assumptions={
            "freq": _timeframe_freq(_execution_timeframe_from_profile(clock_profile)),
            "fees_required": True,
            "slippage_required": True,
            "signal_shift_bars": 1,
            "direction_mode": strategy_spec.direction_mode,
            "clock_profile": clock_profile,
            "indicator_alignment": "native_per_clock",
            "event_alignment": "closed_layer_event_to_execution",
            "indicator_roles": [requirement.to_dict() for requirement in strategy_spec.indicator_requirements],
            "entry_logic": list(strategy_spec.entry_logic),
            "exit_logic": list(strategy_spec.exit_logic),
        },
        max_parameter_combinations=max_parameter_combinations,
        chunking_policy={
            "max_columns_per_vectorbt_call": max_parameter_combinations,
            "min_param_combos_per_call": min(500, max_parameter_combinations),
            "allowed_chunk_axes": ["parameter", "instrument", "fold", "time_split"],
        },
        selection_policy={"ranking_scope": "family_first"},
    )


def build_input_bundle(
    series_frames: Sequence[ResearchSeriesFrame],
    *,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    clock_profile: str | Mapping[str, object],
    execution_timeframe: str | None = None,
) -> VectorBTInputBundle:
    if not series_frames:
        raise ValueError("VectorBTInputBundle requires at least one series frame")
    if isinstance(clock_profile, Mapping):
        clock_profile_contract = dict(clock_profile)
        clock_profile_name = str(clock_profile_contract.get("name", "research_clock_v1"))
    else:
        clock_profile_name = str(clock_profile)
        clock_profile_contract = {"name": clock_profile_name}
    available_timeframes = tuple(sorted({series.timeframe for series in series_frames}, key=_timeframe_sort_key))
    execution_tf = (
        execution_timeframe
        or str(clock_profile_contract.get("execution_tf", "")).strip()
        or (next(iter(available_timeframes)) if len(available_timeframes) == 1 else "")
        or ("15m" if "15m" in available_timeframes else next(iter(available_timeframes)))
    )
    execution_series = tuple(series for series in series_frames if series.timeframe == execution_tf)
    if not execution_series:
        raise ValueError(f"VectorBTInputBundle missing execution timeframe `{execution_tf}`")
    instruments = tuple(series.instrument_id for series in execution_series)
    if len(set(instruments)) != len(instruments):
        raise ValueError("VectorBTInputBundle requires unique execution instrument_id columns")
    series_by_key: dict[tuple[str, str], ResearchSeriesFrame] = {}
    for series in series_frames:
        key = (series.timeframe, series.instrument_id)
        if key in series_by_key:
            raise ValueError(f"VectorBTInputBundle received duplicate series for {series.timeframe}:{series.instrument_id}")
        series_by_key[key] = series

    def _can_build_matrix(column: str, timeframe: str) -> bool:
        for instrument_id in instruments:
            series = series_by_key.get((timeframe, instrument_id))
            if series is None or column not in series.frame.columns:
                return False
        return True

    def _matrix(column: str, timeframe: str) -> pd.DataFrame:
        pieces: dict[str, pd.Series] = {}
        for instrument_id in instruments:
            series = series_by_key[(timeframe, instrument_id)]
            if column not in series.frame.columns:
                raise KeyError(column)
            pieces[series.instrument_id] = pd.to_numeric(series.frame[column], errors="coerce")
        matrix = pd.DataFrame(pieces).sort_index()
        matrix.index = pd.to_datetime(matrix.index, utc=True)
        return matrix

    price_by_tf: dict[str, dict[str, pd.DataFrame]] = {}
    fields_by_tf: dict[str, dict[str, pd.DataFrame]] = {}
    for timeframe in available_timeframes:
        local_frames = [series.frame for series in series_frames if series.timeframe == timeframe]
        local_columns = set().union(*(set(frame.columns) for frame in local_frames)) if local_frames else set()
        price_by_tf[timeframe] = {
            column: _matrix(column, timeframe)
            for column in PRICE_INPUTS
            if column in local_columns and _can_build_matrix(column, timeframe)
        }
        payload_columns = sorted(
            column
            for column in local_columns
            if column not in METADATA_COLUMNS and column not in PRICE_INPUTS and _can_build_matrix(column, timeframe)
        )
        fields_by_tf[timeframe] = {column: _matrix(column, timeframe) for column in payload_columns}

    price = price_by_tf.get(execution_tf, {})
    fields = fields_by_tf.get(execution_tf, {})
    if "close" not in price:
        raise KeyError("close")
    close_index = price["close"].index
    return VectorBTInputBundle(
        index=close_index,
        instruments=instruments,
        price={name: frame.reindex(close_index) for name, frame in price.items()},
        fields={name: frame.reindex(close_index) for name, frame in fields.items()},
        metadata={
            "dataset_id": dataset_version,
            "indicator_profile_version": indicator_set_version,
            "derived_indicator_profile_version": derived_indicator_set_version,
            "clock_profile": clock_profile_name,
            "clock_profile_contract": clock_profile_contract,
            "available_timeframes": list(available_timeframes),
            "execution_tf": execution_tf,
            "timezone": "UTC",
            "as_of_policy": "closed_bars_only",
            "contract_ids": {series.instrument_id: series.contract_id for series in execution_series},
        },
        timeframe_price=price_by_tf,
        timeframe_fields=fields_by_tf,
    )


def _payload_aliases(payload: Mapping[str, object], name: str) -> tuple[str, ...]:
    raw = payload.get(name, ())
    if isinstance(raw, (str, bytes)):
        return _input_aliases((raw,))
    if isinstance(raw, Iterable):
        return _input_aliases(raw)
    return tuple()


def resolve_indicator_plan(bundle: VectorBTInputBundle, spec: StrategyFamilySearchSpec) -> VectorBTIndicatorPlan:
    _validate_bundle_shape(bundle)
    if spec.clock_profile:
        expected_execution_tf = _execution_timeframe_from_profile(spec.clock_profile)
        actual_execution_tf = str(bundle.metadata.get("execution_tf", ""))
        if actual_execution_tf != expected_execution_tf:
            raise InputPlanValidationError(
                "CLOCK_PROFILE_MISMATCH",
                f"{spec.family_key} expected execution timeframe {expected_execution_tf}, got {actual_execution_tf}",
            )
    if spec.required_inputs_by_clock:
        missing: list[str] = []
        missing_mtf: list[str] = []
        for layer, payload in spec.required_inputs_by_clock.items():
            if not isinstance(payload, Mapping):
                continue
            timeframe = str(payload.get("timeframe", "")).strip()
            if not timeframe:
                continue
            price_map = bundle.timeframe_price.get(timeframe, {})
            field_map = bundle.timeframe_fields.get(timeframe, {})
            for column in _payload_aliases(payload, "price_inputs"):
                if column not in price_map:
                    missing.append(f"{layer}:{timeframe}:{column}")
            for column in _payload_aliases(payload, "materialized_indicators"):
                if column not in field_map:
                    missing.append(f"{layer}:{timeframe}:{column}")
            for column in _payload_aliases(payload, "materialized_derived"):
                if column not in field_map:
                    item = f"{layer}:{timeframe}:{column}"
                    if column.startswith("mtf_"):
                        missing_mtf.append(item)
                    missing.append(item)
        if missing:
            failure = "MISSING_MTF_INPUT" if missing_mtf else "MISSING_CLOCK_INPUT"
            raise InputPlanValidationError(
                failure,
                f"{failure}: {spec.family_key} missing native clock inputs: {', '.join(missing)}",
                missing=missing,
            )
    missing_price = [column for column in spec.required_price_inputs if column not in bundle.price]
    missing_indicators = [column for column in spec.required_materialized_indicators if column not in bundle.fields]
    missing_derived = [column for column in spec.required_materialized_derived if column not in bundle.fields]
    if missing_price and not spec.required_inputs_by_clock:
        raise InputPlanValidationError(
            "MISSING_MATERIALIZED_INPUT",
            f"{spec.family_key} missing required price inputs: {', '.join(missing_price)}",
            missing=missing_price,
        )
    if missing_indicators and not spec.required_inputs_by_clock:
        raise InputPlanValidationError(
            "MISSING_MATERIALIZED_INPUT",
            f"{spec.family_key} missing materialized indicators: {', '.join(missing_indicators)}",
            missing=missing_indicators,
        )
    if missing_derived and not spec.required_inputs_by_clock:
        failure = "MISSING_MTF_INPUT" if any(column.startswith("mtf_") for column in missing_derived) else "MISSING_MATERIALIZED_INPUT"
        raise InputPlanValidationError(
            failure,
            f"{failure}: {spec.family_key} missing derived inputs: {', '.join(missing_derived)}",
            missing=missing_derived,
        )
    for optional in spec.optional_indicator_plan:
        if optional.alias in bundle.price or optional.alias in bundle.fields:
            continue
        if optional.missing_policy in {"materialized_required", "fail_fast"}:
            raise InputPlanValidationError(
                "MISSING_MATERIALIZED_INPUT",
                f"{spec.family_key} missing optional-but-required indicator: {optional.alias}",
                missing=(optional.alias,),
            )
        if optional.source == "pandas_ta_direct_precompute":
            raise InputPlanValidationError(
                "EPHEMERAL_PROVIDER_NOT_ALLOWED",
                f"{spec.family_key} direct pandas-ta precompute is not allowed in the vectorbt hot path: {optional.alias}",
                missing=(optional.alias,),
            )
    return VectorBTIndicatorPlan(
        required_price_inputs=spec.required_price_inputs,
        materialized_indicators=spec.required_materialized_indicators,
        materialized_derived=spec.required_materialized_derived,
        optional_indicators=spec.optional_indicator_plan,
        inputs_by_clock=spec.required_inputs_by_clock,
    )


def _validate_bundle_shape(bundle: VectorBTInputBundle) -> None:
    expected_columns = list(bundle.instruments)
    for name, matrix in {**bundle.price, **bundle.fields}.items():
        if not matrix.index.equals(bundle.index):
            raise InputPlanValidationError(
                "SURFACE_SHAPE_MISMATCH",
                f"input `{name}` index is not aligned with execution index",
            )
        if list(matrix.columns) != expected_columns:
            raise InputPlanValidationError(
                "SURFACE_SHAPE_MISMATCH",
                f"input `{name}` columns are not aligned with instruments",
            )
    for timeframe, grouped in {**bundle.timeframe_price, **bundle.timeframe_fields}.items():
        for name, matrix in grouped.items():
            if list(matrix.columns) != expected_columns:
                raise InputPlanValidationError(
                    "SURFACE_SHAPE_MISMATCH",
                    f"input `{timeframe}:{name}` columns are not aligned with instruments",
                )


def _require_inputs(bundle: VectorBTInputBundle, spec: StrategyFamilySearchSpec) -> None:
    resolve_indicator_plan(bundle, spec)


def _field_array(bundle: VectorBTInputBundle, name: str) -> np.ndarray:
    return _numeric_frame(bundle.field(name)).to_numpy(dtype=float)


def _field_array_first(bundle: VectorBTInputBundle, names: Sequence[str]) -> np.ndarray:
    for name in names:
        if name in bundle.price or name in bundle.fields:
            return _field_array(bundle, name)
    raise KeyError(", ".join(names))


def _layer_timeframe(bundle: VectorBTInputBundle, layer: str) -> str:
    profile = bundle.metadata.get("clock_profile_contract", {})
    if not isinstance(profile, Mapping):
        return str(bundle.metadata.get("execution_tf", "15m"))
    key = "signal_tf" if layer == "signal" else f"{layer}_tf"
    return str(profile.get(key, bundle.metadata.get("execution_tf", "15m")))


def _native_field(bundle: VectorBTInputBundle, name: str, layer: str) -> pd.DataFrame:
    timeframe = _layer_timeframe(bundle, layer)
    try:
        return _numeric_frame(bundle.field_at(name, timeframe))
    except KeyError:
        return _numeric_frame(bundle.field(name))


def _has_native_field(bundle: VectorBTInputBundle, name: str, layer: str) -> bool:
    timeframe = _layer_timeframe(bundle, layer)
    return name in bundle.timeframe_price.get(timeframe, {}) or name in bundle.timeframe_fields.get(timeframe, {}) or name in bundle.price or name in bundle.fields


def _align_bool_frame_to_execution(bundle: VectorBTInputBundle, frame: pd.DataFrame) -> np.ndarray:
    if frame.index.equals(bundle.index):
        return frame.fillna(False).to_numpy(dtype=bool)
    return frame.reindex(bundle.index, method="ffill").fillna(False).to_numpy(dtype=bool)


def _align_bool_cube_to_execution(bundle: VectorBTInputBundle, values: np.ndarray, native_index: pd.Index) -> np.ndarray:
    if pd.Index(native_index).equals(bundle.index):
        return values.astype(bool)
    aligned: list[np.ndarray] = []
    for offset in range(values.shape[2]):
        frame = pd.DataFrame(values[:, :, offset], index=native_index, columns=bundle.instruments)
        aligned.append(frame.reindex(bundle.index, method="ffill").fillna(False).to_numpy(dtype=bool))
    return np.stack(aligned, axis=2)


def _param_array(param_rows: Sequence[Mapping[str, object]], name: str, default: float) -> np.ndarray:
    return np.asarray([float(row.get(name, default) if row.get(name, default) is not None else default) for row in param_rows], dtype=float)


def _nullable_param_array(param_rows: Sequence[Mapping[str, object]], name: str, default: float) -> np.ndarray:
    values: list[float] = []
    for row in param_rows:
        raw = row.get(name, default)
        values.append(default if raw is None else float(raw))
    return np.asarray(values, dtype=float)


def _percent_param_array(param_rows: Sequence[Mapping[str, object]], name: str, default: float) -> np.ndarray:
    values = _param_array(param_rows, name, default)
    return np.asarray([value / 100.0 if value > 1.0 else value for value in values], dtype=float)


def _param_texts(param_rows: Sequence[Mapping[str, object]], name: str, default: str) -> tuple[str, ...]:
    return tuple(str(row.get(name, default) if row.get(name, default) is not None else default) for row in param_rows)


def _parameter_space_default(
    parameter_space: Mapping[str, tuple[object, ...]],
    names: Sequence[str],
    default: float,
) -> float:
    for name in names:
        raw = parameter_space.get(name)
        if raw is None:
            continue
        value = raw[0] if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)) and raw else raw
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _rolling_confirm(signal: np.ndarray, confirm_bars: Sequence[int]) -> np.ndarray:
    if not confirm_bars:
        return signal
    result = np.zeros_like(signal, dtype=bool)
    for offset, bars in enumerate(confirm_bars):
        bars = max(1, int(bars))
        if bars <= 1:
            result[:, :, offset] = signal[:, :, offset]
            continue
        frame = pd.DataFrame(signal[:, :, offset].astype(int))
        result[:, :, offset] = (frame.rolling(window=bars, min_periods=bars).sum().to_numpy() >= bars)
    return result


def _reshape_surface(values: np.ndarray) -> np.ndarray:
    return np.transpose(values, (0, 2, 1)).reshape(values.shape[0], values.shape[2] * values.shape[1])


def _broadcast_stop(close: np.ndarray, atr: np.ndarray, param_values: np.ndarray) -> np.ndarray:
    safe_close = np.where(close == 0.0, np.nan, close)
    return np.nan_to_num((atr[:, :, None] * param_values[None, None, :]) / safe_close[:, :, None], nan=0.0)


@njit
def _active_state_entry_choice_nb(
    from_i: int,
    to_i: int,
    col: int,
    active_state: np.ndarray,
    shift_bars: int,
    temp_idx_arr: np.ndarray,
) -> np.ndarray:
    scan_from = from_i - shift_bars
    if scan_from < 0:
        scan_from = 0
    for index in range(scan_from, to_i):
        previous = False
        if index > 0:
            previous = active_state[index - 1, col]
        if active_state[index, col] and not previous:
            signal_index = index + shift_bars
            if signal_index < from_i:
                continue
            if signal_index >= to_i:
                break
            temp_idx_arr[0] = signal_index
            return temp_idx_arr[:1]
    return temp_idx_arr[:0]


@njit
def _active_state_exit_choice_nb(
    from_i: int,
    to_i: int,
    col: int,
    active_state: np.ndarray,
    shift_bars: int,
    temp_idx_arr: np.ndarray,
) -> np.ndarray:
    scan_from = from_i - shift_bars
    if scan_from < 0:
        scan_from = 0
    for index in range(scan_from, to_i):
        previous = False
        if index > 0:
            previous = active_state[index - 1, col]
        if not active_state[index, col] and previous:
            signal_index = index + shift_bars
            if signal_index < from_i:
                continue
            if signal_index >= to_i:
                break
            temp_idx_arr[0] = signal_index
            return temp_idx_arr[:1]
    return temp_idx_arr[:0]


_SIGNAL_FACTORY_CLASS: Any | None = None


def _signal_factory_class() -> Any:
    global _SIGNAL_FACTORY_CLASS
    if _SIGNAL_FACTORY_CLASS is None:
        _SIGNAL_FACTORY_CLASS = vbt.SignalFactory(
            mode="both",
            input_names=["active_state"],
        ).from_choice_func(
            entry_choice_func=_active_state_entry_choice_nb,
            exit_choice_func=_active_state_exit_choice_nb,
            entry_settings={"pass_inputs": ["active_state"], "pass_kwargs": ["temp_idx_arr"]},
            exit_settings={"pass_inputs": ["active_state"], "pass_kwargs": ["temp_idx_arr"]},
        )
    return _SIGNAL_FACTORY_CLASS


def _run_signal_factory_from_state(
    state: np.ndarray,
    *,
    shift_bars: int,
) -> tuple[np.ndarray, np.ndarray]:
    active_state = _reshape_surface(state.astype(bool))
    result = _signal_factory_class().run(
        active_state.shape,
        active_state,
        entry_args=(int(shift_bars),),
        exit_args=(int(shift_bars),),
        entry_kwargs={"wait": 1, "pick_first": True},
        exit_kwargs={"wait": 1, "pick_first": True},
        short_name="ta3000_vbt_signal",
    )
    return result.entries.to_numpy(dtype=bool), result.exits.to_numpy(dtype=bool)


def _surface_input_names(bundle: VectorBTInputBundle, spec: StrategyFamilySearchSpec) -> tuple[str, ...]:
    names: list[str] = []
    for name in (
        *spec.required_price_inputs,
        *spec.required_materialized_indicators,
        *spec.required_materialized_derived,
    ):
        if (name in bundle.price or name in bundle.fields) and name not in names:
            names.append(name)
    if "close" in bundle.price and "close" not in names:
        names.insert(0, "close")
    if "atr_14" in bundle.fields and "atr_14" not in names:
        names.append("atr_14")
    return tuple(names)


def _role_timeframes_from_spec(spec: StrategyFamilySearchSpec, bundle: VectorBTInputBundle | None = None) -> dict[str, str]:
    if spec.required_inputs_by_clock:
        return {
            str(layer): str(payload.get("timeframe", ""))
            for layer, payload in spec.required_inputs_by_clock.items()
            if isinstance(payload, Mapping)
        }
    execution_tf = str(bundle.metadata.get("execution_tf", "")) if bundle is not None else _execution_timeframe_from_profile(spec.clock_profile)
    return {"execution": execution_tf}


def resolve_mtf_signal_factory_inputs(bundle: VectorBTInputBundle, spec: StrategyFamilySearchSpec) -> MTFInputResolverResult:
    indicator_plan = resolve_indicator_plan(bundle, spec)
    input_names: list[str] = []
    if spec.required_inputs_by_clock:
        for layer, payload in spec.required_inputs_by_clock.items():
            if not isinstance(payload, Mapping):
                continue
            timeframe = str(payload.get("timeframe", "")).strip() or str(bundle.metadata.get("execution_tf", ""))
            for payload_key in ("price_inputs", "materialized_indicators", "materialized_derived"):
                for column in _payload_aliases(payload, payload_key):
                    bundle.field_at(column, timeframe, align_to_execution=True)
                    input_name = f"{layer}__{timeframe}__{column}"
                    if input_name not in input_names:
                        input_names.append(input_name)
    else:
        execution_tf = str(bundle.metadata.get("execution_tf", ""))
        for column in _surface_input_names(bundle, spec):
            bundle.field(column)
            input_name = f"execution__{execution_tf}__{column}"
            if input_name not in input_names:
                input_names.append(input_name)
    return MTFInputResolverResult(
        indicator_plan=indicator_plan,
        input_names=tuple(input_names),
        role_timeframes=_role_timeframes_from_spec(spec, bundle),
    )


def build_signal_surface(
    *,
    bundle: VectorBTInputBundle,
    spec: StrategyFamilySearchSpec,
    param_rows: Sequence[dict[str, object]],
    search_run_id: str,
    config: BacktestEngineConfig,
) -> VectorBTSignalSurfaceResult:
    input_resolver = resolve_mtf_signal_factory_inputs(bundle, spec)
    indicator_plan = input_resolver.indicator_plan
    if not param_rows:
        raise ValueError("signal surface requires at least one parameter row")
    param_hashes = tuple(param_hash(spec, row) for row in param_rows)
    surface_key = spec.signal_surface_key.removeprefix("vbt_surface.")
    columns = pd.MultiIndex.from_tuples(
        [
            (spec.family_key, surface_key, spec.template_key, param_id, instrument_id)
            for param_id in param_hashes
            for instrument_id in bundle.instruments
        ],
        names=("family_key", "surface_key", "template_key", "param_hash", "instrument_id"),
    )
    close = _field_array(bundle, "close")
    atr = _field_array(bundle, "atr_14") if "atr_14" in bundle.fields else np.ones_like(close)
    session_mask = _session_entry_mask(bundle.index, config.session_hours_utc)[:, None, None]
    diagnostics: dict[str, object] = {
        "surface_engine": "vectorbt.SignalFactory.from_choice_func",
        "state_builder": "ta3000.numpy_vectorized_state",
        "input_resolver": "mtf_input_resolver",
        "portfolio_engine": "vectorbt.Portfolio.from_signals",
        "surface_mode": spec.signal_surface_mode,
        "input_names": list(input_resolver.input_names),
        "role_timeframes": dict(input_resolver.role_timeframes),
        "param_count": len(param_rows),
        "instrument_count": len(bundle.instruments),
    }

    native_surface_builders = {
        "trend_mtf_pullback_v1": _mtf_pullback_state,
        "trend_movement_cross_v1": _trend_movement_cross_state,
        "channel_breakout_continuation_v1": _channel_breakout_continuation_state,
        "squeeze_release_v1": _squeeze_release_state,
        "range_vwap_band_reversion_v1": _range_vwap_band_reversion_state,
        "failed_breakout_reversal_v1": _failed_breakout_reversal_state,
        "divergence_reversal_v1": _divergence_reversal_state,
    }
    if spec.required_inputs_by_clock and surface_key in native_surface_builders:
        long_state, short_state = native_surface_builders[surface_key](bundle, param_rows, config)
        diagnostics["state_builder"] = "ta3000.mtf_state_builder"
        diagnostics["clock_profile"] = dict(spec.clock_profile)
        diagnostics["required_inputs_by_clock"] = {
            str(layer): dict(payload)
            for layer, payload in spec.required_inputs_by_clock.items()
        }
        diagnostics["event_alignment"] = "closed_layer_event_to_execution"
    elif surface_key in {"trend_movement_cross_v1", "channel_breakout_continuation_v1"}:
        state_builder = _trend_movement_cross_state if surface_key == "trend_movement_cross_v1" else _channel_breakout_continuation_state
        long_state, short_state = state_builder(bundle, param_rows, config)
        diagnostics["state_builder"] = "ta3000.vectorized_state_builder"
    elif surface_key in {"ma_cross_v1", "ma_cross"}:
        long_state, short_state = _ma_cross_state(bundle, param_rows, config)
    elif surface_key in {"breakout_v1", "breakout"}:
        long_state, short_state = _breakout_state(bundle, param_rows, config)
    elif surface_key in {"mean_reversion_v1", "mean_reversion"}:
        long_state, short_state = _mean_reversion_state(bundle, param_rows, config)
    elif surface_key in {"trend_mtf_pullback_v1", "mtf_pullback_v1", "mtf_pullback"}:
        long_state, short_state = _mtf_pullback_state(bundle, param_rows, config)
    elif surface_key in {"squeeze_release_v1", "squeeze_release"}:
        long_state, short_state = _squeeze_release_state(bundle, param_rows, config)
    elif surface_key in {"range_vwap_band_reversion_v1", "range_vwap_band_reversion"}:
        long_state, short_state = _range_vwap_band_reversion_state(bundle, param_rows, config)
    elif surface_key in {"failed_breakout_reversal_v1", "failed_breakout_reversal"}:
        long_state, short_state = _failed_breakout_reversal_state(bundle, param_rows, config)
    elif surface_key in {"divergence_reversal_v1", "divergence_reversal"}:
        long_state, short_state = _divergence_reversal_state(bundle, param_rows, config)
    elif surface_key == "trend_movement_cross_v1":
        long_state, short_state = _trend_movement_cross_state(bundle, param_rows, config)
    elif surface_key == "channel_breakout_continuation_v1":
        long_state, short_state = _channel_breakout_continuation_state(bundle, param_rows, config)
    else:
        raise ValueError(f"unsupported signal surface key: {spec.signal_surface_key}")

    long_state = long_state & session_mask
    short_state = short_state & session_mask & bool(config.allow_short)
    direction_mode = str(spec.execution_assumptions.get("direction_mode", "long_short"))
    if direction_mode == "long_only" or not config.allow_short:
        short_state = np.zeros_like(short_state, dtype=bool)
    elif direction_mode == "short_only":
        long_state = np.zeros_like(long_state, dtype=bool)
    entries, exits = _run_signal_factory_from_state(long_state, shift_bars=config.signal_shift_bars)
    short_entries, short_exits = _run_signal_factory_from_state(short_state, shift_bars=config.signal_shift_bars)
    stop_default = _parameter_space_default(spec.risk_parameter_space, ("stop_atr_mult", "stop_atr_multiple"), 1.0)
    target_default = _parameter_space_default(
        spec.risk_parameter_space,
        ("target_atr_mult", "atr_target_multiple"),
        2.0,
    )
    stop_values = _param_array(param_rows, "stop_atr_mult", _param_array(param_rows, "stop_atr_multiple", stop_default)[0])
    target_values = _param_array(
        param_rows,
        "target_atr_mult",
        _param_array(
            param_rows,
            "atr_target_multiple",
            _param_array(param_rows, "trail_atr_mult", target_default)[0],
        )[0],
    )
    sl_stop = _broadcast_stop(close, atr, stop_values)
    tp_stop = _broadcast_stop(close, atr, target_values)

    def _frame(values: np.ndarray, dtype: str | None = None) -> pd.DataFrame:
        data = _reshape_surface(values) if values.ndim == 3 else values
        frame = pd.DataFrame(data, index=bundle.index, columns=columns)
        if dtype is not None:
            return frame.astype(dtype)
        return frame

    surface_id = "VSURF-" + _stable_hash(f"{search_run_id}|{spec.family_key}|{','.join(param_hashes)}")
    parameter_index = pd.DataFrame(
        [
            {
                "param_hash": param_id,
                **dict(row),
            }
            for param_id, row in zip(param_hashes, param_rows, strict=True)
        ]
    ).set_index("param_hash", drop=False)
    return VectorBTSignalSurfaceResult(
        surface_id=surface_id,
        search_run_id=search_run_id,
        family_key=spec.family_key,
        surface_key=surface_key,
        template_key=spec.template_key,
        columns=columns,
        param_rows=tuple(dict(row) for row in param_rows),
        parameter_index=parameter_index,
        indicator_plan=indicator_plan,
        entries=_frame(entries, "bool"),
        exits=_frame(exits, "bool"),
        short_entries=_frame(short_entries, "bool"),
        short_exits=_frame(short_exits, "bool"),
        sl_stop=_frame(sl_stop),
        tp_stop=_frame(tp_stop),
        diagnostics=diagnostics,
    )


def _ma_cross_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    fast_windows = [int(row.get("fast_window", 10)) for row in param_rows]
    slow_windows = [int(row.get("slow_window", 20)) for row in param_rows]
    states_long: list[np.ndarray] = []
    states_short: list[np.ndarray] = []
    ema_20 = _field_array(bundle, "ema_20")
    ema_50 = _field_array(bundle, "ema_50")
    for fast_window, slow_window in zip(fast_windows, slow_windows, strict=True):
        fast = _field_array(bundle, f"ema_{fast_window}")
        slow = _field_array(bundle, f"ema_{slow_window}")
        states_long.append((fast > slow) & (ema_20 >= ema_50))
        states_short.append((fast < slow) & (ema_20 <= ema_50) & bool(config.allow_short))
    return np.stack(states_long, axis=2), np.stack(states_short, axis=2)


def _breakout_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    close = _numeric_frame(bundle.field("close"))
    high = _numeric_frame(bundle.field("high"))
    low = _numeric_frame(bundle.field("low"))
    atr = _numeric_frame(bundle.field("atr_14"))
    adx = _numeric_frame(bundle.field("adx_14"))
    states_long: list[np.ndarray] = []
    states_short: list[np.ndarray] = []
    for row in param_rows:
        window = int(row.get("breakout_window", 20))
        rolling_high = high.rolling(window=window, min_periods=window).max().shift(1)
        rolling_low = low.rolling(window=window, min_periods=window).min().shift(1)
        buffer = atr * float(row.get("entry_buffer_atr", 0.0))
        min_adx = float(row.get("min_adx", row.get("adx_min", 18.0)))
        states_long.append(((adx >= min_adx) & (close >= (rolling_high + buffer))).to_numpy(dtype=bool))
        states_short.append((((adx >= min_adx) & (close <= (rolling_low - buffer))).to_numpy(dtype=bool)) & bool(config.allow_short))
    return np.stack(states_long, axis=2), np.stack(states_short, axis=2)


def _mean_reversion_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    rsi = _field_array(bundle, "rsi_14")
    distance = _field_array(bundle, "distance_to_session_vwap")
    entry_rsi = _param_array(param_rows, "entry_rsi", 30.0)
    entry_distance = _param_array(param_rows, "entry_distance_atr", 0.75)
    long_state = (rsi[:, :, None] <= entry_rsi[None, None, :]) & (distance[:, :, None] <= -entry_distance[None, None, :])
    short_state = (rsi[:, :, None] >= (100.0 - entry_rsi[None, None, :])) & (distance[:, :, None] >= entry_distance[None, None, :])
    return long_state, short_state


def _mtf_pullback_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    regime_ema_20 = _native_field(bundle, "ema_20", "regime")
    regime_ema_50 = _native_field(bundle, "ema_50", "regime")
    signal_ema_20 = _native_field(bundle, "ema_20", "signal")
    signal_ema_50 = _native_field(bundle, "ema_50", "signal")
    signal_adx = _native_field(bundle, "adx_14", "signal")
    signal_rsi = _native_field(bundle, "rsi_14", "signal")
    trigger_close_slope = _native_field(bundle, "close_slope_20", "trigger")
    trigger_ema_slope = _native_field(bundle, "ema_20_slope_5", "trigger")
    distance_ema_20 = _native_field(bundle, "distance_to_ema_20_atr", "trigger")
    distance_ema_50 = _native_field(bundle, "distance_to_ema_50_atr", "trigger")

    adx_min = _param_array(param_rows, "adx_min", 18.0)
    slope_min = _param_array(param_rows, "slope_min", 0.0)
    pullback_min = _param_array(param_rows, "pullback_atr_min", 0.0)
    pullback_max = _param_array(param_rows, "pullback_atr_max", _param_array(param_rows, "pullback_depth", 0.8)[0])
    rsi_long = _param_array(param_rows, "rsi_reclaim_long", _param_array(param_rows, "min_htf_rsi", 50.0)[0])
    rsi_short = _param_array(param_rows, "rsi_reclaim_short", 50.0)
    confirm = [int(row.get("confirmation_bars", 1)) for row in param_rows]

    regime_up = _align_bool_frame_to_execution(bundle, regime_ema_20 > regime_ema_50)
    regime_down = _align_bool_frame_to_execution(bundle, regime_ema_20 < regime_ema_50)
    signal_up = (signal_ema_20 >= signal_ema_50).to_numpy(dtype=bool)
    signal_down = (signal_ema_20 <= signal_ema_50).to_numpy(dtype=bool)
    signal_adx_values = signal_adx.to_numpy(dtype=float)
    signal_rsi_values = signal_rsi.to_numpy(dtype=float)
    signal_long_native = (
        signal_up[:, :, None]
        & (signal_adx_values[:, :, None] >= adx_min[None, None, :])
        & (signal_rsi_values[:, :, None] >= rsi_long[None, None, :])
    )
    signal_short_native = (
        signal_down[:, :, None]
        & (signal_adx_values[:, :, None] >= adx_min[None, None, :])
        & (signal_rsi_values[:, :, None] <= rsi_short[None, None, :])
    )
    signal_long = _align_bool_cube_to_execution(bundle, signal_long_native, signal_adx.index)
    signal_short = _align_bool_cube_to_execution(bundle, signal_short_native, signal_adx.index)

    pullback_distance = np.minimum(
        np.abs(distance_ema_20.to_numpy(dtype=float)),
        np.abs(distance_ema_50.to_numpy(dtype=float)),
    )
    close_slope = trigger_close_slope.to_numpy(dtype=float)
    ema_slope = trigger_ema_slope.to_numpy(dtype=float)
    trigger_long_native = (
        (pullback_distance[:, :, None] >= pullback_min[None, None, :])
        & (pullback_distance[:, :, None] <= pullback_max[None, None, :])
        & (close_slope[:, :, None] >= slope_min[None, None, :])
        & (ema_slope[:, :, None] >= slope_min[None, None, :])
    )
    trigger_short_native = (
        (pullback_distance[:, :, None] >= pullback_min[None, None, :])
        & (pullback_distance[:, :, None] <= pullback_max[None, None, :])
        & (close_slope[:, :, None] <= -slope_min[None, None, :])
        & (ema_slope[:, :, None] <= -slope_min[None, None, :])
    )
    trigger_long = _align_bool_cube_to_execution(bundle, _rolling_confirm(trigger_long_native, confirm), trigger_close_slope.index)
    trigger_short = _align_bool_cube_to_execution(bundle, _rolling_confirm(trigger_short_native, confirm), trigger_close_slope.index)
    long_state = regime_up[:, :, None] & signal_long & trigger_long
    short_state = regime_down[:, :, None] & signal_short & trigger_short
    return long_state, short_state


def _squeeze_release_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    signal_bb_position = _native_field(bundle, "bb_position_20_2", "signal")
    signal_kc_position = _native_field(bundle, "kc_position_20_1_5", "signal")
    signal_bb_width = (
        _native_field(bundle, "bb_width_20_2", "signal")
        if _has_native_field(bundle, "bb_width_20_2", "signal")
        else _native_field(bundle, "natr_14", "signal")
        if _has_native_field(bundle, "natr_14", "signal")
        else pd.DataFrame(np.zeros((len(signal_bb_position.index), len(bundle.instruments))), index=signal_bb_position.index, columns=bundle.instruments)
    )
    trigger_bb_position = (
        _native_field(bundle, "bb_position_20_2", "trigger")
        if _has_native_field(bundle, "bb_position_20_2", "trigger")
        else signal_bb_position.reindex(bundle.index, method="ffill")
    )
    trigger_kc_position = (
        _native_field(bundle, "kc_position_20_1_5", "trigger")
        if _has_native_field(bundle, "kc_position_20_1_5", "trigger")
        else signal_kc_position.reindex(bundle.index, method="ffill")
    )
    high_cross = _native_field(bundle, "cross_close_rolling_high_20_code", "trigger")
    low_cross = _native_field(bundle, "cross_close_rolling_low_20_code", "trigger")
    slope = (
        _native_field(bundle, "close_slope_20", "trigger")
        if _has_native_field(bundle, "close_slope_20", "trigger")
        else pd.DataFrame(np.zeros((len(bundle.index), len(bundle.instruments))), index=bundle.index, columns=bundle.instruments)
    )
    volume_z = (
        _native_field(bundle, "volume_zscore_20", "trigger")
        if _has_native_field(bundle, "volume_zscore_20", "trigger")
        else pd.DataFrame(np.zeros((len(bundle.index), len(bundle.instruments))), index=bundle.index, columns=bundle.instruments)
    )
    width_limit = _percent_param_array(param_rows, "bb_width_percentile_max", 0.20)
    volume_z_min = _nullable_param_array(param_rows, "volume_z_min", -np.inf)
    slope_min = _param_array(param_rows, "slope_min", 0.0)
    direction_modes = _param_texts(param_rows, "release_direction_mode", "range_break")
    min_squeeze = [int(row.get("min_squeeze_bars", 3)) for row in param_rows]
    confirm = [int(row.get("release_confirmation", 1)) for row in param_rows]
    channel_mid = (
        (signal_bb_position >= 0.35)
        & (signal_bb_position <= 0.65)
        & (signal_kc_position >= 0.35)
        & (signal_kc_position <= 0.65)
    )
    setup_native = np.zeros((len(signal_bb_position.index), len(bundle.instruments), len(param_rows)), dtype=bool)
    channel_mid_values = channel_mid.to_numpy(dtype=bool)
    width_values = signal_bb_width.to_numpy(dtype=float)
    for offset, bars in enumerate(min_squeeze):
        squeeze = channel_mid_values & (width_values <= width_limit[offset])
        frame = pd.DataFrame(squeeze.astype(int))
        setup_native[:, :, offset] = (
            frame.rolling(window=max(1, bars), min_periods=max(1, bars)).sum().shift(1).fillna(0).to_numpy() >= bars
        )
    setup = _align_bool_cube_to_execution(bundle, setup_native, signal_bb_position.index)
    range_up = high_cross.to_numpy(dtype=float) > 0
    range_down = low_cross.to_numpy(dtype=float) < 0
    band_up = (trigger_bb_position.to_numpy(dtype=float) >= 0.8) & (trigger_kc_position.to_numpy(dtype=float) >= 0.65)
    band_down = (trigger_bb_position.to_numpy(dtype=float) <= 0.2) & (trigger_kc_position.to_numpy(dtype=float) <= 0.35)
    release_up = np.stack(
        [
            band_up if mode == "band_break" else (range_up | band_up if mode == "movement_confirmed" else range_up)
            for mode in direction_modes
        ],
        axis=2,
    )
    release_down = np.stack(
        [
            band_down if mode == "band_break" else (range_down | band_down if mode == "movement_confirmed" else range_down)
            for mode in direction_modes
        ],
        axis=2,
    )
    needs_movement = np.asarray([mode == "movement_confirmed" for mode in direction_modes], dtype=bool)
    slope_values = slope.to_numpy(dtype=float)
    volume_z_values = volume_z.to_numpy(dtype=float)
    movement_long = (~needs_movement[None, None, :]) | (slope_values[:, :, None] >= slope_min[None, None, :])
    movement_short = (~needs_movement[None, None, :]) | (slope_values[:, :, None] <= -slope_min[None, None, :])
    raw_long = (
        setup
        & release_up
        & movement_long
        & (volume_z_values[:, :, None] >= volume_z_min[None, None, :])
    )
    raw_short = (
        setup
        & release_down
        & movement_short
        & (volume_z_values[:, :, None] >= volume_z_min[None, None, :])
    )
    long_state = _rolling_confirm(raw_long, confirm)
    short_state = _rolling_confirm(raw_short, confirm)
    return long_state, short_state


def _trend_movement_cross_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    regime_ema_20 = _native_field(bundle, "ema_20", "regime")
    regime_ema_50 = _native_field(bundle, "ema_50", "regime")
    signal_ema_20 = _native_field(bundle, "ema_20", "signal")
    signal_ema_50 = _native_field(bundle, "ema_50", "signal")
    adx = _native_field(bundle, "adx_14", "signal")
    rsi = _native_field(bundle, "rsi_14", "signal")
    close_slope = _native_field(bundle, "close_slope_20", "trigger")
    ema_slope = (
        _native_field(bundle, "ema_20_slope_5", "trigger")
        if _has_native_field(bundle, "ema_20_slope_5", "trigger")
        else close_slope
    )
    roc = (
        _native_field(bundle, "roc_10_change_1", "trigger")
        if _has_native_field(bundle, "roc_10_change_1", "trigger")
        else close_slope
    )
    mom = _native_field(bundle, "mom_10_change_1", "trigger") if _has_native_field(bundle, "mom_10_change_1", "trigger") else roc
    zero_cross = pd.DataFrame(np.zeros_like(close_slope.to_numpy(dtype=float)), index=close_slope.index, columns=close_slope.columns)
    cross_frames = [
        _native_field(bundle, name, "trigger") if _has_native_field(bundle, name, "trigger") else zero_cross
        for name in (
            "cross_close_ema_20_code",
            "cross_close_sma_20_code",
            "macd_signal_cross_code",
            "ppo_signal_cross_code",
            "trix_signal_cross_code",
            "kst_signal_cross_code",
        )
    ]
    adx_min = _param_array(param_rows, "adx_min", 18.0)
    close_slope_min = _param_array(param_rows, "close_slope_min", _param_array(param_rows, "slope_min", 0.0)[0])
    ema_slope_min = _param_array(param_rows, "ema_slope_min", close_slope_min[0])
    roc_min = _param_array(param_rows, "roc_change_min", _param_array(param_rows, "roc_min", 0.0)[0])
    mom_min = _param_array(param_rows, "mom_change_min", 0.0)
    rsi_long = _param_array(param_rows, "rsi_min_long", _param_array(param_rows, "rsi_midline", 50.0)[0])
    rsi_short = _param_array(param_rows, "rsi_max_short", 50.0)
    require_cross = np.asarray([bool(row.get("require_cross_code", False)) for row in param_rows], dtype=bool)
    regime_up = _align_bool_frame_to_execution(bundle, regime_ema_20 > regime_ema_50)
    regime_down = _align_bool_frame_to_execution(bundle, regime_ema_20 < regime_ema_50)

    signal_up = (signal_ema_20 >= signal_ema_50).to_numpy(dtype=bool)
    signal_down = (signal_ema_20 <= signal_ema_50).to_numpy(dtype=bool)
    signal_adx = adx.to_numpy(dtype=float)
    signal_rsi = rsi.to_numpy(dtype=float)
    signal_long_native = (
        signal_up[:, :, None]
        & (signal_adx[:, :, None] >= adx_min[None, None, :])
        & (signal_rsi[:, :, None] >= rsi_long[None, None, :])
    )
    signal_short_native = (
        signal_down[:, :, None]
        & (signal_adx[:, :, None] >= adx_min[None, None, :])
        & (signal_rsi[:, :, None] <= rsi_short[None, None, :])
    )
    signal_long = _align_bool_cube_to_execution(bundle, signal_long_native, signal_ema_20.index)
    signal_short = _align_bool_cube_to_execution(bundle, signal_short_native, signal_ema_20.index)

    trigger_close_slope = close_slope.to_numpy(dtype=float)
    trigger_ema_slope = ema_slope.to_numpy(dtype=float)
    trigger_roc = roc.to_numpy(dtype=float)
    trigger_mom = mom.to_numpy(dtype=float)
    trigger_cross = np.maximum.reduce([frame.to_numpy(dtype=float) for frame in cross_frames])
    trigger_cross_short = np.minimum.reduce([frame.to_numpy(dtype=float) for frame in cross_frames])
    cross_long_ok = (~require_cross[None, None, :]) | (trigger_cross[:, :, None] > 0.0)
    cross_short_ok = (~require_cross[None, None, :]) | (trigger_cross_short[:, :, None] < 0.0)
    trigger_long_native = (
        (trigger_close_slope[:, :, None] >= close_slope_min[None, None, :])
        & (trigger_ema_slope[:, :, None] >= ema_slope_min[None, None, :])
        & (trigger_roc[:, :, None] >= roc_min[None, None, :])
        & (trigger_mom[:, :, None] >= mom_min[None, None, :])
        & cross_long_ok
    )
    trigger_short_native = (
        (trigger_close_slope[:, :, None] <= -close_slope_min[None, None, :])
        & (trigger_ema_slope[:, :, None] <= -ema_slope_min[None, None, :])
        & (trigger_roc[:, :, None] <= -roc_min[None, None, :])
        & (trigger_mom[:, :, None] <= -mom_min[None, None, :])
        & cross_short_ok
    )
    trigger_long = _align_bool_cube_to_execution(bundle, trigger_long_native, close_slope.index)
    trigger_short = _align_bool_cube_to_execution(bundle, trigger_short_native, close_slope.index)
    long_state = regime_up[:, :, None] & signal_long & trigger_long
    short_state = regime_down[:, :, None] & signal_short & trigger_short
    return long_state, short_state


def _channel_breakout_continuation_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    close = _native_field(bundle, "close", "execution")
    adx = _native_field(bundle, "adx_14", "signal")
    high20 = _native_field(bundle, "donchian_high_20", "signal")
    low20 = _native_field(bundle, "donchian_low_20", "signal")
    high55 = _native_field(bundle, "donchian_high_55", "signal")
    low55 = _native_field(bundle, "donchian_low_55", "signal")
    slope = _native_field(bundle, "close_slope_20", "trigger")
    roc = _native_field(bundle, "roc_10_change_1", "trigger") if _has_native_field(bundle, "roc_10_change_1", "trigger") else slope
    high_cross = (
        _native_field(bundle, "cross_close_rolling_high_20_code", "trigger")
        if _has_native_field(bundle, "cross_close_rolling_high_20_code", "trigger")
        else pd.DataFrame(np.zeros((len(slope.index), len(bundle.instruments))), index=slope.index, columns=bundle.instruments)
    )
    low_cross = (
        _native_field(bundle, "cross_close_rolling_low_20_code", "trigger")
        if _has_native_field(bundle, "cross_close_rolling_low_20_code", "trigger")
        else pd.DataFrame(np.zeros((len(slope.index), len(bundle.instruments))), index=slope.index, columns=bundle.instruments)
    )
    distance_high20 = _native_field(bundle, "distance_to_donchian_high_20_atr", "trigger")
    distance_low20 = _native_field(bundle, "distance_to_donchian_low_20_atr", "trigger")
    distance_high55 = _native_field(bundle, "distance_to_donchian_high_55_atr", "trigger")
    distance_low55 = _native_field(bundle, "distance_to_donchian_low_55_atr", "trigger")

    channel_variants = _param_texts(param_rows, "channel_variant", "20")
    adx_min = _param_array(param_rows, "adx_min", 18.0)
    slope_min = _param_array(param_rows, "slope_min", 0.0)
    max_distance = _param_array(param_rows, "max_distance_after_breakout_atr", 1.0)
    confirm = [int(row.get("breakout_confirm_bars", 1)) for row in param_rows]
    selected_high = np.stack([high55.to_numpy(dtype=float) if variant == "55" else high20.to_numpy(dtype=float) for variant in channel_variants], axis=2)
    selected_low = np.stack([low55.to_numpy(dtype=float) if variant == "55" else low20.to_numpy(dtype=float) for variant in channel_variants], axis=2)
    selected_distance_high = np.stack(
        [distance_high55.to_numpy(dtype=float) if variant == "55" else distance_high20.to_numpy(dtype=float) for variant in channel_variants],
        axis=2,
    )
    selected_distance_low = np.stack(
        [distance_low55.to_numpy(dtype=float) if variant == "55" else distance_low20.to_numpy(dtype=float) for variant in channel_variants],
        axis=2,
    )
    if _has_native_field(bundle, "ema_20", "regime") and _has_native_field(bundle, "ema_50", "regime"):
        regime_ema_20 = _native_field(bundle, "ema_20", "regime")
        regime_ema_50 = _native_field(bundle, "ema_50", "regime")
        regime_up = _align_bool_frame_to_execution(bundle, regime_ema_20 > regime_ema_50)
        regime_down = _align_bool_frame_to_execution(bundle, regime_ema_20 < regime_ema_50)
    else:
        regime_up = np.ones((len(bundle.index), len(bundle.instruments)), dtype=bool)
        regime_down = np.ones((len(bundle.index), len(bundle.instruments)), dtype=bool)
    if _has_native_field(bundle, "ema_20", "signal") and _has_native_field(bundle, "ema_50", "signal"):
        signal_ema_20 = _native_field(bundle, "ema_20", "signal")
        signal_ema_50 = _native_field(bundle, "ema_50", "signal")
        signal_trend_up = (signal_ema_20 >= signal_ema_50).to_numpy(dtype=bool)
        signal_trend_down = (signal_ema_20 <= signal_ema_50).to_numpy(dtype=bool)
    else:
        signal_trend_up = np.ones((len(adx.index), len(bundle.instruments)), dtype=bool)
        signal_trend_down = np.ones((len(adx.index), len(bundle.instruments)), dtype=bool)
    signal_adx = adx.to_numpy(dtype=float)
    signal_close_frame = _native_field(bundle, "close", "signal") if _has_native_field(bundle, "close", "signal") else close.reindex(signal_ema_20.index, method="ffill")
    signal_close = signal_close_frame.to_numpy(dtype=float)
    signal_long_native = (
        signal_trend_up[:, :, None]
        & (signal_adx[:, :, None] >= adx_min[None, None, :])
        & (signal_close[:, :, None] >= selected_high)
    )
    signal_short_native = (
        signal_trend_down[:, :, None]
        & (signal_adx[:, :, None] >= adx_min[None, None, :])
        & (signal_close[:, :, None] <= selected_low)
    )
    signal_long = _align_bool_cube_to_execution(bundle, signal_long_native, signal_ema_20.index)
    signal_short = _align_bool_cube_to_execution(bundle, signal_short_native, signal_ema_20.index)

    trigger_slope = slope.to_numpy(dtype=float)
    trigger_roc = roc.to_numpy(dtype=float)
    trigger_high_cross = high_cross.to_numpy(dtype=float)
    trigger_low_cross = low_cross.to_numpy(dtype=float)
    trigger_long_native = (
        (trigger_slope[:, :, None] >= slope_min[None, None, :])
        & (trigger_roc[:, :, None] >= 0.0)
        & (trigger_high_cross[:, :, None] >= 0.0)
        & (np.abs(selected_distance_high) <= max_distance[None, None, :])
    )
    trigger_short_native = (
        (trigger_slope[:, :, None] <= -slope_min[None, None, :])
        & (trigger_roc[:, :, None] <= 0.0)
        & (trigger_low_cross[:, :, None] <= 0.0)
        & (np.abs(selected_distance_low) <= max_distance[None, None, :])
    )
    trigger_long = _align_bool_cube_to_execution(bundle, _rolling_confirm(trigger_long_native, confirm), slope.index)
    trigger_short = _align_bool_cube_to_execution(bundle, _rolling_confirm(trigger_short_native, confirm), slope.index)
    long_state = regime_up[:, :, None] & signal_long & trigger_long
    short_state = regime_down[:, :, None] & signal_short & trigger_short
    return long_state, short_state


def _range_vwap_band_reversion_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    adx = _native_field(bundle, "adx_14", "signal")
    chop = _native_field(bundle, "chop_14", "signal")
    rsi = _native_field(bundle, "rsi_14", "trigger")
    bb_position = _native_field(bundle, "bb_position_20_2", "trigger")
    kc_position = _native_field(bundle, "kc_position_20_1_5", "trigger")
    rolling_position = _native_field(bundle, "rolling_position_20", "trigger")
    session_position = _native_field(bundle, "session_position", "trigger")
    slope = _native_field(bundle, "close_slope_20", "trigger")
    adx_max = _param_array(param_rows, "adx_max", 20.0)
    chop_min = _param_array(param_rows, "chop_min", 55.0)
    long_max = _param_array(param_rows, "entry_band_position_long_max", 0.10)
    short_min = _param_array(param_rows, "entry_band_position_short_min", 0.90)
    rsi_oversold = _param_array(param_rows, "rsi_oversold", 30.0)
    rsi_overbought = _param_array(param_rows, "rsi_overbought", 70.0)

    decision_native = (
        (adx.to_numpy(dtype=float)[:, :, None] <= adx_max[None, None, :])
        & (chop.to_numpy(dtype=float)[:, :, None] >= chop_min[None, None, :])
    )
    decision = _align_bool_cube_to_execution(bundle, decision_native, adx.index)
    positions = np.stack(
        [
            bb_position.to_numpy(dtype=float),
            kc_position.to_numpy(dtype=float),
            rolling_position.to_numpy(dtype=float),
            session_position.to_numpy(dtype=float),
        ],
        axis=2,
    )
    position_min = np.nanmin(positions, axis=2)
    position_max = np.nanmax(positions, axis=2)
    slope_values = slope.to_numpy(dtype=float)
    rsi_values = rsi.to_numpy(dtype=float)
    long_trigger = (
        (position_min[:, :, None] <= long_max[None, None, :])
        & (rsi_values[:, :, None] <= rsi_oversold[None, None, :])
        & (slope_values[:, :, None] <= 0.0)
    )
    short_trigger = (
        (position_max[:, :, None] >= short_min[None, None, :])
        & (rsi_values[:, :, None] >= rsi_overbought[None, None, :])
        & (slope_values[:, :, None] >= 0.0)
    )
    return decision & long_trigger, decision & short_trigger


def _recent_signal(signal: np.ndarray, windows: Sequence[int]) -> np.ndarray:
    result = np.zeros((signal.shape[0], signal.shape[1], len(windows)), dtype=bool)
    for offset, window in enumerate(windows):
        frame = pd.DataFrame(signal.astype(int))
        result[:, :, offset] = frame.rolling(window=max(1, int(window)), min_periods=1).max().shift(1).fillna(0).to_numpy() > 0
    return result


def _failed_breakout_reversal_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    adx = _native_field(bundle, "adx_14", "signal")
    high_cross = _native_field(bundle, "cross_close_rolling_high_20_code", "trigger")
    low_cross = _native_field(bundle, "cross_close_rolling_low_20_code", "trigger")
    rolling_position = _native_field(bundle, "rolling_position_20", "trigger")
    donchian_position = _native_field(bundle, "donchian_position_20", "trigger")
    slope = _native_field(bundle, "close_slope_20", "trigger")
    windows = [int(row.get("failure_window_bars", 2)) for row in param_rows]
    modes = _param_texts(param_rows, "reentry_confirm_mode", "close_back_inside")
    adx_max = _param_array(param_rows, "adx_max", 22.0)

    decision_native = adx.to_numpy(dtype=float)[:, :, None] <= adx_max[None, None, :]
    decision = _align_bool_cube_to_execution(bundle, decision_native, adx.index)
    recent_high_break = _recent_signal(high_cross.to_numpy(dtype=float) > 0, windows)
    recent_low_break = _recent_signal(low_cross.to_numpy(dtype=float) < 0, windows)
    position_mid = (
        (rolling_position.to_numpy(dtype=float) + donchian_position.to_numpy(dtype=float))
        / 2.0
    )
    slope_values = slope.to_numpy(dtype=float)
    needs_momentum = np.asarray([mode == "close_back_plus_momentum" for mode in modes], dtype=bool)
    long_confirm = (~needs_momentum[None, None, :]) | (slope_values[:, :, None] >= 0.0)
    short_confirm = (~needs_momentum[None, None, :]) | (slope_values[:, :, None] <= 0.0)
    long_state = decision & recent_low_break & (position_mid[:, :, None] >= 0.20) & long_confirm
    short_state = decision & recent_high_break & (position_mid[:, :, None] <= 0.80) & short_confirm
    return long_state, short_state


def _divergence_column_for_source(source: str) -> str:
    return {
        "rsi": "divergence_price_rsi_14_score",
        "macd_hist": "divergence_price_macd_hist_12_26_9_score",
        "ppo_hist": "divergence_price_ppo_hist_12_26_9_score",
        "tsi": "divergence_price_tsi_25_13_score",
        "mfi": "divergence_price_mfi_14_score",
        "cmf": "divergence_price_cmf_20_score",
        "obv": "divergence_price_obv_score",
        "oi_change": "divergence_price_oi_change_1_score",
    }.get(source, "divergence_price_rsi_14_score")


def _divergence_reversal_state(
    bundle: VectorBTInputBundle,
    param_rows: Sequence[Mapping[str, object]],
    config: BacktestEngineConfig,
) -> tuple[np.ndarray, np.ndarray]:
    del config
    sources = _param_texts(param_rows, "divergence_source", "rsi")
    min_scores = _param_array(param_rows, "divergence_min_abs_score", 0.5)
    long_max = _param_array(param_rows, "position_extreme_long_max", 0.10)
    short_min = _param_array(param_rows, "position_extreme_short_min", 0.90)
    modes = _param_texts(param_rows, "confirmation_mode", "slope_turn")
    scores = [
        _native_field(bundle, _divergence_column_for_source(source), "signal")
        for source in sources
    ]
    signal_index = scores[0].index if scores else bundle.index
    score_native = np.stack([frame.to_numpy(dtype=float) for frame in scores], axis=2)
    decision_long = _align_bool_cube_to_execution(bundle, score_native >= min_scores[None, None, :], signal_index)
    decision_short = _align_bool_cube_to_execution(bundle, score_native <= -min_scores[None, None, :], signal_index)
    rolling_position = _native_field(bundle, "rolling_position_20", "trigger")
    bb_position = _native_field(bundle, "bb_position_20_2", "trigger")
    slope = _native_field(bundle, "close_slope_20", "trigger")
    position_low = np.minimum(rolling_position.to_numpy(dtype=float), bb_position.to_numpy(dtype=float))
    position_high = np.maximum(rolling_position.to_numpy(dtype=float), bb_position.to_numpy(dtype=float))
    slope_values = slope.to_numpy(dtype=float)
    cross_back_inside = np.asarray([mode == "cross_back_inside" for mode in modes], dtype=bool)
    oscillator_reclaim = np.asarray([mode == "oscillator_reclaim" for mode in modes], dtype=bool)
    long_confirm = (
        (slope_values[:, :, None] >= 0.0)
        | (cross_back_inside[None, None, :] & (position_low[:, :, None] > long_max[None, None, :]))
        | oscillator_reclaim[None, None, :]
    )
    short_confirm = (
        (slope_values[:, :, None] <= 0.0)
        | (cross_back_inside[None, None, :] & (position_high[:, :, None] < short_min[None, None, :]))
        | oscillator_reclaim[None, None, :]
    )
    long_state = decision_long & (position_low[:, :, None] <= long_max[None, None, :]) & long_confirm
    short_state = decision_short & (position_high[:, :, None] >= short_min[None, None, :]) & short_confirm
    return long_state, short_state


def _legacy_signal_payload(
    frame: pd.DataFrame,
    strategy_spec: StrategySpec,
    params: Mapping[str, object],
    config: BacktestEngineConfig,
) -> dict[str, pd.Series]:
    local_frame = frame.copy()
    if "ts" not in local_frame.columns:
        local_frame["ts"] = pd.to_datetime(local_frame.index, utc=True).astype(str)
    local_frame.index = pd.to_datetime(local_frame["ts"], utc=True)
    bundle = build_input_bundle(
        (
            ResearchSeriesFrame(
                contract_id="TEST",
                instrument_id="TEST",
                timeframe="15m",
                frame=local_frame,
            ),
        ),
        dataset_version="test",
        indicator_set_version="test",
        derived_indicator_set_version="test",
        clock_profile="short_swing_1h_v1",
    )
    surface = build_signal_surface(
        bundle=bundle,
        spec=strategy_spec_to_search_spec(strategy_spec),
        param_rows=(dict(params),),
        search_run_id="TEST-SURFACE",
        config=config,
    )
    entries = surface.entries.iloc[:, 0]
    exits = surface.exits.iloc[:, 0]
    short_entries = surface.short_entries.iloc[:, 0]
    short_exits = surface.short_exits.iloc[:, 0]
    return {
        "entries": entries,
        "exits": exits,
        "short_entries": short_entries,
        "short_exits": short_exits,
        "exit_signal": exits | short_exits,
        "sl_stop": surface.sl_stop.iloc[:, 0],
        "tp_stop": surface.tp_stop.iloc[:, 0],
    }


def _ma_cross_signals(
    frame: pd.DataFrame,
    strategy_spec: StrategySpec,
    params: Mapping[str, object],
    config: BacktestEngineConfig,
) -> dict[str, pd.Series]:
    return _legacy_signal_payload(frame, strategy_spec, params, config)


def _breakout_signals(
    frame: pd.DataFrame,
    strategy_spec: StrategySpec,
    params: Mapping[str, object],
    config: BacktestEngineConfig,
) -> dict[str, pd.Series]:
    return _legacy_signal_payload(frame, strategy_spec, params, config)


def _squeeze_release_signals(
    frame: pd.DataFrame,
    strategy_spec: StrategySpec,
    params: Mapping[str, object],
    config: BacktestEngineConfig,
) -> dict[str, pd.Series]:
    return _legacy_signal_payload(frame, strategy_spec, params, config)


def _expanded_close(bundle: VectorBTInputBundle, columns: pd.MultiIndex) -> pd.DataFrame:
    close = _numeric_frame(bundle.field("close"))
    param_count = len(columns.get_level_values("param_hash").unique())
    data = np.concatenate([close.to_numpy(dtype=float) for _ in range(param_count)], axis=1)
    return pd.DataFrame(data, index=bundle.index, columns=columns)


def run_surface_portfolio(
    *,
    bundle: VectorBTInputBundle,
    surface: VectorBTSignalSurfaceResult,
    config: BacktestEngineConfig,
) -> vbt.Portfolio:
    fees = config.fees_bps / 10_000.0
    slippage = config.slippage_bps / 10_000.0
    close = _expanded_close(bundle, surface.columns)
    timeframe = str(bundle.metadata["execution_tf"])
    return vbt.Portfolio.from_signals(
        close,
        surface.entries,
        surface.exits,
        short_entries=surface.short_entries,
        short_exits=surface.short_exits,
        size=config.position_size,
        size_type=enums.SizeType.Amount,
        fees=fees,
        slippage=slippage,
        sl_stop=surface.sl_stop,
        tp_stop=surface.tp_stop,
        init_cash=config.initial_cash,
        cash_sharing=False,
        freq=_timeframe_freq(timeframe),
    )


def _metric_series(columns: pd.MultiIndex, value: object, default: float = 0.0) -> pd.Series:
    if isinstance(value, pd.DataFrame):
        raw = value.mean(axis=0)
    elif isinstance(value, pd.Series):
        raw = value
    else:
        raw = pd.Series(float(value) if value is not None and not pd.isna(value) else default, index=columns)
    result = pd.to_numeric(pd.Series(raw), errors="coerce").reindex(columns).fillna(default)
    return result.astype(float)


def _safe_metric(columns: pd.MultiIndex, callback: Any, default: float = 0.0) -> pd.Series:
    try:
        return _metric_series(columns, callback(), default=default)
    except Exception:
        return pd.Series(default, index=columns, dtype=float)


def _records_by_col(records: pd.DataFrame) -> dict[int, list[dict[str, object]]]:
    grouped: dict[int, list[dict[str, object]]] = {}
    if records.empty or "col" not in records.columns:
        return grouped
    for _, record in records.iterrows():
        grouped.setdefault(int(record["col"]), []).append(record.to_dict())
    return grouped


def _orders_fee_by_col(records: pd.DataFrame, columns: pd.MultiIndex) -> pd.Series:
    fees = pd.Series(0.0, index=columns, dtype=float)
    if records.empty or "col" not in records.columns or "fees" not in records.columns:
        return fees
    for _, record in records.iterrows():
        col = int(record["col"])
        if 0 <= col < len(columns):
            fees.iloc[col] += float(record["fees"])
    return fees


def _run_id(
    *,
    search_run_id: str,
    column: tuple[object, ...],
    window_id: str,
    contract_id: str,
    instrument_id: str,
    timeframe: str,
) -> str:
    return "BTRUN-" + _stable_hash(
        f"{search_run_id}|{window_id}|{contract_id}|{instrument_id}|{timeframe}|{column}"
    )


def _scalar_metric(series: pd.Series, column: tuple[object, ...]) -> float:
    value = series.loc[column] if column in series.index else 0.0
    return float(value) if value is not None and not pd.isna(value) else 0.0


def collect_surface_rows(
    *,
    portfolio: vbt.Portfolio,
    bundle: VectorBTInputBundle,
    surface: VectorBTSignalSurfaceResult,
    spec: StrategyFamilySearchSpec,
    param_lookup: Mapping[str, dict[str, object]],
    backtest_batch_id: str,
    campaign_run_id: str,
    strategy_space_id: str,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    window_id: str,
) -> dict[str, list[dict[str, object]]]:
    columns = surface.columns
    created_at = _created_at()
    total_return = _safe_metric(columns, portfolio.total_return)
    sharpe = _safe_metric(columns, portfolio.sharpe_ratio)
    sortino = _safe_metric(columns, portfolio.sortino_ratio)
    calmar = _safe_metric(columns, portfolio.calmar_ratio)
    max_drawdown = _safe_metric(columns, portfolio.max_drawdown)
    trade_count = _safe_metric(columns, portfolio.trades.count).astype(int)
    profit_factor = _safe_metric(columns, portfolio.trades.profit_factor)
    win_rate = _safe_metric(columns, portfolio.trades.win_rate)
    expectancy = _safe_metric(columns, portfolio.trades.expectancy)
    order_count = _safe_metric(columns, portfolio.orders.count).astype(int)
    fees_paid = _orders_fee_by_col(portfolio.orders.records, columns)
    trade_records = _records_by_col(portfolio.trades.records)
    order_records = _records_by_col(portfolio.orders.records)
    drawdown_records = _records_by_col(portfolio.drawdowns.records)
    run_rows: list[dict[str, object]] = []
    stat_rows: list[dict[str, object]] = []
    param_result_rows: list[dict[str, object]] = []
    gate_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    order_rows: list[dict[str, object]] = []
    drawdown_rows: list[dict[str, object]] = []
    index = list(bundle.index)
    periods = len(index)
    timeframe = str(bundle.metadata["execution_tf"])

    for col_index, column in enumerate(columns):
        family_key, surface_key, template_key, param_id, instrument_id = (str(item) for item in column)
        params = dict(param_lookup[param_id])
        contract_id = _contract_id_for_instrument(bundle, instrument_id)
        run_id = _run_id(
            search_run_id=surface.search_run_id,
            column=column,
            window_id=window_id,
            contract_id=contract_id,
            instrument_id=instrument_id,
            timeframe=timeframe,
        )
        row_seed = {
            "backtest_run_id": run_id,
            "search_run_id": surface.search_run_id,
            "backtest_batch_id": backtest_batch_id,
            "campaign_run_id": campaign_run_id,
            "strategy_space_id": strategy_space_id,
            "strategy_instance_id": f"unpromoted_param_{param_id}",
            "strategy_template_id": template_key,
            "family_id": "sfam_" + _stable_hash(family_key),
            "family_key": family_key,
            "template_key": template_key,
            "strategy_version_label": spec.strategy_version_label,
            "dataset_version": dataset_version,
            "indicator_set_version": indicator_set_version,
            "derived_indicator_set_version": derived_indicator_set_version,
            "contract_id": contract_id,
            "instrument_id": instrument_id,
            "timeframe": timeframe,
            "clock_profile": str(bundle.metadata["clock_profile"]),
            "window_id": window_id,
            "param_hash": param_id,
            "params_hash": param_id,
            "parameter_values_json": params,
            "params_json": params,
            "execution_mode": "from_signals",
            "engine_name": "vectorbt",
            "row_count": periods,
            "trade_count": int(_scalar_metric(trade_count, column)),
            "status": "success",
            "started_at": created_at,
            "finished_at": created_at,
            "stop_ref": 0.0,
            "target_ref": 0.0,
        }
        run_rows.append(row_seed)
        total = _scalar_metric(total_return, column)
        trades = int(_scalar_metric(trade_count, column))
        fees = _scalar_metric(fees_paid, column)
        stat_row = {
            "backtest_run_id": run_id,
            "search_run_id": surface.search_run_id,
            "campaign_run_id": campaign_run_id,
            "strategy_instance_id": row_seed["strategy_instance_id"],
            "strategy_template_id": template_key,
            "family_id": row_seed["family_id"],
            "family_key": family_key,
            "template_key": template_key,
            "strategy_version_label": spec.strategy_version_label,
            "dataset_version": dataset_version,
            "contract_id": contract_id,
            "instrument_id": instrument_id,
            "timeframe": timeframe,
            "window_id": window_id,
            "param_hash": param_id,
            "params_hash": param_id,
            "total_return": total,
            "annualized_return": _annualized_return(total, periods=periods, timeframe=timeframe),
            "sharpe": _scalar_metric(sharpe, column),
            "sortino": _scalar_metric(sortino, column),
            "calmar": _scalar_metric(calmar, column),
            "max_drawdown": abs(_scalar_metric(max_drawdown, column)),
            "profit_factor": _scalar_metric(profit_factor, column),
            "win_rate": _scalar_metric(win_rate, column),
            "expectancy": _scalar_metric(expectancy, column),
            "avg_trade": _scalar_metric(expectancy, column),
            "avg_holding_bars": 0.0,
            "turnover": float(_scalar_metric(order_count, column)) / max(float(periods), 1.0),
            "exposure": 0.0,
            "commission_total": fees,
            "slippage_total": 0.0,
            "trade_count": trades,
            "status": "success",
            "created_at": created_at,
        }
        stat_rows.append(stat_row)
        param_result_rows.append(
            {
                "search_run_id": surface.search_run_id,
                "param_hash": param_id,
                "family_key": family_key,
                "template_key": template_key,
                "clock_profile": str(bundle.metadata["clock_profile"]),
                "instrument_id": instrument_id,
                "fold_id": window_id,
                "params_json": params,
                "indicator_plan_hash": surface.indicator_plan.plan_hash,
                "net_pnl": total * 100.0,
                "sharpe": stat_row["sharpe"],
                "sortino": stat_row["sortino"],
                "calmar": stat_row["calmar"],
                "max_drawdown": stat_row["max_drawdown"],
                "profit_factor": stat_row["profit_factor"],
                "win_rate": stat_row["win_rate"],
                "avg_trade": stat_row["avg_trade"],
                "trade_count": trades,
                "turnover": stat_row["turnover"],
                "avg_holding_minutes": 0.0,
                "fees_paid": fees,
                "slippage_paid": 0.0,
                "exposure_avg": 0.0,
                "stress_label": "base",
                "created_at": created_at,
            }
        )
        gate_snapshot = {
            "trade_count": trades,
            "total_return": total,
            "max_drawdown": stat_row["max_drawdown"],
        }
        for gate_name, passed, failure_code in (
            ("spec_valid", True, ""),
            ("input_bundle_valid", True, ""),
            ("indicator_plan_valid", True, ""),
            ("surface_valid", True, ""),
            ("activity", trades > 0, "TOO_FEW_TRADES"),
            ("backtest_valid", True, ""),
            ("research_survivor", trades > 0, "TOO_FEW_TRADES"),
            ("family_winner", trades > 0, "TOO_FEW_TRADES"),
            ("strategy_candidate", trades > 0, "TOO_FEW_TRADES"),
        ):
            gate_rows.append(
                {
                    "search_run_id": surface.search_run_id,
                    "param_hash": param_id,
                    "gate_name": gate_name,
                    "passed": int(bool(passed)),
                    "failure_code": "" if passed else failure_code,
                    "failure_reason": "" if passed else failure_code,
                    "metric_snapshot_json": gate_snapshot,
                    "created_at": created_at,
                }
            )
        trade_rows.extend(
            _trade_rows(
                records=trade_records.get(col_index, []),
                run_row=row_seed,
                index=index,
            )
        )
        order_rows.extend(
            _order_rows(
                records=order_records.get(col_index, []),
                run_row=row_seed,
                index=index,
            )
        )
        drawdown_rows.extend(
            _drawdown_rows(
                records=drawdown_records.get(col_index, []),
                run_row=row_seed,
                index=index,
            )
        )
    return {
        "run_rows": run_rows,
        "stat_rows": stat_rows,
        "param_result_rows": param_result_rows,
        "gate_rows": gate_rows,
        "trade_rows": trade_rows,
        "order_rows": order_rows,
        "drawdown_rows": drawdown_rows,
    }


def _contract_id_for_instrument(bundle: VectorBTInputBundle, instrument_id: str) -> str:
    raw = bundle.metadata.get("contract_ids", {})
    if isinstance(raw, Mapping):
        return str(raw.get(instrument_id, instrument_id))
    return instrument_id


def _trade_rows(*, records: Sequence[dict[str, object]], run_row: Mapping[str, object], index: Sequence[object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in records:
        entry_idx = int(record.get("entry_idx", 0))
        exit_idx = int(record.get("exit_idx", entry_idx))
        entry_ts = pd.Timestamp(index[min(max(entry_idx, 0), len(index) - 1)]).isoformat().replace("+00:00", "Z")
        exit_ts = pd.Timestamp(index[min(max(exit_idx, 0), len(index) - 1)]).isoformat().replace("+00:00", "Z")
        direction = "long" if int(record.get("direction", 0)) == 0 else "short"
        status = "closed" if int(record.get("status", 1)) == 1 else "open"
        entry_fees = float(record.get("entry_fees", 0.0) or 0.0)
        exit_fees = float(record.get("exit_fees", 0.0) or 0.0)
        pnl = float(record.get("pnl", 0.0) or 0.0)
        rows.append(
            {
                "backtest_run_id": run_row["backtest_run_id"],
                "campaign_run_id": run_row["campaign_run_id"],
                "strategy_instance_id": run_row["strategy_instance_id"],
                "strategy_template_id": run_row["strategy_template_id"],
                "family_id": run_row["family_id"],
                "family_key": run_row["family_key"],
                "contract_id": run_row["contract_id"],
                "instrument_id": run_row["instrument_id"],
                "timeframe": run_row["timeframe"],
                "window_id": run_row["window_id"],
                "param_hash": run_row["param_hash"],
                "trade_id": f"{run_row['backtest_run_id']}-TRD-{int(record.get('id', 0)):04d}",
                "side": direction,
                "status": status,
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_price": float(record.get("entry_price", 0.0) or 0.0),
                "exit_price": float(record.get("exit_price", 0.0) or 0.0),
                "qty": abs(float(record.get("size", 0.0) or 0.0)),
                "gross_pnl": pnl,
                "net_pnl": pnl - (entry_fees + exit_fees),
                "commission": entry_fees + exit_fees,
                "slippage": 0.0,
                "holding_bars": max(0, exit_idx - entry_idx),
                "stop_ref": run_row["stop_ref"],
                "target_ref": run_row["target_ref"],
            }
        )
    return rows


def _order_rows(*, records: Sequence[dict[str, object]], run_row: Mapping[str, object], index: Sequence[object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in records:
        bar_index = int(record.get("idx", 0))
        ts = pd.Timestamp(index[min(max(bar_index, 0), len(index) - 1)]).isoformat().replace("+00:00", "Z")
        action = "buy" if int(record.get("side", 0)) == 0 else "sell"
        price = float(record.get("price", 0.0) or 0.0)
        size = float(record.get("size", 0.0) or 0.0)
        rows.append(
            {
                "backtest_run_id": run_row["backtest_run_id"],
                "campaign_run_id": run_row["campaign_run_id"],
                "strategy_instance_id": run_row["strategy_instance_id"],
                "family_key": run_row["family_key"],
                "contract_id": run_row["contract_id"],
                "instrument_id": run_row["instrument_id"],
                "timeframe": run_row["timeframe"],
                "window_id": run_row["window_id"],
                "param_hash": run_row["param_hash"],
                "order_id": f"{run_row['backtest_run_id']}-ORD-{int(record.get('id', 0)):04d}",
                "ts": ts,
                "side": action,
                "order_type": "market",
                "price": price,
                "qty": abs(size),
                "fill_price": price,
                "fill_qty": abs(size),
                "commission": float(record.get("fees", 0.0) or 0.0),
                "slippage": 0.0,
                "status": "filled",
            }
        )
    return rows


def _drawdown_rows(*, records: Sequence[dict[str, object]], run_row: Mapping[str, object], index: Sequence[object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in records:
        valley_idx = int(record.get("valley_idx", 0))
        peak_val = float(record.get("peak_val", 0.0) or 0.0)
        valley_val = float(record.get("valley_val", 0.0) or 0.0)
        end_val = float(record.get("end_val", valley_val) or valley_val)
        drawdown_pct = ((peak_val - valley_val) / peak_val) if peak_val > 0.0 else 0.0
        status = "active" if end_val < peak_val else "recovered"
        rows.append(
            {
                "backtest_run_id": run_row["backtest_run_id"],
                "campaign_run_id": run_row["campaign_run_id"],
                "strategy_instance_id": run_row["strategy_instance_id"],
                "family_key": run_row["family_key"],
                "timeframe": run_row["timeframe"],
                "ts": pd.Timestamp(index[min(max(valley_idx, 0), len(index) - 1)]).isoformat().replace("+00:00", "Z"),
                "equity": valley_val,
                "drawdown": peak_val - valley_val,
                "drawdown_pct": drawdown_pct,
                "peak_equity": peak_val,
                "window_id": run_row["window_id"],
                "param_hash": run_row["param_hash"],
                "status_code": int(record.get("status", 0) or 0),
                "status": status,
            }
        )
    return rows


def _optimizer_engine(optimizer_policy: Mapping[str, object] | None) -> str:
    if optimizer_policy is None:
        return "grid"
    return str(optimizer_policy.get("engine", "grid"))


def _optuna_ranking_policy(optimizer_policy: Mapping[str, object]) -> RankingPolicy:
    defaults = default_ranking_policy()
    raw_policy = optimizer_policy.get("ranking_policy", {})
    source = raw_policy if isinstance(raw_policy, Mapping) and raw_policy else optimizer_policy
    raw_metric_order = source.get("metric_order", defaults.metric_order)
    if isinstance(raw_metric_order, str):
        metric_order = tuple(item.strip() for item in raw_metric_order.split(",") if item.strip())
    elif isinstance(raw_metric_order, Iterable):
        metric_order = tuple(str(item) for item in raw_metric_order if str(item).strip())
    else:
        metric_order = defaults.metric_order
    return RankingPolicy(
        policy_id=str(source.get("policy_id", defaults.policy_id)),
        metric_order=metric_order or defaults.metric_order,
        require_out_of_sample_pass=bool(source.get("require_out_of_sample_pass", defaults.require_out_of_sample_pass)),
        min_trade_count=int(source.get("min_trade_count", defaults.min_trade_count) or defaults.min_trade_count),
        min_fold_count=int(source.get("min_fold_count", defaults.min_fold_count) or defaults.min_fold_count),
        max_drawdown_cap=float(source.get("max_drawdown_cap", defaults.max_drawdown_cap) or defaults.max_drawdown_cap),
        min_positive_fold_ratio=float(
            source.get("min_positive_fold_ratio", defaults.min_positive_fold_ratio)
            if source.get("min_positive_fold_ratio", None) is not None
            else defaults.min_positive_fold_ratio
        ),
        stress_slippage_bps=float(source.get("stress_slippage_bps", defaults.stress_slippage_bps)),
        min_parameter_stability=float(source.get("min_parameter_stability", defaults.min_parameter_stability)),
        min_slippage_score=float(source.get("min_slippage_score", defaults.min_slippage_score)),
    )


def _ranking_policy_payload(policy: RankingPolicy) -> dict[str, object]:
    return {
        "policy_id": policy.policy_id,
        "metric_order": list(policy.metric_order),
        "require_out_of_sample_pass": policy.require_out_of_sample_pass,
        "min_trade_count": policy.min_trade_count,
        "min_fold_count": policy.min_fold_count,
        "max_drawdown_cap": policy.max_drawdown_cap,
        "min_positive_fold_ratio": policy.min_positive_fold_ratio,
        "stress_slippage_bps": policy.stress_slippage_bps,
        "min_parameter_stability": policy.min_parameter_stability,
        "min_slippage_score": policy.min_slippage_score,
    }


def _resolved_optuna_policy(optimizer_policy: Mapping[str, object], spec: StrategyFamilySearchSpec) -> dict[str, object]:
    n_trials = int(optimizer_policy.get("n_trials", 0) or 0)
    max_neighborhood_trials = int(optimizer_policy.get("max_neighborhood_trials", 64) or 0)
    ranking_policy = _optuna_ranking_policy(optimizer_policy)
    if n_trials <= 0:
        raise ValueError("strategy_space.optimizer.n_trials must be positive for optuna")
    if n_trials > spec.max_parameter_combinations:
        raise ValueError(
            "strategy_space.optimizer trial budget exceeds max_parameter_combinations: "
            f"{n_trials} > {spec.max_parameter_combinations}"
        )
    return {
        "engine": "optuna",
        "sampler": str(optimizer_policy.get("sampler", "tpe")),
        "seed": int(optimizer_policy.get("seed", 0) or 0),
        "objective": str(optimizer_policy.get("objective", "robust_oos_trial_v1")),
        "direction": str(optimizer_policy.get("direction", "maximize")),
        "n_trials": n_trials,
        "top_k": int(optimizer_policy.get("top_k", 8) or 8),
        "radius": int(optimizer_policy.get("radius", 1) or 0),
        "max_neighborhood_trials": max_neighborhood_trials,
        "selection_owner": "optuna.study",
        "constraints_func": "ta3000.robust_oos_trial_constraints",
        "ranking_policy": _ranking_policy_payload(ranking_policy),
    }


def _finite_float(value: object, default: float = 0.0) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return default
    return resolved if np.isfinite(resolved) else default


def _clip_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _optimizer_objective(
    param_rows: Sequence[dict[str, object]],
    *,
    trade_rows: Sequence[dict[str, object]] = (),
    policy: RankingPolicy | None = None,
) -> tuple[float, dict[str, object]]:
    scored = score_optimizer_trial(
        param_rows=[dict(row) for row in param_rows],
        trade_rows=[dict(row) for row in trade_rows],
        policy=policy,
    )
    value = float(scored["value"])
    components = {key: value for key, value in scored.items() if key != "value"}
    return value, components


_TRIAL_CLOCK_PARAM_KEYS = ("regime_tf", "signal_tf", "trigger_tf", "execution_tf")
_CLOCK_LAYER_PARAM_KEYS = {
    "regime": "regime_tf",
    "signal": "signal_tf",
    "trigger": "trigger_tf",
    "execution": "execution_tf",
}


def _effective_search_spec_for_params(spec: StrategyFamilySearchSpec, params: Mapping[str, object]) -> StrategyFamilySearchSpec:
    if not any(params.get(key) for key in _TRIAL_CLOCK_PARAM_KEYS):
        return spec
    payload = spec.to_dict()
    clock_profile = dict(spec.clock_profile)
    for key in _TRIAL_CLOCK_PARAM_KEYS:
        value = params.get(key)
        if value is not None and str(value).strip():
            clock_profile[key] = str(value).strip()
    required_inputs_by_clock: dict[str, dict[str, object]] = {}
    for layer, layer_payload in spec.required_inputs_by_clock.items():
        resolved = dict(layer_payload)
        clock_key = _CLOCK_LAYER_PARAM_KEYS.get(str(layer))
        if clock_key is not None and clock_profile.get(clock_key):
            resolved["timeframe"] = str(clock_profile[clock_key])
        required_inputs_by_clock[str(layer)] = resolved
    payload["clock_profile"] = clock_profile
    payload["required_inputs_by_clock"] = required_inputs_by_clock
    return StrategyFamilySearchSpec.from_dict(payload)


def _optimizer_provenance_components(
    *,
    row: Mapping[str, object],
    spec: StrategyFamilySearchSpec,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    components: dict[str, object] = dict(extra or {})
    components.update(
        {
            "optimizer_engine": "optuna",
            "signal_generator": "vectorbt.SignalFactory.from_choice_func",
            "input_resolver": "mtf_input_resolver",
            "portfolio_engine": "vectorbt.Portfolio.from_signals",
            "role_timeframes": _role_timeframes_from_spec(spec),
        }
    )
    if row.get("mode") is not None:
        components["mode"] = str(row["mode"])
    return components


def _suggest_optuna_row(trial: Any, spec: StrategyFamilySearchSpec) -> dict[str, object]:
    row: dict[str, object] = {}
    for name, values in sorted(spec.parameter_space.items(), key=lambda item: item[0]):
        choices = list(values)
        if choices:
            row[name] = trial.suggest_categorical(name, choices)
    return row


def _sort_jsonable_values(values: Iterable[object]) -> list[object]:
    return sorted(
        values,
        key=lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, default=str),
    )


def _parameter_space_diagnostics(
    spec: StrategyFamilySearchSpec,
    trial_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    choice_counts = {str(name): len(tuple(values)) for name, values in sorted(spec.parameter_space.items())}
    fixed_parameters: dict[str, object] = {}
    active_parameters: list[str] = []
    for name, values in sorted(spec.parameter_space.items()):
        choices = tuple(values)
        if len(choices) == 1:
            fixed_parameters[str(name)] = choices[0]
        elif choices:
            active_parameters.append(str(name))

    observed: dict[str, set[object]] = {name: set() for name in choice_counts}
    for row in trial_rows:
        params = row.get("params_json", {})
        if isinstance(params, str) and params.strip():
            try:
                params = json.loads(params)
            except json.JSONDecodeError:
                params = {}
        if not isinstance(params, Mapping):
            continue
        for name in observed:
            if name in params:
                observed[name].add(params[name])

    return {
        "choice_counts": choice_counts,
        "active_parameters": active_parameters,
        "fixed_parameters": fixed_parameters,
        "observed_unique_value_counts": {name: len(values) for name, values in sorted(observed.items())},
        "observed_values": {
            name: _sort_jsonable_values(values)
            for name, values in sorted(observed.items())
        },
    }


def _optimizer_trial_id(*, optimizer_study_id: str, trial_number: int, trial_kind: str, param_id: str) -> str:
    return "OPTTRIAL-" + _stable_hash(f"{optimizer_study_id}|{trial_number}|{trial_kind}|{param_id}")


def _search_run_row(
    *,
    search_run_id: str,
    search_spec: StrategyFamilySearchSpec,
    campaign_id: str,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    bundle: VectorBTInputBundle,
    window_id: str,
    param_count: int,
    status: str,
    started_at: str,
    error_message: str = "",
) -> dict[str, object]:
    return {
        "search_run_id": search_run_id,
        "search_spec_id": search_spec_id(search_spec),
        "campaign_id": campaign_id,
        "family_key": search_spec.family_key,
        "template_key": search_spec.template_key,
        "clock_profile": str(bundle.metadata["clock_profile"]),
        "dataset_id": dataset_version,
        "dataset_snapshot": dataset_version,
        "indicator_profile_version": indicator_set_version,
        "derived_indicator_profile_version": derived_indicator_set_version,
        "universe_key": ",".join(bundle.instruments),
        "fold_id": window_id,
        "param_count": param_count,
        "instrument_count": len(bundle.instruments),
        "chunk_count": 1 if param_count else 0,
        "status": status,
        "started_at": started_at,
        "finished_at": _created_at(),
        "error_message": error_message,
    }


def _run_optuna_family_search(
    *,
    series_frames: Sequence[ResearchSeriesFrame],
    search_spec: StrategyFamilySearchSpec,
    config: BacktestEngineConfig,
    backtest_batch_id: str,
    campaign_run_id: str,
    strategy_space_id: str,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    split_windows: tuple[dict[str, object], ...] | None,
    param_batch_size: int,
    optimizer_policy: Mapping[str, object],
) -> dict[str, object]:
    try:
        import optuna
    except ImportError as exc:  # pragma: no cover - dependency guard for misconfigured runtimes.
        raise RuntimeError("Optuna optimizer mode requires the `optuna` package") from exc

    policy = _resolved_optuna_policy(optimizer_policy, search_spec)
    if policy["sampler"] != "tpe":
        raise ValueError("strategy_space.optimizer.sampler must be `tpe`")
    if policy["direction"] != "maximize":
        raise ValueError("strategy_space.optimizer.direction must be `maximize`")
    if policy["objective"] != "robust_oos_trial_v1":
        raise ValueError("strategy_space.optimizer.objective must be `robust_oos_trial_v1`")

    ranking_policy = _optuna_ranking_policy(optimizer_policy)
    started_at = _created_at()
    search_run_id_base = "VBTSEARCH-" + _stable_hash(
        f"{backtest_batch_id}|{search_spec.family_key}|{search_spec.template_key}|optuna|{policy['n_trials']}"
    )
    optimizer_study_id = "OPTSTUDY-" + _stable_hash(
        f"{search_run_id_base}|{campaign_run_id}|{strategy_space_id}|{search_spec_id(search_spec)}"
    )
    windows = list(_windowed_series(series_frames, config=config, split_windows=split_windows))
    search_run_rows: list[dict[str, object]] = []
    run_rows: list[dict[str, object]] = []
    stat_rows: list[dict[str, object]] = []
    param_result_rows: list[dict[str, object]] = []
    gate_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    order_rows: list[dict[str, object]] = []
    drawdown_rows: list[dict[str, object]] = []
    optimizer_trial_rows: list[dict[str, object]] = []
    evaluations: dict[str, dict[str, object]] = {}
    tell_attrs_by_trial: dict[int, dict[str, object]] = {}
    ask_tell_batch_summaries: list[dict[str, object]] = []

    def _trial_constraints_from_components(
        components: Mapping[str, object],
        *,
        constraints_passed: bool,
    ) -> tuple[tuple[str, ...], tuple[float, ...]]:
        raw_names = components.get("constraint_names", ())
        raw_values = components.get("constraint_values", ())
        names = tuple(str(item) for item in raw_names) if isinstance(raw_names, Iterable) and not isinstance(raw_names, str) else tuple()
        try:
            values = tuple(float(item) for item in raw_values) if isinstance(raw_values, Iterable) and not isinstance(raw_values, str) else tuple()
        except (TypeError, ValueError):
            values = tuple()
        if values:
            return names, values
        return ("unclassified_failure",), ((-1.0 if constraints_passed else 1.0),)

    def record_trial(
        *,
        trial_number: int,
        trial_kind: str,
        row: Mapping[str, object],
        value: float,
        status: str,
        components: Mapping[str, object],
        constraints_passed: bool,
        failure_reason: str,
        search_run_ids: Sequence[str],
        started: str,
    ) -> None:
        param_id = param_hash(_effective_search_spec_for_params(search_spec, row), row)
        component_payload = dict(components)
        constraint_names, constraint_values = _trial_constraints_from_components(
            component_payload,
            constraints_passed=constraints_passed,
        )
        component_payload["constraint_names"] = constraint_names
        component_payload["constraint_values"] = constraint_values
        component_payload["constraints_passed"] = bool(constraints_passed)
        optimizer_trial_rows.append(
            {
                "optimizer_trial_id": _optimizer_trial_id(
                    optimizer_study_id=optimizer_study_id,
                    trial_number=trial_number,
                    trial_kind=trial_kind,
                    param_id=param_id,
                ),
                "optimizer_study_id": optimizer_study_id,
                "trial_number": trial_number,
                "trial_kind": trial_kind,
                "param_hash": param_id,
                "params_json": dict(row),
                "value": value,
                "status": status,
                "objective_components_json": component_payload,
                "constraints_passed": constraints_passed,
                "failure_reason": failure_reason,
                "search_run_ids_json": list(search_run_ids),
                "started_at": started,
                "finished_at": _created_at(),
            }
        )
        tell_attrs_by_trial[trial_number] = {
            "constraint_names": constraint_names,
            "constraint_values": constraint_values,
            "constraints_passed": bool(constraints_passed),
            "objective_components": component_payload,
        }

    def _bundle_for_window(window_series: Sequence[ResearchSeriesFrame], effective_spec: StrategyFamilySearchSpec) -> VectorBTInputBundle:
        clock_profile_payload = (
            dict(effective_spec.clock_profile)
            if effective_spec.clock_profile
            else {
                "name": _clock_profile_for_timeframe(window_series[0].timeframe),
                "execution_tf": window_series[0].timeframe,
            }
        )
        return build_input_bundle(
            window_series,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            clock_profile=clock_profile_payload,
            execution_timeframe=_execution_timeframe_from_profile(clock_profile_payload, window_series[0].timeframe),
        )

    def evaluate_batch(
        trial_items: Sequence[tuple[Any, str, Mapping[str, object]]],
        *,
        batch_index: int,
    ) -> dict[int, float]:
        values_by_trial: dict[int, float] = {}
        pending: list[dict[str, object]] = []
        for trial_ref, trial_kind, row in trial_items:
            trial_number = int(getattr(trial_ref, "number", trial_ref))
            trial_started = _created_at()
            candidate = dict(row)
            effective_spec = _effective_search_spec_for_params(search_spec, candidate)
            candidate_hash = param_hash(effective_spec, candidate)
            if not _constraints_pass(candidate, effective_spec.parameter_constraints):
                components = _optimizer_provenance_components(
                    row=candidate,
                    spec=effective_spec,
                    extra={
                        "failure": "PARAMETER_CONSTRAINT_FAILED",
                        "constraint_names": ("parameter_constraints",),
                        "constraint_values": (1.0,),
                        "selection_owner": "optuna.study",
                    },
                )
                record_trial(
                    trial_number=trial_number,
                    trial_kind=trial_kind,
                    row=candidate,
                    value=-1.0,
                    status="constraint_failed",
                    components=components,
                    constraints_passed=False,
                    failure_reason="PARAMETER_CONSTRAINT_FAILED",
                    search_run_ids=(),
                    started=trial_started,
                )
                values_by_trial[trial_number] = -1.0
                continue
            cached = evaluations.get(candidate_hash)
            if cached is not None:
                record_trial(
                    trial_number=trial_number,
                    trial_kind=trial_kind,
                    row=candidate,
                    value=float(cached["value"]),
                    status="duplicate",
                    components=dict(cached["components"]),
                    constraints_passed=bool(cached["constraints_passed"]),
                    failure_reason="",
                    search_run_ids=tuple(cached["search_run_ids"]),
                    started=trial_started,
                )
                values_by_trial[trial_number] = float(cached["value"])
                continue
            pending.append(
                {
                    "trial_number": trial_number,
                    "trial_kind": trial_kind,
                    "started": trial_started,
                    "row": candidate,
                    "spec": effective_spec,
                    "param_hash": candidate_hash,
                }
            )

        pending_by_hash: dict[str, dict[str, object]] = {}
        pending_duplicates: dict[str, list[dict[str, object]]] = {}
        for item in pending:
            candidate_hash = str(item["param_hash"])
            if candidate_hash in pending_by_hash:
                pending_duplicates.setdefault(candidate_hash, []).append(item)
                continue
            pending_by_hash[candidate_hash] = item

        grouped: dict[str, list[dict[str, object]]] = {}
        for item in pending_by_hash.values():
            effective_spec = item["spec"]
            assert isinstance(effective_spec, StrategyFamilySearchSpec)
            key = search_spec_id(effective_spec)
            grouped.setdefault(key, []).append(item)

        for group_offset, group_items in enumerate(grouped.values(), start=1):
            effective_spec = group_items[0]["spec"]
            assert isinstance(effective_spec, StrategyFamilySearchSpec)
            param_rows = tuple(dict(item["row"]) for item in group_items if isinstance(item["row"], Mapping))
            param_lookup = {str(item["param_hash"]): dict(item["row"]) for item in group_items}
            local_search_run_rows: list[dict[str, object]] = []
            local_run_rows: list[dict[str, object]] = []
            local_stat_rows: list[dict[str, object]] = []
            local_param_result_rows: list[dict[str, object]] = []
            local_gate_rows: list[dict[str, object]] = []
            local_trade_rows: list[dict[str, object]] = []
            local_order_rows: list[dict[str, object]] = []
            local_drawdown_rows: list[dict[str, object]] = []
            local_search_run_ids: list[str] = []
            try:
                for window_id, window_series in windows:
                    bundle = _bundle_for_window(window_series, effective_spec)
                    search_run_id = f"{search_run_id_base}-ASKTELL-{batch_index:04d}-{group_offset:02d}-{window_id}"
                    surface = build_signal_surface(
                        bundle=bundle,
                        spec=effective_spec,
                        param_rows=param_rows,
                        search_run_id=search_run_id,
                        config=config,
                    )
                    portfolio = run_surface_portfolio(bundle=bundle, surface=surface, config=config)
                    collected = collect_surface_rows(
                        portfolio=portfolio,
                        bundle=bundle,
                        surface=surface,
                        spec=effective_spec,
                        param_lookup=param_lookup,
                        backtest_batch_id=backtest_batch_id,
                        campaign_run_id=campaign_run_id,
                        strategy_space_id=strategy_space_id,
                        dataset_version=dataset_version,
                        indicator_set_version=indicator_set_version,
                        derived_indicator_set_version=derived_indicator_set_version,
                        window_id=window_id,
                    )
                    local_run_rows.extend(collected["run_rows"])
                    local_stat_rows.extend(collected["stat_rows"])
                    local_param_result_rows.extend(collected["param_result_rows"])
                    local_gate_rows.extend(collected["gate_rows"])
                    local_trade_rows.extend(collected["trade_rows"])
                    local_order_rows.extend(collected["order_rows"])
                    local_drawdown_rows.extend(collected["drawdown_rows"])
                    local_search_run_ids.append(search_run_id)
                    local_search_run_rows.append(
                        _search_run_row(
                            search_run_id=search_run_id,
                            search_spec=effective_spec,
                            campaign_id=strategy_space_id,
                            dataset_version=dataset_version,
                            indicator_set_version=indicator_set_version,
                            derived_indicator_set_version=derived_indicator_set_version,
                            bundle=bundle,
                            window_id=window_id,
                            param_count=len(param_rows),
                            status="success",
                            started_at=str(group_items[0]["started"]),
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                failed_items: list[dict[str, object]] = []
                for item in group_items:
                    failed_items.append(item)
                    failed_items.extend(pending_duplicates.get(str(item["param_hash"]), ()))
                for item in failed_items:
                    components = _optimizer_provenance_components(
                        row=dict(item["row"]),
                        spec=effective_spec,
                        extra={
                            "failure": type(exc).__name__,
                            "constraint_names": ("evaluation_failure",),
                            "constraint_values": (1.0,),
                            "selection_owner": "optuna.study",
                        },
                    )
                    record_trial(
                        trial_number=int(item["trial_number"]),
                        trial_kind=str(item["trial_kind"]),
                        row=dict(item["row"]),
                        value=-1.0,
                        status="failed",
                        components=components,
                        constraints_passed=False,
                        failure_reason=str(exc),
                        search_run_ids=(),
                        started=str(item["started"]),
                    )
                    values_by_trial[int(item["trial_number"])] = -1.0
                continue

            search_run_rows.extend(local_search_run_rows)
            run_rows.extend(local_run_rows)
            stat_rows.extend(local_stat_rows)
            param_result_rows.extend(local_param_result_rows)
            gate_rows.extend(local_gate_rows)
            trade_rows.extend(local_trade_rows)
            order_rows.extend(local_order_rows)
            drawdown_rows.extend(local_drawdown_rows)

            result_rows_by_hash: dict[str, list[dict[str, object]]] = {str(item["param_hash"]): [] for item in group_items}
            for result_row in local_param_result_rows:
                result_rows_by_hash.setdefault(str(result_row["param_hash"]), []).append(result_row)
            trade_rows_by_hash: dict[str, list[dict[str, object]]] = {str(item["param_hash"]): [] for item in group_items}
            for trade_row in local_trade_rows:
                trade_rows_by_hash.setdefault(str(trade_row.get("param_hash", "")), []).append(trade_row)
            for item in group_items:
                candidate = dict(item["row"])
                candidate_hash = str(item["param_hash"])
                value, raw_components = _optimizer_objective(
                    result_rows_by_hash.get(candidate_hash, ()),
                    trade_rows=trade_rows_by_hash.get(candidate_hash, ()),
                    policy=ranking_policy,
                )
                components = _optimizer_provenance_components(
                    row=candidate,
                    spec=effective_spec,
                    extra=raw_components,
                )
                constraints_passed = bool(components.get("constraints_passed", False))
                evaluations[candidate_hash] = {
                    "value": value,
                    "components": components,
                    "constraints_passed": constraints_passed,
                    "search_run_ids": tuple(local_search_run_ids),
                    "params": candidate,
                    "trial_number": int(item["trial_number"]),
                    "trial_kind": str(item["trial_kind"]),
                }
                record_trial(
                    trial_number=int(item["trial_number"]),
                    trial_kind=str(item["trial_kind"]),
                    row=candidate,
                    value=value,
                    status="completed",
                    components=components,
                    constraints_passed=constraints_passed,
                    failure_reason="",
                    search_run_ids=tuple(local_search_run_ids),
                    started=str(item["started"]),
                )
                values_by_trial[int(item["trial_number"])] = value
                for duplicate_item in pending_duplicates.get(candidate_hash, ()):
                    record_trial(
                        trial_number=int(duplicate_item["trial_number"]),
                        trial_kind=str(duplicate_item["trial_kind"]),
                        row=dict(duplicate_item["row"]),
                        value=value,
                        status="duplicate",
                        components=components,
                        constraints_passed=constraints_passed,
                        failure_reason="",
                        search_run_ids=tuple(local_search_run_ids),
                        started=str(duplicate_item["started"]),
                    )
                    values_by_trial[int(duplicate_item["trial_number"])] = value
        return values_by_trial

    def _constraints_func(frozen_trial: Any) -> tuple[float, ...]:
        values = frozen_trial.user_attrs.get(_OPTUNA_CONSTRAINTS_ATTR)
        if values is None:
            return (1.0,)
        try:
            resolved = tuple(float(item) for item in values)
        except (TypeError, ValueError):
            return (1.0,)
        return resolved or (1.0,)

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = optuna.samplers.TPESampler(seed=int(policy["seed"]), constraints_func=_constraints_func)
    study = optuna.create_study(direction=str(policy["direction"]), sampler=sampler)

    requested_trials = int(policy["n_trials"])
    batch_size = max(1, int(param_batch_size))
    completed_asks = 0
    batch_index = 0
    while completed_asks < requested_trials:
        batch_index += 1
        trial_items: list[tuple[Any, str, Mapping[str, object]]] = []
        for _ in range(min(batch_size, requested_trials - completed_asks)):
            trial = study.ask()
            row = _suggest_optuna_row(trial, search_spec)
            trial_items.append((trial, "optuna_trial", row))
            completed_asks += 1
        batch_started = perf_counter()
        trial_rows_before = len(optimizer_trial_rows)
        evaluated_before = len(evaluations)
        values = evaluate_batch(trial_items, batch_index=batch_index)
        batch_duration = max(perf_counter() - batch_started, 1e-9)
        for trial, _, _ in trial_items:
            trial_number = int(trial.number)
            value = float(values.get(trial_number, -1.0))
            attrs = tell_attrs_by_trial.get(trial_number, {})
            trial.set_user_attr(_OPTUNA_CONSTRAINT_NAMES_ATTR, tuple(attrs.get("constraint_names", ())))
            trial.set_user_attr(_OPTUNA_CONSTRAINTS_ATTR, tuple(attrs.get("constraint_values", (1.0,))))
            trial.set_user_attr("ta3000_constraints_passed", bool(attrs.get("constraints_passed", False)))
            trial.set_user_attr("ta3000_objective_components", dict(attrs.get("objective_components", {})))
            study.tell(trial, value)
        batch_rows = optimizer_trial_rows[trial_rows_before:]
        status_counts: dict[str, int] = {}
        for row in batch_rows:
            status = str(row.get("status", ""))
            status_counts[status] = status_counts.get(status, 0) + 1
        ask_tell_batch_summaries.append(
            {
                "batch_index": batch_index,
                "asked_trial_count": len(trial_items),
                "recorded_trial_count": len(batch_rows),
                "new_unique_evaluations": len(evaluations) - evaluated_before,
                "duration_seconds": round(batch_duration, 6),
                "trials_per_second": round(len(trial_items) / batch_duration, 6),
                "status_counts": dict(sorted(status_counts.items())),
            }
        )

    completed = [
        row
        for row in optimizer_trial_rows
        if str(row["status"]) == "completed"
    ]
    completed_trials = [
        row
        for row in optimizer_trial_rows
        if str(row["status"]) in {"completed", "duplicate"}
    ]
    feasible_trial_numbers = {
        int(row["trial_number"])
        for row in completed_trials
        if bool(row.get("constraints_passed", False))
    }
    best_trial = None
    try:
        best_trial = study.best_trial
    except ValueError:
        best_trial = None
    if best_trial is not None and int(best_trial.number) not in feasible_trial_numbers:
        best_trial = None
    best_row = (
        next(
            (
                row
                for row in optimizer_trial_rows
                if best_trial is not None and int(row["trial_number"]) == int(best_trial.number)
            ),
            None,
        )
        if best_trial is not None
        else None
    )
    study_status = "success" if best_row else ("no_feasible_trials" if completed_trials else "failed")
    optimizer_study_rows = [
        {
            "optimizer_study_id": optimizer_study_id,
            "campaign_run_id": campaign_run_id,
            "strategy_space_id": strategy_space_id,
            "search_spec_id": search_spec_id(search_spec),
            "family_key": search_spec.family_key,
            "template_key": search_spec.template_key,
            "optimizer_engine": "optuna",
            "sampler": str(policy["sampler"]),
            "seed": int(policy["seed"]),
            "objective_name": str(policy["objective"]),
            "direction": str(policy["direction"]),
            "n_trials_requested": int(policy["n_trials"]),
            "n_trials_completed": len(completed_trials),
            "best_trial_number": int(best_row["trial_number"]) if best_row else -1,
            "best_param_hash": str(best_row["param_hash"]) if best_row else "",
            "best_value": float(best_row["value"]) if best_row else 0.0,
            "status": study_status,
            "started_at": started_at,
            "finished_at": _created_at(),
            "study_config_json": {
                **policy,
                "evaluated_param_count": len(evaluations),
                "trial_row_count": len(optimizer_trial_rows),
                "ask_tell_batch_size": batch_size,
                "ask_tell_batch_count": batch_index,
                "ask_tell_batch_summaries": ask_tell_batch_summaries,
                "parameter_space_diagnostics": _parameter_space_diagnostics(search_spec, optimizer_trial_rows),
            },
        }
    ]
    return {
        "search_run_rows": search_run_rows,
        "optimizer_study_rows": optimizer_study_rows,
        "optimizer_trial_rows": optimizer_trial_rows,
        "run_rows": run_rows,
        "stat_rows": stat_rows,
        "param_result_rows": param_result_rows,
        "gate_rows": gate_rows,
        "trade_rows": trade_rows,
        "order_rows": order_rows,
        "drawdown_rows": drawdown_rows,
    }


def run_vectorbt_family_search(
    *,
    series_frames: Sequence[ResearchSeriesFrame],
    search_spec: StrategyFamilySearchSpec,
    config: BacktestEngineConfig,
    backtest_batch_id: str,
    campaign_run_id: str,
    strategy_space_id: str,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    split_windows: tuple[dict[str, object], ...] | None,
    param_batch_size: int,
    optimizer_policy: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if _optimizer_engine(optimizer_policy) == "optuna":
        return _run_optuna_family_search(
            series_frames=series_frames,
            search_spec=search_spec,
            config=config,
            backtest_batch_id=backtest_batch_id,
            campaign_run_id=campaign_run_id,
            strategy_space_id=strategy_space_id,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            split_windows=split_windows,
            param_batch_size=param_batch_size,
            optimizer_policy=dict(optimizer_policy or {}),
        )
    all_params = _parameter_rows(search_spec)
    search_run_id_base = "VBTSEARCH-" + _stable_hash(
        f"{backtest_batch_id}|{search_spec.family_key}|{search_spec.template_key}|{len(all_params)}"
    )
    search_run_rows: list[dict[str, object]] = []
    run_rows: list[dict[str, object]] = []
    stat_rows: list[dict[str, object]] = []
    param_result_rows: list[dict[str, object]] = []
    gate_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    order_rows: list[dict[str, object]] = []
    drawdown_rows: list[dict[str, object]] = []
    started_at = _created_at()

    for window_id, window_series in _windowed_series(series_frames, config=config, split_windows=split_windows):
        clock_profile_payload = (
            dict(search_spec.clock_profile)
            if search_spec.clock_profile
            else {
                "name": _clock_profile_for_timeframe(window_series[0].timeframe),
                "execution_tf": window_series[0].timeframe,
            }
        )
        bundle = build_input_bundle(
            window_series,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            clock_profile=clock_profile_payload,
            execution_timeframe=_execution_timeframe_from_profile(clock_profile_payload, window_series[0].timeframe),
        )
        try:
            indicator_plan = resolve_indicator_plan(bundle, search_spec)
        except InputPlanValidationError as exc:
            search_run_id = f"{search_run_id_base}-{window_id}-FAILED"
            search_run_rows.append(
                {
                    "search_run_id": search_run_id,
                    "search_spec_id": search_spec_id(search_spec),
                    "campaign_id": strategy_space_id,
                    "family_key": search_spec.family_key,
                    "template_key": search_spec.template_key,
                    "clock_profile": str(bundle.metadata["clock_profile"]),
                    "dataset_id": dataset_version,
                    "dataset_snapshot": dataset_version,
                    "indicator_profile_version": indicator_set_version,
                    "derived_indicator_profile_version": derived_indicator_set_version,
                    "universe_key": ",".join(bundle.instruments),
                    "fold_id": window_id,
                    "param_count": 0,
                    "instrument_count": len(bundle.instruments),
                    "chunk_count": 0,
                    "status": "failed",
                    "started_at": started_at,
                    "finished_at": _created_at(),
                    "error_message": str(exc),
                }
            )
            gate_rows.append(
                {
                    "search_run_id": search_run_id,
                    "param_hash": search_spec_id(search_spec),
                    "gate_name": "indicator_plan_valid",
                    "passed": 0,
                    "failure_code": exc.failure_code,
                    "failure_reason": str(exc),
                    "metric_snapshot_json": {
                        "missing": list(exc.missing),
                        "indicator_plan": {
                            "required_price_inputs": list(search_spec.required_price_inputs),
                            "materialized_indicators": list(search_spec.required_materialized_indicators),
                            "materialized_derived": list(search_spec.required_materialized_derived),
                        },
                    },
                    "created_at": _created_at(),
                }
            )
            continue
        for chunk_index, param_chunk in enumerate(_chunked(all_params, param_batch_size), start=1):
            search_run_id = f"{search_run_id_base}-{window_id}-{chunk_index:03d}"
            surface = build_signal_surface(
                bundle=bundle,
                spec=search_spec,
                param_rows=param_chunk,
                search_run_id=search_run_id,
                config=config,
            )
            portfolio = run_surface_portfolio(bundle=bundle, surface=surface, config=config)
            param_lookup = {param_hash(search_spec, row): row for row in param_chunk}
            collected = collect_surface_rows(
                portfolio=portfolio,
                bundle=bundle,
                surface=surface,
                spec=search_spec,
                param_lookup=param_lookup,
                backtest_batch_id=backtest_batch_id,
                campaign_run_id=campaign_run_id,
                strategy_space_id=strategy_space_id,
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                derived_indicator_set_version=derived_indicator_set_version,
                window_id=window_id,
            )
            run_rows.extend(collected["run_rows"])
            stat_rows.extend(collected["stat_rows"])
            param_result_rows.extend(collected["param_result_rows"])
            gate_rows.extend(collected["gate_rows"])
            trade_rows.extend(collected["trade_rows"])
            order_rows.extend(collected["order_rows"])
            drawdown_rows.extend(collected["drawdown_rows"])
            search_run_rows.append(
                {
                    "search_run_id": search_run_id,
                    "search_spec_id": search_spec_id(search_spec),
                    "campaign_id": strategy_space_id,
                    "family_key": search_spec.family_key,
                    "template_key": search_spec.template_key,
                    "clock_profile": str(bundle.metadata["clock_profile"]),
                    "dataset_id": dataset_version,
                    "dataset_snapshot": dataset_version,
                    "indicator_profile_version": indicator_set_version,
                    "derived_indicator_profile_version": derived_indicator_set_version,
                    "universe_key": ",".join(bundle.instruments),
                    "fold_id": window_id,
                    "param_count": len(param_chunk),
                    "instrument_count": len(bundle.instruments),
                    "chunk_count": 1,
                    "status": "success",
                    "started_at": started_at,
                    "finished_at": _created_at(),
                    "error_message": "",
                }
            )
    return {
        "search_run_rows": search_run_rows,
        "optimizer_study_rows": [],
        "optimizer_trial_rows": [],
        "run_rows": run_rows,
        "stat_rows": stat_rows,
        "param_result_rows": param_result_rows,
        "gate_rows": gate_rows,
        "trade_rows": trade_rows,
        "order_rows": order_rows,
        "drawdown_rows": drawdown_rows,
    }


def _chunked(items: Sequence[dict[str, object]], chunk_size: int) -> tuple[tuple[dict[str, object], ...], ...]:
    size = max(1, int(chunk_size))
    return tuple(tuple(items[index : index + size]) for index in range(0, len(items), size))


def _windowed_series(
    series_frames: Sequence[ResearchSeriesFrame],
    *,
    config: BacktestEngineConfig,
    split_windows: tuple[dict[str, object], ...] | None,
) -> tuple[tuple[str, tuple[ResearchSeriesFrame, ...]], ...]:
    per_series: list[tuple[ResearchSeriesFrame, tuple[tuple[str, pd.DataFrame], ...]]] = [
        (series, _window_frames(series.frame, window_count=config.window_count, split_windows=split_windows))
        for series in series_frames
    ]
    first_window_ids = tuple(window_id for window_id, _ in per_series[0][1])
    available_by_series = [
        {window_id: frame for window_id, frame in windows}
        for _, windows in per_series
    ]
    common_window_ids = tuple(
        window_id
        for window_id in first_window_ids
        if all(window_id in windows for windows in available_by_series)
    )
    if not common_window_ids:
        raise ValueError("split window layout has no common windows across selected research series")
    resolved: list[tuple[str, tuple[ResearchSeriesFrame, ...]]] = []
    for window_id in common_window_ids:
        window_frames: list[ResearchSeriesFrame] = []
        for series, windows in zip((item[0] for item in per_series), available_by_series, strict=True):
            window_frames.append(
                ResearchSeriesFrame(
                    contract_id=series.contract_id,
                    instrument_id=series.instrument_id,
                    timeframe=series.timeframe,
                    frame=windows[window_id],
                )
            )
        resolved.append((window_id, tuple(window_frames)))
    return tuple(resolved)


def _clock_profile_for_timeframe(timeframe: str) -> str:
    if timeframe == "4h":
        return "swing_4h_v1"
    if timeframe == "1h":
        return "short_swing_1h_v1"
    if timeframe == "15m":
        return "short_swing_1h_v1"
    return "research_clock_v1"


def project_family_candidate(
    *,
    series: ResearchSeriesFrame | Sequence[ResearchSeriesFrame],
    search_spec: StrategyFamilySearchSpec,
    params: dict[str, object],
    config: BacktestEngineConfig,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    decision_lag_bars_max: int = 1,
) -> dict[str, object] | None:
    if decision_lag_bars_max < 0:
        raise ValueError("decision_lag_bars_max must be non-negative")
    input_series = (series,) if isinstance(series, ResearchSeriesFrame) else tuple(series)
    if not input_series:
        return None
    clock_profile_payload = (
        dict(search_spec.clock_profile)
        if search_spec.clock_profile
        else {
            "name": _clock_profile_for_timeframe(input_series[0].timeframe),
            "execution_tf": input_series[0].timeframe,
        }
    )
    execution_tf = _execution_timeframe_from_profile(clock_profile_payload, input_series[0].timeframe)
    execution_series = next((item for item in input_series if item.timeframe == execution_tf), input_series[0])
    bundle = build_input_bundle(
        input_series,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        clock_profile=clock_profile_payload,
        execution_timeframe=execution_tf,
    )
    surface = build_signal_surface(
        bundle=bundle,
        spec=search_spec,
        param_rows=(params,),
        search_run_id="PROJECT-" + _stable_hash(f"{search_spec.family_key}|{execution_series.instrument_id}|{params}"),
        config=config,
    )
    close = _numeric_frame(bundle.field("close")).iloc[:, 0]
    long_entries = surface.entries.iloc[:, 0].fillna(False)
    short_entries = surface.short_entries.iloc[:, 0].fillna(False)
    entry_candidates: list[tuple[int, str]] = []
    entry_candidates.extend((idx, "long") for idx, active in enumerate(long_entries.tolist()) if bool(active))
    entry_candidates.extend((idx, "short") for idx, active in enumerate(short_entries.tolist()) if bool(active))
    if not entry_candidates:
        return None
    signal_index, side = max(entry_candidates, key=lambda item: item[0])
    freshness_bars = (len(close) - 1) - signal_index
    if freshness_bars > decision_lag_bars_max:
        return None
    entry_price = float(close.iloc[signal_index])
    if pd.isna(entry_price):
        return None
    stop_relative = float(surface.sl_stop.iloc[signal_index, 0])
    target_relative = float(surface.tp_stop.iloc[signal_index, 0])
    if side == "long":
        stop_ref = entry_price * max(0.0, 1.0 - stop_relative)
        target_ref = entry_price * (1.0 + target_relative)
    else:
        stop_ref = entry_price * (1.0 + stop_relative)
        target_ref = entry_price * max(0.0, 1.0 - target_relative)
    risk_distance = abs(entry_price - stop_ref)
    reward_distance = abs(target_ref - entry_price)
    if risk_distance <= 0.0 or reward_distance <= 0.0:
        return None
    risk_reward = reward_distance / max(risk_distance, 1e-9)
    freshness_score = max(0.0, 1.0 - (freshness_bars / max(decision_lag_bars_max + 1, 1)))
    signal_strength = max(0.0, min(1.0, 0.35 + (0.35 * min(risk_reward / 2.0, 1.0)) + (0.30 * freshness_score)))
    ts_decision = str(execution_series.frame["ts"].iloc[signal_index])
    context_id = "ICTX-" + _stable_hash(
        f"{dataset_version}|{indicator_set_version}|{derived_indicator_set_version}|{execution_series.contract_id}|{execution_series.timeframe}|{ts_decision}"
    )
    return {
        "side": side,
        "entry_ref": entry_price,
        "stop_ref": stop_ref,
        "target_ref": target_ref,
        "ts_decision": ts_decision,
        "signal_strength_score": signal_strength,
        "indicator_context": {
            "dataset_version": dataset_version,
            "snapshot_id": context_id,
        },
    }
