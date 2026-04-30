from __future__ import annotations

from dataclasses import dataclass, field
from itertools import product
import operator
from typing import Literal, Mapping


StrategyExecutionMode = Literal["signals", "order_func"]
StrategyDirectionMode = Literal["long_only", "short_only", "long_short"]
StrategyIndicatorRole = Literal["regime", "decision", "trigger", "entry", "exit", "risk"]
StrategyIndicatorSource = Literal["price", "indicator", "derived", "ephemeral"]
StrategyParameterRole = Literal["regime", "decision", "trigger", "entry", "exit", "risk", "execution"]

_CONSTRAINT_OPERATORS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
}


def _constraint_value(token: str, row: Mapping[str, object]) -> object:
    token = token.strip()
    if token in row:
        return row[token]
    if token.lower() == "true":
        return True
    if token.lower() == "false":
        return False
    try:
        return float(token)
    except ValueError:
        return token.strip("\"'")


def _constraint_passes(expression: str, row: Mapping[str, object]) -> bool:
    for symbol in ("<=", ">=", "==", "<", ">"):
        if symbol not in expression:
            continue
        left, right = expression.split(symbol, 1)
        left_value = _constraint_value(left, row)
        right_value = _constraint_value(right, row)
        try:
            return bool(_CONSTRAINT_OPERATORS[symbol](left_value, right_value))
        except TypeError:
            return False
    return True


@dataclass(frozen=True)
class StrategyParameter:
    name: str
    values: tuple[object, ...]
    role: StrategyParameterRole = "trigger"
    timeframe: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("parameter name must be non-empty")
        if not self.values:
            raise ValueError("parameter values must not be empty")
        if self.timeframe is not None and not self.timeframe.strip():
            raise ValueError("parameter timeframe must be non-empty when provided")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "values": list(self.values),
            "role": self.role,
            "timeframe": self.timeframe,
        }


@dataclass(frozen=True)
class StrategyClockProfileSpec:
    name: str
    regime_tf: str
    signal_tf: str
    trigger_tf: str
    execution_tf: str
    bar_type: str = "time"
    closed_bars_only: bool = True
    layer_alignment: str = "native_then_event_to_execution"

    def __post_init__(self) -> None:
        for field_name in ("name", "regime_tf", "signal_tf", "trigger_tf", "execution_tf", "bar_type", "layer_alignment"):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} must be non-empty")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "regime_tf": self.regime_tf,
            "signal_tf": self.signal_tf,
            "trigger_tf": self.trigger_tf,
            "execution_tf": self.execution_tf,
            "bar_type": self.bar_type,
            "closed_bars_only": self.closed_bars_only,
            "layer_alignment": self.layer_alignment,
        }


@dataclass(frozen=True)
class StrategyRiskPolicy:
    stop_atr_multiple: float
    target_atr_multiple: float

    def __post_init__(self) -> None:
        if self.stop_atr_multiple <= 0:
            raise ValueError("stop_atr_multiple must be positive")
        if self.target_atr_multiple <= 0:
            raise ValueError("target_atr_multiple must be positive")


@dataclass(frozen=True)
class StrategyRankingMetadata:
    preferred_metrics: tuple[str, ...] = ("total_return", "sharpe", "max_drawdown")
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.preferred_metrics:
            raise ValueError("preferred_metrics must not be empty")


@dataclass(frozen=True)
class StrategyIndicatorRequirement:
    column: str
    role: StrategyIndicatorRole
    timeframe: str
    source: StrategyIndicatorSource
    native_column: str | None = None

    def __post_init__(self) -> None:
        if not self.column.strip():
            raise ValueError("indicator requirement column must be non-empty")
        if not self.timeframe.strip():
            raise ValueError("indicator requirement timeframe must be non-empty")
        if self.native_column is not None and not self.native_column.strip():
            raise ValueError("indicator requirement native_column must be non-empty")

    def to_dict(self) -> dict[str, object]:
        return {
            "column": self.column,
            "role": self.role,
            "timeframe": self.timeframe,
            "source": self.source,
            "native_column": self.native_column,
        }


@dataclass(frozen=True)
class StrategySpec:
    version: str
    family: str
    description: str
    required_columns: tuple[str, ...]
    parameter_grid: tuple[StrategyParameter, ...]
    signal_builder_key: str
    execution_mode: StrategyExecutionMode = "signals"
    direction_mode: StrategyDirectionMode = "long_short"
    risk_policy: StrategyRiskPolicy = field(
        default_factory=lambda: StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0)
    )
    ranking_metadata: StrategyRankingMetadata = field(default_factory=StrategyRankingMetadata)
    intent: str | None = None
    allowed_clock_profiles: tuple[str, ...] = ("swing_4h_v1", "short_swing_1h_v1")
    market_regimes: tuple[str, ...] = ()
    indicator_requirements: tuple[StrategyIndicatorRequirement, ...] = ()
    entry_logic: tuple[str, ...] = ()
    exit_logic: tuple[str, ...] = ()
    verification_questions: tuple[str, ...] = ()
    clock_profile: StrategyClockProfileSpec | None = None
    parameter_constraints: tuple[str, ...] = ()
    optional_indicator_plan: tuple[Mapping[str, object], ...] = ()

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("version must be non-empty")
        if not self.family.strip():
            raise ValueError("family must be non-empty")
        if not self.description.strip():
            raise ValueError("description must be non-empty")
        if not self.required_columns:
            raise ValueError("required_columns must not be empty")
        if not self.signal_builder_key.strip():
            raise ValueError("signal_builder_key must be non-empty")
        object.__setattr__(
            self,
            "required_columns",
            tuple(column.strip() for column in self.required_columns if column.strip()),
        )
        object.__setattr__(
            self,
            "allowed_clock_profiles",
            tuple(item.strip() for item in self.allowed_clock_profiles if item.strip()),
        )
        object.__setattr__(
            self,
            "market_regimes",
            tuple(item.strip() for item in self.market_regimes if item.strip()),
        )
        object.__setattr__(
            self,
            "entry_logic",
            tuple(item.strip() for item in self.entry_logic if item.strip()),
        )
        object.__setattr__(
            self,
            "exit_logic",
            tuple(item.strip() for item in self.exit_logic if item.strip()),
        )
        object.__setattr__(
            self,
            "verification_questions",
            tuple(item.strip() for item in self.verification_questions if item.strip()),
        )
        object.__setattr__(
            self,
            "parameter_constraints",
            tuple(item.strip() for item in self.parameter_constraints if item.strip()),
        )
        object.__setattr__(
            self,
            "optional_indicator_plan",
            tuple(dict(item) for item in self.optional_indicator_plan),
        )
        if not self.allowed_clock_profiles:
            raise ValueError("allowed_clock_profiles must not be empty")
        if self.intent is not None and not self.intent.strip():
            raise ValueError("intent must be non-empty when provided")
        role_columns = {requirement.column for requirement in self.indicator_requirements}
        missing_role_columns = tuple(sorted(role_columns - set(self.required_columns)))
        if missing_role_columns:
            raise ValueError(
                "indicator_requirements columns must be present in required_columns: "
                + ", ".join(missing_role_columns)
            )

    def parameter_names(self) -> tuple[str, ...]:
        return tuple(parameter.name for parameter in self.parameter_grid)

    def parameter_space_by_role(self) -> dict[str, dict[str, tuple[object, ...]]]:
        grouped: dict[str, dict[str, tuple[object, ...]]] = {}
        for parameter in self.parameter_grid:
            grouped.setdefault(parameter.role, {})[parameter.name] = tuple(parameter.values)
        return grouped

    def parameter_clock_map(self) -> tuple[dict[str, object], ...]:
        return tuple(parameter.to_dict() for parameter in self.parameter_grid)

    def required_columns_for_role(self, role: StrategyIndicatorRole) -> tuple[str, ...]:
        return tuple(
            requirement.column
            for requirement in self.indicator_requirements
            if requirement.role == role
        )

    def required_columns_for_timeframe(self, timeframe: str) -> tuple[str, ...]:
        return tuple(
            requirement.column
            for requirement in self.indicator_requirements
            if requirement.timeframe == timeframe
        )

    def parameter_combinations(self) -> tuple[dict[str, object], ...]:
        if not self.parameter_grid:
            return ({},)
        names = self.parameter_names()
        value_lists = [parameter.values for parameter in self.parameter_grid]
        rows = tuple(
            dict(zip(names, combo, strict=True))
            for combo in product(*value_lists)
        )
        if not self.parameter_constraints:
            return rows
        return tuple(
            row
            for row in rows
            if all(_constraint_passes(expression, row) for expression in self.parameter_constraints)
        )
