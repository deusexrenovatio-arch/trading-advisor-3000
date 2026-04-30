from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .engine import PRICE_INPUTS, StrategyFamilySearchSpec


@dataclass(frozen=True)
class BacktestInputColumns:
    price_columns: tuple[str, ...]
    indicator_columns: tuple[str, ...]
    derived_columns: tuple[str, ...]


def _append_unique(values: list[str], value: object) -> None:
    text = str(value).strip()
    if text and text not in values:
        values.append(text)


def _payload_aliases(payload: Mapping[str, object], name: str) -> tuple[str, ...]:
    raw = payload.get(name, ())
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, Iterable):
        return tuple(str(item) for item in raw)
    return tuple()


def loader_columns_for_search_specs(search_specs: Iterable[StrategyFamilySearchSpec]) -> BacktestInputColumns:
    price_columns = ["close"]
    indicator_columns: list[str] = []
    derived_columns: list[str] = []
    price_universe = set(PRICE_INPUTS)

    for spec in search_specs:
        for column in spec.required_price_inputs:
            if column in price_universe:
                _append_unique(price_columns, column)
        for column in spec.required_materialized_indicators:
            _append_unique(indicator_columns, column)
        for column in spec.required_materialized_derived:
            _append_unique(derived_columns, column)
        for optional in spec.optional_indicator_plan:
            if optional.source == "materialized_delta":
                _append_unique(indicator_columns, optional.alias)
        for payload in spec.required_inputs_by_clock.values():
            if not isinstance(payload, Mapping):
                continue
            for column in _payload_aliases(payload, "price_inputs"):
                if column in price_universe:
                    _append_unique(price_columns, column)
            for column in _payload_aliases(payload, "materialized_indicators"):
                _append_unique(indicator_columns, column)
            for column in _payload_aliases(payload, "materialized_derived"):
                _append_unique(derived_columns, column)

    return BacktestInputColumns(
        price_columns=tuple(price_columns),
        indicator_columns=tuple(indicator_columns),
        derived_columns=tuple(derived_columns),
    )
