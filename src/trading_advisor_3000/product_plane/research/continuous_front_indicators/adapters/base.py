from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd

from trading_advisor_3000.product_plane.research.continuous_front_indicators.rules import IndicatorRollRule


@dataclass(frozen=True)
class AdapterContext:
    dataset_version: str
    rule_set_version: str
    indicator_set_version: str
    derived_set_version: str = ""


class IndicatorAdapter(Protocol):
    adapter_id: str
    adapter_version: str
    supported_formula_ids: set[str]

    def required_columns(self, rules: tuple[IndicatorRollRule, ...]) -> tuple[str, ...]:
        ...

    def compute_partition(
        self,
        partition_df: pd.DataFrame,
        rules: tuple[IndicatorRollRule, ...],
        context: AdapterContext,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        ...


def required_columns_from_rules(rules: tuple[IndicatorRollRule, ...]) -> tuple[str, ...]:
    columns: list[str] = []
    for rule in rules:
        columns.extend(rule.adapter_input_columns)
    return tuple(dict.fromkeys(columns))
