from __future__ import annotations

import pandas as pd

from trading_advisor_3000.product_plane.research.continuous_front_indicators.rules import (
    ADAPTER_VERSION,
    IndicatorRollRule,
)

from .base import AdapterContext, required_columns_from_rules


class PandasTaOnP0Adapter:
    adapter_id = "pandas_ta_on_p0_adapter"
    adapter_version = ADAPTER_VERSION
    supported_formula_ids: set[str] = {
        "atr",
        "true_range",
        "rsi",
        "stoch",
        "adx",
        "chop",
        "aroon",
        "cci",
        "willr",
        "stochrsi",
        "ultimate_oscillator",
        "macd",
        "mom",
        "slope",
        "tsi",
        "trix",
        "kst",
    }

    def required_columns(self, rules: tuple[IndicatorRollRule, ...]) -> tuple[str, ...]:
        return required_columns_from_rules(rules)

    def compute_partition(
        self,
        partition_df: pd.DataFrame,
        rules: tuple[IndicatorRollRule, ...],
        context: AdapterContext,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        del context
        approved = {rule.output_column for rule in rules if rule.group.adapter_id == self.adapter_id}
        unexpected = sorted(set(partition_df.columns) & approved - approved)
        qc = pd.DataFrame(
            [
                {
                    "check_id": "pandas_ta_on_p0_adapter_contract",
                    "status": "pass" if not unexpected else "fail",
                    "observed_value": ",".join(unexpected),
                }
            ]
        )
        return partition_df[[column for column in partition_df.columns if column in approved]].copy(), qc
