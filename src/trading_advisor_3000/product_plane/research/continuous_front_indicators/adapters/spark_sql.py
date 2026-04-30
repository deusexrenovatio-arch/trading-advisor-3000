from __future__ import annotations

import pandas as pd

from trading_advisor_3000.product_plane.research.continuous_front_indicators.rules import (
    ADAPTER_VERSION,
    IndicatorRollRule,
)

from .base import AdapterContext, required_columns_from_rules


class SparkSqlAdapter:
    adapter_id = "spark_sql_adapter"
    adapter_version = ADAPTER_VERSION
    supported_formula_ids: set[str] = set()

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
        result = partition_df[[column for column in partition_df.columns if column in approved]].copy()
        qc = pd.DataFrame(
            [
                {
                    "check_id": "spark_sql_adapter_contract",
                    "status": "pass",
                    "observed_value": ",".join(sorted(result.columns)),
                }
            ]
        )
        return result, qc
