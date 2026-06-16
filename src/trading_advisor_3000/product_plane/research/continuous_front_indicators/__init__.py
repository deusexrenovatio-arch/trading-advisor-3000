from __future__ import annotations

from typing import Any

from .contracts import CF_INDICATOR_TABLES, continuous_front_indicator_store_contract
from .input_projection import build_cf_indicator_input_rows, materialize_cf_indicator_input_frame
from .rules import (
    CALCULATION_GROUPS,
    DEFAULT_RULE_SET_VERSION,
    IndicatorRollRule,
    adapter_bundle_hash,
    default_indicator_roll_rules,
    rule_set_hash,
)

__all__ = [
    "CALCULATION_GROUPS",
    "CF_INDICATOR_TABLES",
    "DEFAULT_RULE_SET_VERSION",
    "IndicatorRollRule",
    "adapter_bundle_hash",
    "build_cf_indicator_input_rows",
    "continuous_front_indicator_store_contract",
    "default_indicator_roll_rules",
    "materialize_cf_indicator_input_frame",
    "rule_set_hash",
    "run_continuous_front_base_indicator_sidecar_job",
    "run_continuous_front_indicator_pandas_job",
]


def __getattr__(name: str) -> Any:
    if name in {
        "run_continuous_front_base_indicator_sidecar_job",
        "run_continuous_front_indicator_pandas_job",
    }:
        from .pandas_job import (
            run_continuous_front_base_indicator_sidecar_job,
            run_continuous_front_indicator_pandas_job,
        )

        return {
            "run_continuous_front_base_indicator_sidecar_job": (
                run_continuous_front_base_indicator_sidecar_job
            ),
            "run_continuous_front_indicator_pandas_job": (
                run_continuous_front_indicator_pandas_job
            ),
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
