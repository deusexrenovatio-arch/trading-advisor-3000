from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SparkJobSpec:
    app_name: str
    source_table: str
    target_table: str


def default_spec() -> SparkJobSpec:
    return SparkJobSpec(
        app_name="ta3000-phase2a-canonical-bars",
        source_table="raw_market_backfill",
        target_table="canonical_bars",
    )


def build_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_table}
SELECT contract_id, timeframe, ts_open, ts_close, open, high, low, close, volume
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY contract_id, timeframe, ts_open
               ORDER BY ts_close DESC
           ) AS rn
    FROM {spec.source_table}
) source
WHERE rn = 1
""".strip()
