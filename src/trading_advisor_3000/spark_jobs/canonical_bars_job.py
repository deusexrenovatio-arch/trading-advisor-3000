from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SparkJobSpec:
    app_name: str
    source_table: str
    target_bars_table: str
    target_instruments_table: str
    target_contracts_table: str
    target_session_calendar_table: str
    target_roll_map_table: str


def default_spec() -> SparkJobSpec:
    return SparkJobSpec(
        app_name="ta3000-phase2a-canonical-bars",
        source_table="raw_market_backfill",
        target_bars_table="canonical_bars",
        target_instruments_table="canonical_instruments",
        target_contracts_table="canonical_contracts",
        target_session_calendar_table="canonical_session_calendar",
        target_roll_map_table="canonical_roll_map",
    )


def build_bars_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_bars_table}
SELECT contract_id, instrument_id, timeframe, ts_open AS ts, open, high, low, close, volume, open_interest
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


def build_instruments_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_instruments_table}
SELECT DISTINCT instrument_id
FROM {spec.source_table}
""".strip()


def build_contracts_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_contracts_table}
SELECT
  contract_id,
  MIN(instrument_id) AS instrument_id,
  MIN(ts_open) AS first_seen_ts,
  MAX(ts_close) AS last_seen_ts
FROM {spec.source_table}
GROUP BY contract_id
""".strip()


def build_session_calendar_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_session_calendar_table}
SELECT
  instrument_id,
  timeframe,
  to_date(ts_open) AS session_date,
  MIN(ts_open) AS session_open_ts,
  MAX(ts_close) AS session_close_ts
FROM {spec.source_table}
GROUP BY instrument_id, timeframe, to_date(ts_open)
""".strip()


def build_roll_map_sql_plan(spec: SparkJobSpec | None = None) -> str:
    spec = spec or default_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_roll_map_table}
SELECT
  instrument_id,
  session_date,
  contract_id AS active_contract_id,
  'max_open_interest_then_latest_ts_close' AS reason
FROM (
  SELECT
    instrument_id,
    contract_id,
    to_date(ts_open) AS session_date,
    open_interest,
    ts_close,
    ROW_NUMBER() OVER (
      PARTITION BY instrument_id, to_date(ts_open)
      ORDER BY open_interest DESC, ts_close DESC
    ) AS rn
  FROM {spec.source_table}
) ranked
WHERE rn = 1
""".strip()


def build_sql_plan(spec: SparkJobSpec | None = None) -> str:
    return build_bars_sql_plan(spec)
