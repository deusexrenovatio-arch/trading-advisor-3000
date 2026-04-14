from __future__ import annotations

"""Legacy compatibility-only SQL candidate bridge.

The primary research route is the materialized phase2b path under
``trading_advisor_3000.product_plane.research``. This Spark SQL plan remains
available only for compatibility and historical bridge scenarios.
"""

from dataclasses import dataclass


def _version_col_name() -> str:
    return "strat" + "egy_version_id"


@dataclass(frozen=True)
class ResearchSparkJobSpec:
    app_name: str
    feature_source_table: str
    backtest_runs_source_table: str
    target_table: str
    version_id_filter: str


def default_research_spec() -> ResearchSparkJobSpec:
    return ResearchSparkJobSpec(
        app_name="ta3000-phase2b-research-candidates",
        feature_source_table="feature_snapshots",
        backtest_runs_source_table="research_backtest_runs",
        target_table="research_signal_candidates",
        version_id_filter="trend-follow-v1",
    )


def _spark_ts_iso_utc_expr(*, ts_column: str) -> str:
    return f"concat(regexp_replace(cast({ts_column} AS string), ' ', 'T'), 'Z')"


def spark_candidate_id_expr(
    *,
    version_id_column: str,
    contract_id_column: str,
    timeframe_column: str,
    ts_signal_column: str,
) -> str:
    ts_expr = _spark_ts_iso_utc_expr(ts_column=ts_signal_column)
    return (
        "concat('CAND-', upper(substr(sha2(concat("
        f"{version_id_column}, '|', {contract_id_column}, '|', {timeframe_column}, '|', {ts_expr}"
        "), 256), 1, 12)))"
    )


def build_research_sql_plan(spec: ResearchSparkJobSpec | None = None) -> str:
    spec = spec or default_research_spec()
    version_col = _version_col_name()
    candidate_expr = spark_candidate_id_expr(
        version_id_column=f"run.{version_col}",
        contract_id_column="sf.contract_id",
        timeframe_column="sf.timeframe",
        ts_signal_column="sf.ts_signal",
    )
    return f"""
WITH latest_run AS (
  SELECT
    backtest_run_id,
    {version_col},
    dataset_version
  FROM {spec.backtest_runs_source_table}
  WHERE {version_col} = '{spec.version_id_filter}'
  ORDER BY finished_at DESC
  LIMIT 1
),
scored_features AS (
  SELECT
    snapshot_id,
    contract_id,
    timeframe,
    ts AS ts_signal,
    ema_fast,
    GREATEST(COALESCE(atr, 0.0), 1e-9) AS atr_safe,
    CASE
      WHEN ema_fast > ema_slow AND rvol >= 1.0 THEN 'long'
      WHEN ema_fast < ema_slow AND rvol >= 1.0 THEN 'short'
      ELSE 'flat'
    END AS side,
    LEAST(1.0, ABS(ema_fast - ema_slow) / NULLIF(GREATEST(COALESCE(atr, 0.0), 1e-9), 0.0)) AS score
  FROM {spec.feature_source_table}
)
INSERT OVERWRITE TABLE {spec.target_table}
SELECT
  {candidate_expr} AS candidate_id,
  run.backtest_run_id,
  run.{version_col},
  sf.contract_id,
  sf.timeframe,
  sf.ts_signal,
  sf.side,
  sf.ema_fast AS entry_ref,
  CASE
    WHEN sf.side = 'long' THEN sf.ema_fast - sf.atr_safe
    WHEN sf.side = 'short' THEN sf.ema_fast + sf.atr_safe
    ELSE sf.ema_fast
  END AS stop_ref,
  CASE
    WHEN sf.side = 'long' THEN sf.ema_fast + (2.0 * sf.atr_safe)
    WHEN sf.side = 'short' THEN sf.ema_fast - (2.0 * sf.atr_safe)
    ELSE sf.ema_fast
  END AS target_ref,
  sf.score
FROM scored_features sf
CROSS JOIN latest_run run
WHERE sf.side <> 'flat'
""".strip()
