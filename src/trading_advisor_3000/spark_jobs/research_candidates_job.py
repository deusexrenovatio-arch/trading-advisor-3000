from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchSparkJobSpec:
    app_name: str
    feature_source_table: str
    backtest_runs_source_table: str
    target_table: str
    strategy_version_id: str


def default_research_spec() -> ResearchSparkJobSpec:
    return ResearchSparkJobSpec(
        app_name="ta3000-phase2b-research-candidates",
        feature_source_table="feature_snapshots",
        backtest_runs_source_table="research_backtest_runs",
        target_table="research_signal_candidates",
        strategy_version_id="trend-follow-v1",
    )


def _spark_ts_iso_utc_expr(*, ts_column: str) -> str:
    return f"concat(regexp_replace(cast({ts_column} AS string), ' ', 'T'), 'Z')"


def spark_candidate_id_expr(
    *,
    strategy_version_column: str,
    contract_id_column: str,
    timeframe_column: str,
    ts_signal_column: str,
) -> str:
    ts_expr = _spark_ts_iso_utc_expr(ts_column=ts_signal_column)
    return (
        "concat('CAND-', upper(substr(sha2(concat("
        f"{strategy_version_column}, '|', {contract_id_column}, '|', {timeframe_column}, '|', {ts_expr}"
        "), 256), 1, 12)))"
    )


def build_research_sql_plan(spec: ResearchSparkJobSpec | None = None) -> str:
    spec = spec or default_research_spec()
    candidate_expr = spark_candidate_id_expr(
        strategy_version_column="run.strategy_version_id",
        contract_id_column="sf.contract_id",
        timeframe_column="sf.timeframe",
        ts_signal_column="sf.ts_signal",
    )
    return f"""
WITH latest_run AS (
  SELECT
    backtest_run_id,
    strategy_version_id,
    dataset_version
  FROM {spec.backtest_runs_source_table}
  WHERE strategy_version_id = '{spec.strategy_version_id}'
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
  run.strategy_version_id,
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
