from __future__ import annotations

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
        app_name="ta3000-research-governed-candidates",
        feature_source_table="gold_feature_snapshot",
        backtest_runs_source_table="research_backtest_runs",
        target_table="research_runtime_candidate_projection",
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
    projection_id_expr = (
        "concat('CP-', upper(substr(sha2(concat(" + candidate_expr + ", '|', run.backtest_run_id), 256), 1, 12)))"
    )
    reproducibility_expr = (
        "concat('RFP-', upper(substr(sha2(concat(run.backtest_run_id, '|', run.params_hash, '|1000000'), 256), 1, 12)))"
    )
    return f"""
WITH latest_run AS (
  SELECT
    backtest_run_id,
    {version_col},
    dataset_version,
    params_hash
  FROM {spec.backtest_runs_source_table}
  WHERE {version_col} = '{spec.version_id_filter}'
  ORDER BY finished_at DESC
  LIMIT 1
),
scored_features AS (
  SELECT
    snapshot_id,
    contract_id,
    instrument_id,
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
  WHERE timeframe IN ('15m', '1h', '4h', '1d', '1w')
)
INSERT OVERWRITE TABLE {spec.target_table}
SELECT
  {projection_id_expr} AS candidate_projection_id,
  {candidate_expr} AS candidate_id,
  run.backtest_run_id,
  run.{version_col} AS strategy_version_id,
  CASE
    WHEN run.{version_col} LIKE 'trend-%' THEN 'trend-following'
    WHEN run.{version_col} LIKE 'mean-%' THEN 'mean-reversion'
    ELSE 'breakout-volatility'
  END AS strategy_family,
  sf.contract_id,
  sf.instrument_id,
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
  sf.score,
  'wf-1' AS window_id,
  0.0 AS estimated_commission,
  0.0 AS estimated_slippage,
  CAST(1000000 AS BIGINT) AS capital_rub,
  {reproducibility_expr} AS reproducibility_fingerprint
FROM scored_features sf
CROSS JOIN latest_run run
WHERE sf.side <> 'flat'
""".strip()
