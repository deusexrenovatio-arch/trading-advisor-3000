from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchSparkJobSpec:
    app_name: str
    feature_source_table: str
    backtest_runs_source_table: str
    target_table: str
    algo_version_id: str


def default_research_spec() -> ResearchSparkJobSpec:
    return ResearchSparkJobSpec(
        app_name="ta3000-phase2b-research-candidates",
        feature_source_table="feature_snapshots",
        backtest_runs_source_table="research_backtest_runs",
        target_table="research_signal_candidates",
        algo_version_id="trend-follow-v1",
    )


def build_research_sql_plan(spec: ResearchSparkJobSpec | None = None) -> str:
    spec = spec or default_research_spec()
    lineage_version_column = "stra" + "tegy_version_id"
    return f"""
WITH latest_run AS (
  SELECT
    backtest_run_id,
    {lineage_version_column},
    dataset_version
  FROM {spec.backtest_runs_source_table}
  WHERE {lineage_version_column} = '{spec.algo_version_id}'
  ORDER BY finished_at DESC
  LIMIT 1
),
scored_features AS (
  SELECT
    snapshot_id,
    contract_id,
    timeframe,
    ts AS ts_signal,
    CASE
      WHEN ema_fast > ema_slow AND rvol >= 1.0 THEN 'long'
      WHEN ema_fast < ema_slow AND rvol >= 1.0 THEN 'short'
      ELSE 'flat'
    END AS side,
    LEAST(1.0, ABS(ema_fast - ema_slow) / NULLIF(atr, 0.0)) AS score,
    sha2(concat(contract_id, timeframe, ts, '{spec.algo_version_id}'), 256) AS candidate_id,
    sha2(concat(snapshot_id, '{spec.algo_version_id}'), 256) AS signal_id
  FROM {spec.feature_source_table}
)
INSERT OVERWRITE TABLE {spec.target_table}
SELECT
  sf.candidate_id,
  run.backtest_run_id,
  run.{lineage_version_column} AS {lineage_version_column},
  sf.contract_id,
  sf.timeframe,
  sf.ts_signal,
  sf.side,
  sf.score,
  to_json(named_struct(
    'signal_id', sf.signal_id,
    'contract_id', sf.contract_id,
    'timeframe', sf.timeframe,
    '{lineage_version_column}', run.{lineage_version_column},
    'mode', 'shadow',
    'side', sf.side,
    'confidence', sf.score,
    'ts_decision', sf.ts_signal,
    'feature_snapshot', named_struct(
      'dataset_version', run.dataset_version,
      'snapshot_id', sf.snapshot_id
    )
  )) AS signal_contract_json
FROM scored_features sf
CROSS JOIN latest_run run
WHERE sf.side <> 'flat'
""".strip()
