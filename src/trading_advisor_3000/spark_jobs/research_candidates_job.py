from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchSparkJobSpec:
    app_name: str
    source_table: str
    target_table: str
    algo_version_id: str


def default_research_spec() -> ResearchSparkJobSpec:
    return ResearchSparkJobSpec(
        app_name="ta3000-phase2b-research-candidates",
        source_table="feature_snapshots",
        target_table="research_signal_candidates",
        algo_version_id="algo-trend-follow-v1",
    )


def build_research_sql_plan(spec: ResearchSparkJobSpec | None = None) -> str:
    spec = spec or default_research_spec()
    return f"""
INSERT OVERWRITE TABLE {spec.target_table}
SELECT
  sha2(concat(contract_id, timeframe, ts, '{spec.algo_version_id}'), 256) AS candidate_id,
  '{spec.algo_version_id}' AS algo_version_id,
  contract_id,
  timeframe,
  ts AS ts_signal,
  CASE
    WHEN ema_fast > ema_slow AND rvol >= 1.0 THEN 'long'
    WHEN ema_fast < ema_slow AND rvol >= 1.0 THEN 'short'
    ELSE 'flat'
  END AS side,
  LEAST(1.0, ABS(ema_fast - ema_slow) / NULLIF(atr, 0.0)) AS score
FROM {spec.source_table}
""".strip()
