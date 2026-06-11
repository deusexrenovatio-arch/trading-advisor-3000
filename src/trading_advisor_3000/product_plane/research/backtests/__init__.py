from __future__ import annotations

from .batch_runner import (
    BacktestBatchRequest,
    EphemeralStrategySpace,
    build_ephemeral_strategy_space,
    run_backtest_batch,
)
from .engine import (
    BacktestEngineConfig,
    IndicatorInputPlan,
    StrategyFamilySearchSpec,
    VectorBTIndicatorPlan,
    VectorBTInputBundle,
    VectorBTSignalSurfaceResult,
    project_family_candidate,
)
from .evaluation import build_strategy_evaluation_profiles
from .projection import (
    CandidateProjectionRequest,
    project_runtime_candidates,
    supported_selection_policies,
)
from .ranking import (
    DEFAULT_RANKING_MAX_DRAWDOWN_CAP,
    DEFAULT_RANKING_METRIC_ORDER,
    DEFAULT_RANKING_MIN_FOLD_COUNT,
    DEFAULT_RANKING_MIN_PARAMETER_STABILITY,
    DEFAULT_RANKING_MIN_POSITIVE_FOLD_RATIO,
    DEFAULT_RANKING_MIN_SLIPPAGE_SCORE,
    DEFAULT_RANKING_MIN_TRADE_COUNT,
    DEFAULT_RANKING_MIN_TRADE_COUNT_PER_FOLD,
    DEFAULT_RANKING_POLICY_ID,
    DEFAULT_RANKING_STRESS_SLIPPAGE_BPS,
    RankingPolicy,
    default_ranking_policy,
    rank_backtest_results,
    score_optimizer_trial,
)
from .results import (
    BacktestBatchArtifact,
    BacktestRunArtifact,
    backtest_store_contract,
    results_store_contract,
)

__all__ = [
    "BacktestBatchArtifact",
    "BacktestBatchRequest",
    "BacktestEngineConfig",
    "BacktestRunArtifact",
    "CandidateProjectionRequest",
    "DEFAULT_RANKING_MAX_DRAWDOWN_CAP",
    "DEFAULT_RANKING_METRIC_ORDER",
    "DEFAULT_RANKING_MIN_FOLD_COUNT",
    "DEFAULT_RANKING_MIN_PARAMETER_STABILITY",
    "DEFAULT_RANKING_MIN_POSITIVE_FOLD_RATIO",
    "DEFAULT_RANKING_MIN_SLIPPAGE_SCORE",
    "DEFAULT_RANKING_MIN_TRADE_COUNT",
    "DEFAULT_RANKING_MIN_TRADE_COUNT_PER_FOLD",
    "DEFAULT_RANKING_POLICY_ID",
    "DEFAULT_RANKING_STRESS_SLIPPAGE_BPS",
    "EphemeralStrategySpace",
    "RankingPolicy",
    "IndicatorInputPlan",
    "StrategyFamilySearchSpec",
    "VectorBTIndicatorPlan",
    "VectorBTInputBundle",
    "VectorBTSignalSurfaceResult",
    "build_ephemeral_strategy_space",
    "build_strategy_evaluation_profiles",
    "default_ranking_policy",
    "backtest_store_contract",
    "results_store_contract",
    "project_runtime_candidates",
    "project_family_candidate",
    "rank_backtest_results",
    "run_backtest_batch",
    "score_optimizer_trial",
    "supported_selection_policies",
]
