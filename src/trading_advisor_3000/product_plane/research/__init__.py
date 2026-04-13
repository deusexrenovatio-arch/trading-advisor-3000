from __future__ import annotations

from .dependencies import MissingResearchDependencyError, ensure_research_dependencies
from .forward import ForwardObservation, build_forward_observations, candidate_id_from_signal


def run_research_from_bars(*args: object, **kwargs: object) -> dict[str, object]:
    """Compatibility bridge over the legacy snapshot-centric path.

    The primary Stage 5 path is the materialized research-plane backtest flow
    in ``trading_advisor_3000.product_plane.research.backtests``.
    """
    from .pipeline import run_research_from_bars as _run_research_from_bars

    return _run_research_from_bars(*args, **kwargs)

__all__ = [
    "ForwardObservation",
    "MissingResearchDependencyError",
    "build_forward_observations",
    "candidate_id_from_signal",
    "ensure_research_dependencies",
    "run_research_from_bars",
]
