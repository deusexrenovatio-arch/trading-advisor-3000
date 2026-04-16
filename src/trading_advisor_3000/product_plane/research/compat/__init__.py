from __future__ import annotations

import warnings


def run_legacy_research_from_bars(*args: object, **kwargs: object) -> dict[str, object]:
    warnings.warn(
        "run_legacy_research_from_bars is deprecated and kept only as a temporary compatibility bridge. "
        "Use trading_advisor_3000.product_plane.research.run_research_from_bars instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    from .legacy_pipeline import run_research_from_bars

    return run_research_from_bars(*args, **kwargs)


__all__ = ["run_legacy_research_from_bars"]
