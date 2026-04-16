from __future__ import annotations

import warnings

from trading_advisor_3000.product_plane.research.compat import run_legacy_research_from_bars
from trading_advisor_3000.spark_jobs.research_candidates_job import build_research_sql_plan


def test_legacy_research_entrypoint_warns_as_deprecated() -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        try:
            run_legacy_research_from_bars(
                bars=[],
                instrument_by_contract={},
                strategy_version_id="trend-follow-v1",
                dataset_version="noop",
                output_dir=None,  # type: ignore[arg-type]
            )
        except Exception:
            pass
    assert any(item.category is DeprecationWarning for item in captured)


def test_legacy_spark_candidate_bridge_warns_as_deprecated() -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        build_research_sql_plan()
    assert any(item.category is DeprecationWarning for item in captured)
