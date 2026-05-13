from __future__ import annotations

import importlib.util
from pathlib import Path


def test_legacy_research_benchmark_python_l0_bootstrap_is_removed(tmp_path: Path) -> None:
    del tmp_path
    import trading_advisor_3000.product_plane.research.jobs as research_jobs

    assert not hasattr(research_jobs, "run_benchmark_job")
    assert importlib.util.find_spec("trading_advisor_3000.product_plane.research.jobs.benchmark") is None
