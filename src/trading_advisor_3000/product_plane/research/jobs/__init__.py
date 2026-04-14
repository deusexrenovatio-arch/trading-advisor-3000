from __future__ import annotations

from typing import Any


def __getattr__(name: str) -> Any:
    if name == "run_bootstrap_job":
        from .bootstrap import run_bootstrap_job

        return run_bootstrap_job
    if name == "run_backtest_job":
        from .backtest import run_backtest_job

        return run_backtest_job
    if name == "run_projection_job":
        from .project_candidates import run_projection_job

        return run_projection_job
    if name == "run_benchmark_job":
        from .benchmark import run_benchmark_job

        return run_benchmark_job
    raise AttributeError(name)


__all__ = ["run_bootstrap_job", "run_backtest_job", "run_projection_job", "run_benchmark_job"]
