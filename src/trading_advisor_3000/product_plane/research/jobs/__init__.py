from __future__ import annotations

from typing import Any


def __getattr__(name: str) -> Any:
    if name == "run_campaign_job":
        from .run_campaign import run_campaign_job

        return run_campaign_job
    raise AttributeError(name)


__all__ = [
    "run_campaign_job",
]
