from __future__ import annotations

from typing import TYPE_CHECKING

from .pipeline import run_sample_backfill
from .providers import DataProviderRegistry, DataProviderSpec, default_data_provider_registry


if TYPE_CHECKING:
    from .moex import (
        run_phase01_foundation,
        run_phase02_canonical,
        run_phase03_reconciliation,
        run_phase04_production_hardening,
    )


_MOEX_EXPORTS = {
    "run_phase01_foundation",
    "run_phase02_canonical",
    "run_phase03_reconciliation",
    "run_phase04_production_hardening",
}


def __getattr__(name: str) -> object:
    if name in _MOEX_EXPORTS:
        from . import moex as _moex

        value = getattr(_moex, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DataProviderRegistry",
    "DataProviderSpec",
    "default_data_provider_registry",
    "run_phase01_foundation",
    "run_phase02_canonical",
    "run_phase03_reconciliation",
    "run_phase04_production_hardening",
    "run_sample_backfill",
]
