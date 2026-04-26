from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from types import ModuleType


@dataclass(frozen=True)
class ResearchDependencyRequirement:
    capability_name: str
    distributions: tuple[str, ...]
    import_names: tuple[str, ...]
    rationale: str


@dataclass(frozen=True)
class ResolvedResearchDependency:
    requirement: ResearchDependencyRequirement
    import_name: str
    module: ModuleType


class MissingResearchDependencyError(ImportError):
    def __init__(
        self,
        requirement: ResearchDependencyRequirement,
        *,
        attempts: tuple[str, ...],
    ) -> None:
        self.requirement = requirement
        self.attempts = attempts
        packages = " or ".join(requirement.distributions)
        modules = ", ".join(attempts)
        super().__init__(
            f"{requirement.capability_name} is part of the mandatory research contour and could not be imported. "
            f"Tried imports: {modules}. "
            f"Install {packages} in the base environment."
        )


VECTORBT_REQUIREMENT = ResearchDependencyRequirement(
    capability_name="vectorbt",
    distributions=("vectorbt",),
    import_names=("vectorbt",),
    rationale="VectorBT owns the future vectorized backtest and portfolio simulation hot path.",
)

PANDAS_TA_REQUIREMENT = ResearchDependencyRequirement(
    capability_name="pandas-ta",
    distributions=("pandas-ta-classic", "pandas-ta"),
    import_names=("pandas_ta_classic", "pandas_ta"),
    rationale=(
        "Phase 1 targets pandas-ta semantics, but the Python 3.11-compatible distribution is "
        "currently provided through pandas-ta-classic."
    ),
)


def research_dependencies() -> tuple[ResearchDependencyRequirement, ...]:
    return (VECTORBT_REQUIREMENT, PANDAS_TA_REQUIREMENT)


def resolve_research_dependency(requirement: ResearchDependencyRequirement) -> ResolvedResearchDependency:
    attempted: list[str] = []
    for import_name in requirement.import_names:
        attempted.append(import_name)
        try:
            module = import_module(import_name)
        except ModuleNotFoundError:
            continue
        return ResolvedResearchDependency(
            requirement=requirement,
            import_name=import_name,
            module=module,
        )
    raise MissingResearchDependencyError(requirement, attempts=tuple(attempted))


def ensure_research_dependencies() -> dict[str, ResolvedResearchDependency]:
    resolved: dict[str, ResolvedResearchDependency] = {}
    for requirement in research_dependencies():
        resolved[requirement.capability_name] = resolve_research_dependency(requirement)
    return resolved
