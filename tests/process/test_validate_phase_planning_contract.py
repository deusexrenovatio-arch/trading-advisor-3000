from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_phase_planning_contract import run  # noqa: E402


def _valid_execution_contract() -> str:
    return """# Execution Contract
Updated: 2026-03-30 12:00 UTC

## Objective
- Close release blockers honestly.

## Release Target Contract
- Target Decision: ALLOW_RELEASE_READINESS
- Target Environment: real production contour with live data, configured publication chat, real broker path, and governed artifacts
- Mandatory Real Contours: live data ingestion, real publication contour, real broker contour
- Forbidden Proof Substitutes: docs-only, schema-only, fixture-only, mock-only, stub-only, smoke-only, staging-only
- Release-Blocking Surfaces: truth-source integrity, publication contour, broker contour, production_readiness
- Release-Ready Proof Class: live-real
"""


def _valid_phase_brief() -> str:
    return """# Module Phase Brief
Updated: 2026-03-30 12:00 UTC

## Phase
- Name: F1-X
- Status: planned

## Objective
- Close one real contour honestly.

## Release Gate Impact
- Surface Transition: publication_contour `planned -> implemented`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## What This Phase Does Not Prove
- Release readiness: this phase does not prove ALLOW_RELEASE_READINESS.
- Production-wide closure: this phase does not prove full production rollout or every external contour.
"""


def test_validate_phase_planning_contract_accepts_valid_artifacts(tmp_path: Path) -> None:
    contract_path = tmp_path / "docs" / "codex" / "contracts" / "demo.execution-contract.md"
    phase_path = tmp_path / "docs" / "codex" / "modules" / "demo.phase-01.md"
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    phase_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(_valid_execution_contract(), encoding="utf-8")
    phase_path.write_text(_valid_phase_brief(), encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=[
            "docs/codex/contracts/demo.execution-contract.md",
            "docs/codex/modules/demo.phase-01.md",
        ],
    )
    assert code == 0


def test_validate_phase_planning_contract_rejects_missing_phase_limits(tmp_path: Path) -> None:
    contract_path = tmp_path / "docs" / "codex" / "contracts" / "demo.execution-contract.md"
    phase_path = tmp_path / "docs" / "codex" / "modules" / "demo.phase-01.md"
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    phase_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(_valid_execution_contract(), encoding="utf-8")
    phase_path.write_text(
        """# Module Phase Brief
## Release Gate Impact
- Surface Transition: none
- Minimum Proof Class: doc
- Accepted State Label: prep_closed
""",
        encoding="utf-8",
    )

    code = run(
        tmp_path,
        changed_files_override=[
            "docs/codex/contracts/demo.execution-contract.md",
            "docs/codex/modules/demo.phase-01.md",
        ],
    )
    assert code == 1


def test_validate_phase_planning_contract_rejects_non_live_release_target(tmp_path: Path) -> None:
    contract_path = tmp_path / "docs" / "codex" / "contracts" / "demo.execution-contract.md"
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(
        """# Execution Contract
## Release Target Contract
- Target Decision: ALLOW_RELEASE_READINESS
- Target Environment: real production contour
- Mandatory Real Contours: live data, publication, broker
- Forbidden Proof Substitutes: docs-only, schema-only, fixture-only, mock-only, stub-only
- Release-Blocking Surfaces: publication_contour, broker_contour, production_readiness
- Release-Ready Proof Class: integration
""",
        encoding="utf-8",
    )

    code = run(
        tmp_path,
        changed_files_override=["docs/codex/contracts/demo.execution-contract.md"],
    )
    assert code == 1
