from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_phase_policy import (  # noqa: E402
    ACCEPTANCE_ROUTE_SIGNAL,
    REMEDIATION_ROUTE_SIGNAL,
    WORKER_ROUTE_SIGNAL,
    normalize_acceptance_payload,
    normalize_worker_payload,
)


def _worker_payload(route_signal: str) -> dict[str, object]:
    return {
        "status": "DONE",
        "summary": "ok",
        "route_signal": route_signal,
        "files_touched": [],
        "checks_run": [],
        "remaining_risks": [],
        "assumptions": [],
        "skips": [],
        "fallbacks": [],
        "deferred_work": [],
    }


def _acceptance_payload(route_signal: str) -> dict[str, object]:
    return {
        "verdict": "PASS",
        "summary": "ok",
        "route_signal": route_signal,
        "used_skills": [
            "phase-acceptance-governor",
            "architecture-review",
            "testing-suite",
            "docs-sync",
        ],
        "blockers": [],
        "rerun_checks": [],
        "evidence_gaps": [],
        "prohibited_findings": [],
    }


def test_normalize_worker_payload_requires_contract_route_signal() -> None:
    normalized = normalize_worker_payload(_worker_payload(WORKER_ROUTE_SIGNAL), role="worker")
    assert normalized.route_signal == WORKER_ROUTE_SIGNAL


def test_normalize_worker_payload_rejects_unexpected_route_signal() -> None:
    with pytest.raises(ValueError, match="invalid `route_signal`"):
        normalize_worker_payload(_worker_payload("worker:anything-else"), role="worker")


def test_normalize_remediation_payload_requires_remediation_route_signal() -> None:
    normalized = normalize_worker_payload(_worker_payload(REMEDIATION_ROUTE_SIGNAL), role="remediation")
    assert normalized.route_signal == REMEDIATION_ROUTE_SIGNAL


def test_normalize_remediation_payload_rejects_worker_route_signal() -> None:
    with pytest.raises(ValueError, match="invalid `route_signal`"):
        normalize_worker_payload(_worker_payload(WORKER_ROUTE_SIGNAL), role="remediation")


def test_normalize_acceptance_payload_requires_contract_route_signal() -> None:
    normalized = normalize_acceptance_payload(_acceptance_payload(ACCEPTANCE_ROUTE_SIGNAL))
    assert normalized.route_signal == ACCEPTANCE_ROUTE_SIGNAL


def test_normalize_acceptance_payload_rejects_unexpected_route_signal() -> None:
    with pytest.raises(ValueError, match="invalid `route_signal`"):
        normalize_acceptance_payload(_acceptance_payload("acceptance:anything-else"))
