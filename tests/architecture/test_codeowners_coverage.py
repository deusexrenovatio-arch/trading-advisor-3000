from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


REQUIRED_COVERAGE = (
    "/docs/agent/",
    "/docs/agent-contexts/",
    "/docs/checklists/",
    "/docs/workflows/",
    "/docs/runbooks/",
    "/docs/architecture/",
    "/scripts/",
    "/plans/",
    "/memory/",
    "/tests/",
    "/src/trading_advisor_3000/",
)


def test_codeowners_covers_required_shell_paths() -> None:
    text = (ROOT / "CODEOWNERS").read_text(encoding="utf-8")
    for token in REQUIRED_COVERAGE:
        assert token in text, f"missing CODEOWNERS coverage for {token}"
