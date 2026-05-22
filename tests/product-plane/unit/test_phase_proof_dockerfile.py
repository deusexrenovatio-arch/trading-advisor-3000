from __future__ import annotations

from pathlib import Path


def test_phase_proof_dockerfile_installs_research_indicator_dependency() -> None:
    dockerfile = Path("deployment/docker/phase-proofs/Dockerfile")
    text = dockerfile.read_text(encoding="utf-8")

    assert '"pandas-ta-classic>=0.4.47,<0.5"' in text
    assert '"vectorbt>=0.28.5,<0.29"' in text
    assert '"optuna>=4,<5"' in text
