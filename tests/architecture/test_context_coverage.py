from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_validate_agent_contexts_passes() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/validate_agent_contexts.py"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_context_coverage_config_declares_high_risk_contract_context() -> None:
    payload = yaml.safe_load((ROOT / "configs/context_coverage.yaml").read_text(encoding="utf-8"))
    assert payload["required_high_risk_context"] == "CTX-CONTRACTS"
    assert "plans/" in payload["high_risk_paths"]
    required_cards = set(payload["required_context_cards"])
    assert "CTX-DATA" in required_cards
    assert "CTX-RESEARCH" in required_cards
    assert "CTX-ORCHESTRATION" in required_cards
    assert "CTX-API-UI" in required_cards
