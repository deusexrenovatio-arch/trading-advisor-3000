from __future__ import annotations

import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_plans import run  # noqa: E402


def test_validate_plans_current_registry_ok() -> None:
    assert run(ROOT / "plans" / "PLANS.yaml") == 0


def test_validate_plans_rejects_duplicate_ids(tmp_path: Path) -> None:
    payload = {
        "version": 1,
        "updated_at": "2026-03-16",
        "items": [
            {
                "id": "P1-DUP",
                "title": "One",
                "lane": "lane-a",
                "status": "active",
                "execution_mode": "assisted",
                "owner": "team",
                "acceptance": ["ok"],
                "checks": ["python scripts/validate_plans.py"],
                "dependencies": [],
                "started_at": "2026-03-16",
            },
            {
                "id": "P1-DUP",
                "title": "Two",
                "lane": "lane-b",
                "status": "planned",
                "execution_mode": "manual",
                "owner": "team",
                "acceptance": ["ok"],
                "checks": ["python scripts/validate_plans.py"],
                "dependencies": [],
            },
        ],
    }
    target = tmp_path / "PLANS.yaml"
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    assert run(target) == 1
