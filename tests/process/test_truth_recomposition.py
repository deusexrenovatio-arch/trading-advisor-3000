from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from truth_recomposition import build_report, validate_report  # noqa: E402


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _followup_contract(path: Path) -> None:
    _write(
        path,
        {
            "route": "stacked-followup",
            "predecessor_merge_context": {
                "ref": "merge-123",
                "resolved_sha": "sha-merge",
                "merged_into_new_base": True,
            },
            "surface_contract": {
                "allowed_to_carry_forward": ["runtime_api", "publisher"],
                "temporary_downgrade_surfaces": ["legacy_truth_patch"],
            },
        },
    )


def test_build_report_marks_ready_when_temporary_downgrades_are_restored(tmp_path: Path) -> None:
    contract = tmp_path / "followup.json"
    report = tmp_path / "recomposition.json"
    _followup_contract(contract)

    payload = build_report(
        followup_contract=contract,
        merged_surfaces=["legacy_truth_patch", "runtime_api"],
        candidate_surfaces=["runtime_api", "publisher"],
        output_path=report,
    )

    assert payload["status"] == "ready"
    assert payload["analysis"]["lingering_temporary_downgrades"] == []
    assert payload["analysis"]["out_of_contract_surfaces"] == []


def test_build_report_marks_blocked_when_temporary_downgrade_lingers(tmp_path: Path) -> None:
    contract = tmp_path / "followup.json"
    report = tmp_path / "recomposition.json"
    _followup_contract(contract)

    payload = build_report(
        followup_contract=contract,
        merged_surfaces=["runtime_api"],
        candidate_surfaces=["runtime_api", "legacy_truth_patch"],
        output_path=report,
    )

    assert payload["status"] == "blocked"
    assert payload["analysis"]["lingering_temporary_downgrades"] == ["legacy_truth_patch"]


def test_validate_report_fails_closed_for_blocked_payload(tmp_path: Path) -> None:
    report = tmp_path / "recomposition.json"
    _write(
        report,
        {
            "status": "blocked",
            "contract": {"predecessor_merged": True},
            "analysis": {
                "lingering_temporary_downgrades": ["legacy_truth_patch"],
                "out_of_contract_surfaces": [],
            },
        },
    )

    ok, errors = validate_report(report)

    assert ok is False
    assert any("status is not `ready`" in item for item in errors)
    assert any("temporary downgrade surfaces" in item for item in errors)

