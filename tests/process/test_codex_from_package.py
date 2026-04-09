from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_from_package import (  # noqa: E402
    build_intake_handoff,
    build_intake_human_summary,
    build_intake_lane_prompt,
    build_materialization_prompt,
    build_prompt,
    choose_latest_package,
    collect_document_candidates,
    evaluate_intake_gate_payload,
    extract_docx_title,
    render_intake_human_summary_markdown,
    safe_extract_zip,
)


def test_choose_latest_package_prefers_newest_zip(tmp_path: Path) -> None:
    older = tmp_path / "older.zip"
    newer = tmp_path / "newer.zip"
    older.write_bytes(b"old")
    newer.write_bytes(b"new")
    os.utime(older, (1000, 1000))
    os.utime(newer, (2000, 2000))
    assert choose_latest_package(tmp_path) == newer


def test_safe_extract_zip_rejects_zip_slip(tmp_path: Path) -> None:
    package = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(package, "w") as archive:
        archive.writestr("../escape.md", "bad")

    target = tmp_path / "out"
    try:
        safe_extract_zip(package, target)
    except ValueError as exc:
        assert "unsafe path" in str(exc)
    else:
        raise AssertionError("expected zip slip validation to fail")


def test_collect_document_candidates_prefers_requirements_over_readme(tmp_path: Path) -> None:
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    (extracted / "README.md").write_text("# Overview\n", encoding="utf-8")
    (extracted / "TECHNICAL_REQUIREMENTS.md").write_text("# Technical Requirements\n", encoding="utf-8")

    candidates = collect_document_candidates(extracted)
    assert candidates
    assert candidates[0].rel_path == "TECHNICAL_REQUIREMENTS.md"
    assert candidates[0].score > candidates[1].score


def test_collect_document_candidates_prefers_phase_tz_over_verdict_outputs(tmp_path: Path) -> None:
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    (extracted / "01_phase_acceptance_verdict.md").write_text(
        "# Phase acceptance verdict\n\nThis is the acceptance verdict summary.\n",
        encoding="utf-8",
    )
    (extracted / "03_f1_full_closure_TZ.md").write_text(
        """# Technical requirements for F1

### Phase F1-A - Truth source
**Objective:** Align truth sources honestly.

**Acceptance gate**
- Truth sources agree.

**Disprover**
- Reinsert false claim and confirm validation fails.
""",
        encoding="utf-8",
    )

    candidates = collect_document_candidates(extracted)

    assert candidates
    assert candidates[0].rel_path == "03_f1_full_closure_TZ.md"
    assert candidates[0].score > candidates[1].score


def test_extract_docx_title_reads_first_text(tmp_path: Path) -> None:
    docx = tmp_path / "spec.docx"
    with zipfile.ZipFile(docx, "w") as archive:
        archive.writestr(
            "word/document.xml",
            "<w:document><w:body><w:p><w:r><w:t>Primary Specification</w:t></w:r></w:p></w:body></w:document>",
        )

    assert extract_docx_title(docx) == "Primary Specification"


def test_intake_gate_blocks_when_required_digests_are_missing() -> None:
    gate = evaluate_intake_gate_payload(
        {
            "technical_intake": {
                "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
                "review_summary": "Technical lane checked architecture assumptions.",
                "blockers": [],
            },
            "product_intake": {
                "created_docs": ["docs/codex/modules/demo.parent.md"],
                "review_summary": "Product lane checked user value assumptions.",
                "blockers": [],
            },
            "combined_gate": {"decision": "PASS"},
        }
    )
    assert gate["combined_gate"]["decision"] == "BLOCKED"
    technical_titles = [item["title"] for item in gate["technical_intake"]["blockers"]]
    product_titles = [item["title"] for item in gate["product_intake"]["blockers"]]
    assert "Goals digest is missing" in technical_titles
    assert "Acceptance criteria digest is missing" in technical_titles
    assert "Structural recommendations section is missing" in technical_titles
    assert "Goals digest is missing" in product_titles


def test_intake_human_summary_merges_structural_recommendations() -> None:
    gate = evaluate_intake_gate_payload(
        {
            "technical_intake": {
                "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
                "review_summary": "Technical lane reviewed architecture fit.",
                "goals_digest": ["Preserve route integrity"],
                "acceptance_criteria_digest": ["All required checks are executable"],
                "structural_recommendations": [
                    {
                        "id": "TECH-SR-001",
                        "priority": "HIGH",
                        "title": "Split migration and runtime cutover",
                        "why": "Reduce rollback blast radius during rollout.",
                        "proposal": "Introduce explicit migration phase before cutover phase.",
                        "impact_on_tz": "Adds explicit checkpoint and acceptance gate.",
                    }
                ],
                "blockers": [],
            },
            "product_intake": {
                "created_docs": ["docs/codex/modules/demo.parent.md"],
                "review_summary": "Product lane reviewed value and acceptance alignment.",
                "goals_digest": ["Preserve route integrity", "Expose human summaries in chat"],
                "acceptance_criteria_digest": [
                    "All required checks are executable",
                    "Intake output includes explicit goals and acceptance criteria digest",
                ],
                "structural_recommendations": [
                    {
                        "id": "PROD-SR-001",
                        "priority": "CRITICAL",
                        "title": "Promote KPI section to mandatory input",
                        "why": "Execution criteria currently allow value drift.",
                        "proposal": "Add explicit KPI and baseline metric fields in source TZ.",
                        "impact_on_tz": "Changes mandatory intake checklist and acceptance flow.",
                    }
                ],
                "blockers": [],
            },
            "combined_gate": {"decision": "PASS"},
        }
    )
    assert gate["combined_gate"]["decision"] == "PASS"
    summary = build_intake_human_summary(gate)
    assert summary["gate_decision"] == "PASS"
    assert len(summary["structural_recommendations"]) == 2
    titles = [item["title"] for item in summary["structural_recommendations"]]
    assert "Split migration and runtime cutover" in titles
    assert "Promote KPI section to mandatory input" in titles
    markdown = render_intake_human_summary_markdown(summary)
    assert "Structural Recommendations" in markdown
    assert "Expose human summaries in chat" in markdown


def test_intake_handoff_is_compact_and_materialization_uses_handoff_path(tmp_path: Path) -> None:
    gate = evaluate_intake_gate_payload(
        {
            "technical_intake": {
                "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
                "review_summary": "Technical lane reviewed architecture fit.",
                "goals_digest": ["Keep gate deterministic"],
                "acceptance_criteria_digest": ["Selector parity remains strict"],
                "structural_recommendations": [
                    {
                        "id": "TECH-SR-001",
                        "priority": "MEDIUM",
                        "title": "Add compatibility matrix",
                        "why": "Implicit compatibility assumptions are risky.",
                        "proposal": "Document matrix in execution contract.",
                        "impact_on_tz": "Expands technical constraints section.",
                    }
                ],
                "blockers": [],
            },
            "product_intake": {
                "created_docs": ["docs/codex/modules/demo.parent.md"],
                "review_summary": "Product lane reviewed value fit.",
                "goals_digest": ["Keep gate deterministic"],
                "acceptance_criteria_digest": ["Selector parity remains strict"],
                "structural_recommendations": [],
                "blockers": [
                    {
                        "id": "LANE-001",
                        "severity": "P1",
                        "scale": "S",
                        "title": "Need owner mapping",
                        "why": "Owner mapping is incomplete",
                        "required_action": "Provide owner mapping",
                    }
                ],
            },
            "combined_gate": {"decision": "PASS"},
        }
    )
    summary = build_intake_human_summary(gate)
    lane_payloads = {
        "technical_intake": gate["technical_intake"],
        "product_intake": gate["product_intake"],
    }
    handoff = build_intake_handoff(
        package_path=tmp_path / "spec.zip",
        extracted_root=tmp_path / "extracted",
        manifest_path=tmp_path / "manifest.md",
        suggested_primary=(tmp_path / "extracted/spec.md").as_posix(),
        suggested_phase_compiler_artifact=(tmp_path / "suggested-phase-plan.json").as_posix(),
        suggested_phase_ids=["phase-01"],
        lane_payloads=lane_payloads,
        intake_gate=gate,
        intake_human_summary=summary,
    )
    assert handoff["gate_decision"] == "BLOCKED"
    assert sorted(handoff["materialization_targets"]["docs_to_refresh"]) == sorted(
        [
            "docs/codex/contracts/demo.execution-contract.md",
            "docs/codex/modules/demo.parent.md",
        ]
    )
    assert handoff["blocking_items"][0]["severity"] == "P1"
    requirements = handoff["materialization_requirements"]
    assert requirements["documents"][0]["type"] == "execution_contract"
    assert requirements["documents"][1]["type"] == "module_parent_brief"
    assert any(item["type"] == "module_phase_brief" for item in requirements["documents"])
    assert requirements["required_outcomes"]
    assert requirements["phase_brief_mandatory_sections"]

    runtime_context = build_prompt(
        prompt_path=tmp_path / "from_package.md",
        package_path=tmp_path / "spec.zip",
        extracted_root=tmp_path / "extracted",
        manifest_path=tmp_path / "manifest.md",
        suggested_primary=(tmp_path / "extracted/spec.md").as_posix(),
        suggested_phase_compiler_artifact=(tmp_path / "suggested-phase-plan.json").as_posix(),
        suggested_phase_ids=["phase-01"],
        mode="auto",
    )
    materialization_prompt = build_materialization_prompt(
        runtime_context=runtime_context,
        policy_prompt_path=tmp_path / "from_package.md",
        intake_handoff_path=tmp_path / "intake-handoff.json",
        intake_gate_path=tmp_path / "intake-gate.json",
        intake_handoff=handoff,
    )
    assert "Intake handoff JSON" in materialization_prompt
    assert "Lane Inputs (technical_intake)" not in materialization_prompt
    assert "BEGIN_MATERIALIZATION_RESULT_JSON" in materialization_prompt
    assert "Required Documents, Constraints, Expected Results" in materialization_prompt
    assert "Mandatory phase brief sections" in materialization_prompt
    assert "Traceability Map" in materialization_prompt
    assert "Structural recommendations and critical TZ changes to preserve" in materialization_prompt
    assert "Section Goals" in materialization_prompt


def test_lane_prompt_is_compact_and_policy_referenced_once(tmp_path: Path) -> None:
    runtime_context = build_prompt(
        prompt_path=tmp_path / "from_package.md",
        package_path=tmp_path / "spec.zip",
        extracted_root=tmp_path / "extracted",
        manifest_path=tmp_path / "manifest.md",
        suggested_primary=(tmp_path / "extracted/spec.md").as_posix(),
        suggested_phase_compiler_artifact=(tmp_path / "suggested-phase-plan.json").as_posix(),
        suggested_phase_ids=["0", "1", "2"],
        mode="auto",
    )
    prompt = build_intake_lane_prompt(
        runtime_context=runtime_context,
        policy_prompt_path=tmp_path / "from_package.md",
        lane="technical_intake",
    )
    assert "Intake Policy Reference" in prompt
    assert "BEGIN_TECHNICAL_INTAKE_JSON" in prompt
    assert "Lossless transfer contract" not in prompt
    assert "Section Goals" in prompt
