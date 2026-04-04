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
    build_intake_lane_prompt,
    build_materialization_prompt,
    build_prompt,
    choose_latest_package,
    collect_document_candidates,
    extract_lane_payload_from_text,
    evaluate_intake_gate_from_text,
    evaluate_intake_gate_payload,
    extract_docx_title,
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


def test_build_prompt_includes_suggested_phase_ids_line(tmp_path: Path) -> None:
    prompt_file = tmp_path / "from_package.md"
    prompt_file.write_text("base prompt", encoding="utf-8")
    package = tmp_path / "incoming.zip"
    extracted = tmp_path / "extracted"
    manifest = tmp_path / "manifest.md"

    prompt = build_prompt(
        prompt_path=prompt_file,
        package_path=package,
        extracted_root=extracted,
        manifest_path=manifest,
        suggested_primary=(extracted / "spec.md").as_posix(),
        suggested_phase_compiler_artifact=(tmp_path / "suggested-phase-plan.json").as_posix(),
        suggested_phase_ids=["F1-A", "F1-B"],
        mode="plan-only",
    )

    assert "Suggested phase ids: F1-A,F1-B" in prompt


def test_build_intake_lane_prompt_embeds_lane_tags() -> None:
    technical_prompt = build_intake_lane_prompt(base_prompt="base", lane="technical_intake")
    product_prompt = build_intake_lane_prompt(base_prompt="base", lane="product_intake")

    assert "BEGIN_TECHNICAL_INTAKE_JSON" in technical_prompt
    assert "END_TECHNICAL_INTAKE_JSON" in technical_prompt
    assert "Do not modify repository files in this lane pass." in technical_prompt
    assert "workflow-architect" in technical_prompt
    assert "BEGIN_PRODUCT_INTAKE_JSON" in product_prompt
    assert "END_PRODUCT_INTAKE_JSON" in product_prompt
    assert "workflow-architect" not in product_prompt


def test_extract_lane_payload_from_text_supports_plain_lane_object() -> None:
    text = """
BEGIN_TECHNICAL_INTAKE_JSON
{
  "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
  "review_summary": "Technical review complete.",
  "blockers": []
}
END_TECHNICAL_INTAKE_JSON
"""
    payload = extract_lane_payload_from_text(text, lane="technical_intake")
    assert payload["created_docs"] == ["docs/codex/contracts/demo.execution-contract.md"]
    assert payload["review_summary"] == "Technical review complete."


def test_build_materialization_prompt_contains_lane_inputs_and_gate_path(tmp_path: Path) -> None:
    gate_path = tmp_path / "intake-gate.json"
    prompt = build_materialization_prompt(
        base_prompt="base",
        technical_lane_payload={
            "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
            "review_summary": "tech",
            "blockers": [],
        },
        product_lane_payload={
            "created_docs": ["docs/codex/modules/demo.parent.md"],
            "review_summary": "product",
            "blockers": [],
        },
        intake_gate={"combined_gate": {"decision": "PASS"}},
        intake_gate_path=gate_path,
    )

    assert "Intake Runtime Mode:" in prompt
    assert gate_path.as_posix() in prompt
    assert '"review_summary": "tech"' in prompt
    assert '"review_summary": "product"' in prompt


def test_evaluate_intake_gate_payload_passes_without_p0_p1_blockers() -> None:
    payload = {
        "technical_intake": {
            "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
            "review_summary": "Architecture review passed with minor notes.",
            "blockers": [
                {
                    "id": "T-1",
                    "severity": "P2",
                    "scale": "S",
                    "title": "Minor docs gap",
                    "why": "One optional note is missing.",
                    "required_action": "Add optional note later.",
                }
            ],
        },
        "product_intake": {
            "created_docs": ["docs/codex/modules/demo.parent.md"],
            "review_summary": "Value proposition is acceptable for this phase.",
            "blockers": [],
        },
        "combined_gate": {"decision": "PASS"},
    }

    gate = evaluate_intake_gate_payload(payload)
    assert gate["combined_gate"]["decision"] == "PASS"
    assert gate["combined_gate"]["blocking_total"] == 0


def test_evaluate_intake_gate_payload_blocks_when_p0_or_p1_present() -> None:
    payload = {
        "technical_intake": {
            "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
            "review_summary": "Architecture mismatch detected.",
            "blockers": [
                {
                    "id": "T-ARCH-01",
                    "severity": "P0",
                    "scale": "L",
                    "title": "Architecture conflict",
                    "why": "Proposed integration violates current app boundaries.",
                    "required_action": "Rework integration boundaries.",
                }
            ],
        },
        "product_intake": {
            "created_docs": ["docs/codex/modules/demo.parent.md"],
            "review_summary": "Value unclear until architecture issue is resolved.",
            "blockers": [],
        },
        "combined_gate": {"decision": "PASS"},
    }

    gate = evaluate_intake_gate_payload(payload)
    assert gate["combined_gate"]["decision"] == "BLOCKED"
    assert gate["combined_gate"]["blocking_total"] == 1
    assert gate["combined_gate"]["max_problem_scale"] == "L"


def test_evaluate_intake_gate_payload_autoblocks_missing_docs_and_review() -> None:
    payload = {
        "technical_intake": {
            "created_docs": [],
            "review_summary": "",
            "blockers": [],
        },
        "product_intake": {
            "created_docs": ["docs/codex/modules/demo.parent.md"],
            "review_summary": "Product review drafted.",
            "blockers": [],
        },
        "combined_gate": {"decision": "PASS"},
    }

    gate = evaluate_intake_gate_payload(payload)
    assert gate["combined_gate"]["decision"] == "BLOCKED"
    assert gate["combined_gate"]["severity_counts"]["P0"] >= 1
    assert any(
        item["id"] == "AUTO-TECHNICAL_INTAKE-DOCS"
        for item in gate["technical_intake"]["blockers"]
    )


def test_evaluate_intake_gate_from_text_parses_tagged_json() -> None:
    text = """
Intro narrative.
BEGIN_INTAKE_GATE_JSON
{
  "technical_intake": {
    "created_docs": ["docs/codex/contracts/demo.execution-contract.md"],
    "review_summary": "ok",
    "blockers": []
  },
  "product_intake": {
    "created_docs": ["docs/codex/modules/demo.parent.md"],
    "review_summary": "ok",
    "blockers": []
  },
  "combined_gate": {"decision": "PASS"}
}
END_INTAKE_GATE_JSON
"""
    gate = evaluate_intake_gate_from_text(text)
    assert gate["combined_gate"]["decision"] == "PASS"

def test_build_prompt_includes_required_technical_intake_skill_binding(tmp_path: Path) -> None:
    prompt_path = tmp_path / "from_package.md"
    prompt_path.write_text("Use the package-intake flow for this repository.", encoding="utf-8")
    prompt = build_prompt(
        prompt_path=prompt_path,
        package_path=tmp_path / "spec.zip",
        extracted_root=tmp_path / "extracted",
        manifest_path=tmp_path / "manifest.md",
        suggested_primary=None,
        suggested_phase_compiler_artifact=None,
        suggested_phase_ids=[],
        mode="auto",
    )

    assert "Required technical intake skills: .cursor/skills/workflow-architect/SKILL.md" in prompt
