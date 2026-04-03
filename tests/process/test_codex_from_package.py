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
    build_prompt,
    choose_latest_package,
    collect_document_candidates,
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


def test_build_prompt_includes_required_intake_skill_binding(tmp_path: Path) -> None:
    prompt_path = tmp_path / "from_package.md"
    prompt_path.write_text("Use the package-intake flow for this repository.", encoding="utf-8")
    prompt = build_prompt(
        prompt_path=prompt_path,
        package_path=tmp_path / "spec.zip",
        extracted_root=tmp_path / "extracted",
        manifest_path=tmp_path / "manifest.md",
        suggested_primary=None,
        suggested_phase_compiler_artifact=None,
        mode="auto",
    )

    assert "Required intake skills: .cursor/skills/workflow-architect/SKILL.md" in prompt
