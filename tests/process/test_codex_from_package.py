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


def test_extract_docx_title_reads_first_text(tmp_path: Path) -> None:
    docx = tmp_path / "spec.docx"
    with zipfile.ZipFile(docx, "w") as archive:
        archive.writestr(
            "word/document.xml",
            "<w:document><w:body><w:p><w:r><w:t>Primary Specification</w:t></w:r></w:p></w:body></w:document>",
        )

    assert extract_docx_title(docx) == "Primary Specification"
