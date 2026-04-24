from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_product_surface_naming import extract_added_lines_from_patch, run


def test_extract_added_lines_from_patch_tracks_new_line_numbers() -> None:
    patch = """diff --git a/demo.md b/demo.md
index 1111111..2222222 100644
--- a/demo.md
+++ b/demo.md
@@ -1,2 +1,3 @@
 unchanged
+# Phase 2A Data Plane
 tail
"""
    rows = extract_added_lines_from_patch(patch)
    assert rows == [(2, "# Phase 2A Data Plane")]


def test_product_surface_naming_fails_on_active_file_path(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "architecture" / "product-plane" / "phase2a-data-plane.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# Historical Data Plane\n", encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=["docs/architecture/product-plane/phase2a-data-plane.md"],
    )
    assert code == 1


def test_product_surface_naming_fails_on_active_markdown_heading(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "runbooks" / "app" / "historical-data-runbook.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# Phase-02 Historical Data Runbook\n", encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=["docs/runbooks/app/historical-data-runbook.md"],
    )
    assert code == 1


def test_product_surface_naming_fails_on_test_declaration(tmp_path: Path) -> None:
    target = tmp_path / "tests" / "product-plane" / "unit" / "test_historical_data.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("def test_phase2a_builder():\n    assert True\n", encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=["tests/product-plane/unit/test_historical_data.py"],
    )
    assert code == 1


def test_product_surface_naming_allows_provenance_body_references(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "runbooks" / "app" / "historical-data-runbook.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "# Historical Data Runbook\n"
        "\n"
        "- Immutable evidence remains under `artifacts/codex/moex-phase01/<run_id>/`.\n",
        encoding="utf-8",
    )

    code = run(
        tmp_path,
        changed_files_override=["docs/runbooks/app/historical-data-runbook.md"],
    )
    assert code == 0


def test_product_surface_naming_ignores_deleted_legacy_paths(tmp_path: Path) -> None:
    code = run(
        tmp_path,
        changed_files_override=["docs/architecture/product-plane/phase3-old-file.md"],
    )
    assert code == 0


def test_product_surface_naming_allows_shell_governance_context(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "agent" / "checks.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# Phase 2 Shell Note\n", encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=["docs/agent/checks.md"],
    )
    assert code == 0


def test_product_surface_naming_handles_missing_stdout(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "docs" / "architecture" / "product-plane" / "historical-data-plane.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# Historical Data Plane\n", encoding="utf-8")

    import validate_product_surface_naming as naming

    def _fake_run_git(_repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
        command = " ".join(args)
        if command.startswith("diff --unified=0 --no-color"):
            return subprocess.CompletedProcess(args=["git", *args], returncode=0, stdout=None, stderr="")
        if command.startswith("ls-files --error-unmatch"):
            return subprocess.CompletedProcess(args=["git", *args], returncode=0, stdout="", stderr="")
        return subprocess.CompletedProcess(args=["git", *args], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(naming, "_run_git", _fake_run_git)

    code = run(
        tmp_path,
        changed_files_override=["docs/architecture/product-plane/historical-data-plane.md"],
    )
    assert code == 0
