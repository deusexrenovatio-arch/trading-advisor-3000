from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import validate_legacy_namespace_growth as legacy_growth  # noqa: E402
from validate_legacy_namespace_growth import (  # noqa: E402
    extract_added_lines_from_patch,
    run,
)


def test_extract_added_lines_from_patch_tracks_new_line_numbers() -> None:
    patch = """diff --git a/demo.txt b/demo.txt
index 1111111..2222222 100644
--- a/demo.txt
+++ b/demo.txt
@@ -1,2 +1,3 @@
 unchanged
+legacy docs/architecture/app/path
 tail
"""
    rows = extract_added_lines_from_patch(patch)
    assert rows == [(2, "legacy docs/architecture/app/path")]


def test_validate_legacy_namespace_growth_fails_on_forbidden_new_reference(tmp_path: Path) -> None:
    target = tmp_path / "src" / "demo.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text('VALUE = "src/trading_advisor_3000/app/runtime/"\n', encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=["src/demo.py"],
    )
    assert code == 1


def test_validate_legacy_namespace_growth_allows_migration_planning_paths(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "codex" / "modules" / "dual-surface-safe-rename.phase-99.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("- tests/app/ is a planned migration token\n", encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=["docs/codex/modules/dual-surface-safe-rename.phase-99.md"],
    )
    assert code == 0


def test_validate_legacy_namespace_growth_allows_cold_historical_paths(tmp_path: Path) -> None:
    archive_note = tmp_path / "docs" / "archive" / "historical" / "old-task.md"
    archive_note.parent.mkdir(parents=True, exist_ok=True)
    archive_note.write_text("- tests/app/ is preserved as historical evidence\n", encoding="utf-8")

    candidate_note = tmp_path / "docs" / "project-map" / "state" / "candidates" / "project-map-candidates.md"
    candidate_note.parent.mkdir(parents=True, exist_ok=True)
    candidate_note.write_text("- docs/architecture/app/ appears in an unpromoted candidate quote\n", encoding="utf-8")

    code = run(
        tmp_path,
        changed_files_override=[
            "docs/archive/historical/old-task.md",
            "docs/project-map/state/candidates/project-map-candidates.md",
        ],
    )
    assert code == 0


def test_validate_legacy_namespace_growth_passes_when_no_changes() -> None:
    code = run(ROOT, changed_files_override=[])
    assert code == 0


def test_validate_legacy_namespace_growth_handles_missing_stdout(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "docs" / "sample.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# sample\n", encoding="utf-8")

    def _fake_run_git(_repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
        command = " ".join(args)
        if command.startswith("diff --unified=0 --no-color"):
            return subprocess.CompletedProcess(args=["git", *args], returncode=0, stdout=None, stderr="")
        if command.startswith("ls-files --error-unmatch"):
            return subprocess.CompletedProcess(args=["git", *args], returncode=0, stdout="", stderr="")
        return subprocess.CompletedProcess(args=["git", *args], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(legacy_growth, "_run_git", _fake_run_git)

    code = run(tmp_path, changed_files_override=["docs/sample.md"])
    assert code == 0
