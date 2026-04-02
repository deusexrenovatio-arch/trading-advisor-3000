from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

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


def test_validate_legacy_namespace_growth_passes_when_no_changes() -> None:
    code = run(ROOT, changed_files_override=[])
    assert code == 0
