from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _build_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("utf-8"))


def _run(args: list[str]) -> dict[str, object]:
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.harness.run_harness", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    return json.loads(completed.stdout)


def test_run_harness_cli_modes_cover_local_e2e_flow(tmp_path: Path) -> None:
    run_id = "RUN-WP07-CLI"
    zip_path = tmp_path / "wp07_cli.zip"
    registry_root = tmp_path / "registry"
    docs_root = tmp_path / "docs" / "generated"
    session_handoff = tmp_path / "docs" / "session_handoff.md"
    session_handoff.parent.mkdir(parents=True, exist_ok=True)
    session_handoff.write_text(
        "\n".join(
            [
                "# Session Handoff",
                "Updated: 2026-03-27 12:00 UTC",
                "",
                "## Active Task Note",
                "- Path: docs/tasks/active/TASK-demo.md",
                "- Mode: full",
                "- Status: in_progress",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    _build_zip(
        zip_path,
        {
            "spec/main.md": "\n".join(
                [
                    "# CLI Flow Sample",
                    "- Data: registry schemas must stay canonical.",
                    "- Functional: harness should execute phase loop.",
                    "- Acceptance: phase gate must block unaccepted progression.",
                    "- Integration: run_harness should expose WP-07 modes.",
                    "- Open question: what reporting format is required?",
                ]
            )
        },
    )

    intake = _run(
        [
            "intake",
            "--run-id",
            run_id,
            "--input-zip",
            str(zip_path),
            "--registry-root",
            str(registry_root),
        ]
    )
    assert intake["run_id"] == run_id

    plan = _run(
        [
            "plan",
            "--run-id",
            run_id,
            "--registry-root",
            str(registry_root),
            "--docs-root",
            str(docs_root),
        ]
    )
    assert plan["run_id"] == run_id

    run_current = _run(
        [
            "run-current-phase",
            "--run-id",
            run_id,
            "--registry-root",
            str(registry_root),
            "--docs-root",
            str(docs_root),
            "--backend",
            "simulate",
            "--quality-profile",
            "always-pass",
            "--retry-ceiling",
            "1",
        ]
    )
    assert run_current["final_verdict"] == "accepted"

    render = _run(
        [
            "render-docs",
            "--run-id",
            run_id,
            "--registry-root",
            str(registry_root),
            "--docs-root",
            str(docs_root),
        ]
    )
    assert render["run_id"] == run_id

    validate = _run(
        [
            "validate",
            "--run-id",
            run_id,
            "--registry-root",
            str(registry_root),
            "--docs-root",
            str(docs_root),
            "--session-handoff",
            str(session_handoff),
        ]
    )
    assert validate["status"] == "ok"

    completion = _run(
        [
            "run-to-completion",
            "--run-id",
            run_id,
            "--registry-root",
            str(registry_root),
            "--docs-root",
            str(docs_root),
            "--backend",
            "simulate",
            "--quality-profile",
            "always-pass",
            "--retry-ceiling",
            "1",
        ]
    )
    assert completion["final_status"] == "completed"
