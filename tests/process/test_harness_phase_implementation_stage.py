from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from harness.run_phase_implementation import (  # noqa: E402
    IMPLEMENTATION_BEGIN,
    IMPLEMENTATION_END,
    _run_codex_backend,
)


def test_run_phase_implementation_codex_backend_forces_utf8_encoding(monkeypatch, tmp_path: Path) -> None:
    prompt_file = tmp_path / "implementer.prompt.md"
    phase_context = tmp_path / "phase_context.json"
    output_last_message = tmp_path / "last-message.txt"
    prompt_file.write_text("prompt template", encoding="utf-8")
    phase_context.write_text("{}", encoding="utf-8")

    captured: dict[str, object] = {}

    def fake_subprocess_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured.update(kwargs)
        output_last_message.write_text(
            "\n".join(
                [
                    "implementation output",
                    IMPLEMENTATION_BEGIN,
                    '{"summary":"ok","changed_files":["scripts/harness/run_phase_implementation.py"],'
                    '"checks_run":["tests/process/test_harness_phase_execution.py"],'
                    '"passed_tests":["tests/process/test_harness_phase_execution.py"],'
                    '"failed_tests":[],"covered_requirements":["REQ-1"],"unresolved_risks":[]}',
                    IMPLEMENTATION_END,
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("harness.run_phase_implementation.subprocess.run", fake_subprocess_run)

    payload = _run_codex_backend(
        repo_root=tmp_path,
        codex_bin="codex",
        prompt_file=prompt_file,
        phase_context_path=phase_context,
        output_last_message=output_last_message,
    )

    assert payload["summary"] == "ok"
    assert captured["text"] is True
    assert captured["encoding"] == "utf-8"
