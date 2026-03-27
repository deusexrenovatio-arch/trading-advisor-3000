from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .models import parse_implementation_summary, parse_phase_context, parse_run_state
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import parse_implementation_summary, parse_phase_context, parse_run_state


IMPLEMENTATION_BEGIN = "BEGIN_PHASE_IMPLEMENTATION_JSON"
IMPLEMENTATION_END = "END_PHASE_IMPLEMENTATION_JSON"


class ImplementationStageError(RuntimeError):
    """Raised when implementation stage cannot produce canonical output."""


@dataclass(frozen=True)
class ImplementationStageResult:
    run_id: str
    phase_id: str
    iteration: int
    output_path: Path
    changed_files_count: int
    covered_requirements_count: int
    failed_tests_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise ImplementationStageError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ImplementationStageError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise ImplementationStageError(f"payload at `{path}` must be JSON object")
    return payload


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _to_posix(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _load_iteration(registry_root: Path, run_id: str) -> int:
    state_path = registry_root / "runs" / run_id / "run_state.json"
    if not state_path.exists():
        return 1
    state = parse_run_state(_read_json(state_path))
    if state.run_id != run_id:
        raise ImplementationStageError(
            f"run_state run_id mismatch: expected `{run_id}`, got `{state.run_id}`"
        )
    return int(state.iteration_count) + 1


def _surface_to_file(surface: str, phase_id: str) -> str:
    normalized = surface.strip().replace("\\", "/")
    token = phase_id.lower().replace("-", "_")
    if "*" not in normalized:
        return normalized
    if normalized.endswith("/*"):
        base = normalized[:-2]
        if base.endswith("scripts/harness"):
            return f"{base}/{token}_implementation.py"
        if base.endswith("tests/process"):
            return f"{base}/test_{token}.py"
        if base.endswith("docs/generated"):
            return f"{base}/{token}.md"
        if base.endswith("registry/phases"):
            return f"{base}/{token}.json"
        return f"{base}/{token}.txt"
    return normalized.replace("*", token)


def _simulate_payload(
    *,
    phase_id: str,
    quality_profile: str,
    phase_context: dict[str, object],
) -> dict[str, object]:
    requirement_ids = [
        str(item)
        for item in phase_context.get("requirement_ids", [])
        if isinstance(item, str) and item.strip()
    ]
    test_scope = [
        str(item)
        for item in phase_context.get("test_scope", [])
        if isinstance(item, str) and item.strip()
    ]
    allowed_surfaces = [
        str(item)
        for item in phase_context.get("allowed_change_surfaces", [])
        if isinstance(item, str) and item.strip()
    ]
    prior_findings = phase_context.get("prior_findings", [])
    has_prior_findings = isinstance(prior_findings, list) and len(prior_findings) > 0
    should_pass = quality_profile == "always-pass" or (
        quality_profile == "improve-on-rework" and has_prior_findings
    )

    changed_files = _unique([_surface_to_file(surface, phase_id) for surface in allowed_surfaces][:4])
    if not changed_files:
        changed_files = [f"scripts/harness/{phase_id.lower().replace('-', '_')}_implementation.py"]

    if should_pass:
        covered_requirements = list(requirement_ids)
        passed_tests = list(test_scope)
        failed_tests: list[str] = []
        unresolved_risks: list[str] = []
        summary = "Implementation stage covered all phase requirements and executed required tests."
    else:
        covered_requirements = list(requirement_ids[:-1]) if len(requirement_ids) > 1 else []
        passed_tests = list(test_scope[:-1]) if len(test_scope) > 1 else []
        failed_tests = [test_scope[-1]] if test_scope else ["phase-gate-evidence-test"]
        unresolved_risks = [
            "Phase still has uncovered requirements or failing tests; rework is required.",
        ]
        summary = "Implementation stage left actionable gaps for rework cycle."

    checks_run = _unique(passed_tests + failed_tests)
    return {
        "summary": summary,
        "changed_files": changed_files,
        "checks_run": checks_run,
        "passed_tests": passed_tests,
        "failed_tests": failed_tests,
        "covered_requirements": covered_requirements,
        "unresolved_risks": unresolved_risks,
    }


def _parse_tagged_json(text: str, begin: str, end: str) -> dict[str, Any]:
    start = text.rfind(begin)
    stop = text.rfind(end)
    if start < 0 or stop < 0 or stop <= start:
        raise ImplementationStageError(f"missing tagged json block {begin} ... {end}")
    payload_text = text[start + len(begin) : stop].strip()
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise ImplementationStageError(f"failed to parse tagged implementation payload: {exc}") from exc
    if not isinstance(payload, dict):
        raise ImplementationStageError("implementation payload must be JSON object")
    return payload


def _build_codex_prompt(
    *,
    template_text: str,
    phase_context_path: Path,
) -> str:
    return (
        f"{template_text.rstrip()}\n\n"
        "Return strictly one JSON object between markers.\n"
        f"{IMPLEMENTATION_BEGIN}\n"
        '{"summary":"...","changed_files":["..."],"checks_run":["..."],'
        '"passed_tests":["..."],"failed_tests":["..."],"covered_requirements":["..."],'
        '"unresolved_risks":["..."]}\n'
        f"{IMPLEMENTATION_END}\n\n"
        f"Phase context path: {phase_context_path.as_posix()}\n"
        "Use only canonical phase context and produce bounded changes.\n"
    )


def _run_codex_backend(
    *,
    repo_root: Path,
    codex_bin: str | None,
    prompt_file: Path,
    phase_context_path: Path,
    output_last_message: Path,
) -> dict[str, object]:
    binary = codex_bin or shutil.which("codex")
    if not binary:
        raise ImplementationStageError("codex executable not found in PATH")
    if not prompt_file.exists():
        raise ImplementationStageError(f"prompt template not found: {prompt_file}")

    prompt = _build_codex_prompt(
        template_text=prompt_file.read_text(encoding="utf-8"),
        phase_context_path=phase_context_path,
    )
    output_last_message.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        binary,
        "exec",
        "-C",
        str(repo_root),
        "--output-last-message",
        str(output_last_message),
        "-",
    ]
    completed = subprocess.run(
        cmd,
        input=prompt,
        text=True,
        cwd=repo_root,
        check=False,
    )
    if completed.returncode != 0:
        raise ImplementationStageError(f"codex exec failed with exit code {completed.returncode}")
    output_text = output_last_message.read_text(encoding="utf-8")
    payload = _parse_tagged_json(output_text, IMPLEMENTATION_BEGIN, IMPLEMENTATION_END)
    return {
        "summary": str(payload.get("summary", "")).strip() or "Implementation completed via codex backend.",
        "changed_files": [
            str(item).strip()
            for item in payload.get("changed_files", [])
            if str(item).strip()
        ],
        "checks_run": [
            str(item).strip()
            for item in payload.get("checks_run", [])
            if str(item).strip()
        ],
        "passed_tests": [
            str(item).strip()
            for item in payload.get("passed_tests", [])
            if str(item).strip()
        ],
        "failed_tests": [
            str(item).strip()
            for item in payload.get("failed_tests", [])
            if str(item).strip()
        ],
        "covered_requirements": [
            str(item).strip()
            for item in payload.get("covered_requirements", [])
            if str(item).strip()
        ],
        "unresolved_risks": [
            str(item).strip()
            for item in payload.get("unresolved_risks", [])
            if str(item).strip()
        ],
    }


def run_phase_implementation_stage(
    *,
    registry_root: Path,
    run_id: str,
    phase_id: str,
    backend: str,
    quality_profile: str,
    prompt_file: Path,
    repo_root: Path,
    codex_bin: str | None = None,
    output_last_message: Path | None = None,
) -> ImplementationStageResult:
    phase_context_path = registry_root / "phases" / run_id / phase_id / "phase_context.json"
    phase_context = parse_phase_context(_read_json(phase_context_path))
    if phase_context.run_id != run_id:
        raise ImplementationStageError(
            f"phase context run_id mismatch: expected `{run_id}`, got `{phase_context.run_id}`"
        )
    if phase_context.phase_id != phase_id:
        raise ImplementationStageError(
            f"phase context phase_id mismatch: expected `{phase_id}`, got `{phase_context.phase_id}`"
        )

    iteration = _load_iteration(registry_root=registry_root, run_id=run_id)
    if backend == "simulate":
        stage_payload = _simulate_payload(
            phase_id=phase_id,
            quality_profile=quality_profile,
            phase_context=phase_context.model_dump(mode="json"),
        )
    elif backend == "codex-cli":
        last_message_path = output_last_message or (
            registry_root / "phases" / run_id / phase_id / "implementation-last-message.txt"
        )
        stage_payload = _run_codex_backend(
            repo_root=repo_root,
            codex_bin=codex_bin,
            prompt_file=prompt_file,
            phase_context_path=phase_context_path,
            output_last_message=last_message_path,
        )
    else:
        raise ImplementationStageError(f"unsupported backend: {backend}")

    payload = {
        "run_id": run_id,
        "phase_id": phase_id,
        "iteration": iteration,
        "generated_at": _utc_now(),
        "backend": backend,
        "prompt_template": _to_posix(prompt_file, repo_root),
        "phase_context_ref": _to_posix(phase_context_path, registry_root),
        "summary": stage_payload["summary"],
        "changed_files": _unique(list(stage_payload["changed_files"])),
        "checks_run": _unique(list(stage_payload["checks_run"])),
        "required_tests": list(phase_context.test_scope),
        "passed_tests": _unique(list(stage_payload["passed_tests"])),
        "failed_tests": _unique(list(stage_payload["failed_tests"])),
        "covered_requirements": _unique(list(stage_payload["covered_requirements"])),
        "unresolved_risks": _unique(list(stage_payload["unresolved_risks"])),
    }
    model = parse_implementation_summary(payload)

    output_path = registry_root / "phases" / run_id / phase_id / "implementation_summary.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(model.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return ImplementationStageResult(
        run_id=run_id,
        phase_id=phase_id,
        iteration=model.iteration,
        output_path=output_path,
        changed_files_count=len(model.changed_files),
        covered_requirements_count=len(model.covered_requirements),
        failed_tests_count=len(model.failed_tests),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute implementation stage for a single phase.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--phase-id", required=True, help="Phase identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    parser.add_argument(
        "--prompt-file",
        default="configs/harness/prompts/implementer.prompt.md",
        help="Implementer prompt template path.",
    )
    parser.add_argument("--backend", choices=("simulate", "codex-cli"), default="simulate")
    parser.add_argument(
        "--quality-profile",
        choices=("improve-on-rework", "always-pass", "always-fail"),
        default="improve-on-rework",
        help="Simulation quality mode for deterministic local execution.",
    )
    parser.add_argument("--codex-bin", default=None, help="Optional codex binary path.")
    parser.add_argument(
        "--output-last-message",
        default=None,
        help="Optional output file for codex backend final message.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = run_phase_implementation_stage(
            registry_root=_resolve_path(repo_root, args.registry_root),
            run_id=args.run_id,
            phase_id=args.phase_id,
            backend=args.backend,
            quality_profile=args.quality_profile,
            prompt_file=_resolve_path(repo_root, args.prompt_file),
            repo_root=repo_root,
            codex_bin=args.codex_bin,
            output_last_message=_resolve_path(repo_root, args.output_last_message)
            if args.output_last_message
            else None,
        )
    except ImplementationStageError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "phase_id": result.phase_id,
                "iteration": result.iteration,
                "implementation_summary": result.output_path.as_posix(),
                "changed_files_count": result.changed_files_count,
                "covered_requirements_count": result.covered_requirements_count,
                "failed_tests_count": result.failed_tests_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
