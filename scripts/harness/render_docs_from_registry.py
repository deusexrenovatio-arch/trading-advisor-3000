from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

try:
    from .models import (
        parse_phase_context,
        parse_phase_plan,
        parse_project_docs_bundle,
        parse_run_state,
        parse_traceability_matrix,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        parse_phase_context,
        parse_phase_plan,
        parse_project_docs_bundle,
        parse_run_state,
        parse_traceability_matrix,
    )


class DocsRenderError(RuntimeError):
    """Raised when generated docs cannot be rendered from canonical registry state."""


@dataclass(frozen=True)
class DocsRenderResult:
    run_id: str
    docs_root: Path
    registry_generated_root: Path
    rendered_files: tuple[Path, ...]
    manifest_path: Path


DOC_FILE_ORDER = (
    "current_project_brief.md",
    "phase_plan.md",
    "current_phase.md",
    "acceptance_matrix.md",
    "open_questions.md",
)


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise DocsRenderError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise DocsRenderError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise DocsRenderError(f"payload at `{path}` must be JSON object")
    return payload


def _resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _phase_status(run_state, phase_id: str) -> str:
    if phase_id in run_state.accepted_phase_ids:
        return "accepted"
    if phase_id in run_state.rejected_phase_ids:
        return "blocked"
    if run_state.current_phase_id == phase_id:
        return "active"
    return "planned"


def build_rendered_docs(*, registry_root: Path, run_id: str) -> dict[str, str]:
    docs_bundle = parse_project_docs_bundle(_read_json(registry_root / "intake" / run_id / "project_docs_bundle.json"))
    phase_plan = parse_phase_plan(_read_json(registry_root / "phases" / run_id / "phase_plan.json"))
    run_state = parse_run_state(_read_json(registry_root / "runs" / run_id / "run_state.json"))
    traceability = parse_traceability_matrix(_read_json(registry_root / "traceability" / run_id / "traceability_matrix.json"))

    if any(model.run_id != run_id for model in (docs_bundle, phase_plan, run_state, traceability)):
        raise DocsRenderError("run_id mismatch across registry artifacts for docs rendering")

    current_phase_id = run_state.current_phase_id
    current_phase = next((phase for phase in phase_plan.phases if phase.phase_id == current_phase_id), None)
    phase_context = None
    if current_phase_id:
        context_path = registry_root / "phases" / run_id / current_phase_id / "phase_context.json"
        if context_path.exists():
            phase_context = parse_phase_context(_read_json(context_path))

    intake_open_questions: list[str] = []
    intake_questions_path = registry_root / "intake" / run_id / "open_questions.json"
    if intake_questions_path.exists():
        intake_payload = _read_json(intake_questions_path)
        raw = intake_payload.get("open_questions")
        if isinstance(raw, list):
            for item in raw:
                if not isinstance(item, dict):
                    continue
                question = item.get("question")
                if isinstance(question, str) and question.strip():
                    intake_open_questions.append(question.strip())

    current_project_lines = [
        "# Current Project Brief",
        "",
        "Generated from canonical registry artifacts. This document is a read model, not source of truth.",
        "",
        f"- Run ID: `{run_id}`",
        f"- Run Status: `{run_state.status}`",
        f"- Current Phase: `{run_state.current_phase_id or 'none'}`",
        "",
        "## Problem Statement",
        docs_bundle.problem_statement,
        "",
        "## Scope In",
    ]
    current_project_lines.extend([f"- {item}" for item in docs_bundle.scope_in] or ["- (none)"])
    current_project_lines.extend(["", "## Scope Out"])
    current_project_lines.extend([f"- {item}" for item in docs_bundle.scope_out] or ["- (none)"])
    current_project_lines.extend(["", "## Key Risks"])
    current_project_lines.extend([f"- {item.statement}" for item in docs_bundle.risk_register] or ["- (none)"])

    phase_plan_lines = [
        "# Phase Plan",
        "",
        "Generated from canonical registry artifacts. This document is a read model, not source of truth.",
        "",
        f"- Run ID: `{run_id}`",
        "",
    ]
    for phase in phase_plan.phases:
        status = _phase_status(run_state, phase.phase_id)
        phase_plan_lines.extend(
            [
                f"## {phase.phase_id} - {phase.name}",
                f"- Status: `{status}`",
                f"- Goal: {phase.goal}",
                f"- Dependencies: {', '.join(phase.dependencies) if phase.dependencies else 'none'}",
                "- Requirements:",
            ]
        )
        phase_plan_lines.extend([f"  - {req_id}" for req_id in phase.requirement_ids] or ["  - (none)"])
        phase_plan_lines.append("")

    current_phase_lines = [
        "# Current Phase",
        "",
        "Generated from canonical registry artifacts. This document is a read model, not source of truth.",
        "",
        f"- Run ID: `{run_id}`",
    ]
    if current_phase is None:
        current_phase_lines.extend(
            [
                "- Active Phase: `none`",
                "",
                "All phases are accepted or the run is blocked without an active phase pointer.",
            ]
        )
    else:
        current_phase_lines.extend(
            [
                f"- Active Phase: `{current_phase.phase_id}`",
                f"- Goal: {current_phase.goal}",
                "",
                "## Done Definition",
            ]
        )
        current_phase_lines.extend([f"- {item}" for item in current_phase.done_definition] or ["- (none)"])
        current_phase_lines.extend(["", "## Acceptance Checks"])
        current_phase_lines.extend([f"- {item}" for item in current_phase.acceptance_checks] or ["- (none)"])
        current_phase_lines.extend(["", "## Allowed Change Surfaces"])
        current_phase_lines.extend([f"- {item}" for item in current_phase.allowed_change_surfaces] or ["- (none)"])
        if phase_context is not None:
            current_phase_lines.extend(["", "## Relevant Requirements"])
            current_phase_lines.extend([f"- {item.requirement_id}: {item.statement}" for item in phase_context.requirements])

    acceptance_matrix_lines = [
        "# Acceptance Matrix",
        "",
        "Generated from canonical registry artifacts. This document is a read model, not source of truth.",
        "",
        f"- Run ID: `{run_id}`",
        "",
        "| Requirement | Phases | Status | Artifact Refs |",
        "|---|---|---|---|",
    ]
    for mapping in traceability.mappings:
        phases_cell = ", ".join(mapping.phase_ids) if mapping.phase_ids else "none"
        refs_cell = ", ".join(mapping.artifact_refs) if mapping.artifact_refs else "none"
        acceptance_matrix_lines.append(
            f"| {mapping.requirement_id} | {phases_cell} | {mapping.status} | {refs_cell} |"
        )

    open_questions = _unique(
        list(docs_bundle.open_questions)
        + intake_open_questions
        + [f"Traceability pending for {item.requirement_id}" for item in traceability.mappings if item.status != "covered"]
    )
    open_questions_lines = [
        "# Open Questions",
        "",
        "Generated from canonical registry artifacts. This document is a read model, not source of truth.",
        "",
        f"- Run ID: `{run_id}`",
        "",
    ]
    open_questions_lines.extend([f"- {item}" for item in open_questions] or ["- (none)"])

    return {
        "current_project_brief.md": "\n".join(current_project_lines).rstrip() + "\n",
        "phase_plan.md": "\n".join(phase_plan_lines).rstrip() + "\n",
        "current_phase.md": "\n".join(current_phase_lines).rstrip() + "\n",
        "acceptance_matrix.md": "\n".join(acceptance_matrix_lines).rstrip() + "\n",
        "open_questions.md": "\n".join(open_questions_lines).rstrip() + "\n",
    }


def run_docs_render(
    *,
    registry_root: Path,
    run_id: str,
    docs_root: Path,
) -> DocsRenderResult:
    rendered = build_rendered_docs(registry_root=registry_root, run_id=run_id)

    rendered_paths: list[Path] = []
    hashes: dict[str, str] = {}
    docs_root.mkdir(parents=True, exist_ok=True)
    registry_generated_root = registry_root / "generated" / run_id
    registry_generated_root.mkdir(parents=True, exist_ok=True)

    for file_name in DOC_FILE_ORDER:
        content = rendered[file_name]
        target = docs_root / file_name
        target.write_text(content, encoding="utf-8")
        rendered_paths.append(target)
        hashes[file_name] = hashlib.sha256(content.encode("utf-8")).hexdigest()

        snapshot = registry_generated_root / file_name
        snapshot.write_text(content, encoding="utf-8")

    manifest_path = registry_generated_root / "render_manifest.json"
    manifest_payload = {
        "run_id": run_id,
        "files": [
            {"file": file_name, "sha256": hashes[file_name]}
            for file_name in DOC_FILE_ORDER
        ],
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return DocsRenderResult(
        run_id=run_id,
        docs_root=docs_root,
        registry_generated_root=registry_generated_root,
        rendered_files=tuple(rendered_paths),
        manifest_path=manifest_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render human-readable docs from canonical registry artifacts.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    parser.add_argument("--docs-root", default="docs/generated", help="Output docs directory (default: docs/generated).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = run_docs_render(
            registry_root=_resolve_path(repo_root, args.registry_root),
            run_id=args.run_id,
            docs_root=_resolve_path(repo_root, args.docs_root),
        )
    except DocsRenderError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "docs_root": result.docs_root.as_posix(),
                "registry_generated_root": result.registry_generated_root.as_posix(),
                "render_manifest": result.manifest_path.as_posix(),
                "rendered_files": [path.as_posix() for path in result.rendered_files],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
