from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import parse_normalized_requirements, parse_phase_context, parse_phase_plan, parse_project_docs_bundle
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        parse_normalized_requirements,
        parse_phase_context,
        parse_phase_plan,
        parse_project_docs_bundle,
    )


class PhaseContextBuildError(RuntimeError):
    """Raised when phase context generation cannot complete."""


@dataclass(frozen=True)
class PhaseContextBuildResult:
    run_id: str
    phase_id: str
    output_path: Path
    requirement_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise PhaseContextBuildError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PhaseContextBuildError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise PhaseContextBuildError(f"payload at `{path}` must be JSON object")
    return payload


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _collect_prior_findings(registry_root: Path, run_id: str, phase_id: str) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []

    review_paths = [
        registry_root / "phases" / run_id / phase_id / "phase_review_report.json",
        registry_root / "phases" / run_id / "phase_review_report.json",
    ]
    for review_path in review_paths:
        if not review_path.exists():
            continue
        payload = _read_json(review_path)
        raw_findings = payload.get("findings")
        if not isinstance(raw_findings, list):
            continue
        for item in raw_findings:
            if not isinstance(item, dict):
                continue
            summary = item.get("problem")
            severity = item.get("severity")
            if not isinstance(summary, str) or not summary.strip():
                continue
            if not isinstance(severity, str) or severity not in {"low", "medium", "high", "critical"}:
                severity = "medium"
            findings.append({"source": "review", "summary": summary.strip(), "severity": severity})

    acceptance_path = registry_root / "acceptance" / run_id / phase_id / "phase_acceptance_report.json"
    if acceptance_path.exists():
        payload = _read_json(acceptance_path)
        failed_checks = payload.get("failed_checks")
        if isinstance(failed_checks, list):
            for item in failed_checks:
                if isinstance(item, str) and item.strip():
                    findings.append({"source": "acceptance", "summary": item.strip(), "severity": "high"})

    return findings


def run_phase_context_build(
    *,
    registry_root: Path,
    run_id: str,
    phase_id: str,
) -> PhaseContextBuildResult:
    intake_root = registry_root / "intake" / run_id
    phase_plan_path = registry_root / "phases" / run_id / "phase_plan.json"

    phase_plan = parse_phase_plan(_read_json(phase_plan_path))
    normalized = parse_normalized_requirements(_read_json(intake_root / "normalized_requirements.json"))
    docs_bundle = parse_project_docs_bundle(_read_json(intake_root / "project_docs_bundle.json"))

    if phase_plan.run_id != run_id:
        raise PhaseContextBuildError(f"phase plan run_id mismatch: expected `{run_id}`, got `{phase_plan.run_id}`")
    if normalized.run_id != run_id:
        raise PhaseContextBuildError(
            f"normalized requirements run_id mismatch: expected `{run_id}`, got `{normalized.run_id}`"
        )
    if docs_bundle.run_id != run_id:
        raise PhaseContextBuildError(
            f"docs bundle run_id mismatch: expected `{run_id}`, got `{docs_bundle.run_id}`"
        )

    selected_phase = next((phase for phase in phase_plan.phases if phase.phase_id == phase_id), None)
    if selected_phase is None:
        known_ids = ", ".join(phase.phase_id for phase in phase_plan.phases) or "<none>"
        raise PhaseContextBuildError(f"phase_id `{phase_id}` not found in phase plan; known phases: {known_ids}")

    requirement_map = {item.requirement_id: item for item in normalized.requirements}
    relevant_requirements = []
    for requirement_id in selected_phase.requirement_ids:
        requirement = requirement_map.get(requirement_id)
        if requirement is None:
            raise PhaseContextBuildError(
                f"phase `{phase_id}` references unknown requirement_id `{requirement_id}`"
            )
        relevant_requirements.append(requirement)

    doc_excerpts = [
        {"title": "Problem Statement", "content": docs_bundle.problem_statement},
        {
            "title": "Phase Goal",
            "content": selected_phase.goal,
        },
        {
            "title": "Design Decisions",
            "content": "; ".join(docs_bundle.design_decisions[:4]) or "No design decisions captured.",
        },
    ]
    if docs_bundle.architecture_constraints:
        doc_excerpts.append(
            {
                "title": "Architecture Constraints",
                "content": "; ".join(docs_bundle.architecture_constraints[:6]),
            }
        )
    if relevant_requirements:
        doc_excerpts.append(
            {
                "title": "Phase Requirement Summary",
                "content": "; ".join(req.statement for req in relevant_requirements[:8]),
            }
        )

    risk_notes = _unique(
        list(selected_phase.risk_notes)
        + [risk.statement for risk in docs_bundle.risk_register][:4]
        + [req.statement for req in relevant_requirements if req.ambiguity_flag][:4]
    )
    open_questions = _unique(
        list(docs_bundle.open_questions)[:10]
        + [req.statement for req in relevant_requirements if req.category == "open_question"][:10]
    )
    prior_findings = _collect_prior_findings(registry_root=registry_root, run_id=run_id, phase_id=phase_id)

    phase_context_payload = {
        "run_id": run_id,
        "phase_id": phase_id,
        "generated_at": _utc_now(),
        "requirement_ids": list(selected_phase.requirement_ids),
        "requirements": [req.model_dump(mode="json") for req in relevant_requirements],
        "doc_excerpts": doc_excerpts,
        "risk_notes": risk_notes,
        "open_questions": open_questions,
        "done_definition": list(selected_phase.done_definition),
        "acceptance_checks": list(selected_phase.acceptance_checks),
        "allowed_change_surfaces": list(selected_phase.allowed_change_surfaces),
        "test_scope": list(selected_phase.required_tests),
        "prior_findings": prior_findings,
    }
    phase_context = parse_phase_context(phase_context_payload)

    output_path = registry_root / "phases" / run_id / phase_id / "phase_context.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(phase_context.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return PhaseContextBuildResult(
        run_id=run_id,
        phase_id=phase_id,
        output_path=output_path,
        requirement_count=len(selected_phase.requirement_ids),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build bounded phase context for a selected phase.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--phase-id", required=True, help="Phase identifier from phase_plan.json.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        result = run_phase_context_build(
            registry_root=Path(args.registry_root).resolve(),
            run_id=args.run_id,
            phase_id=args.phase_id,
        )
    except PhaseContextBuildError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "phase_id": result.phase_id,
                "phase_context": result.output_path.as_posix(),
                "requirement_count": result.requirement_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
