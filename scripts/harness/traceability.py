from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import (
        parse_normalized_requirements,
        parse_phase_acceptance_report,
        parse_phase_plan,
        parse_phase_review_report,
        parse_traceability_matrix,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        parse_normalized_requirements,
        parse_phase_acceptance_report,
        parse_phase_plan,
        parse_phase_review_report,
        parse_traceability_matrix,
    )


class TraceabilityBuildError(RuntimeError):
    """Raised when traceability matrix rebuild cannot complete safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise TraceabilityBuildError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise TraceabilityBuildError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise TraceabilityBuildError(f"payload at `{path}` must be JSON object")
    return payload


def _to_posix(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


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


def rebuild_traceability_matrix(*, registry_root: Path, run_id: str) -> Path:
    intake_root = registry_root / "intake" / run_id
    phase_root = registry_root / "phases" / run_id
    acceptance_root = registry_root / "acceptance" / run_id

    normalized = parse_normalized_requirements(_read_json(intake_root / "normalized_requirements.json"))
    phase_plan = parse_phase_plan(_read_json(phase_root / "phase_plan.json"))
    if normalized.run_id != run_id:
        raise TraceabilityBuildError(
            f"normalized requirements run_id mismatch: expected `{run_id}`, got `{normalized.run_id}`"
        )
    if phase_plan.run_id != run_id:
        raise TraceabilityBuildError(f"phase plan run_id mismatch: expected `{run_id}`, got `{phase_plan.run_id}`")

    requirement_to_phases: dict[str, list[str]] = {
        req.requirement_id: [] for req in normalized.requirements
    }
    for phase in phase_plan.phases:
        for requirement_id in phase.requirement_ids:
            requirement_to_phases.setdefault(requirement_id, []).append(phase.phase_id)

    mappings: list[dict[str, object]] = []
    for requirement in normalized.requirements:
        assigned_phase_ids = _unique(requirement_to_phases.get(requirement.requirement_id, []))
        artifact_refs: list[str] = []
        accepted = False
        seen_in_execution = False

        for phase_id in assigned_phase_ids:
            phase_context_path = phase_root / phase_id / "phase_context.json"
            implementation_path = phase_root / phase_id / "implementation_summary.json"
            review_path = phase_root / phase_id / "phase_review_report.json"
            acceptance_path = acceptance_root / phase_id / "phase_acceptance_report.json"

            if phase_context_path.exists():
                artifact_refs.append(_to_posix(phase_context_path, registry_root))

            if implementation_path.exists():
                implementation_payload = _read_json(implementation_path)
                covered_requirements = implementation_payload.get("covered_requirements")
                if isinstance(covered_requirements, list) and requirement.requirement_id in covered_requirements:
                    artifact_refs.append(_to_posix(implementation_path, registry_root))
                    seen_in_execution = True

            if review_path.exists():
                review_report = parse_phase_review_report(_read_json(review_path))
                if any(
                    requirement.requirement_id in finding.requirement_refs
                    for finding in review_report.findings
                ) or requirement.requirement_id in review_report.traceability_gaps:
                    artifact_refs.append(_to_posix(review_path, registry_root))
                    seen_in_execution = True

            if acceptance_path.exists():
                acceptance_report = parse_phase_acceptance_report(_read_json(acceptance_path))
                if requirement.requirement_id in acceptance_report.accepted_requirements:
                    accepted = True
                    artifact_refs.append(_to_posix(acceptance_path, registry_root))
                elif requirement.requirement_id in acceptance_report.rejected_requirements:
                    seen_in_execution = True
                    artifact_refs.append(_to_posix(acceptance_path, registry_root))

        if accepted:
            status = "covered"
            notes = "Requirement accepted by phase gate."
        elif seen_in_execution or artifact_refs:
            status = "partial"
            notes = "Requirement has execution evidence but is not yet accepted."
        else:
            status = "missing"
            notes = "No execution evidence attached yet."

        mappings.append(
            {
                "requirement_id": requirement.requirement_id,
                "phase_ids": assigned_phase_ids,
                "artifact_refs": _unique(artifact_refs),
                "status": status,
                "notes": notes,
            }
        )

    matrix_payload = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "mappings": mappings,
    }
    matrix = parse_traceability_matrix(matrix_payload)

    output_path = registry_root / "traceability" / run_id / "traceability_matrix.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(matrix.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


__all__ = ["TraceabilityBuildError", "rebuild_traceability_matrix"]
