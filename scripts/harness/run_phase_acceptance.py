from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import (
        parse_implementation_summary,
        parse_normalized_requirements,
        parse_phase_acceptance_report,
        parse_phase_context,
        parse_phase_plan,
        parse_phase_review_report,
        parse_phase_rework_request,
        parse_traceability_matrix,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        parse_implementation_summary,
        parse_normalized_requirements,
        parse_phase_acceptance_report,
        parse_phase_context,
        parse_phase_plan,
        parse_phase_review_report,
        parse_phase_rework_request,
        parse_traceability_matrix,
    )


class AcceptanceStageError(RuntimeError):
    """Raised when acceptance stage cannot evaluate phase gates safely."""


@dataclass(frozen=True)
class AcceptanceStageResult:
    run_id: str
    phase_id: str
    output_path: Path
    verdict: str
    rejected_requirements_count: int
    rework_request_path: Path | None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise AcceptanceStageError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise AcceptanceStageError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise AcceptanceStageError(f"payload at `{path}` must be JSON object")
    return payload


def _resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


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


def run_phase_acceptance_stage(
    *,
    registry_root: Path,
    run_id: str,
    phase_id: str,
) -> AcceptanceStageResult:
    phase_root = registry_root / "phases" / run_id / phase_id
    acceptance_root = registry_root / "acceptance" / run_id / phase_id

    normalized = parse_normalized_requirements(_read_json(registry_root / "intake" / run_id / "normalized_requirements.json"))
    phase_plan = parse_phase_plan(_read_json(registry_root / "phases" / run_id / "phase_plan.json"))
    phase_context = parse_phase_context(_read_json(phase_root / "phase_context.json"))
    implementation = parse_implementation_summary(_read_json(phase_root / "implementation_summary.json"))
    review = parse_phase_review_report(_read_json(phase_root / "phase_review_report.json"))
    traceability = parse_traceability_matrix(
        _read_json(registry_root / "traceability" / run_id / "traceability_matrix.json")
    )

    if any(
        value != run_id
        for value in (
            normalized.run_id,
            phase_plan.run_id,
            phase_context.run_id,
            implementation.run_id,
            review.run_id,
            traceability.run_id,
        )
    ):
        raise AcceptanceStageError("run_id mismatch across acceptance canonical artifacts")
    if any(value != phase_id for value in (phase_context.phase_id, implementation.phase_id, review.phase_id)):
        raise AcceptanceStageError("phase_id mismatch across acceptance canonical artifacts")

    selected_phase = next((item for item in phase_plan.phases if item.phase_id == phase_id), None)
    if selected_phase is None:
        raise AcceptanceStageError(f"phase `{phase_id}` not present in phase plan")

    requirement_ids = list(phase_context.requirement_ids)
    if not requirement_ids:
        raise AcceptanceStageError("phase context has empty requirement_ids; cannot accept empty phase")
    requirement_set = set(requirement_ids)
    traceability_map = {item.requirement_id: item for item in traceability.mappings}
    covered_requirements = set(implementation.covered_requirements)
    review_gap_set = set(review.traceability_gaps)

    evidence_refs = _unique(
        [
            _to_posix(phase_root / "implementation_summary.json", registry_root),
            _to_posix(phase_root / "phase_review_report.json", registry_root),
            _to_posix(registry_root / "traceability" / run_id / "traceability_matrix.json", registry_root),
        ]
        + implementation.passed_tests
    )

    missing_evidence: list[str] = []
    if not implementation.changed_files:
        missing_evidence.append("Implementation diff evidence is missing (changed_files is empty).")
    if not implementation.checks_run:
        missing_evidence.append("Test evidence is missing (checks_run is empty).")
    for requirement_id in requirement_ids:
        if requirement_id not in traceability_map:
            missing_evidence.append(f"Traceability mapping missing for requirement `{requirement_id}`.")

    failed_checks: list[str] = []
    if review.verdict == "fail":
        failed_checks.append("Independent review verdict is fail.")
    if implementation.failed_tests:
        failed_checks.extend(f"Failed test evidence: {item}" for item in implementation.failed_tests)
    if review.missing_tests:
        failed_checks.extend(f"Missing required test execution: {item}" for item in review.missing_tests)
    if any(finding.severity in {"high", "critical"} for finding in review.findings):
        failed_checks.append("Independent review reported high severity findings.")

    rejected_requirements: list[str] = []
    accepted_requirements: list[str] = []
    for requirement_id in requirement_ids:
        reasons: list[str] = []
        if requirement_id not in covered_requirements:
            reasons.append("requirement is not covered in implementation summary")
        if requirement_id in review_gap_set:
            reasons.append("requirement appears in review traceability gaps")
        mapping = traceability_map.get(requirement_id)
        if mapping is None or mapping.status == "missing":
            reasons.append("traceability matrix has no coverage for requirement")
        if failed_checks or missing_evidence:
            reasons.append("global acceptance checks are not satisfied")
        if reasons:
            rejected_requirements.append(requirement_id)
        else:
            accepted_requirements.append(requirement_id)

    # Defensive fail-closed check: phase context must stay aligned with phase plan scope.
    if not requirement_set.issubset(set(selected_phase.requirement_ids)):
        failed_checks.append("phase context requirement set is not aligned with phase plan scope")
        rejected_requirements = _unique(rejected_requirements + requirement_ids)
        accepted_requirements = []

    follow_up_actions = _unique(
        [
            "Address review findings and traceability gaps before rerunning acceptance.",
            "Run all required tests from phase_context.test_scope and attach evidence.",
        ]
        + failed_checks
        + missing_evidence
    )
    missing_evidence = _unique(missing_evidence)
    failed_checks = _unique(failed_checks)
    rejected_requirements = _unique(rejected_requirements)
    accepted_requirements = _unique(accepted_requirements)
    verdict = "accepted" if not rejected_requirements and not failed_checks and not missing_evidence else "rejected"

    report_payload = {
        "run_id": run_id,
        "phase_id": phase_id,
        "verdict": verdict,
        "accepted_requirements": accepted_requirements,
        "rejected_requirements": rejected_requirements,
        "evidence_refs": evidence_refs,
        "missing_evidence": missing_evidence,
        "failed_checks": failed_checks,
        "follow_up_actions": follow_up_actions,
        "rework_required": verdict == "rejected",
    }
    report_model = parse_phase_acceptance_report(report_payload)
    acceptance_root.mkdir(parents=True, exist_ok=True)
    output_path = acceptance_root / "phase_acceptance_report.json"
    output_path.write_text(
        json.dumps(report_model.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    rework_request_path: Path | None = None
    if report_model.verdict == "rejected":
        required_fixes = _unique(follow_up_actions + [f"Close requirement gap: {req}" for req in rejected_requirements])
        reason = (
            failed_checks[0]
            if failed_checks
            else (missing_evidence[0] if missing_evidence else "Acceptance gate rejected unresolved requirement gaps.")
        )
        request_payload = {
            "run_id": run_id,
            "phase_id": phase_id,
            "generated_at": _utc_now(),
            "reason": reason,
            "required_fixes": required_fixes,
            "requirement_ids": rejected_requirements or requirement_ids,
            "target_iteration": implementation.iteration + 1,
        }
        request_model = parse_phase_rework_request(request_payload)
        rework_request_path = phase_root / "phase_rework_request.json"
        rework_request_path.write_text(
            json.dumps(request_model.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    return AcceptanceStageResult(
        run_id=run_id,
        phase_id=phase_id,
        output_path=output_path,
        verdict=report_model.verdict,
        rejected_requirements_count=len(report_model.rejected_requirements),
        rework_request_path=rework_request_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute acceptance stage and produce verdict artifacts.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--phase-id", required=True, help="Phase identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = run_phase_acceptance_stage(
            registry_root=_resolve_path(repo_root, args.registry_root),
            run_id=args.run_id,
            phase_id=args.phase_id,
        )
    except AcceptanceStageError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "phase_id": result.phase_id,
                "phase_acceptance_report": result.output_path.as_posix(),
                "verdict": result.verdict,
                "rejected_requirements_count": result.rejected_requirements_count,
                "phase_rework_request": result.rework_request_path.as_posix()
                if result.rework_request_path
                else None,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
