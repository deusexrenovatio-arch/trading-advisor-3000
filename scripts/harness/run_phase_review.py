from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

try:
    from .models import (
        parse_implementation_summary,
        parse_phase_context,
        parse_phase_review_report,
        parse_traceability_matrix,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        parse_implementation_summary,
        parse_phase_context,
        parse_phase_review_report,
        parse_traceability_matrix,
    )


class ReviewStageError(RuntimeError):
    """Raised when independent review stage cannot complete."""


@dataclass(frozen=True)
class ReviewStageResult:
    run_id: str
    phase_id: str
    output_path: Path
    verdict: str
    findings_count: int
    missing_tests_count: int
    traceability_gaps_count: int


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise ReviewStageError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ReviewStageError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReviewStageError(f"payload at `{path}` must be JSON object")
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


def _matches_allowed(path: str, allowed_surfaces: list[str]) -> bool:
    normalized = path.replace("\\", "/")
    for surface in allowed_surfaces:
        pattern = surface.strip().replace("\\", "/")
        if not pattern:
            continue
        if fnmatch(normalized, pattern):
            return True
        if pattern.endswith("/*") and normalized.startswith(pattern[:-1]):
            return True
    return False


def run_phase_review_stage(
    *,
    registry_root: Path,
    run_id: str,
    phase_id: str,
) -> ReviewStageResult:
    phase_root = registry_root / "phases" / run_id / phase_id
    phase_context = parse_phase_context(_read_json(phase_root / "phase_context.json"))
    implementation = parse_implementation_summary(_read_json(phase_root / "implementation_summary.json"))
    traceability = parse_traceability_matrix(
        _read_json(registry_root / "traceability" / run_id / "traceability_matrix.json")
    )

    if phase_context.run_id != run_id or implementation.run_id != run_id or traceability.run_id != run_id:
        raise ReviewStageError("run_id mismatch across canonical review inputs")
    if phase_context.phase_id != phase_id or implementation.phase_id != phase_id:
        raise ReviewStageError("phase_id mismatch across canonical review inputs")

    allowed_surfaces = list(phase_context.allowed_change_surfaces)
    findings: list[dict[str, object]] = []
    for changed_file in implementation.changed_files:
        if _matches_allowed(changed_file, allowed_surfaces):
            continue
        findings.append(
            {
                "severity": "high",
                "file": changed_file,
                "problem": "Changed file is outside allowed change surfaces for this phase.",
                "requirement_refs": [],
                "fix_hint": "Restrict implementation scope to phase_context.allowed_change_surfaces.",
            }
        )

    for risk in implementation.unresolved_risks:
        findings.append(
            {
                "severity": "medium",
                "file": phase_root.as_posix(),
                "problem": risk,
                "requirement_refs": [],
                "fix_hint": "Address unresolved risk item before requesting phase acceptance.",
            }
        )

    required_tests = list(implementation.required_tests)
    executed_tests = set(implementation.checks_run)
    missing_tests = _unique(
        [test_name for test_name in required_tests if test_name not in executed_tests]
        + list(implementation.failed_tests)
    )

    covered_requirements = set(implementation.covered_requirements)
    traceability_mapping = {item.requirement_id: item for item in traceability.mappings}
    traceability_gaps: list[str] = []
    for requirement_id in phase_context.requirement_ids:
        if requirement_id not in covered_requirements:
            traceability_gaps.append(requirement_id)
            continue
        mapping = traceability_mapping.get(requirement_id)
        if mapping is None or mapping.status == "missing":
            traceability_gaps.append(requirement_id)

    traceability_gaps = _unique(traceability_gaps)
    if any(item["severity"] in {"high", "critical"} for item in findings) or missing_tests or traceability_gaps:
        verdict = "fail"
    elif findings:
        verdict = "pass_with_notes"
    else:
        verdict = "pass"

    report_payload = {
        "run_id": run_id,
        "phase_id": phase_id,
        "verdict": verdict,
        "findings": findings,
        "missing_tests": missing_tests,
        "traceability_gaps": traceability_gaps,
    }
    report_model = parse_phase_review_report(report_payload)

    output_path = phase_root / "phase_review_report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report_model.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return ReviewStageResult(
        run_id=run_id,
        phase_id=phase_id,
        output_path=output_path,
        verdict=report_model.verdict,
        findings_count=len(report_model.findings),
        missing_tests_count=len(report_model.missing_tests),
        traceability_gaps_count=len(report_model.traceability_gaps),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute independent review stage for a single phase.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--phase-id", required=True, help="Phase identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = run_phase_review_stage(
            registry_root=_resolve_path(repo_root, args.registry_root),
            run_id=args.run_id,
            phase_id=args.phase_id,
        )
    except ReviewStageError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "phase_id": result.phase_id,
                "phase_review_report": result.output_path.as_posix(),
                "verdict": result.verdict,
                "findings_count": result.findings_count,
                "missing_tests_count": result.missing_tests_count,
                "traceability_gaps_count": result.traceability_gaps_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
