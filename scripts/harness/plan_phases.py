from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import parse_normalized_requirements, parse_phase_plan, parse_project_docs_bundle
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import parse_normalized_requirements, parse_phase_plan, parse_project_docs_bundle


BASE_PHASES = (
    {
        "bucket": "foundation",
        "name": "Foundation Guardrails",
        "goal": "Establish canonical contracts, safety constraints, and data foundations.",
        "categories": {"data", "constraint", "security", "assumption"},
        "required_tests": [
            "tests/process/test_harness_intake_spec_bundle.py",
            "tests/process/test_harness_wp01_foundation.py",
        ],
        "allowed_change_surfaces": [
            "configs/harness/schemas/*",
            "scripts/harness/*",
            "registry/intake/*",
        ],
    },
    {
        "bucket": "implementation",
        "name": "Implementation Core",
        "goal": "Implement bounded functionality and integration logic for active requirements.",
        "categories": {"functional", "integration"},
        "required_tests": [
            "tests/process/test_harness_requirements_pipeline.py",
        ],
        "allowed_change_surfaces": [
            "scripts/harness/*",
            "registry/phases/*",
            "tests/process/*",
        ],
    },
    {
        "bucket": "verification",
        "name": "Verification and Quality",
        "goal": "Verify non-functional and acceptance-oriented quality requirements.",
        "categories": {"non_functional", "acceptance"},
        "required_tests": [
            "tests/process/test_harness_requirements_pipeline.py",
            "tests/process/test_harness_phase_planning.py",
        ],
        "allowed_change_surfaces": [
            "scripts/harness/*",
            "tests/process/*",
            "docs/generated/*",
        ],
    },
    {
        "bucket": "closure",
        "name": "Open Questions and Closure",
        "goal": "Close unresolved questions and preserve explicit assumptions.",
        "categories": {"open_question"},
        "required_tests": [
            "tests/process/test_harness_phase_planning.py",
        ],
        "allowed_change_surfaces": [
            "registry/phases/*",
            "registry/traceability/*",
            "docs/generated/*",
        ],
    },
)


class PhasePlanningError(RuntimeError):
    """Raised when phase planning cannot produce canonical output."""


@dataclass(frozen=True)
class PhasePlanningResult:
    run_id: str
    output_path: Path
    phase_count: int
    requirement_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise PhasePlanningError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PhasePlanningError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise PhasePlanningError(f"payload at `{path}` must be JSON object")
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


def _bucket_for_category(category: str) -> str:
    for phase in BASE_PHASES:
        if category in phase["categories"]:
            return str(phase["bucket"])
    return "implementation"


def run_phase_planning(
    *,
    registry_root: Path,
    run_id: str,
) -> PhasePlanningResult:
    intake_root = registry_root / "intake" / run_id
    phase_root = registry_root / "phases" / run_id
    normalized_path = intake_root / "normalized_requirements.json"
    docs_bundle_path = intake_root / "project_docs_bundle.json"

    normalized = parse_normalized_requirements(_read_json(normalized_path))
    docs_bundle = parse_project_docs_bundle(_read_json(docs_bundle_path))
    if normalized.run_id != run_id:
        raise PhasePlanningError(
            f"normalized requirements run_id mismatch: expected `{run_id}`, got `{normalized.run_id}`"
        )
    if docs_bundle.run_id != run_id:
        raise PhasePlanningError(
            f"docs bundle run_id mismatch: expected `{run_id}`, got `{docs_bundle.run_id}`"
        )
    if not normalized.requirements:
        raise PhasePlanningError("cannot build phase plan from empty requirements list")

    by_bucket: dict[str, list[object]] = {}
    for req in normalized.requirements:
        bucket = _bucket_for_category(req.category)
        by_bucket.setdefault(bucket, []).append(req)

    phases: list[dict[str, object]] = []
    for phase_index, base in enumerate(BASE_PHASES, start=1):
        bucket = str(base["bucket"])
        requirements = by_bucket.get(bucket, [])
        if not requirements:
            continue

        requirement_ids = [req.requirement_id for req in requirements]
        scope_in = _unique([req.statement for req in requirements][:12])
        scope_out = _unique(list(docs_bundle.scope_out)[:8])
        if not scope_out:
            scope_out = ["No changes outside allowed change surfaces."]

        risk_notes = _unique(
            [risk.statement for risk in docs_bundle.risk_register][:4]
            + [req.statement for req in requirements if req.ambiguity_flag][:4]
        )
        done_definition = _unique(
            [
                f"All assigned requirements are implemented and validated ({len(requirement_ids)} items).",
                "Canonical artifacts are updated in registry-first order.",
                "Change set is bounded to declared allowed surfaces.",
            ]
        )
        acceptance_checks = _unique(
            [req.statement for req in requirements if req.category == "acceptance"][:6]
            + [
                "No unresolved critical findings remain for phase scope.",
                "Executed tests provide evidence for each assigned requirement.",
            ]
        )
        doc_outputs = _unique(
            [
                f"registry/phases/{run_id}/PHASE-{phase_index:02d}/phase_context.json",
                "docs/generated/current_phase.md",
            ]
        )

        phases.append(
            {
                "phase_id": f"PHASE-{phase_index:02d}",
                "name": str(base["name"]),
                "goal": str(base["goal"]),
                "scope_in": scope_in,
                "scope_out": scope_out,
                "requirement_ids": requirement_ids,
                "dependencies": [],
                "done_definition": done_definition,
                "acceptance_checks": acceptance_checks,
                "required_tests": list(base["required_tests"]),
                "risk_notes": risk_notes,
                "doc_outputs": doc_outputs,
                "allowed_change_surfaces": list(base["allowed_change_surfaces"]),
            }
        )

    if not phases:
        raise PhasePlanningError("phase planner produced no phases; check requirement categorization")

    for index in range(1, len(phases)):
        phases[index]["dependencies"] = [str(phases[index - 1]["phase_id"])]

    phase_plan_payload = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "phases": phases,
    }
    phase_plan = parse_phase_plan(phase_plan_payload)

    output_path = phase_root / "phase_plan.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(phase_plan.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return PhasePlanningResult(
        run_id=run_id,
        output_path=output_path,
        phase_count=len(phase_plan.phases),
        requirement_count=len(normalized.requirements),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build ordered phase plan from normalized requirements.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        result = run_phase_planning(
            registry_root=Path(args.registry_root).resolve(),
            run_id=args.run_id,
        )
    except PhasePlanningError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "phase_plan": result.output_path.as_posix(),
                "phase_count": result.phase_count,
                "requirement_count": result.requirement_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
