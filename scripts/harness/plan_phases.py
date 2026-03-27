from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import NormalizedRequirementModel, parse_normalized_requirements, parse_phase_plan, parse_project_docs_bundle
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import (
        NormalizedRequirementModel,
        parse_normalized_requirements,
        parse_phase_plan,
        parse_project_docs_bundle,
    )


PHASE_HINT_RE = re.compile(r"(?:WP|PHASE)[-_ ]?(\d+)([A-Z]?)", re.IGNORECASE)

BASE_PHASE_PROFILES = {
    "foundation": {
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
    "implementation": {
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
    "verification": {
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
    "closure": {
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
}

BUCKET_ORDER = {
    "foundation": 0,
    "implementation": 1,
    "verification": 2,
    "closure": 3,
}


class PhasePlanningError(RuntimeError):
    """Raised when phase planning cannot produce canonical output."""


@dataclass(frozen=True)
class PhasePlanningResult:
    run_id: str
    output_path: Path
    phase_count: int
    requirement_count: int


@dataclass
class PhaseDraft:
    key: str
    hint_label: str | None
    hint_rank: tuple[int, str] | None
    profile_buckets: set[str] = field(default_factory=set)
    requirements: list[NormalizedRequirementModel] = field(default_factory=list)
    dependency_keys: set[str] = field(default_factory=set)
    unresolved_dependency_refs: set[str] = field(default_factory=set)


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
    for bucket, profile in BASE_PHASE_PROFILES.items():
        if category in profile["categories"]:
            return bucket
    return "implementation"


def _parse_phase_hint(phase_hint: str | None) -> tuple[str, tuple[int, str]] | None:
    if not phase_hint:
        return None
    cleaned = phase_hint.strip().upper()
    match = PHASE_HINT_RE.search(cleaned)
    if not match:
        return None
    number = int(match.group(1))
    suffix = match.group(2).upper()
    label = f"WP-{number:02d}{suffix}"
    return label, (number, suffix)


def _phase_key_for_requirement(requirement: NormalizedRequirementModel) -> tuple[str, str | None, tuple[int, str] | None]:
    hint = _parse_phase_hint(requirement.phase_hint)
    if hint:
        label, rank = hint
        return f"hint:{label}", label, rank
    bucket = _bucket_for_category(requirement.category)
    return f"bucket:{bucket}", None, None


def _draft_sort_key(draft: PhaseDraft) -> tuple[int, int, str, str]:
    if draft.hint_rank is not None:
        number, suffix = draft.hint_rank
        return (0, number, suffix or "", draft.key)

    bucket = min((BUCKET_ORDER.get(item, 99) for item in draft.profile_buckets), default=99)
    return (1, bucket, "", draft.key)


def _topological_order(drafts: dict[str, PhaseDraft]) -> list[PhaseDraft]:
    remaining_dependencies = {key: set(draft.dependency_keys) for key, draft in drafts.items()}
    ordered: list[PhaseDraft] = []
    available = sorted(
        [draft for key, draft in drafts.items() if not remaining_dependencies[key]],
        key=_draft_sort_key,
    )

    while available:
        current = available.pop(0)
        ordered.append(current)

        for key, deps in remaining_dependencies.items():
            if current.key not in deps:
                continue
            deps.remove(current.key)
            if not deps and all(item.key != key for item in ordered) and all(item.key != key for item in available):
                available.append(drafts[key])
        available.sort(key=_draft_sort_key)

    if len(ordered) != len(drafts):
        unresolved = sorted(set(drafts) - {item.key for item in ordered})
        raise PhasePlanningError(
            f"phase dependencies contain cycle or unresolved nodes: {', '.join(unresolved)}"
        )
    return ordered


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

    drafts: dict[str, PhaseDraft] = {}
    requirement_to_phase_key: dict[str, str] = {}

    for requirement in normalized.requirements:
        key, hint_label, hint_rank = _phase_key_for_requirement(requirement)
        draft = drafts.get(key)
        if draft is None:
            draft = PhaseDraft(
                key=key,
                hint_label=hint_label,
                hint_rank=hint_rank,
            )
            drafts[key] = draft
        draft.requirements.append(requirement)
        draft.profile_buckets.add(_bucket_for_category(requirement.category))
        requirement_to_phase_key[requirement.requirement_id] = key

    for draft in drafts.values():
        for requirement in draft.requirements:
            for dependency_ref in requirement.depends_on:
                dependency_phase_key = requirement_to_phase_key.get(dependency_ref)
                if dependency_phase_key is None:
                    if dependency_ref.upper().startswith("REQ-"):
                        draft.unresolved_dependency_refs.add(dependency_ref)
                    continue
                if dependency_phase_key != draft.key:
                    draft.dependency_keys.add(dependency_phase_key)

    unresolved_refs = sorted(
        {
            ref
            for draft in drafts.values()
            for ref in draft.unresolved_dependency_refs
        }
    )
    if unresolved_refs:
        raise PhasePlanningError(
            "requirements contain unresolved dependency references: " + ", ".join(unresolved_refs)
        )

    ordered_drafts = _topological_order(drafts)
    phase_id_by_key = {draft.key: f"PHASE-{index:02d}" for index, draft in enumerate(ordered_drafts, start=1)}

    phases: list[dict[str, object]] = []
    for draft in ordered_drafts:
        phase_id = phase_id_by_key[draft.key]
        requirement_ids = [item.requirement_id for item in draft.requirements]

        profile_tests: list[str] = []
        profile_surfaces: list[str] = []
        profile_goals: list[str] = []
        for bucket in sorted(draft.profile_buckets, key=lambda item: BUCKET_ORDER.get(item, 99)):
            profile = BASE_PHASE_PROFILES[bucket]
            profile_tests.extend(profile["required_tests"])
            profile_surfaces.extend(profile["allowed_change_surfaces"])
            profile_goals.append(profile["goal"])

        if draft.hint_label:
            name = f"Hinted Delivery {draft.hint_label}"
            goal = (
                f"Deliver requirements tagged with {draft.hint_label} "
                "while preserving dependency-consistent execution."
            )
        else:
            primary_bucket = min(draft.profile_buckets, key=lambda item: BUCKET_ORDER.get(item, 99))
            name = BASE_PHASE_PROFILES[primary_bucket]["name"]
            goal = BASE_PHASE_PROFILES[primary_bucket]["goal"]

        scope_in = _unique([req.statement for req in draft.requirements][:16])
        scope_out = _unique(list(docs_bundle.scope_out)[:8])
        if not scope_out:
            scope_out = ["No changes outside allowed change surfaces."]

        dependency_phase_ids = [phase_id_by_key[key] for key in sorted(draft.dependency_keys)]
        risk_notes = _unique(
            [risk.statement for risk in docs_bundle.risk_register][:6]
            + [req.statement for req in draft.requirements if req.ambiguity_flag][:6]
            + ([f"Dependent phases: {', '.join(dependency_phase_ids)}"] if dependency_phase_ids else [])
        )
        done_definition = _unique(
            [
                f"All assigned requirements are implemented and validated ({len(requirement_ids)} items).",
                "Canonical artifacts are updated in registry-first order.",
                "Change set is bounded to declared allowed surfaces.",
            ]
            + (["Dependency graph for this phase is satisfied by accepted upstream phases."] if dependency_phase_ids else [])
        )
        acceptance_checks = _unique(
            [req.statement for req in draft.requirements if req.category == "acceptance"][:8]
            + [
                "No unresolved critical findings remain for phase scope.",
                "Executed tests provide evidence for each assigned requirement.",
            ]
            + ([f"Dependency evidence present for: {', '.join(dependency_phase_ids)}"] if dependency_phase_ids else [])
        )
        doc_outputs = _unique(
            [
                f"registry/phases/{run_id}/{phase_id}/phase_context.json",
                "docs/generated/current_phase.md",
            ]
        )

        phases.append(
            {
                "phase_id": phase_id,
                "name": name,
                "goal": goal if not profile_goals else _unique([goal] + profile_goals)[0],
                "scope_in": scope_in,
                "scope_out": scope_out,
                "requirement_ids": requirement_ids,
                "dependencies": dependency_phase_ids,
                "done_definition": done_definition,
                "acceptance_checks": acceptance_checks,
                "required_tests": _unique(profile_tests),
                "risk_notes": risk_notes,
                "doc_outputs": doc_outputs,
                "allowed_change_surfaces": _unique(profile_surfaces),
            }
        )

    if not phases:
        raise PhasePlanningError("phase planner produced no phases; check requirement categorization")

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
