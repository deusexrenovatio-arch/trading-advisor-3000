from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from .models import parse_normalized_requirements, parse_project_docs_bundle
except ImportError:  # pragma: no cover - script execution fallback
    from scripts.harness.models import parse_normalized_requirements, parse_project_docs_bundle


class ProjectDocsSynthesisError(RuntimeError):
    """Raised when project docs bundle synthesis cannot complete."""


@dataclass(frozen=True)
class ProjectDocsSynthesisResult:
    run_id: str
    output_path: Path
    requirement_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise ProjectDocsSynthesisError(f"required input not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ProjectDocsSynthesisError(f"invalid JSON in `{path}`: {exc}") from exc
    if not isinstance(payload, dict):
        raise ProjectDocsSynthesisError(f"payload at `{path}` must be JSON object")
    return payload


def _first_non_empty(candidates: list[str]) -> str:
    for item in candidates:
        cleaned = item.strip()
        if cleaned:
            return cleaned
    return ""


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def run_project_docs_synthesis(
    *,
    registry_root: Path,
    run_id: str,
) -> ProjectDocsSynthesisResult:
    run_root = registry_root / "intake" / run_id
    normalized_path = run_root / "normalized_requirements.json"
    normalized_payload = _read_json(normalized_path)
    normalized = parse_normalized_requirements(normalized_payload)

    if normalized.run_id != run_id:
        raise ProjectDocsSynthesisError(
            f"normalized requirements run_id mismatch: expected `{run_id}`, got `{normalized.run_id}`"
        )
    if not normalized.requirements:
        raise ProjectDocsSynthesisError("normalized requirements are empty")

    by_category: dict[str, list[object]] = {}
    for req in normalized.requirements:
        by_category.setdefault(req.category, []).append(req)

    functional = by_category.get("functional", [])
    integration = by_category.get("integration", [])
    data_items = by_category.get("data", [])
    non_functional = by_category.get("non_functional", [])
    constraints = by_category.get("constraint", [])
    assumptions = by_category.get("assumption", [])
    open_questions = by_category.get("open_question", [])
    acceptance = by_category.get("acceptance", [])
    security = by_category.get("security", [])

    problem_statement = _first_non_empty(
        [req.statement for req in functional] + [req.statement for req in constraints] + [req.statement for req in normalized.requirements]
    )
    if not problem_statement:
        raise ProjectDocsSynthesisError("cannot infer problem statement from normalized requirements")

    scope_in = _unique([req.statement for req in functional + integration + data_items][:20])
    scope_out = _unique([req.statement for req in constraints if "out of scope" in req.statement.lower()][:10])
    actors = _unique(["platform_operator", "requirements_analyst", "implementation_engineer"])
    business_flows = _unique([req.statement for req in functional[:8]] + [req.statement for req in acceptance[:4]])

    domain_glossary = [
        {"term": "registry", "definition": "Canonical machine-readable source of truth for harness artifacts."},
        {"term": "phase", "definition": "Ordered implementation stage governed by explicit acceptance gates."},
        {"term": "rework", "definition": "Targeted remediation loop initiated after rejected acceptance verdict."},
    ]
    data_objects = _unique(
        ["spec_manifest.json", "extracted_text_cache.json", "normalized_requirements.json", "project_docs_bundle.json"]
        + [req.title for req in data_items[:8]]
    )
    integration_points = _unique([req.statement for req in integration[:10]] + ["local filesystem", "codex exec runtime"])
    nfrs = _unique([req.statement for req in non_functional[:12]])
    architecture_constraints = _unique([req.statement for req in constraints[:12]] + [req.statement for req in security[:6]])
    design_decisions = _unique(
        [
            "Registry-first canonical artifacts remain the source of truth.",
            "Stage transitions are fail-closed on missing or invalid artifacts.",
            "Sequential-first orchestration is used before introducing swarm fan-out.",
        ]
    )

    risk_statements = _unique([req.statement for req in open_questions[:8]] + [req.statement for req in security[:8]])
    risk_register = []
    for index, statement in enumerate(risk_statements, start=1):
        risk_register.append(
            {
                "risk_id": f"RISK-{index:03d}",
                "statement": statement,
                "mitigation": "Clarify requirement and attach verifiable acceptance evidence before phase advancement.",
            }
        )
    if not risk_register:
        risk_register.append(
            {
                "risk_id": "RISK-001",
                "statement": "Potential requirement ambiguity between intake and acceptance criteria.",
                "mitigation": "Keep traceability matrix current and fail-closed on missing evidence.",
            }
        )

    open_questions_list = _unique([req.statement for req in open_questions[:20]])
    assumptions_list = _unique([req.statement for req in assumptions[:20]])
    adr_seeds = _unique(
        [
            "ADR: enforce typed handoff artifacts across all stage boundaries.",
            "ADR: keep generated docs as read-model derived from canonical registry state.",
            "ADR: maintain additive-first rollout without destabilizing existing shell gates.",
        ]
    )
    implementation_boundaries = _unique(
        [
            "No domain trading business logic in shell control-plane surfaces.",
            "No heavy observability platform during baseline harness rollout.",
            "No uncontrolled changes outside active phase surfaces.",
        ]
    )

    docs_payload = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "problem_statement": problem_statement,
        "scope_in": scope_in,
        "scope_out": scope_out,
        "actors": actors,
        "business_flows": business_flows,
        "domain_glossary": domain_glossary,
        "data_objects": data_objects,
        "integration_points": integration_points,
        "nfrs": nfrs,
        "architecture_constraints": architecture_constraints,
        "design_decisions": design_decisions,
        "risk_register": risk_register,
        "open_questions": open_questions_list,
        "assumptions": assumptions_list,
        "adr_seeds": adr_seeds,
        "implementation_boundaries": implementation_boundaries,
    }
    docs_model = parse_project_docs_bundle(docs_payload)
    output_path = run_root / "project_docs_bundle.json"
    output_path.write_text(
        json.dumps(docs_model.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return ProjectDocsSynthesisResult(
        run_id=run_id,
        output_path=output_path,
        requirement_count=len(normalized.requirements),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synthesize canonical project docs bundle from normalized requirements.")
    parser.add_argument("--run-id", required=True, help="Harness run identifier.")
    parser.add_argument("--registry-root", default="registry", help="Registry root (default: registry).")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        result = run_project_docs_synthesis(
            registry_root=Path(args.registry_root).resolve(),
            run_id=args.run_id,
        )
    except ProjectDocsSynthesisError as exc:
        raise SystemExit(str(exc)) from exc

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "project_docs_bundle": result.output_path.as_posix(),
                "requirement_count": result.requirement_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
