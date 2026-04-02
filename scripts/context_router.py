from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from critical_contours import load_critical_contours, match_critical_contours
from handoff_resolver import read_task_note_lines


TOKEN_RE = re.compile(r"[0-9]+|[^\W\d_]+", re.UNICODE)
STOP_WORDS = {
    "and",
    "artifact",
    "artifacts",
    "check",
    "checks",
    "code",
    "context",
    "contexts",
    "docs",
    "file",
    "files",
    "for",
    "from",
    "goal",
    "input",
    "inputs",
    "module",
    "modules",
    "output",
    "outputs",
    "path",
    "paths",
    "task",
    "tasks",
    "the",
    "use",
    "with",
}
DEFAULT_SESSION_HANDOFF_PATH = "docs/session_handoff.md"
COLD_CONTEXT_PREFIXES: tuple[str, ...] = (
    "plans/",
    "memory/",
    "codex_ai_delivery_shell_package/",
    "docs/tasks/archive/",
)
CRITICAL_CONTOUR_REVIEW_LENSES: tuple[str, ...] = (
    "architecture-review",
    "qa-test-engineer",
    "phase-acceptance-governor",
    "verification-before-completion",
)
CI_WORKFLOW_PREFIXES: tuple[str, ...] = (".github/workflows/",)
CI_REVIEW_LENSES: tuple[str, ...] = (
    "ci-bootstrap",
    "github-actions-ops",
    "commit-and-pr-hygiene",
)
ORCHESTRATION_REVIEW_PREFIXES: tuple[str, ...] = (
    "scripts/codex_phase_orchestrator.py",
    "scripts/codex_phase_policy.py",
    "docs/codex/prompts/phases/",
)
ORCHESTRATION_REVIEW_LENSES: tuple[str, ...] = (
    "phase-acceptance-governor",
    "verification-before-completion",
    "testing-suite",
)


@dataclass(frozen=True)
class ContextSpec:
    context_id: str
    summary: str
    owned_paths: tuple[str, ...]
    guarded_paths: tuple[str, ...]
    source_of_truth: tuple[str, ...]
    minimal_checks: tuple[str, ...]
    intent_keywords: tuple[str, ...]
    risk: str = "normal"


CONTEXTS: tuple[ContextSpec, ...] = (
    ContextSpec(
        context_id="CTX-OPS",
        summary="Governance scripts, process contracts, and lifecycle automation.",
        owned_paths=(
            "agents.md",
            "codeowners",
            "harness-guideline.md",
            "agent-runbook.md",
            "docs/agent/",
            "docs/agent-contexts/README.md",
            "docs/agent-contexts/CTX-OPS.md",
            "docs/workflows/",
            "docs/runbooks/",
            "docs/tasks/",
            "docs/session_handoff.md",
            "src/trading_advisor_3000/agents.md",
            "scripts/",
            "tests/process/",
            ".githooks/",
            ".github/workflows/",
        ),
        guarded_paths=("src/trading_advisor_3000/",),
        source_of_truth=(
            "AGENTS.md",
            "docs/agent/entrypoint.md",
            "docs/DEV_WORKFLOW.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python scripts/validate_task_request_contract.py",
        ),
        intent_keywords=("governance", "policy", "gate", "workflow", "handoff", "session", "plan", "memory"),
    ),
    ContextSpec(
        context_id="CTX-CONTRACTS",
        summary="High-risk contract and state surfaces: plans, memory, and policy validators.",
        owned_paths=(
            "configs/",
            "plans/",
            "memory/",
            "docs/checklists/",
            "docs/agent-contexts/CTX-CONTRACTS.md",
            "src/trading_advisor_3000/app/contracts/",
            "tests/app/contracts/",
            "scripts/critical_contours.py",
            "scripts/validate_task_request_contract.py",
            "scripts/validate_solution_intent.py",
            "scripts/validate_critical_contour_closure.py",
            "scripts/validate_plans.py",
            "scripts/validate_agent_memory.py",
            "scripts/validate_task_outcomes.py",
            "scripts/sync_task_outcomes.py",
            "scripts/sync_state_layout.py",
        ),
        guarded_paths=("src/trading_advisor_3000/app/interfaces/", "docs/architecture/"),
        source_of_truth=(
            "docs/checklists/task-request-contract.md",
            "docs/checklists/first-time-right-gate.md",
            "plans/items/index.yaml",
            "memory/task_outcomes.yaml",
        ),
        minimal_checks=(
            "python scripts/validate_task_request_contract.py",
            "python scripts/validate_plans.py",
            "python scripts/validate_agent_memory.py",
            "python scripts/validate_task_outcomes.py",
        ),
        intent_keywords=("contract", "state", "ledger", "plan", "memory", "schema", "high-risk"),
        risk="high",
    ),
    ContextSpec(
        context_id="CTX-ARCHITECTURE",
        summary="Architecture-as-docs package, ADRs, and boundary tests.",
        owned_paths=(
            "docs/architecture/",
            "docs/agent-contexts/CTX-ARCHITECTURE.md",
            "tests/architecture/",
        ),
        guarded_paths=("src/trading_advisor_3000/",),
        source_of_truth=(
            "docs/architecture/README.md",
            "docs/agent/domains.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python -m pytest tests/architecture -q",
        ),
        intent_keywords=("architecture", "adr", "boundary", "module", "layer"),
        risk="high",
    ),
    ContextSpec(
        context_id="CTX-DATA",
        summary="Data ingestion, normalization, and canonical data-plane flows.",
        owned_paths=(
            "docs/agent-contexts/CTX-DATA.md",
            "src/trading_advisor_3000/app/data_plane/",
            "src/trading_advisor_3000/migrations/",
            "tests/app/integration/test_phase2a_data_plane.py",
            "tests/app/unit/test_phase2a_",
            "tests/app/fixtures/data_plane/",
        ),
        guarded_paths=("src/trading_advisor_3000/app/research/", "src/trading_advisor_3000/app/runtime/"),
        source_of_truth=(
            "docs/agent-contexts/CTX-DATA.md",
            "docs/architecture/layers-v2.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python -m pytest tests/app/integration/test_phase2a_data_plane.py -q",
        ),
        intent_keywords=("data", "ingestion", "canonical", "provider", "dataset", "pipeline"),
    ),
    ContextSpec(
        context_id="CTX-RESEARCH",
        summary="Research and analysis surfaces for feature and experimental workflows.",
        owned_paths=(
            "docs/agent-contexts/CTX-RESEARCH.md",
            "src/trading_advisor_3000/app/research/",
            "src/trading_advisor_3000/spark_jobs/",
            "src/trading_advisor_3000/dagster_defs/phase2b_assets.py",
            "tests/app/integration/test_phase2b_research_plane.py",
            "tests/app/unit/test_phase2b_",
            "tests/app/fixtures/research/",
        ),
        guarded_paths=("src/trading_advisor_3000/app/runtime/", "src/trading_advisor_3000/app/interfaces/"),
        source_of_truth=(
            "docs/agent-contexts/CTX-RESEARCH.md",
            "docs/architecture/layers-v2.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python -m pytest tests/app/integration/test_phase2b_research_plane.py -q",
        ),
        intent_keywords=("research", "analysis", "features", "experiment", "backtest", "forward"),
    ),
    ContextSpec(
        context_id="CTX-ORCHESTRATION",
        summary="Runtime orchestration, execution coordination, and entrypoint flow wiring.",
        owned_paths=(
            "docs/agent-contexts/CTX-ORCHESTRATION.md",
            "src/trading_advisor_3000/__init__.py",
            "src/trading_advisor_3000/__main__.py",
            "src/trading_advisor_3000/app/__init__.py",
            "src/trading_advisor_3000/app/common/",
            "src/trading_advisor_3000/app/config/",
            "src/trading_advisor_3000/app.py",
            "src/trading_advisor_3000/app_metadata.py",
            "src/trading_advisor_3000/product_plane/",
            "src/trading_advisor_3000/app/runtime/",
            "src/trading_advisor_3000/app/execution/",
            "src/trading_advisor_3000/dagster_defs/",
            "tests/app/integration/test_phase2c_runtime.py",
            "tests/app/integration/test_phase2d_execution.py",
            "tests/app/integration/test_phase3_system_replay.py",
            "tests/app/integration/test_phase5_review_observability.py",
            "tests/app/integration/test_phase6_operational_hardening.py",
            "tests/app/unit/test_phase2c_",
            "tests/app/unit/test_phase2d_",
            "tests/app/unit/test_phase3_",
            "tests/app/unit/test_phase4_broker_sync.py",
            "tests/app/unit/test_phase4_reconciliation.py",
            "tests/app/unit/test_phase5_latency_metrics.py",
            "tests/app/unit/test_phase5_review_metrics.py",
            "tests/app/unit/test_phase6_",
            "tests/app/unit/test_phase7_",
        ),
        guarded_paths=("src/trading_advisor_3000/app/contracts/", "src/trading_advisor_3000/app/interfaces/"),
        source_of_truth=(
            "docs/agent-contexts/CTX-ORCHESTRATION.md",
            "docs/agent/runtime.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python -m pytest tests/app/integration/test_phase2c_runtime.py -q",
        ),
        intent_keywords=("runtime", "orchestration", "execution", "flow", "entrypoint", "coordination"),
    ),
    ContextSpec(
        context_id="CTX-API-UI",
        summary="API and operator-facing interface behavior.",
        owned_paths=(
            "docs/agent-contexts/CTX-API-UI.md",
            "src/trading_advisor_3000/app/interfaces/",
            "tests/app/integration/test_phase4_live_execution_controlled.py",
            "tests/app/unit/test_phase4_live_bridge.py",
            "tests/app/unit/test_phase5_observability_export.py",
        ),
        guarded_paths=("src/trading_advisor_3000/app/contracts/", "src/trading_advisor_3000/app/domain/"),
        source_of_truth=(
            "docs/agent-contexts/CTX-API-UI.md",
            "docs/architecture/modules.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python -m pytest tests/app/integration/test_phase4_live_execution_controlled.py -q",
        ),
        intent_keywords=("api", "interface", "delivery", "ui", "operator", "endpoint"),
    ),
    ContextSpec(
        context_id="CTX-DOMAIN",
        summary="Residual app-plane internals and package metadata not covered by data/research/runtime/interface contexts.",
        owned_paths=(
            "docs/agent-contexts/CTX-DOMAIN.md",
            "src/trading_advisor_3000/app/domain/",
            "tests/app/test_app_plane_metadata.py",
        ),
        guarded_paths=("docs/architecture/", "scripts/"),
        source_of_truth=(
            "docs/agent-contexts/CTX-DOMAIN.md",
            "docs/architecture/modules.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python -m pytest tests/app/test_app_plane_metadata.py -q",
        ),
        intent_keywords=("domain", "core behavior", "business rules"),
    ),
    ContextSpec(
        context_id="CTX-EXTERNAL-SOURCES",
        summary="External source contracts, ingestion interfaces, and lineage policy stubs.",
        owned_paths=(
            "docs/agent-contexts/CTX-EXTERNAL-SOURCES.md",
        ),
        guarded_paths=("src/trading_advisor_3000/",),
        source_of_truth=(
            "docs/agent-contexts/CTX-EXTERNAL-SOURCES.md",
            "docs/workflows/context-budget.md",
        ),
        minimal_checks=(
            "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
            "python scripts/validate_docs_links.py --roots AGENTS.md docs",
        ),
        intent_keywords=("external", "source", "integration", "lineage", "ingestion"),
    ),
    ContextSpec(
        context_id="CTX-SKILLS",
        summary="Local runtime skills catalog and governance policy.",
        owned_paths=(
            "docs/agent-contexts/CTX-SKILLS.md",
            ".cursor/skills/",
            "docs/agent/skills-catalog.md",
            "docs/agent/skills-routing.md",
            "docs/workflows/skill-governance-sync.md",
            "docs/planning/skills-roadmap.md",
            "scripts/sync_skills_catalog.py",
            "scripts/validate_skills.py",
            "scripts/skill_update_decision.py",
            "scripts/skill_precommit_gate.py",
            "tests/process/test_sync_skills_catalog.py",
            "tests/process/test_validate_skills.py",
            "tests/process/test_skill_update_decision.py",
            "tests/process/test_skill_precommit_gate.py",
        ),
        guarded_paths=("src/trading_advisor_3000/",),
        source_of_truth=(
            "docs/agent/skills-routing.md",
            "docs/workflows/skill-governance-sync.md",
        ),
        minimal_checks=(
            "python scripts/validate_skills.py --strict",
            "python scripts/sync_skills_catalog.py --check",
        ),
        intent_keywords=("skill", "skills", "catalog", "routing", "governance"),
    ),
)
CONTEXT_PRIORITY: tuple[str, ...] = tuple(spec.context_id for spec in CONTEXTS)


def _normalize_path(raw: str) -> str:
    return raw.replace("\\", "/").strip().lower()


def _deduplicate(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        marker = _normalize_path(item)
        if not marker or marker in seen:
            continue
        seen.add(marker)
        out.append(item)
    return out


def _tokenize(text: str) -> set[str]:
    return {
        token.lower()
        for token in TOKEN_RE.findall(text.lower())
        if token.lower() not in STOP_WORDS and len(token) > 2
    }


def _prefix_match(path: str, owned: str) -> bool:
    normalized_owned = _normalize_path(owned).rstrip("/")
    return path == normalized_owned or path.startswith(f"{normalized_owned}/")


def _collect_changed_from_git(git_ref: str) -> list[str]:
    changed_cmd = ["git", "diff", "--name-only", git_ref]
    changed = subprocess.run(changed_cmd, check=False, capture_output=True, text=True)
    if changed.returncode != 0:
        return []

    untracked_cmd = ["git", "ls-files", "--others", "--exclude-standard"]
    untracked = subprocess.run(untracked_cmd, check=False, capture_output=True, text=True)
    untracked_lines: list[str] = []
    if untracked.returncode == 0:
        untracked_lines = [line.strip() for line in untracked.stdout.splitlines() if line.strip()]

    changed_lines = [line.strip() for line in changed.stdout.splitlines() if line.strip()]
    return _deduplicate(changed_lines + untracked_lines)


def _collect_changed_from_stdin() -> list[str]:
    return [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]


def _context_token_pool(spec: ContextSpec) -> set[str]:
    fields = [spec.context_id.replace("CTX-", " "), spec.summary, *spec.intent_keywords]
    return _tokenize(" ".join(fields))


def _score_intent_contexts(
    *,
    request_text: str,
    session_handoff_text: str,
    target_modules: list[str],
) -> tuple[dict[str, int], list[str]]:
    intent_sources: list[str] = []
    intent_parts: list[str] = []
    if request_text.strip():
        intent_parts.append(request_text)
        intent_sources.append("request")
    if session_handoff_text.strip():
        intent_parts.append(session_handoff_text)
        intent_sources.append("session_handoff")
    if target_modules:
        intent_parts.extend(target_modules)
        intent_sources.append("target_module")
    intent_tokens = _tokenize(" ".join(intent_parts))
    if not intent_tokens and not target_modules:
        return {}, intent_sources
    scores: dict[str, int] = {}
    for spec in CONTEXTS:
        overlap = len(intent_tokens & _context_token_pool(spec))
        if overlap > 0:
            scores[spec.context_id] = overlap
    return scores, intent_sources


def _select_primary(counts: dict[str, int]) -> str | None:
    if not counts:
        return None
    ranked = sorted(
        counts.items(),
        key=lambda item: (-item[1], CONTEXT_PRIORITY.index(item[0])),
    )
    return ranked[0][0]


def _load_session_handoff_text(path_value: str | None) -> str:
    if not path_value:
        return ""
    path = Path(path_value)
    if not path.exists():
        return ""
    _resolved_path, lines, _is_pointer = read_task_note_lines(path)
    return "\n".join(lines)


def _detect_critical_contours(changed_files: list[str]) -> list[str]:
    config_path = Path("configs/critical_contours.yaml")
    if not changed_files or not config_path.exists():
        return []
    try:
        contours = load_critical_contours(config_path)
    except ValueError:
        return []
    return [contour.contour_id for contour in match_critical_contours(changed_files, contours)]


def _collect_required_review_lenses(*, changed_files: list[str], critical_contours: list[str]) -> list[str]:
    lenses: list[str] = []
    normalized_paths = [_normalize_path(path) for path in changed_files]

    def _append_many(values: tuple[str, ...]) -> None:
        for value in values:
            if value not in lenses:
                lenses.append(value)

    if critical_contours:
        _append_many(CRITICAL_CONTOUR_REVIEW_LENSES)

    if any(_prefix_match(path, prefix) for path in normalized_paths for prefix in CI_WORKFLOW_PREFIXES):
        _append_many(CI_REVIEW_LENSES)

    if any(_prefix_match(path, prefix) for path in normalized_paths for prefix in ORCHESTRATION_REVIEW_PREFIXES):
        _append_many(ORCHESTRATION_REVIEW_LENSES)

    return lenses


def route_files(
    changed_files: list[str],
    *,
    request_text: str = "",
    target_modules: list[str] | None = None,
    session_handoff_text: str = "",
    include_cold_paths: bool = False,
) -> dict[str, object]:
    target_modules = target_modules or []
    normalized = [(_normalize_path(path), path) for path in _deduplicate(changed_files)]
    matched: dict[str, list[str]] = defaultdict(list)
    cold_context_files: list[str] = []
    unmapped: list[str] = []
    critical_contours = _detect_critical_contours([original_path for _normalized, original_path in normalized])
    required_review_lenses = _collect_required_review_lenses(
        changed_files=[original_path for _normalized, original_path in normalized],
        critical_contours=critical_contours,
    )

    for normalized_path, original_path in normalized:
        if (not include_cold_paths) and any(
            _prefix_match(normalized_path, prefix) for prefix in COLD_CONTEXT_PREFIXES
        ):
            cold_context_files.append(original_path)
            continue
        owners = [
            spec.context_id
            for spec in CONTEXTS
            if any(_prefix_match(normalized_path, owned) for owned in spec.owned_paths)
        ]
        if not owners:
            unmapped.append(original_path)
            continue
        for context_id in owners:
            matched[context_id].append(original_path)

    intent_scores, intent_sources = _score_intent_contexts(
        request_text=request_text,
        session_handoff_text=session_handoff_text,
        target_modules=target_modules,
    )
    counts = {context_id: len(paths) for context_id, paths in matched.items()}
    primary = _select_primary(counts) or _select_primary(intent_scores)

    if not normalized and not intent_scores:
        return {
            "primary_context": None,
            "contexts": [],
            "intent_sources": intent_sources,
            "cold_context_files": [],
            "unmapped_files": [],
            "required_review_lenses": [],
            "recommendations": ["No files provided. Use --from-git, --stdin, or --changed-files."],
        }

    if matched:
        visible_contexts = sorted(matched, key=lambda cid: CONTEXT_PRIORITY.index(cid))
    else:
        max_score = max(intent_scores.values()) if intent_scores else 0
        visible_contexts = [
            context_id
            for context_id, score in sorted(
                intent_scores.items(),
                key=lambda item: (-item[1], CONTEXT_PRIORITY.index(item[0])),
            )
            if score >= max(max_score - 1, 1)
        ]
    if critical_contours and "CTX-ARCHITECTURE" not in visible_contexts:
        visible_contexts = sorted(
            [*visible_contexts, "CTX-ARCHITECTURE"],
            key=lambda cid: CONTEXT_PRIORITY.index(cid),
        )

    context_entries: list[dict[str, object]] = []
    for context_id in visible_contexts:
        spec = next(spec for spec in CONTEXTS if spec.context_id == context_id)
        matched_files = sorted(matched.get(context_id, []))
        context_entries.append(
            {
                "id": context_id,
                "summary": spec.summary,
                "risk": spec.risk,
                "matched_files_count": len(matched_files),
                "matched_files": matched_files,
                "guarded_paths": list(spec.guarded_paths),
                "source_of_truth": list(spec.source_of_truth),
                "minimal_checks": list(spec.minimal_checks),
                "intent_score": int(intent_scores.get(context_id, 0)),
                "policy_role": (
                    "companion"
                    if critical_contours and context_id == "CTX-ARCHITECTURE" and not matched_files
                    else "owned"
                ),
            }
        )

    recommendations: list[str] = []
    if not normalized and intent_scores:
        recommendations.append("No diff yet. Using request/session intent fallback.")
    if len(context_entries) > 1:
        recommendations.append(
            "Patch touches multiple contexts. Split by ownership to keep review and retrieval small."
        )
    if "CTX-CONTRACTS" in [entry["id"] for entry in context_entries] and len(context_entries) > 1:
        recommendations.append("High-risk mix detected. Use order: contracts -> code -> docs.")
    if unmapped:
        recommendations.append("Some files are unmapped. Classify manually before implementation.")
    if cold_context_files:
        recommendations.append(
            "Cold-context files are present. Keep them out of hot retrieval unless explicitly required."
        )
    if critical_contours:
        recommendations.append(
            "Critical contour detected. Declare Solution Intent in the task note before coding."
        )
        recommendations.append(
            "Critical contour requires CTX-ARCHITECTURE plus architecture, QA, acceptance, and completion-verification lenses."
        )
    if required_review_lenses and not critical_contours:
        recommendations.append(
            "Suggested review lenses: " + ", ".join(required_review_lenses) + "."
        )
    if not recommendations:
        recommendations.append("Patch is scoped to one context.")

    return {
        "primary_context": primary,
        "contexts": context_entries,
        "intent_sources": intent_sources,
        "cold_context_files": sorted(cold_context_files),
        "unmapped_files": sorted(unmapped),
        "critical_contours": critical_contours,
        "required_review_lenses": required_review_lenses,
        "recommendations": recommendations,
    }


def _render_text(result: dict[str, object]) -> str:
    lines: list[str] = [f"primary_context: {result.get('primary_context')}"]
    intent_sources = result.get("intent_sources", [])
    if isinstance(intent_sources, list) and intent_sources:
        lines.append(f"intent_sources: {', '.join(intent_sources)}")
    contexts = result.get("contexts", [])
    if isinstance(contexts, list):
        for item in contexts:
            if not isinstance(item, dict):
                continue
            lines.append(f"- {item.get('id')} files={item.get('matched_files_count')} risk={item.get('risk')}")
            matched_files = item.get("matched_files", [])
            if isinstance(matched_files, list):
                for path in matched_files:
                    lines.append(f"  * {path}")
    critical_contours = result.get("critical_contours", [])
    if isinstance(critical_contours, list) and critical_contours:
        lines.append(f"critical_contours: {', '.join(critical_contours)}")
    review_lenses = result.get("required_review_lenses", [])
    if isinstance(review_lenses, list) and review_lenses:
        lines.append(f"required_review_lenses: {', '.join(review_lenses)}")
    unmapped = result.get("unmapped_files", [])
    if isinstance(unmapped, list) and unmapped:
        lines.append("unmapped_files:")
        for path in unmapped:
            lines.append(f"- {path}")
    cold_files = result.get("cold_context_files", [])
    if isinstance(cold_files, list) and cold_files:
        lines.append("cold_context_files:")
        for path in cold_files:
            lines.append(f"- {path}")
    recommendations = result.get("recommendations", [])
    if isinstance(recommendations, list):
        lines.append("recommendations:")
        for note in recommendations:
            lines.append(f"- {note}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Route changed files to shell ownership contexts.")
    parser.add_argument("--from-git", action="store_true", help="Load changed files from git diff.")
    parser.add_argument("--git-ref", type=str, default="HEAD", help="Git ref used with --from-git.")
    parser.add_argument("--stdin", action="store_true", help="Load newline-separated file paths from stdin.")
    parser.add_argument("--changed-files", nargs="*", default=[], help="Explicit changed file list.")
    parser.add_argument("--request", type=str, default="", help="Optional request text for intent fallback.")
    parser.add_argument(
        "--target-module",
        action="append",
        default=[],
        help="Optional module/path hint. Repeat for multiple targets.",
    )
    parser.add_argument(
        "--session-handoff-path",
        type=str,
        default=DEFAULT_SESSION_HANDOFF_PATH,
        help=f"Optional session handoff path (default: {DEFAULT_SESSION_HANDOFF_PATH}).",
    )
    parser.add_argument(
        "--include-cold-paths",
        action="store_true",
        help="Include cold-context paths in ownership routing (used by coverage validators).",
    )
    parser.add_argument("--format", choices=("json", "text"), default="json", help="Output format.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    changed_files: list[str] = list(args.changed_files)
    if args.from_git:
        changed_files.extend(_collect_changed_from_git(args.git_ref))
    if args.stdin:
        changed_files.extend(_collect_changed_from_stdin())

    result = route_files(
        changed_files,
        request_text=args.request,
        target_modules=list(args.target_module),
        session_handoff_text=_load_session_handoff_text(args.session_handoff_path),
        include_cold_paths=bool(args.include_cold_paths),
    )
    if args.format == "text":
        print(_render_text(result))
        return 0
    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
