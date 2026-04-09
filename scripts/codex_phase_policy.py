from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from typing import Any


DEFAULT_WORKER_MODEL = "gpt-5.3-codex"
DEFAULT_ACCEPTOR_MODEL = "gpt-5.4"
DEFAULT_REMEDIATION_MODEL = DEFAULT_WORKER_MODEL
ESCALATED_REMEDIATION_MODEL = DEFAULT_ACCEPTOR_MODEL
REMEDIATION_ESCALATION_ATTEMPT = 3
WORKER_BEGIN = "BEGIN_PHASE_WORKER_JSON"
WORKER_END = "END_PHASE_WORKER_JSON"
ACCEPTANCE_BEGIN = "BEGIN_PHASE_ACCEPTANCE_JSON"
ACCEPTANCE_END = "END_PHASE_ACCEPTANCE_JSON"
ROUTE_MODE = "governed-phase-orchestration"
ROUTE_GUARDRAILS = (
    "no silent assumptions",
    "no skipped required checks",
    "no silent fallbacks",
    "no deferred critical work",
    "no unresolved recurrence risk",
    "no unresolved operational exceptions",
    "acceptance requires architecture, test, and docs closure",
    "completion claims require executable evidence before unlock",
)
REQUIRED_ACCEPTOR_SKILLS = (
    "phase-acceptance-governor",
    "code-reviewer",
    "testing-suite",
    "docs-sync",
    "verification-before-completion",
)
PLACEHOLDER_TOKEN_RE = re.compile(r"<[^>\n]+>")
DOC_CONTEXT_REQUIRED_FIELDS = (
    "source_documents",
    "materialized_documents",
    "preserved_goals",
    "preserved_acceptance_criteria",
)
LEARNING_MODES = ("off", "soft", "strict")
LEARNING_BLOCKING_DECISION_STATUSES = {"blocked", "update_required"}


@dataclass
class RoleLaunchConfig:
    profile: str
    model: str
    config_overrides: tuple[str, ...]


@dataclass
class SkillBinding:
    skill_id: str
    path: str
    sha256: str


@dataclass
class WorkerReport:
    status: str
    summary: str
    route_signal: str
    files_touched: list[str]
    checks_run: list[str]
    remaining_risks: list[str]
    assumptions: list[str]
    skips: list[str]
    fallbacks: list[str]
    deferred_work: list[str]
    evidence_contract: dict[str, Any] | None = None
    documentation_context: dict[str, Any] | None = None


@dataclass
class PhaseEvidenceRequirement:
    owned_surfaces: list[str]
    delivered_proof_class: str
    requires_real_bindings: bool


@dataclass
class AcceptanceBlocker:
    id: str
    title: str
    why: str
    remediation: str


@dataclass
class AcceptanceResult:
    verdict: str
    summary: str
    route_signal: str
    used_skills: list[str]
    blockers: list[AcceptanceBlocker]
    rerun_checks: list[str]
    evidence_gaps: list[str]
    prohibited_findings: list[str]
    policy_blockers: list[AcceptanceBlocker] = field(default_factory=list)
    recurrence_risks: list[str] = field(default_factory=list)
    operational_exceptions: list[str] = field(default_factory=list)


@dataclass
class AttemptRecord:
    attempt: int
    kind: str
    execution_model: str
    worker_summary: str
    worker_route_signal: str
    worker_report_path: str
    changed_files_path: str
    acceptance_json_path: str
    acceptance_md_path: str
    acceptor_route_signal: str
    acceptor_used_skills: list[str]
    verdict: str
    blockers_total: int
    policy_blockers_total: int
    openspace_learning_mode: str = "off"
    openspace_learning_status: str = "disabled"
    openspace_learning_report_path: str = ""


def normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def normalize_worker_payload(payload: dict[str, Any]) -> WorkerReport:
    status = str(payload.get("status", "")).strip().upper()
    if status != "DONE":
        raise ValueError(f"worker status must be DONE, got {status!r}")
    route_signal = str(payload.get("route_signal", "")).strip()
    if not route_signal:
        raise ValueError("worker payload missing required `route_signal`")
    evidence_contract = payload.get("evidence_contract")
    if evidence_contract is not None and not isinstance(evidence_contract, dict):
        raise ValueError("worker payload `evidence_contract` must be an object when present")
    documentation_context = payload.get("documentation_context")
    if documentation_context is not None and not isinstance(documentation_context, dict):
        raise ValueError("worker payload `documentation_context` must be an object when present")
    return WorkerReport(
        status=status,
        summary=str(payload.get("summary", "")).strip() or "No summary provided.",
        route_signal=route_signal,
        files_touched=normalize_string_list(payload.get("files_touched", [])),
        checks_run=normalize_string_list(payload.get("checks_run", [])),
        remaining_risks=normalize_string_list(payload.get("remaining_risks", [])),
        assumptions=normalize_string_list(payload.get("assumptions", [])),
        skips=normalize_string_list(payload.get("skips", [])),
        fallbacks=normalize_string_list(payload.get("fallbacks", [])),
        deferred_work=normalize_string_list(payload.get("deferred_work", [])),
        evidence_contract=evidence_contract if isinstance(evidence_contract, dict) else None,
        documentation_context=documentation_context if isinstance(documentation_context, dict) else None,
    )


def normalize_acceptance_payload(payload: dict[str, Any]) -> AcceptanceResult:
    blockers: list[AcceptanceBlocker] = []
    for item in payload.get("blockers", []):
        if not isinstance(item, dict):
            continue
        blockers.append(
            AcceptanceBlocker(
                id=str(item.get("id", "")).strip() or f"B{len(blockers) + 1}",
                title=str(item.get("title", "")).strip() or "Unspecified blocker",
                why=str(item.get("why", "")).strip() or "No reason provided.",
                remediation=str(item.get("remediation", "")).strip() or "No remediation provided.",
            )
        )
    verdict = str(payload.get("verdict", "")).strip().upper()
    if verdict not in {"PASS", "BLOCKED"}:
        raise ValueError(f"acceptance verdict must be PASS or BLOCKED, got {verdict!r}")
    route_signal = str(payload.get("route_signal", "")).strip()
    if not route_signal:
        raise ValueError("acceptance payload missing required `route_signal`")
    used_skills = normalize_string_list(payload.get("used_skills", []))
    if not used_skills:
        raise ValueError("acceptance payload missing required `used_skills`")
    if "recurrence_risks" not in payload:
        raise ValueError("acceptance payload missing required `recurrence_risks`")
    if "operational_exceptions" not in payload:
        raise ValueError("acceptance payload missing required `operational_exceptions`")
    return AcceptanceResult(
        verdict=verdict,
        summary=str(payload.get("summary", "")).strip() or "No summary provided.",
        route_signal=route_signal,
        used_skills=used_skills,
        blockers=blockers,
        rerun_checks=normalize_string_list(payload.get("rerun_checks", [])),
        evidence_gaps=normalize_string_list(payload.get("evidence_gaps", [])),
        prohibited_findings=normalize_string_list(payload.get("prohibited_findings", [])),
        policy_blockers=[],
        recurrence_risks=normalize_string_list(payload.get("recurrence_risks", [])),
        operational_exceptions=normalize_string_list(payload.get("operational_exceptions", [])),
    )


def make_policy_blocker(kind: str, index: int, detail: str) -> AcceptanceBlocker:
    normalized_kind = kind.upper().replace(" ", "_")
    titles = {
        "ASSUMPTION": "Unresolved implementation assumption",
        "SKIP": "Skipped required check or step",
        "FALLBACK": "Silent or unapproved fallback path",
        "DEFERRED": "Deferred critical work remains in phase",
        "EVIDENCE_GAP": "Required evidence is missing",
        "PROHIBITED_FINDING": "Prohibited acceptance finding present",
        "PASS_WITH_BLOCKERS": "Acceptance declared PASS despite blockers",
        "WORKER_DOC_EDIT": "Worker edited documentation surface directly",
        "DOC_CONTEXT_GAP": "Documentation context coverage is incomplete",
        "REVIEW_LENS_GAP": "Acceptance review lens set is incomplete",
        "RECURRENCE_RISK": "Recurrence risk remains unresolved",
        "OPERATIONAL_EXCEPTION": "Operational exception remains unresolved",
        "LEARNING_HOOK": "OpenSpace learning evidence is not compliant",
    }
    remediations = {
        "ASSUMPTION": "Replace the assumption with implemented behavior or stop and update the phase contract explicitly before rerun.",
        "SKIP": "Run the required check or reduce scope until the phase can prove closure without a skip.",
        "FALLBACK": "Remove the fallback or obtain an explicit contract decision before rerun; do not leave it silent.",
        "DEFERRED": "Complete the critical work now or change the phase contract before claiming the phase is ready.",
        "EVIDENCE_GAP": "Produce the missing evidence and rerun acceptance.",
        "PROHIBITED_FINDING": "Resolve the prohibited condition and rerun acceptance.",
        "PASS_WITH_BLOCKERS": "Do not pass the phase while blockers remain; rerun acceptance with a consistent verdict.",
        "WORKER_DOC_EDIT": "Move documentation edits into remediation and rerun acceptance with a phase-scoped docs handoff.",
        "DOC_CONTEXT_GAP": "Provide full original/materialized docs context and explicit goal-preservation evidence in remediation output.",
        "REVIEW_LENS_GAP": "Add the missing acceptance review lenses and rerun with an explicit adversarial review payload.",
        "RECURRENCE_RISK": "Close the recurrence path with a prevention check or keep the phase blocked.",
        "OPERATIONAL_EXCEPTION": "Resolve the operational exception or turn it into an explicit blocker instead of a note/workaround.",
        "LEARNING_HOOK": "Generate a valid OpenSpace learning report for this attempt and rerun acceptance.",
    }
    return AcceptanceBlocker(
        id=f"P-{normalized_kind}-{index}",
        title=titles.get(normalized_kind, "Policy blocker"),
        why=detail,
        remediation=remediations.get(normalized_kind, "Resolve the policy blocker before rerun."),
    )


def _normalize_learning_mode(raw: str) -> str:
    value = str(raw or "off").strip().lower()
    if value not in LEARNING_MODES:
        raise ValueError(f"learning mode must be one of {', '.join(LEARNING_MODES)}, got {raw!r}")
    return value


def _proof_rank(value: str) -> int:
    order = ["doc", "schema", "unit", "integration", "staging-real", "live-real"]
    try:
        return order.index(value)
    except ValueError:
        return -1


def _parse_worker_evidence_contract(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {
            "surfaces": [],
            "proof_class": "",
            "artifact_paths": [],
            "checks": [],
            "real_bindings": [],
        }
    return {
        "surfaces": normalize_string_list(value.get("surfaces", [])),
        "proof_class": str(value.get("proof_class", "")).strip().lower(),
        "artifact_paths": normalize_string_list(value.get("artifact_paths", [])),
        "checks": normalize_string_list(value.get("checks", [])),
        "real_bindings": normalize_string_list(value.get("real_bindings", [])),
    }


def _has_placeholder_token(text: str) -> bool:
    return bool(PLACEHOLDER_TOKEN_RE.search(text or ""))


def _is_release_decision_emission_command(text: str) -> bool:
    normalized = (text or "").strip().lower().replace("\\", "/")
    if not normalized:
        return False
    return bool(
        re.search(
            r"(^|\s)(python(\.exe)?\s+)?[^\s\"']*scripts/build_governed_release_decision\.py(\s|$)",
            normalized,
        )
    )


def _is_release_decision_output_artifact(text: str) -> bool:
    normalized = (text or "").strip().lower().replace("\\", "/")
    if not normalized:
        return False
    return normalized.endswith("/release-decision.json") or normalized == "release-decision.json"


def _is_continue_route_command(text: str) -> bool:
    normalized = (text or "").strip().lower()
    return "codex_governed_entry.py" in normalized and "--route continue" in normalized


def _is_documentation_path(text: str) -> bool:
    normalized = (text or "").strip().lower().replace("\\", "/")
    if not normalized:
        return False
    if normalized.startswith("artifacts/") or normalized.startswith(".runlogs/"):
        return False
    if normalized.startswith("docs/"):
        return True
    filename = normalized.rsplit("/", 1)[-1]
    if filename in {"readme.md", "agents.md"}:
        return True
    return normalized.endswith(".md") or normalized.endswith(".rst") or normalized.endswith(".txt")


def _parse_documentation_context(value: dict[str, Any] | None) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {
            "source_documents": [],
            "materialized_documents": [],
            "preserved_goals": [],
            "preserved_acceptance_criteria": [],
            "unresolved_conflicts": [],
        }
    return {
        "source_documents": normalize_string_list(
            value.get("source_documents") or value.get("original_documents") or []
        ),
        "materialized_documents": normalize_string_list(value.get("materialized_documents") or []),
        "preserved_goals": normalize_string_list(
            value.get("preserved_goals") or value.get("goals_digest") or []
        ),
        "preserved_acceptance_criteria": normalize_string_list(
            value.get("preserved_acceptance_criteria") or value.get("acceptance_criteria_digest") or []
        ),
        "unresolved_conflicts": normalize_string_list(value.get("unresolved_conflicts") or []),
    }


def apply_acceptance_policy(
    worker: WorkerReport,
    acceptance: AcceptanceResult,
    phase_requirement: PhaseEvidenceRequirement | None = None,
    learning_mode: str = "off",
    openspace_learning_report: dict[str, Any] | None = None,
) -> AcceptanceResult:
    normalized_learning_mode = _normalize_learning_mode(learning_mode)
    policy_blockers: list[AcceptanceBlocker] = []
    missing_acceptor_skills = [
        skill_id for skill_id in REQUIRED_ACCEPTOR_SKILLS if skill_id not in acceptance.used_skills
    ]
    if missing_acceptor_skills:
        policy_blockers.append(
            make_policy_blocker(
                "review_lens_gap",
                1,
                "Acceptance payload is missing required review lenses: " + ", ".join(missing_acceptor_skills),
            )
        )
    for idx, item in enumerate(worker.assumptions, start=1):
        policy_blockers.append(make_policy_blocker("assumption", idx, item))
    for idx, item in enumerate(worker.skips, start=1):
        policy_blockers.append(make_policy_blocker("skip", idx, item))
    for idx, item in enumerate(worker.fallbacks, start=1):
        policy_blockers.append(make_policy_blocker("fallback", idx, item))
    for idx, item in enumerate(worker.deferred_work, start=1):
        policy_blockers.append(make_policy_blocker("deferred", idx, item))
    for idx, item in enumerate(acceptance.evidence_gaps, start=1):
        policy_blockers.append(make_policy_blocker("evidence_gap", idx, item))
    for idx, item in enumerate(acceptance.prohibited_findings, start=1):
        policy_blockers.append(make_policy_blocker("prohibited_finding", idx, item))
    for idx, item in enumerate(acceptance.recurrence_risks, start=1):
        policy_blockers.append(make_policy_blocker("recurrence_risk", idx, item))
    for idx, item in enumerate(acceptance.operational_exceptions, start=1):
        policy_blockers.append(make_policy_blocker("operational_exception", idx, item))

    evidence = _parse_worker_evidence_contract(worker.evidence_contract)
    all_checks = normalize_string_list([*worker.checks_run, *evidence["checks"]])
    all_artifact_paths = normalize_string_list([*worker.files_touched, *evidence["artifact_paths"]])

    placeholder_checks = [item for item in all_checks if _has_placeholder_token(item)]
    if placeholder_checks:
        policy_blockers.append(
            make_policy_blocker(
                "evidence_gap",
                len(policy_blockers) + 1,
                "Worker evidence checks must be exact executed commands; placeholder tokens are not allowed: "
                + ", ".join(placeholder_checks),
            )
        )

    release_decision_emission_detected = any(
        _is_release_decision_emission_command(item) for item in all_checks
    ) or any(_is_release_decision_output_artifact(item) for item in all_artifact_paths)
    if release_decision_emission_detected:
        policy_blockers.append(
            make_policy_blocker(
                "prohibited_finding",
                len(policy_blockers) + 1,
                "Worker/remediation evidence includes release-decision emission before acceptance-owned closeout.",
            )
        )

    route_signal = worker.route_signal.strip().lower()
    worker_docs_edits = [item for item in worker.files_touched if _is_documentation_path(item)]
    if route_signal.startswith("worker:") and worker_docs_edits:
        policy_blockers.append(
            make_policy_blocker(
                "worker_doc_edit",
                len(policy_blockers) + 1,
                "Worker phase attempted to modify documentation files directly: "
                + ", ".join(worker_docs_edits),
            )
        )
    if route_signal.startswith("remediation:") and worker_docs_edits:
        doc_context = _parse_documentation_context(worker.documentation_context)
        missing_doc_context_fields = [field for field in DOC_CONTEXT_REQUIRED_FIELDS if not doc_context[field]]
        if missing_doc_context_fields:
            policy_blockers.append(
                make_policy_blocker(
                    "doc_context_gap",
                    len(policy_blockers) + 1,
                    "Remediation changed documentation but documentation_context is incomplete; missing: "
                    + ", ".join(missing_doc_context_fields),
                )
            )
        placeholder_doc_context = [
            item
            for field in DOC_CONTEXT_REQUIRED_FIELDS
            for item in doc_context[field]
            if _has_placeholder_token(item)
        ]
        if placeholder_doc_context:
            policy_blockers.append(
                make_policy_blocker(
                    "doc_context_gap",
                    len(policy_blockers) + 1,
                    "documentation_context must not contain placeholder tokens: "
                    + ", ".join(placeholder_doc_context),
                )
            )
        materialized_docs = [item.lower().replace("\\", "/") for item in doc_context["materialized_documents"]]
        has_contract_context = any(item.startswith("docs/codex/contracts/") for item in materialized_docs)
        has_module_context = any(item.startswith("docs/codex/modules/") for item in materialized_docs)
        if not has_contract_context or not has_module_context:
            missing_context = []
            if not has_contract_context:
                missing_context.append("docs/codex/contracts/*")
            if not has_module_context:
                missing_context.append("docs/codex/modules/*")
            policy_blockers.append(
                make_policy_blocker(
                    "doc_context_gap",
                    len(policy_blockers) + 1,
                    "Remediation documentation_context.materialized_documents is incomplete; expected coverage for "
                    + ", ".join(missing_context),
                )
            )

    if phase_requirement and phase_requirement.owned_surfaces:
        if worker.evidence_contract is None:
            policy_blockers.append(
                make_policy_blocker(
                    "evidence_gap",
                    len(policy_blockers) + 1,
                    "Worker evidence contract is missing for a phase that owns one or more surfaces.",
                )
            )
        else:
            missing_surfaces = [item for item in phase_requirement.owned_surfaces if item not in evidence["surfaces"]]
            if missing_surfaces:
                policy_blockers.append(
                    make_policy_blocker(
                        "evidence_gap",
                        len(policy_blockers) + 1,
                        "Worker evidence contract does not cover owned surfaces: " + ", ".join(missing_surfaces),
                    )
                )
            if not evidence["artifact_paths"]:
                policy_blockers.append(
                    make_policy_blocker(
                        "evidence_gap",
                        len(policy_blockers) + 1,
                        "Worker evidence contract has no artifact_paths.",
                    )
                )
            if not evidence["checks"]:
                policy_blockers.append(
                    make_policy_blocker(
                        "evidence_gap",
                        len(policy_blockers) + 1,
                        "Worker evidence contract has no checks.",
                    )
                )
            if _proof_rank(evidence["proof_class"]) < _proof_rank(phase_requirement.delivered_proof_class):
                policy_blockers.append(
                    make_policy_blocker(
                        "evidence_gap",
                        len(policy_blockers) + 1,
                        "Worker evidence proof_class "
                        f"`{evidence['proof_class'] or 'missing'}` is weaker than phase requirement "
                        f"`{phase_requirement.delivered_proof_class}`.",
                    )
                )
            if phase_requirement.requires_real_bindings and not evidence["real_bindings"]:
                policy_blockers.append(
                    make_policy_blocker(
                        "evidence_gap",
                        len(policy_blockers) + 1,
                        "Worker evidence contract is missing real_bindings for a phase that requires real bindings.",
                    )
                )
            if phase_requirement.delivered_proof_class == "live-real":
                continue_checks = [item for item in all_checks if _is_continue_route_command(item)]
                if continue_checks and all("--dry-run" in item.lower() for item in continue_checks):
                    policy_blockers.append(
                        make_policy_blocker(
                            "prohibited_finding",
                            len(policy_blockers) + 1,
                            "Live-real route evidence used only dry-run governed continue commands.",
                        )
                    )

    if acceptance.verdict == "PASS" and acceptance.blockers:
        policy_blockers.append(
            make_policy_blocker(
                "pass_with_blockers",
                1,
                "Acceptance returned PASS even though blocker items were present in the payload.",
            )
        )

    if normalized_learning_mode == "strict":
        if not isinstance(openspace_learning_report, dict):
            policy_blockers.append(
                make_policy_blocker(
                    "learning_hook",
                    len(policy_blockers) + 1,
                    "Strict learning mode requires an OpenSpace learning report, but the report is missing.",
                )
            )
        else:
            learning_status = str(openspace_learning_report.get("status", "")).strip().lower()
            decision_status = str(openspace_learning_report.get("decision_status", "")).strip().lower()
            recommendation = str(openspace_learning_report.get("recommendation", "")).strip()
            if learning_status != "ok":
                detail = (
                    "OpenSpace learning hook did not complete successfully in strict mode "
                    f"(status={learning_status or 'missing'})."
                )
                error = str(openspace_learning_report.get("error", "")).strip()
                if error:
                    detail += f" error={error}"
                policy_blockers.append(
                    make_policy_blocker(
                        "learning_hook",
                        len(policy_blockers) + 1,
                        detail,
                    )
                )
            elif not decision_status:
                policy_blockers.append(
                    make_policy_blocker(
                        "learning_hook",
                        len(policy_blockers) + 1,
                        "OpenSpace learning report is missing `decision_status` in strict mode.",
                    )
                )
            elif decision_status in LEARNING_BLOCKING_DECISION_STATUSES:
                detail = (
                    "OpenSpace learning report requires governance updates "
                    f"(decision_status={decision_status})."
                )
                if recommendation:
                    detail += f" recommendation={recommendation}"
                policy_blockers.append(
                    make_policy_blocker(
                        "learning_hook",
                        len(policy_blockers) + 1,
                        detail,
                    )
                )

    final_blockers = [*acceptance.blockers, *policy_blockers]
    final_verdict = "PASS" if acceptance.verdict == "PASS" and not final_blockers else "BLOCKED"
    summary = acceptance.summary
    if policy_blockers and acceptance.verdict == "PASS":
        summary = (
            "Orchestrator policy converted PASS to BLOCKED because prohibited unresolved items "
            "or missing evidence were reported."
        )
    return AcceptanceResult(
        verdict=final_verdict,
        summary=summary,
        route_signal=acceptance.route_signal,
        used_skills=acceptance.used_skills,
        blockers=final_blockers,
        rerun_checks=acceptance.rerun_checks,
        evidence_gaps=acceptance.evidence_gaps,
        prohibited_findings=acceptance.prohibited_findings,
        policy_blockers=policy_blockers,
        recurrence_risks=acceptance.recurrence_risks,
        operational_exceptions=acceptance.operational_exceptions,
    )


def render_acceptance_markdown(result: AcceptanceResult) -> str:
    lines = [
        "# Acceptance Result",
        "",
        f"- Verdict: {result.verdict}",
        f"- Summary: {result.summary}",
        f"- Route Signal: {result.route_signal}",
        f"- Used Skills: {', '.join(result.used_skills) if result.used_skills else 'none'}",
        "",
        "## Blockers",
    ]
    if not result.blockers:
        lines.append("- none")
    else:
        for blocker in result.blockers:
            lines.append(f"- {blocker.id}: {blocker.title}")
            lines.append(f"  why: {blocker.why}")
            lines.append(f"  remediation: {blocker.remediation}")
    lines.extend(["", "## Evidence Gaps"])
    if not result.evidence_gaps:
        lines.append("- none")
    else:
        for item in result.evidence_gaps:
            lines.append(f"- {item}")
    lines.extend(["", "## Prohibited Findings"])
    if not result.prohibited_findings:
        lines.append("- none")
    else:
        for item in result.prohibited_findings:
            lines.append(f"- {item}")
    lines.extend(["", "## Recurrence Risks"])
    if not result.recurrence_risks:
        lines.append("- none")
    else:
        for item in result.recurrence_risks:
            lines.append(f"- {item}")
    lines.extend(["", "## Operational Exceptions"])
    if not result.operational_exceptions:
        lines.append("- none")
    else:
        for item in result.operational_exceptions:
            lines.append(f"- {item}")
    lines.extend(["", "## Policy Blockers"])
    if not result.policy_blockers:
        lines.append("- none")
    else:
        for blocker in result.policy_blockers:
            lines.append(f"- {blocker.id}: {blocker.title}")
            lines.append(f"  why: {blocker.why}")
            lines.append(f"  remediation: {blocker.remediation}")
    lines.extend(["", "## Rerun Checks"])
    if not result.rerun_checks:
        lines.append("- none")
    else:
        for item in result.rerun_checks:
            lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def route_guardrail_text() -> str:
    return "\n".join(f"- {item}" for item in ROUTE_GUARDRAILS)


def route_trace_for_attempts(attempt_records: list[AttemptRecord], next_phase: str, final_status: str) -> list[str]:
    trace: list[str] = []
    for item in attempt_records:
        trace.append(f"{item.kind}[{item.attempt}] {item.worker_route_signal}")
        trace.append(
            f"acceptance[{item.attempt}] {item.acceptor_route_signal} => {item.verdict} "
            f"(policy_blockers={item.policy_blockers_total})"
        )
    if final_status == "accepted":
        trace.append(f"unlock next phase => {next_phase}")
    else:
        trace.append("phase remains locked")
    return trace


def state_payload(
    *,
    run_id: str,
    updated_at: str,
    backend: str,
    phase_brief: str,
    phase_name: str,
    phase_status: str,
    attempt_records: list[AttemptRecord],
    final_status: str,
    next_phase: str,
    role_configs: dict[str, RoleLaunchConfig],
    role_skill_bindings: dict[str, list[SkillBinding]],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "updated_at": updated_at,
        "backend": backend,
        "route_mode": ROUTE_MODE,
        "route_guardrails": list(ROUTE_GUARDRAILS),
        "role_configs": {
            name: {
                "profile": config.profile,
                "model": config.model,
                "config_overrides": list(config.config_overrides),
            }
            for name, config in role_configs.items()
        },
        "role_skill_bindings": {
            name: [asdict(binding) for binding in bindings]
            for name, bindings in role_skill_bindings.items()
        },
        "phase_brief": phase_brief,
        "phase_name": phase_name,
        "phase_status": phase_status,
        "attempts": [asdict(item) for item in attempt_records],
        "attempts_total": len(attempt_records),
        "final_status": final_status,
        "next_phase": next_phase,
        "route_trace": route_trace_for_attempts(attempt_records, next_phase=next_phase, final_status=final_status),
    }


def render_route_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Route Report",
        "",
        f"- Route Mode: {payload.get('route_mode', ROUTE_MODE)}",
        f"- Phase: {payload.get('phase_name', 'unknown')}",
        f"- Backend: {payload.get('backend', 'unknown')}",
        f"- Final Status: {payload.get('final_status', 'unknown')}",
        "",
        "## Guardrails",
    ]
    for item in payload.get("route_guardrails", []):
        lines.append(f"- {item}")

    role_configs = payload.get("role_configs", {})
    if isinstance(role_configs, dict):
        lines.extend(["", "## Role Models"])
        for role_name in ("worker", "acceptor", "remediation"):
            role_payload = role_configs.get(role_name, {})
            if not isinstance(role_payload, dict):
                continue
            lines.append(
                f"- {role_name}: model={role_payload.get('model') or 'default'}, "
                f"profile={role_payload.get('profile') or 'none'}"
            )

    role_skill_bindings = payload.get("role_skill_bindings", {})
    if isinstance(role_skill_bindings, dict):
        lines.extend(["", "## Bound Skills"])
        has_any = False
        for role_name in ("worker", "acceptor", "remediation"):
            bindings = role_skill_bindings.get(role_name, [])
            if not isinstance(bindings, list) or not bindings:
                continue
            has_any = True
            for binding in bindings:
                if not isinstance(binding, dict):
                    continue
                lines.append(
                    f"- {role_name}: {binding.get('skill_id')} "
                    f"(sha256={binding.get('sha256')}, path={binding.get('path')})"
                )
        if not has_any:
            lines.append("- none")

    lines.extend(["", "## Route Trace"])
    route_trace = payload.get("route_trace", [])
    if isinstance(route_trace, list) and route_trace:
        for item in route_trace:
            lines.append(f"- {item}")
    else:
        lines.append("- none")

    lines.extend(["", "## Attempts"])
    attempts = payload.get("attempts", [])
    if isinstance(attempts, list) and attempts:
        for item in attempts:
            if not isinstance(item, dict):
                continue
            lines.append(
                "- attempt {attempt}: {kind} -> {verdict} "
                "(model={model}; worker_route={worker_route}; acceptor_route={acceptor_route}; policy_blockers={policy})".format(
                    attempt=item.get("attempt"),
                    kind=item.get("kind"),
                    verdict=item.get("verdict"),
                    model=item.get("execution_model", "default"),
                    worker_route=item.get("worker_route_signal", "unknown"),
                    acceptor_route=item.get("acceptor_route_signal", "unknown"),
                    policy=item.get("policy_blockers_total", 0),
                )
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Next Phase"])
    lines.append(f"- {payload.get('next_phase', 'unknown')}")
    lines.append("")
    return "\n".join(lines)
