from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


DEFAULT_WORKER_MODEL = "gpt-5.3-codex"
DEFAULT_ACCEPTOR_MODEL = "gpt-5.4"
WORKER_BEGIN = "BEGIN_PHASE_WORKER_JSON"
WORKER_END = "END_PHASE_WORKER_JSON"
ACCEPTANCE_BEGIN = "BEGIN_PHASE_ACCEPTANCE_JSON"
ACCEPTANCE_END = "END_PHASE_ACCEPTANCE_JSON"
WORKER_ROUTE_SIGNAL = "worker:phase-only"
REMEDIATION_ROUTE_SIGNAL = "remediation:phase-only"
ACCEPTANCE_ROUTE_SIGNAL = "acceptance:governed-phase-route"
REQUIRED_ACCEPTANCE_SKILLS = (
    "phase-acceptance-governor",
    "architecture-review",
    "testing-suite",
    "docs-sync",
)
ROUTE_MODE = "governed-phase-orchestration"
ROUTE_GUARDRAILS = (
    "no silent assumptions",
    "no skipped required checks",
    "no silent fallbacks",
    "no deferred critical work",
    "acceptance requires architecture, test, and docs closure",
)


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
    evidence_contract: dict[str, Any] | None


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
    policy_blockers: list[AcceptanceBlocker]


@dataclass
class AttemptRecord:
    attempt: int
    kind: str
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


def normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def normalize_worker_payload(payload: dict[str, Any], role: str = "worker") -> WorkerReport:
    status = str(payload.get("status", "")).strip().upper()
    if status != "DONE":
        raise ValueError(f"worker status must be DONE, got {status!r}")
    route_signal = str(payload.get("route_signal", "")).strip()
    if not route_signal:
        raise ValueError("worker payload missing required `route_signal`")
    expected_route = WORKER_ROUTE_SIGNAL if role == "worker" else REMEDIATION_ROUTE_SIGNAL
    if route_signal != expected_route:
        raise ValueError(f"invalid `route_signal` for {role}: expected {expected_route!r}, got {route_signal!r}")
    evidence_contract = payload.get("evidence_contract")
    if evidence_contract is not None and not isinstance(evidence_contract, dict):
        raise ValueError("worker payload `evidence_contract` must be an object when present")
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
    if route_signal != ACCEPTANCE_ROUTE_SIGNAL:
        raise ValueError(
            f"invalid `route_signal` for acceptance: expected {ACCEPTANCE_ROUTE_SIGNAL!r}, got {route_signal!r}"
        )
    used_skills = normalize_string_list(payload.get("used_skills", []))
    if not used_skills:
        raise ValueError("acceptance payload missing required `used_skills`")
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
        "MISSING_SKILL": "Acceptance missed a required review lens",
        "PASS_WITH_BLOCKERS": "Acceptance declared PASS despite blockers",
    }
    remediations = {
        "ASSUMPTION": "Replace the assumption with implemented behavior or stop and update the phase contract explicitly before rerun.",
        "SKIP": "Run the required check or reduce scope until the phase can prove closure without a skip.",
        "FALLBACK": "Remove the fallback or obtain an explicit contract decision before rerun; do not leave it silent.",
        "DEFERRED": "Complete the critical work now or change the phase contract before claiming the phase is ready.",
        "EVIDENCE_GAP": "Produce the missing evidence and rerun acceptance.",
        "PROHIBITED_FINDING": "Resolve the prohibited condition and rerun acceptance.",
        "MISSING_SKILL": "Re-run acceptance using the required review lenses and report them explicitly.",
        "PASS_WITH_BLOCKERS": "Do not pass the phase while blockers remain; rerun acceptance with a consistent verdict.",
    }
    return AcceptanceBlocker(
        id=f"P-{normalized_kind}-{index}",
        title=titles.get(normalized_kind, "Policy blocker"),
        why=detail,
        remediation=remediations.get(normalized_kind, "Resolve the policy blocker before rerun."),
    )


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


def apply_acceptance_policy(
    worker: WorkerReport,
    acceptance: AcceptanceResult,
    phase_requirement: PhaseEvidenceRequirement | None = None,
) -> AcceptanceResult:
    policy_blockers: list[AcceptanceBlocker] = []
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

    if phase_requirement and phase_requirement.owned_surfaces:
        evidence = _parse_worker_evidence_contract(worker.evidence_contract)
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

    missing_skills = [item for item in REQUIRED_ACCEPTANCE_SKILLS if item not in acceptance.used_skills]
    for idx, skill_id in enumerate(missing_skills, start=1):
        policy_blockers.append(
            make_policy_blocker(
                "missing_skill",
                idx,
                f"Acceptance did not report required skill `{skill_id}` in used_skills.",
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
                "(worker_route={worker_route}; acceptor_route={acceptor_route}; policy_blockers={policy})".format(
                    attempt=item.get("attempt"),
                    kind=item.get("kind"),
                    verdict=item.get("verdict"),
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
