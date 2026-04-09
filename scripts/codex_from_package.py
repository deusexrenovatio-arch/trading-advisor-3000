#!/usr/bin/env python3
"""Run Codex from a zip package that contains a full task document set."""

from __future__ import annotations

import argparse
import html
import json
import re
import shutil
import subprocess
import sys
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phase_tz_compiler import compile_phase_plan, phase_plan_payload


DEFAULT_INBOX = Path("docs/codex/packages/inbox")
DEFAULT_PROMPT = Path("docs/codex/prompts/entry/from_package.md")
DEFAULT_ARTIFACT_ROOT = Path("artifacts/codex/package-intake")
DEFAULT_OUTPUT = Path("artifacts/codex/from-package-last-message.txt")
ALLOWED_MODES = {"auto", "plan-only", "implement-only", "continue", "repair"}
SUPPORTED_DOC_EXTENSIONS = {".md", ".txt", ".rst", ".docx", ".pdf"}
INTAKE_GATE_BEGIN = "BEGIN_INTAKE_GATE_JSON"
INTAKE_GATE_END = "END_INTAKE_GATE_JSON"
TECHNICAL_INTAKE_BEGIN = "BEGIN_TECHNICAL_INTAKE_JSON"
TECHNICAL_INTAKE_END = "END_TECHNICAL_INTAKE_JSON"
PRODUCT_INTAKE_BEGIN = "BEGIN_PRODUCT_INTAKE_JSON"
PRODUCT_INTAKE_END = "END_PRODUCT_INTAKE_JSON"
INTAKE_HUMAN_SUMMARY_BEGIN = "BEGIN_INTAKE_HUMAN_SUMMARY"
INTAKE_HUMAN_SUMMARY_END = "END_INTAKE_HUMAN_SUMMARY"
MATERIALIZATION_RESULT_BEGIN = "BEGIN_MATERIALIZATION_RESULT_JSON"
MATERIALIZATION_RESULT_END = "END_MATERIALIZATION_RESULT_JSON"
MATERIALIZATION_CONTEXT_REQUIRED_FIELDS = (
    "source_documents",
    "materialized_documents",
    "preserved_goals",
    "preserved_acceptance_criteria",
)
LANE_SEQUENCE = ("product_intake", "technical_intake")
INTAKE_BLOCKED_EXIT = 3
BLOCKER_SEVERITIES = ("P0", "P1", "P2")
BLOCKER_SCALES = ("S", "M", "L", "XL")
INTAKE_REQUIRED_SKILLS = ("workflow-architect",)
TECHNICAL_INTAKE_MODEL = "gpt-5.3-codex"
POSITIVE_HINTS = (
    ("technical_requirements", 140, "filename looks like technical requirements"),
    ("requirements", 120, "filename looks like requirements"),
    ("specification", 120, "filename looks like specification"),
    ("spec", 110, "filename looks like specification"),
    ("tz", 130, "filename looks like TZ"),
    ("тз", 130, "filename looks like TZ"),
    ("техническ", 110, "filename looks like technical doc"),
    ("task", 70, "filename looks task-oriented"),
    ("brief", 60, "filename looks like brief"),
    ("architecture", 50, "filename looks architectural"),
)
NEGATIVE_HINTS = (
    ("readme", -40, "generic README is usually supporting material"),
    ("verdict", -80, "verdict docs are usually downstream acceptance outputs"),
    ("findings", -55, "findings docs are usually analytical supporting material"),
    ("manifest", -45, "manifest docs are usually inventory/supporting material"),
    ("evidence", -25, "evidence docs are usually supporting material"),
    ("notes", -20, "notes are usually supporting material"),
    ("appendix", -15, "appendix is usually supporting material"),
    ("draft", -10, "draft is less stable than a finalized spec"),
)
EXTENSION_WEIGHTS = {
    ".md": 40,
    ".txt": 30,
    ".rst": 25,
    ".docx": 20,
    ".pdf": 10,
}


@dataclass(frozen=True)
class DocumentCandidate:
    rel_path: str
    score: int
    reasons: tuple[str, ...]
    title: str | None


def extract_tagged_json(text: str, *, begin: str, end: str) -> dict[str, Any]:
    start = text.rfind(begin)
    stop = text.rfind(end)
    if start < 0 or stop < 0 or stop <= start:
        raise ValueError(f"missing tagged json block {begin} ... {end}")
    payload = text[start + len(begin) : stop].strip()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"failed to parse tagged json: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("tagged json payload must be an object")
    return data


def _normalize_blockers(raw_blockers: Any, *, lane: str) -> list[dict[str, str]]:
    if raw_blockers is None:
        return []
    if not isinstance(raw_blockers, list):
        raise ValueError(f"{lane}.blockers must be an array")
    normalized: list[dict[str, str]] = []
    for idx, item in enumerate(raw_blockers, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{lane}.blockers[{idx}] must be an object")
        severity = str(item.get("severity", "")).strip().upper()
        if severity not in BLOCKER_SEVERITIES:
            raise ValueError(f"{lane}.blockers[{idx}].severity must be one of {BLOCKER_SEVERITIES}")
        scale = str(item.get("scale", "")).strip().upper()
        if scale not in BLOCKER_SCALES:
            raise ValueError(f"{lane}.blockers[{idx}].scale must be one of {BLOCKER_SCALES}")
        title = str(item.get("title", "")).strip()
        why = str(item.get("why", item.get("reason", ""))).strip()
        if not title:
            raise ValueError(f"{lane}.blockers[{idx}].title is required")
        if not why:
            raise ValueError(f"{lane}.blockers[{idx}].why is required")
        normalized.append(
            {
                "id": str(item.get("id", f"{lane.upper()}-{idx:02d}")).strip(),
                "severity": severity,
                "scale": scale,
                "title": title,
                "why": why,
                "required_action": str(item.get("required_action", item.get("remediation", ""))).strip(),
            }
        )
    return normalized


def _normalize_digest_list(raw: Any, *, lane: str, field: str) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{lane}.{field} must be an array")
    normalized: list[str] = []
    for idx, item in enumerate(raw, start=1):
        text = str(item).strip()
        if not text:
            raise ValueError(f"{lane}.{field}[{idx}] must be a non-empty string")
        if text not in normalized:
            normalized.append(text)
    return normalized


def _normalize_string_list(raw: Any, *, field: str) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{field} must be an array")
    normalized: list[str] = []
    for idx, item in enumerate(raw, start=1):
        text = str(item).strip()
        if not text:
            raise ValueError(f"{field}[{idx}] must be a non-empty string")
        if text not in normalized:
            normalized.append(text)
    return normalized


def _normalize_structural_recommendations(raw: Any, *, lane: str) -> list[dict[str, str]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{lane}.structural_recommendations must be an array")
    normalized: list[dict[str, str]] = []
    allowed_priority = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
    for idx, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{lane}.structural_recommendations[{idx}] must be an object")
        title = str(item.get("title", "")).strip()
        why = str(item.get("why", "")).strip()
        proposal = str(item.get("proposal", item.get("suggested_change", ""))).strip()
        impact_on_tz = str(item.get("impact_on_tz", "")).strip()
        priority = str(item.get("priority", "MEDIUM")).strip().upper()
        if priority not in allowed_priority:
            raise ValueError(
                f"{lane}.structural_recommendations[{idx}].priority must be one of {sorted(allowed_priority)}"
            )
        if not title:
            raise ValueError(f"{lane}.structural_recommendations[{idx}].title is required")
        if not why:
            raise ValueError(f"{lane}.structural_recommendations[{idx}].why is required")
        normalized.append(
            {
                "id": str(item.get("id", f"{lane.upper()}-SR-{idx:02d}")).strip(),
                "priority": priority,
                "title": title,
                "why": why,
                "proposal": proposal,
                "impact_on_tz": impact_on_tz,
            }
        )
    return normalized


def _normalize_lane(payload: dict[str, Any], lane: str) -> dict[str, Any]:
    lane_payload = payload.get(lane)
    if not isinstance(lane_payload, dict):
        raise ValueError(f"{lane} object is required")
    created_docs_raw = lane_payload.get("created_docs", [])
    if created_docs_raw is None:
        created_docs_raw = []
    if not isinstance(created_docs_raw, list):
        raise ValueError(f"{lane}.created_docs must be an array")
    created_docs = [str(item).strip() for item in created_docs_raw if str(item).strip()]
    blockers = _normalize_blockers(lane_payload.get("blockers"), lane=lane)
    review_summary = str(lane_payload.get("review_summary", "")).strip()
    goals_digest = _normalize_digest_list(lane_payload.get("goals_digest"), lane=lane, field="goals_digest")
    acceptance_criteria_digest = _normalize_digest_list(
        lane_payload.get("acceptance_criteria_digest"),
        lane=lane,
        field="acceptance_criteria_digest",
    )
    structural_recommendations_provided = "structural_recommendations" in lane_payload
    structural_recommendations = _normalize_structural_recommendations(
        lane_payload.get("structural_recommendations"),
        lane=lane,
    )
    return {
        "created_docs": created_docs,
        "review_summary": review_summary,
        "goals_digest": goals_digest,
        "acceptance_criteria_digest": acceptance_criteria_digest,
        "structural_recommendations_provided": structural_recommendations_provided,
        "structural_recommendations": structural_recommendations,
        "blockers": blockers,
    }


def _auto_blockers_for_lane(
    *,
    lane: str,
    created_docs: list[str],
    review_summary: str,
    goals_digest: list[str],
    acceptance_criteria_digest: list[str],
    structural_recommendations_provided: bool,
) -> list[dict[str, str]]:
    auto: list[dict[str, str]] = []
    if not created_docs:
        auto.append(
            {
                "id": f"AUTO-{lane.upper()}-DOCS",
                "severity": "P0",
                "scale": "M",
                "title": "Mandatory documentation artifacts are missing",
                "why": "Intake must produce documentation outputs before the phase gate can pass.",
                "required_action": "Create/refresh execution contract, parent brief, phase briefs, and linked intake docs.",
            }
        )
    if not review_summary:
        auto.append(
            {
                "id": f"AUTO-{lane.upper()}-REVIEW",
                "severity": "P1",
                "scale": "S",
                "title": "Review summary is missing",
                "why": "Intake must include explicit review findings and closure status.",
                "required_action": "Add a concise review summary with accepted risks and unresolved findings.",
            }
        )
    if not goals_digest:
        auto.append(
            {
                "id": f"AUTO-{lane.upper()}-GOALS",
                "severity": "P1",
                "scale": "M",
                "title": "Goals digest is missing",
                "why": "Intake output must include a concise goals digest extracted from source requirements.",
                "required_action": "Add `goals_digest` with explicit user/business objectives.",
            }
        )
    if not acceptance_criteria_digest:
        auto.append(
            {
                "id": f"AUTO-{lane.upper()}-ACCEPTANCE",
                "severity": "P1",
                "scale": "M",
                "title": "Acceptance criteria digest is missing",
                "why": "Intake output must include acceptance criteria so downstream phases preserve source DoD.",
                "required_action": "Add `acceptance_criteria_digest` with measurable acceptance checks.",
            }
        )
    if not structural_recommendations_provided:
        auto.append(
            {
                "id": f"AUTO-{lane.upper()}-STRUCTURE",
                "severity": "P1",
                "scale": "M",
                "title": "Structural recommendations section is missing",
                "why": "Intake must explicitly report structural improvements/critical changes or provide an empty list.",
                "required_action": "Add `structural_recommendations` as an array (can be empty when no changes are needed).",
            }
        )
    return auto


def _severity_counts(blockers: list[dict[str, str]]) -> dict[str, int]:
    counts = {"P0": 0, "P1": 0, "P2": 0}
    for blocker in blockers:
        counts[blocker["severity"]] += 1
    return counts


def _max_scale(blockers: list[dict[str, str]]) -> str:
    if not blockers:
        return "NONE"
    weights = {"S": 1, "M": 2, "L": 3, "XL": 4}
    strongest = max(blockers, key=lambda item: weights.get(item["scale"], 0))
    return strongest["scale"]


def evaluate_intake_gate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    technical = _normalize_lane(payload, "technical_intake")
    product = _normalize_lane(payload, "product_intake")
    technical_blockers = list(technical["blockers"]) + _auto_blockers_for_lane(
        lane="technical_intake",
        created_docs=technical["created_docs"],
        review_summary=technical["review_summary"],
        goals_digest=technical["goals_digest"],
        acceptance_criteria_digest=technical["acceptance_criteria_digest"],
        structural_recommendations_provided=bool(technical["structural_recommendations_provided"]),
    )
    product_blockers = list(product["blockers"]) + _auto_blockers_for_lane(
        lane="product_intake",
        created_docs=product["created_docs"],
        review_summary=product["review_summary"],
        goals_digest=product["goals_digest"],
        acceptance_criteria_digest=product["acceptance_criteria_digest"],
        structural_recommendations_provided=bool(product["structural_recommendations_provided"]),
    )
    all_blockers = technical_blockers + product_blockers
    severity = _severity_counts(all_blockers)
    blocking_total = severity["P0"] + severity["P1"]
    decision = "BLOCKED" if blocking_total > 0 else "PASS"
    return {
        "schema_version": 1,
        "gate_mode": "formal-intake-gate",
        "formal_rule": "BLOCK when any P0/P1 blocker exists across technical_intake and product_intake.",
        "technical_intake": {
            "created_docs": technical["created_docs"],
            "review_summary": technical["review_summary"],
            "goals_digest": technical["goals_digest"],
            "acceptance_criteria_digest": technical["acceptance_criteria_digest"],
            "structural_recommendations": technical["structural_recommendations"],
            "blockers": technical_blockers,
            "severity_counts": _severity_counts(technical_blockers),
        },
        "product_intake": {
            "created_docs": product["created_docs"],
            "review_summary": product["review_summary"],
            "goals_digest": product["goals_digest"],
            "acceptance_criteria_digest": product["acceptance_criteria_digest"],
            "structural_recommendations": product["structural_recommendations"],
            "blockers": product_blockers,
            "severity_counts": _severity_counts(product_blockers),
        },
        "combined_gate": {
            "decision": decision,
            "blocking_total": blocking_total,
            "severity_counts": severity,
            "max_problem_scale": _max_scale([item for item in all_blockers if item["severity"] in {"P0", "P1"}]),
            "reported_decision": str(payload.get("combined_gate", {}).get("decision", "")).strip().upper(),
        },
    }


def evaluate_intake_gate_from_text(text: str) -> dict[str, Any]:
    payload = extract_tagged_json(text, begin=INTAKE_GATE_BEGIN, end=INTAKE_GATE_END)
    return evaluate_intake_gate_payload(payload)


def render_intake_gate_markdown(gate: dict[str, Any]) -> str:
    combined = gate["combined_gate"]
    lines = [
        "# Intake Gate",
        "",
        f"- Decision: {combined['decision']}",
        f"- Blocking Total (P0/P1): {combined['blocking_total']}",
        f"- Severity Counts: P0={combined['severity_counts']['P0']}, P1={combined['severity_counts']['P1']}, P2={combined['severity_counts']['P2']}",
        f"- Max Problem Scale: {combined['max_problem_scale']}",
        "",
        "## Technical Intake",
        f"- Created Docs: {len(gate['technical_intake']['created_docs'])}",
        f"- Review Summary Present: {'yes' if gate['technical_intake']['review_summary'] else 'no'}",
        f"- Goals Digest Items: {len(gate['technical_intake']['goals_digest'])}",
        f"- Acceptance Criteria Items: {len(gate['technical_intake']['acceptance_criteria_digest'])}",
        f"- Structural Recommendations: {len(gate['technical_intake']['structural_recommendations'])}",
        f"- Blockers: {len(gate['technical_intake']['blockers'])}",
        "",
        "## Product Intake",
        f"- Created Docs: {len(gate['product_intake']['created_docs'])}",
        f"- Review Summary Present: {'yes' if gate['product_intake']['review_summary'] else 'no'}",
        f"- Goals Digest Items: {len(gate['product_intake']['goals_digest'])}",
        f"- Acceptance Criteria Items: {len(gate['product_intake']['acceptance_criteria_digest'])}",
        f"- Structural Recommendations: {len(gate['product_intake']['structural_recommendations'])}",
        f"- Blockers: {len(gate['product_intake']['blockers'])}",
        "",
    ]
    return "\n".join(lines)


def _dedupe_strings(items: list[str]) -> list[str]:
    out: list[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def build_intake_human_summary(gate: dict[str, Any]) -> dict[str, Any]:
    technical = gate.get("technical_intake", {})
    product = gate.get("product_intake", {})
    goals = _dedupe_strings(
        [*list(product.get("goals_digest", [])), *list(technical.get("goals_digest", []))]
        if isinstance(product, dict) and isinstance(technical, dict)
        else []
    )
    acceptance = _dedupe_strings(
        [
            *list(product.get("acceptance_criteria_digest", [])),
            *list(technical.get("acceptance_criteria_digest", [])),
        ]
        if isinstance(product, dict) and isinstance(technical, dict)
        else []
    )
    structural_recommendations: list[dict[str, Any]] = []
    for lane_name in ("technical_intake", "product_intake"):
        lane_payload = gate.get(lane_name, {})
        if not isinstance(lane_payload, dict):
            continue
        items = lane_payload.get("structural_recommendations", [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            candidate = {"lane": lane_name, **item}
            if candidate not in structural_recommendations:
                structural_recommendations.append(candidate)
    decision = str(gate.get("combined_gate", {}).get("decision", "UNKNOWN")).strip().upper()
    return {
        "schema_version": 1,
        "gate_decision": decision,
        "goals_digest": goals,
        "acceptance_criteria_digest": acceptance,
        "structural_recommendations": structural_recommendations,
    }


def render_intake_human_summary_markdown(summary: dict[str, Any]) -> str:
    goals = summary.get("goals_digest", []) if isinstance(summary, dict) else []
    acceptance = summary.get("acceptance_criteria_digest", []) if isinstance(summary, dict) else []
    structural = summary.get("structural_recommendations", []) if isinstance(summary, dict) else []
    lines = [
        "# Intake Human Summary",
        "",
        f"- Gate Decision: {summary.get('gate_decision', 'UNKNOWN')}",
        "",
        "## Goals Digest",
    ]
    if isinstance(goals, list) and goals:
        lines.extend([f"- {item}" for item in goals])
    else:
        lines.append("- none")
    lines.extend(["", "## Acceptance Criteria Digest"])
    if isinstance(acceptance, list) and acceptance:
        lines.extend([f"- {item}" for item in acceptance])
    else:
        lines.append("- none")
    lines.extend(["", "## Structural Recommendations"])
    if isinstance(structural, list) and structural:
        for item in structural:
            if not isinstance(item, dict):
                continue
            lane = str(item.get("lane", "unknown")).strip()
            priority = str(item.get("priority", "MEDIUM")).strip()
            title = str(item.get("title", "")).strip()
            proposal = str(item.get("proposal", "")).strip()
            impact = str(item.get("impact_on_tz", "")).strip()
            line = f"- [{lane}] ({priority}) {title}"
            if proposal:
                line += f"; proposal: {proposal}"
            if impact:
                line += f"; impact_on_tz: {impact}"
            lines.append(line)
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def lane_tags(lane: str) -> tuple[str, str]:
    if lane == "technical_intake":
        return TECHNICAL_INTAKE_BEGIN, TECHNICAL_INTAKE_END
    if lane == "product_intake":
        return PRODUCT_INTAKE_BEGIN, PRODUCT_INTAKE_END
    raise ValueError(f"unsupported intake lane: {lane}")


def build_intake_lane_prompt(*, runtime_context: str, policy_prompt_path: Path, lane: str) -> str:
    begin, end = lane_tags(lane)
    if lane == "technical_intake":
        lane_scope = (
            "- lane: technical_intake\n"
            "- focus: architecture fit, implementation feasibility, delivery risks, technical blockers\n"
            "- required lenses: workflow-architect, architecture-review, business-analyst, tz-oss-scout\n"
        )
        lane_goal = (
            "Review architecture feasibility, delivery risks, and technical blockers without rewriting "
            "or softening source requirements."
        )
    else:
        lane_scope = (
            "- lane: product_intake\n"
            "- focus: product value, user impact, business viability, value-risk blockers\n"
            "- required lenses: product-owner, business-analyst, tz-oss-scout\n"
        )
        lane_goal = (
            "Review product completeness, value risks, and business blockers without rewriting "
            "or softening source requirements."
        )
    return (
        f"{runtime_context}\n\n"
        "Intake Policy Reference:\n"
        f"- Read and obey: {policy_prompt_path.as_posix()}\n\n"
        "Section Goals:\n"
        "- Part 1 (Context): use Runtime Package Context to lock boundaries and deterministic phase ids.\n"
        f"- Part 2 (Lane Review): {lane_goal}\n"
        "- Part 3 (Output Contract): return strict tagged JSON for fail-closed gate evaluation.\n\n"
        "Intake Runtime Mode:\n"
        "- You are running a lane-only intake pass in a sequential governed gate.\n"
        "- Gate order is product_intake -> technical_intake -> materialization.\n"
        "- Do not modify repository files in this lane pass.\n"
        "- Provide only lane analysis and blocker classification.\n"
        f"{lane_scope}"
        "Return exactly one tagged JSON block in this lane format:\n"
        f"{begin}\n"
        "{\n"
        '  "created_docs": ["docs/codex/contracts/<slug>.execution-contract.md"],\n'
        '  "review_summary": "Concise lane review summary.",\n'
        '  "goals_digest": ["Lossless goal from source requirements"],\n'
        '  "acceptance_criteria_digest": ["Measurable acceptance criterion from source requirements"],\n'
        '  "structural_recommendations": [\n'
        "    {\n"
        '      "id": "SR-001",\n'
        '      "priority": "CRITICAL|HIGH|MEDIUM|LOW",\n'
        '      "title": "Potential structural improvement or critical change",\n'
        '      "why": "Why this matters for target goals and quality",\n'
        '      "proposal": "What should be changed/expanded in the source TZ",\n'
        '      "impact_on_tz": "Expected impact on requirements/architecture/plan"\n'
        "    }\n"
        "  ],\n"
        '  "blockers": [\n'
        "    {\n"
        '      "id": "LANE-001",\n'
        '      "severity": "P0",\n'
        '      "scale": "M",\n'
        '      "title": "Short blocker title",\n'
        '      "why": "Why this blocks/risks intake",\n'
        '      "required_action": "What must be resolved"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        f"{end}\n"
    )


def extract_lane_payload_from_text(text: str, *, lane: str) -> dict[str, Any]:
    begin, end = lane_tags(lane)
    raw = extract_tagged_json(text, begin=begin, end=end)
    if lane in raw and isinstance(raw.get(lane), dict):
        raw = raw[lane]
    if not isinstance(raw, dict):
        raise ValueError(f"{lane} payload must be an object")
    normalized = _normalize_lane({lane: raw}, lane)
    return {
        "created_docs": list(normalized["created_docs"]),
        "review_summary": str(normalized["review_summary"]),
        "goals_digest": list(normalized["goals_digest"]),
        "acceptance_criteria_digest": list(normalized["acceptance_criteria_digest"]),
        "structural_recommendations": [dict(item) for item in normalized["structural_recommendations"]],
        "blockers": [dict(item) for item in normalized["blockers"]],
    }


def extract_materialization_result_from_text(text: str) -> dict[str, Any]:
    payload = extract_tagged_json(text, begin=MATERIALIZATION_RESULT_BEGIN, end=MATERIALIZATION_RESULT_END)
    status = str(payload.get("status", "")).strip().upper()
    if status != "DONE":
        raise ValueError(f"materialization status must be DONE, got {status!r}")
    context_coverage_raw = payload.get("context_coverage")
    if context_coverage_raw is None:
        context_coverage_raw = {}
    if not isinstance(context_coverage_raw, dict):
        raise ValueError("materialization context_coverage must be an object when present")
    context_coverage = {
        "source_documents": _normalize_string_list(
            context_coverage_raw.get("source_documents"),
            field="materialization.context_coverage.source_documents",
        ),
        "materialized_documents": _normalize_string_list(
            context_coverage_raw.get("materialized_documents"),
            field="materialization.context_coverage.materialized_documents",
        ),
        "preserved_goals": _normalize_string_list(
            context_coverage_raw.get("preserved_goals"),
            field="materialization.context_coverage.preserved_goals",
        ),
        "preserved_acceptance_criteria": _normalize_string_list(
            context_coverage_raw.get("preserved_acceptance_criteria"),
            field="materialization.context_coverage.preserved_acceptance_criteria",
        ),
    }
    return {
        "status": status,
        "updated_docs": _normalize_string_list(payload.get("updated_docs"), field="materialization.updated_docs"),
        "notes": str(payload.get("notes", "")).strip(),
        "residual_blockers": _normalize_string_list(
            payload.get("residual_blockers"),
            field="materialization.residual_blockers",
        ),
        "context_coverage": context_coverage,
    }


def evaluate_materialization_result(*, result: dict[str, Any], handoff: dict[str, Any]) -> dict[str, Any]:
    required_documents_payload = (
        handoff.get("materialization_requirements", {}).get("documents", [])
        if isinstance(handoff, dict)
        else []
    )
    required_documents: list[str] = []
    if isinstance(required_documents_payload, list):
        for item in required_documents_payload:
            if not isinstance(item, dict):
                continue
            path = str(item.get("path", "")).strip()
            if path and path not in required_documents:
                required_documents.append(path)

    updated_docs = _normalize_string_list(result.get("updated_docs"), field="materialization.updated_docs")
    residual_blockers = _normalize_string_list(
        result.get("residual_blockers"),
        field="materialization.residual_blockers",
    )
    auto_blockers: list[str] = []
    missing_updated_docs = [path for path in required_documents if path not in updated_docs]
    if missing_updated_docs:
        auto_blockers.append(
            "Missing required updated_docs entries: " + ", ".join(missing_updated_docs)
        )

    documentation_context_contract = (
        handoff.get("documentation_context_contract", {}) if isinstance(handoff, dict) else {}
    )
    must_preserve = (
        documentation_context_contract.get("must_preserve", {})
        if isinstance(documentation_context_contract, dict)
        else {}
    )
    required_source_documents = (
        documentation_context_contract.get("source_documents", [])
        if isinstance(documentation_context_contract, dict)
        else []
    )
    required_materialized_documents = (
        documentation_context_contract.get("materialized_documents", [])
        if isinstance(documentation_context_contract, dict)
        else []
    )
    required_goals = must_preserve.get("goals_digest", []) if isinstance(must_preserve, dict) else []
    required_acceptance = (
        must_preserve.get("acceptance_criteria_digest", []) if isinstance(must_preserve, dict) else []
    )
    context_coverage = result.get("context_coverage", {}) if isinstance(result, dict) else {}
    if not isinstance(context_coverage, dict):
        context_coverage = {}
    coverage_source = _normalize_string_list(
        context_coverage.get("source_documents"),
        field="materialization.context_coverage.source_documents",
    )
    coverage_materialized = _normalize_string_list(
        context_coverage.get("materialized_documents"),
        field="materialization.context_coverage.materialized_documents",
    )
    coverage_goals = _normalize_string_list(
        context_coverage.get("preserved_goals"),
        field="materialization.context_coverage.preserved_goals",
    )
    coverage_acceptance = _normalize_string_list(
        context_coverage.get("preserved_acceptance_criteria"),
        field="materialization.context_coverage.preserved_acceptance_criteria",
    )

    for field_name in MATERIALIZATION_CONTEXT_REQUIRED_FIELDS:
        if not context_coverage.get(field_name):
            auto_blockers.append(f"Missing context_coverage field: {field_name}")
    missing_context_source = [item for item in required_source_documents if item not in coverage_source]
    if missing_context_source:
        auto_blockers.append(
            "context_coverage.source_documents is missing required references: "
            + ", ".join(missing_context_source)
        )
    missing_context_materialized = [
        item for item in required_materialized_documents if item not in coverage_materialized
    ]
    if missing_context_materialized:
        auto_blockers.append(
            "context_coverage.materialized_documents is missing required references: "
            + ", ".join(missing_context_materialized)
        )
    missing_context_goals = [item for item in required_goals if item not in coverage_goals]
    if missing_context_goals:
        auto_blockers.append(
            "context_coverage.preserved_goals is missing required items: "
            + ", ".join(missing_context_goals)
        )
    missing_context_acceptance = [
        item for item in required_acceptance if item not in coverage_acceptance
    ]
    if missing_context_acceptance:
        auto_blockers.append(
            "context_coverage.preserved_acceptance_criteria is missing required items: "
            + ", ".join(missing_context_acceptance)
        )

    combined_blockers = [*residual_blockers, *auto_blockers]
    return {
        "status": "DONE",
        "decision": "PASS" if not combined_blockers else "BLOCKED",
        "updated_docs": updated_docs,
        "notes": str(result.get("notes", "")).strip(),
        "residual_blockers": residual_blockers,
        "auto_blockers": auto_blockers,
        "combined_blockers": combined_blockers,
        "context_coverage": {
            "source_documents": coverage_source,
            "materialized_documents": coverage_materialized,
            "preserved_goals": coverage_goals,
            "preserved_acceptance_criteria": coverage_acceptance,
        },
    }


def render_materialization_result_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Materialization Result",
        "",
        f"- Decision: {result.get('decision', 'UNKNOWN')}",
        f"- Updated Docs: {len(result.get('updated_docs', []))}",
        f"- Residual Blockers: {len(result.get('residual_blockers', []))}",
        f"- Auto Blockers: {len(result.get('auto_blockers', []))}",
    ]
    notes = str(result.get("notes", "")).strip()
    if notes:
        lines.append(f"- Notes: {notes}")
    lines.extend(["", "## Combined Blockers"])
    blockers = result.get("combined_blockers", [])
    if isinstance(blockers, list) and blockers:
        for item in blockers:
            lines.append(f"- {item}")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def build_intake_handoff(
    *,
    package_path: Path,
    extracted_root: Path,
    manifest_path: Path,
    suggested_primary: str | None,
    suggested_phase_compiler_artifact: str | None,
    suggested_phase_ids: list[str],
    lane_payloads: dict[str, dict[str, Any]],
    intake_gate: dict[str, Any],
    intake_human_summary: dict[str, Any],
) -> dict[str, Any]:
    blocking_items: list[dict[str, Any]] = []
    for lane in LANE_SEQUENCE:
        lane_gate = intake_gate.get(lane, {})
        if not isinstance(lane_gate, dict):
            continue
        blockers = lane_gate.get("blockers", [])
        if not isinstance(blockers, list):
            continue
        for item in blockers:
            if not isinstance(item, dict):
                continue
            severity = str(item.get("severity", "")).strip().upper()
            if severity not in {"P0", "P1"}:
                continue
            blocking_items.append({"lane": lane, **item})

    docs_targets: list[str] = []
    for lane in LANE_SEQUENCE:
        lane_payload = lane_payloads.get(lane, {})
        if not isinstance(lane_payload, dict):
            continue
        for path in lane_payload.get("created_docs", []):
            text = str(path).strip()
            if text and text not in docs_targets:
                docs_targets.append(text)

    lane_reviews: dict[str, dict[str, Any]] = {}
    for lane in LANE_SEQUENCE:
        lane_payload = lane_payloads.get(lane, {})
        if not isinstance(lane_payload, dict):
            continue
        lane_reviews[lane] = {
            "review_summary": str(lane_payload.get("review_summary", "")).strip(),
            "blockers_total": len(list(lane_payload.get("blockers", []))),
        }

    def infer_module_slug() -> str | None:
        for item in docs_targets:
            contract_match = re.search(r"docs/codex/contracts/([^/]+)\.execution-contract\.md$", item)
            if contract_match:
                return contract_match.group(1)
            parent_match = re.search(r"docs/codex/modules/([^/]+)\.parent\.md$", item)
            if parent_match:
                return parent_match.group(1)
        return None

    def normalize_phase_token(raw_phase_id: str) -> str:
        token = str(raw_phase_id).strip().lower()
        if token.startswith("phase-"):
            token = token[6:]
        token = token.strip()
        if token.isdigit():
            return token.zfill(2)
        cleaned = re.sub(r"[^a-z0-9]+", "-", token).strip("-")
        return cleaned or "xx"

    module_slug = infer_module_slug()
    contract_path = (
        f"docs/codex/contracts/{module_slug}.execution-contract.md"
        if module_slug
        else "docs/codex/contracts/<module-slug>.execution-contract.md"
    )
    parent_path = (
        f"docs/codex/modules/{module_slug}.parent.md"
        if module_slug
        else "docs/codex/modules/<module-slug>.parent.md"
    )
    phase_paths = [
        (
            f"docs/codex/modules/{module_slug}.phase-{normalize_phase_token(phase_id)}.md"
            if module_slug
            else f"docs/codex/modules/<module-slug>.phase-{normalize_phase_token(phase_id)}.md"
        )
        for phase_id in suggested_phase_ids
    ]
    materialization_documents: list[dict[str, Any]] = [
        {
            "type": "execution_contract",
            "path": contract_path,
            "constraints": [
                "Preserve source goals and acceptance criteria losslessly; do not reinterpret or downgrade.",
                "Keep deterministic source phase ids/order exactly as provided.",
                "Keep fail-closed gate language; unresolved gaps must stay blockers.",
            ],
            "expected_result": "Execution contract is refreshed and ready for governed phase continuation.",
        },
        {
            "type": "module_parent_brief",
            "path": parent_path,
            "constraints": [
                "Next phase pointer must align with deterministic phase order.",
                "Do not skip, merge, or reorder phases.",
                "Keep references consistent with contract and phase briefs.",
            ],
            "expected_result": "Parent brief is refreshed with correct next phase sequencing and links.",
        },
    ]
    for phase_path in phase_paths:
        materialization_documents.append(
            {
                "type": "module_phase_brief",
                "path": phase_path,
                "constraints": [
                    "Scope remains limited to this phase id only.",
                    "Objective, acceptance gate, disprover logic, and DoD stay explicit and testable.",
                    "No cross-phase drift, no hidden deferred critical work.",
                    "All mandatory phase-brief sections from `phase_brief_mandatory_sections` must be present and fully populated.",
                ],
                "expected_result": "Phase brief is refreshed and execution-ready for worker/acceptor handoff.",
            }
        )
    extra_targets = [item for item in docs_targets if item not in {contract_path, parent_path, *phase_paths}]
    for extra_path in extra_targets:
        materialization_documents.append(
            {
                "type": "lane_declared_document",
                "path": extra_path,
                "constraints": [
                    "Preserve source requirement semantics and boundary constraints.",
                    "Keep document internally consistent with execution contract and parent/phase briefs.",
                ],
                "expected_result": "Document is refreshed without requirement drift.",
            }
        )

    required_outcomes = [
        "Canonical docs are refreshed under docs/codex/contracts and docs/codex/modules.",
        "Deterministic phase mapping is preserved exactly.",
        "Any unresolved blocker is explicitly reported; no silent fallback.",
        "Every module_phase_brief includes all mandatory sections required for downstream worker/acceptor execution.",
        "Materialization output preserves source goals and acceptance criteria without degradation.",
        "Full documentation context is used: source/original docs + materialized docs.",
    ]
    phase_brief_mandatory_sections = [
        {
            "id": "traceability_map",
            "title": "Traceability Map",
            "requirement": "Map each phase objective and acceptance criterion to exact source TZ/supporting document references.",
        },
        {
            "id": "non_goals_and_scope_limits",
            "title": "Non-Goals / Scope Limits",
            "requirement": "Explicitly list what this phase must not change, reinterpret, or implement.",
        },
        {
            "id": "assumptions_and_open_questions",
            "title": "Assumptions and Open Questions",
            "requirement": "Capture unresolved ambiguities with owner and required resolution action.",
        },
        {
            "id": "phase_dependencies_and_preconditions",
            "title": "Dependencies and Preconditions",
            "requirement": "List required inputs from previous phases and blockers for starting this phase.",
        },
        {
            "id": "acceptance_evidence_contract",
            "title": "Acceptance Evidence Contract",
            "requirement": "Define concrete evidence artifacts/checks required to prove each acceptance criterion.",
        },
        {
            "id": "cross_doc_conflict_resolution",
            "title": "Cross-Document Conflict Resolution",
            "requirement": "Define how to resolve contradictions between primary TZ and supporting documents.",
        },
        {
            "id": "source_versioning_baseline",
            "title": "Source Versioning Baseline",
            "requirement": "Record source document versions/commit references used for this phase to prevent context drift.",
        },
        {
            "id": "risk_and_rollback_triggers",
            "title": "Risk and Rollback Triggers",
            "requirement": "Describe critical risks, rollback triggers, and escalation conditions for this phase.",
        },
    ]
    source_documents_context = [
        item
        for item in [
            suggested_primary,
            suggested_phase_compiler_artifact,
            manifest_path.as_posix(),
        ]
        if item
    ]
    materialized_documents_context = [contract_path, parent_path, *phase_paths, *extra_targets]
    documentation_context_contract = {
        "source_documents": source_documents_context,
        "materialized_documents": materialized_documents_context,
        "must_preserve": {
            "goals_digest": list(intake_human_summary.get("goals_digest", [])),
            "acceptance_criteria_digest": list(intake_human_summary.get("acceptance_criteria_digest", [])),
        },
        "guardrails": [
            "Do not degrade or reinterpret source goals/acceptance criteria.",
            "Resolve source-vs-materialized conflicts explicitly; never silently choose one side.",
            "If conflict cannot be resolved safely, emit blocker instead of best-effort rewrite.",
        ],
    }

    return {
        "schema_version": 1,
        "gate_decision": str(intake_gate.get("combined_gate", {}).get("decision", "")).strip().upper() or "UNKNOWN",
        "runtime_context": {
            "package_zip_path": package_path.as_posix(),
            "extracted_package_root": extracted_root.as_posix(),
            "manifest_path": manifest_path.as_posix(),
            "suggested_primary_document": suggested_primary,
            "suggested_phase_compiler_artifact": suggested_phase_compiler_artifact,
            "suggested_phase_ids": list(suggested_phase_ids),
        },
        "goals_digest": list(intake_human_summary.get("goals_digest", [])),
        "acceptance_criteria_digest": list(intake_human_summary.get("acceptance_criteria_digest", [])),
        "structural_recommendations": list(intake_human_summary.get("structural_recommendations", [])),
        "materialization_targets": {"docs_to_refresh": docs_targets},
        "materialization_requirements": {
            "documents": materialization_documents,
            "required_outcomes": required_outcomes,
            "phase_brief_mandatory_sections": phase_brief_mandatory_sections,
        },
        "documentation_context_contract": documentation_context_contract,
        "lane_reviews": lane_reviews,
        "blocking_items": blocking_items,
    }


def render_intake_handoff_markdown(handoff: dict[str, Any]) -> str:
    runtime = handoff.get("runtime_context", {}) if isinstance(handoff, dict) else {}
    lines = [
        "# Intake Handoff",
        "",
        f"- Gate Decision: {handoff.get('gate_decision', 'UNKNOWN')}",
        f"- Package: {runtime.get('package_zip_path', 'NONE')}",
        f"- Suggested Primary: {runtime.get('suggested_primary_document', 'NONE')}",
        f"- Suggested Phase IDs: {','.join(runtime.get('suggested_phase_ids', [])) if runtime.get('suggested_phase_ids') else 'NONE'}",
        "",
        "## Materialization Targets",
    ]
    targets = handoff.get("materialization_targets", {}).get("docs_to_refresh", [])
    if isinstance(targets, list) and targets:
        lines.extend([f"- {item}" for item in targets])
    else:
        lines.append("- none")
    lines.extend(["", "## Blocking Items (P0/P1)"])
    blockers = handoff.get("blocking_items", [])
    if isinstance(blockers, list) and blockers:
        for item in blockers:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {item.get('lane')}: {item.get('id')} [{item.get('severity')}] {item.get('title')}"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Structural Recommendations"])
    structural = handoff.get("structural_recommendations", [])
    if isinstance(structural, list) and structural:
        for item in structural:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- [{item.get('lane', 'unknown')}] ({item.get('priority', 'MEDIUM')}) {item.get('title', '')}"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Materialization Requirements"])
    requirements = handoff.get("materialization_requirements", {})
    documents = requirements.get("documents", []) if isinstance(requirements, dict) else []
    if isinstance(documents, list) and documents:
        for item in documents:
            if not isinstance(item, dict):
                continue
            lines.append(f"- {item.get('type')}: {item.get('path')}")
    else:
        lines.append("- none")
    lines.extend(["", "## Documentation Context Contract"])
    documentation_context = handoff.get("documentation_context_contract", {})
    source_documents = (
        documentation_context.get("source_documents", []) if isinstance(documentation_context, dict) else []
    )
    materialized_documents = (
        documentation_context.get("materialized_documents", []) if isinstance(documentation_context, dict) else []
    )
    lines.append("- Source Documents:")
    if isinstance(source_documents, list) and source_documents:
        for item in source_documents:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- Materialized Documents:")
    if isinstance(materialized_documents, list) and materialized_documents:
        for item in materialized_documents:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("")
    return "\n".join(lines)


def render_materialization_requirements_prompt(handoff: dict[str, Any]) -> str:
    requirements = handoff.get("materialization_requirements", {}) if isinstance(handoff, dict) else {}
    documents = requirements.get("documents", []) if isinstance(requirements, dict) else []
    outcomes = requirements.get("required_outcomes", []) if isinstance(requirements, dict) else []
    mandatory_sections = (
        requirements.get("phase_brief_mandatory_sections", []) if isinstance(requirements, dict) else []
    )
    documentation_context = (
        handoff.get("documentation_context_contract", {}) if isinstance(handoff, dict) else {}
    )
    lines = ["Required Documents, Constraints, Expected Results:"]
    if isinstance(documents, list) and documents:
        for idx, item in enumerate(documents, start=1):
            if not isinstance(item, dict):
                continue
            lines.append(f"- Document {idx}: {item.get('path')}")
            lines.append(f"  Type: {item.get('type')}")
            constraints = item.get("constraints", [])
            if isinstance(constraints, list) and constraints:
                lines.append("  Constraints:")
                for constraint in constraints:
                    lines.append(f"  - {constraint}")
            else:
                lines.append("  Constraints: none")
            lines.append(f"  Expected result: {item.get('expected_result', 'N/A')}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Mandatory phase brief sections (required in every module_phase_brief):")
    if isinstance(mandatory_sections, list) and mandatory_sections:
        for item in mandatory_sections:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            requirement = str(item.get("requirement", "")).strip()
            lines.append(f"- {title}")
            if requirement:
                lines.append(f"  - {requirement}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Required final outcomes:")
    if isinstance(outcomes, list) and outcomes:
        for item in outcomes:
            lines.append(f"- {item}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Documentation context contract (mandatory for drift prevention):")
    source_documents = (
        documentation_context.get("source_documents", []) if isinstance(documentation_context, dict) else []
    )
    materialized_documents = (
        documentation_context.get("materialized_documents", []) if isinstance(documentation_context, dict) else []
    )
    must_preserve = documentation_context.get("must_preserve", {}) if isinstance(documentation_context, dict) else {}
    guardrails = documentation_context.get("guardrails", []) if isinstance(documentation_context, dict) else []
    lines.append("- Source/original documents:")
    if isinstance(source_documents, list) and source_documents:
        for item in source_documents:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- Materialized documents:")
    if isinstance(materialized_documents, list) and materialized_documents:
        for item in materialized_documents:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    goals_digest = must_preserve.get("goals_digest", []) if isinstance(must_preserve, dict) else []
    acceptance_digest = (
        must_preserve.get("acceptance_criteria_digest", []) if isinstance(must_preserve, dict) else []
    )
    lines.append("- Must preserve goals digest:")
    if isinstance(goals_digest, list) and goals_digest:
        for item in goals_digest:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- Must preserve acceptance criteria digest:")
    if isinstance(acceptance_digest, list) and acceptance_digest:
        for item in acceptance_digest:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- Context guardrails:")
    if isinstance(guardrails, list) and guardrails:
        for item in guardrails:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    return "\n".join(lines)


def render_structural_recommendations_prompt(handoff: dict[str, Any]) -> str:
    structural = handoff.get("structural_recommendations", []) if isinstance(handoff, dict) else []
    lines = ["Structural recommendations and critical TZ changes to preserve:"]
    if isinstance(structural, list) and structural:
        for item in structural:
            if not isinstance(item, dict):
                continue
            lane = str(item.get("lane", "unknown")).strip()
            priority = str(item.get("priority", "MEDIUM")).strip().upper()
            title = str(item.get("title", "")).strip()
            why = str(item.get("why", "")).strip()
            proposal = str(item.get("proposal", "")).strip()
            impact = str(item.get("impact_on_tz", "")).strip()
            lines.append(f"- [{lane}] ({priority}) {title}")
            if why:
                lines.append(f"  why: {why}")
            if proposal:
                lines.append(f"  proposal: {proposal}")
            if impact:
                lines.append(f"  impact_on_tz: {impact}")
    else:
        lines.append("- none")
    return "\n".join(lines)


def build_materialization_prompt(
    *,
    runtime_context: str,
    policy_prompt_path: Path,
    intake_handoff_path: Path,
    intake_gate_path: Path,
    intake_handoff: dict[str, Any],
) -> str:
    requirements_block = render_materialization_requirements_prompt(intake_handoff)
    structural_block = render_structural_recommendations_prompt(intake_handoff)
    return (
        f"{runtime_context}\n\n"
        "Intake Policy Reference:\n"
        f"- Read and obey: {policy_prompt_path.as_posix()}\n\n"
        "Section Goals:\n"
        "- Part 1 (Input Validation): use intake handoff and gate artifacts as immutable sources.\n"
        "- Part 2 (Materialization): refresh required canonical docs without requirement drift.\n"
        "- Part 3 (Output Contract): return machine-parseable result with explicit residual blockers.\n\n"
        "Materialization Runtime Mode:\n"
        "- You are running materialization after sequential lane gate PASS.\n"
        "- Materialize/refresh canonical intake documentation now.\n"
        "- Preserve deterministic phase mapping from the source phase IR.\n"
        "- Do not invent extra phases or reorder source phases.\n"
        "- Use intake handoff as the single source of truth for goals, acceptance criteria, blockers, and structural recommendations.\n"
        "- Pull full context from both source/original docs and materialized docs before editing.\n"
        "- If goal/acceptance preservation cannot be proven, report residual blockers (do not soften requirements).\n"
        f"- Intake handoff JSON: {intake_handoff_path.as_posix()}\n"
        f"- Intake gate JSON: {intake_gate_path.as_posix()}\n\n"
        f"{requirements_block}\n\n"
        f"{structural_block}\n\n"
        "Expected output:\n"
        "- Update/refresh every required document listed above.\n"
        "- Return a short human summary.\n"
        "- Finish with exactly one tagged JSON block:\n"
        f"{MATERIALIZATION_RESULT_BEGIN}\n"
        '{"status":"DONE","updated_docs":["docs/codex/contracts/<slug>.execution-contract.md"],"notes":"what changed","residual_blockers":[],"context_coverage":{"source_documents":["..."],"materialized_documents":["docs/codex/contracts/<slug>.execution-contract.md","docs/codex/modules/<slug>.parent.md"],"preserved_goals":["..."],"preserved_acceptance_criteria":["..."]}}\n'
        f"{MATERIALIZATION_RESULT_END}\n"
    )


def lane_model_override(lane: str) -> str | None:
    if lane == "technical_intake":
        return TECHNICAL_INTAKE_MODEL
    return None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def slugify(text: str, fallback: str = "package") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:48] if slug else fallback


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def choose_latest_package(inbox: Path) -> Path | None:
    if not inbox.exists():
        return None
    packages = [path for path in inbox.iterdir() if path.is_file() and path.suffix.lower() == ".zip"]
    if not packages:
        return None
    return max(packages, key=lambda path: (path.stat().st_mtime, path.name.lower()))


def ensure_zip_package(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"package file not found: {path}")
    if not path.is_file():
        raise ValueError(f"package path is not a file: {path}")
    if path.suffix.lower() != ".zip":
        raise ValueError(f"package must be a .zip archive: {path}")


def safe_extract_zip(package_path: Path, extracted_root: Path) -> None:
    extracted_root.mkdir(parents=True, exist_ok=True)
    root = extracted_root.resolve()
    with zipfile.ZipFile(package_path) as archive:
        for member in archive.infolist():
            destination = (extracted_root / member.filename).resolve()
            if destination != root and root not in destination.parents:
                raise ValueError(f"zip contains unsafe path: {member.filename}")
        archive.extractall(extracted_root)


def extract_docx_title(path: Path) -> str | None:
    try:
        with zipfile.ZipFile(path) as archive:
            raw = archive.read("word/document.xml").decode("utf-8", errors="ignore")
    except Exception:
        return None
    texts = [html.unescape(token).strip() for token in re.findall(r"<w:t[^>]*>(.*?)</w:t>", raw)]
    joined = " ".join(token for token in texts if token)
    return first_line(joined)


def first_line(text: str | None) -> str | None:
    if not text:
        return None
    for raw in text.splitlines():
        cleaned = raw.strip().lstrip("#").strip()
        if cleaned:
            return cleaned[:160]
    return None


def extract_title(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix in {".md", ".txt", ".rst"}:
        try:
            return first_line(path.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            return None
    if suffix == ".docx":
        return extract_docx_title(path)
    return None


def read_plain_text(path: Path) -> str | None:
    if path.suffix.lower() not in {".md", ".txt", ".rst"}:
        return None
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None


def normalized_hint_text(path: Path) -> str:
    return path.as_posix().lower().replace(" ", "_").replace("-", "_")


def score_candidate(path: Path, *, extracted_root: Path) -> DocumentCandidate:
    rel_path = path.relative_to(extracted_root).as_posix()
    normalized = normalized_hint_text(Path(rel_path))
    reasons: list[str] = []
    score = EXTENSION_WEIGHTS.get(path.suffix.lower(), 0)
    reasons.append(f"extension weight: {path.suffix.lower() or '<none>'}")

    depth = max(len(Path(rel_path).parts) - 1, 0)
    depth_score = max(0, 18 - depth * 4)
    score += depth_score
    reasons.append(f"path depth bias: +{depth_score}")

    size = path.stat().st_size
    if size >= 2048:
        score += 5
        reasons.append("non-trivial file size")

    for token, weight, reason in POSITIVE_HINTS:
        if token in normalized:
            score += weight
            reasons.append(reason)
    for token, weight, reason in NEGATIVE_HINTS:
        if token in normalized:
            score += weight
            reasons.append(reason)

    title = extract_title(path)
    title_normalized = (title or "").lower()
    if title_normalized:
        if "requirements" in title_normalized or "требован" in title_normalized:
            score += 30
            reasons.append("title looks like requirements")
        if "spec" in title_normalized or "specification" in title_normalized or "тз" in title_normalized or "tz" in title_normalized:
            score += 25
            reasons.append("title looks like specification")
        if "verdict" in title_normalized:
            score -= 50
            reasons.append("title looks like verdict output")
        if "findings" in title_normalized:
            score -= 30
            reasons.append("title looks like findings output")

    text = read_plain_text(path)
    if text:
        normalized_text = text.lower()
        phase_headings = len(re.findall(r"^###\s+phase\s+[a-z0-9-]+", text, flags=re.IGNORECASE | re.MULTILINE))
        if phase_headings:
            score += 180 + phase_headings * 12
            reasons.append(f"content defines explicit phase rollout ({phase_headings} phases)")
        if "**acceptance gate**" in normalized_text and "**disprover**" in normalized_text:
            score += 90
            reasons.append("content carries acceptance/disprover contract")
        if "allow_release_readiness" in normalized_text:
            score += 45
            reasons.append("content names explicit release decision target")
        if "phase acceptance verdict" in normalized_text or "acceptance verdict" in normalized_text:
            score -= 40
            reasons.append("content reads like downstream verdict output")
        if "detailed phase findings" in normalized_text:
            score -= 25
            reasons.append("content reads like findings output")

    return DocumentCandidate(
        rel_path=rel_path,
        score=score,
        reasons=tuple(reasons),
        title=title,
    )


def collect_document_candidates(extracted_root: Path) -> list[DocumentCandidate]:
    candidates: list[DocumentCandidate] = []
    for path in sorted(extracted_root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in SUPPORTED_DOC_EXTENSIONS:
            continue
        candidates.append(score_candidate(path, extracted_root=extracted_root))
    candidates.sort(key=lambda item: (-item.score, item.rel_path))
    return candidates


def summarize_extensions(extracted_root: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in extracted_root.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower() or "<none>"
        counts[suffix] = counts.get(suffix, 0) + 1
    return dict(sorted(counts.items()))


def render_manifest(
    *,
    package_path: Path,
    extracted_root: Path,
    candidates: list[DocumentCandidate],
) -> str:
    top_candidate = candidates[0] if candidates else None
    lines = [
        "# Package Intake Manifest",
        "",
        f"Updated: {utc_now().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## Source",
        f"- Package Zip: {package_path.as_posix()}",
        f"- Extracted Root: {extracted_root.as_posix()}",
        "",
        "## Package Summary",
    ]
    for suffix, count in summarize_extensions(extracted_root).items():
        lines.append(f"- {suffix}: {count}")

    lines.extend(["", "## Suggested Primary Document"])
    if top_candidate is None:
        lines.append("- None")
    else:
        lines.append(f"- Path: {top_candidate.rel_path}")
        lines.append(f"- Score: {top_candidate.score}")
        if top_candidate.title:
            lines.append(f"- Title: {top_candidate.title}")

    lines.extend(["", "## Candidate Ranking"])
    if not candidates:
        lines.append("- No supported candidate documents were found.")
    else:
        for candidate in candidates[:8]:
            lines.append(f"- {candidate.rel_path} | score={candidate.score}")
            if candidate.title:
                lines.append(f"  title: {candidate.title}")
            lines.append(f"  reasons: {', '.join(candidate.reasons[:4])}")

    lines.extend(["", "## Clarification Policy"])
    lines.append("- Ask at most one compact clarification block only when safe progress is impossible.")

    lines.extend(["", "## Risks"])
    if top_candidate is None:
        lines.append("- No trusted primary document was detected from supported file types.")
    elif len(candidates) > 1 and candidates[1].score == top_candidate.score:
        lines.append("- Top candidate tie detected; confirm the primary document if the content conflicts.")
    else:
        lines.append("- No immediate intake blocker detected from filename heuristics.")

    lines.append("")
    return "\n".join(lines)


def write_manifest_json(
    *,
    path: Path,
    package_path: Path,
    extracted_root: Path,
    candidates: list[DocumentCandidate],
    compiler_artifact: Path | None,
    compiler_phase_ids: list[str],
) -> None:
    payload = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "package_zip": package_path.as_posix(),
        "extracted_root": extracted_root.as_posix(),
        "suggested_primary_document": candidates[0].rel_path if candidates else None,
        "candidates": [asdict(candidate) for candidate in candidates],
        "extension_counts": summarize_extensions(extracted_root),
        "suggested_phase_compiler_artifact": compiler_artifact.as_posix() if compiler_artifact else None,
        "suggested_phase_ids": compiler_phase_ids,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_prompt(
    *,
    prompt_path: Path,
    package_path: Path,
    extracted_root: Path,
    manifest_path: Path,
    suggested_primary: str | None,
    suggested_phase_compiler_artifact: str | None,
    suggested_phase_ids: list[str],
    mode: str,
) -> str:
    primary_line = suggested_primary if suggested_primary else "NONE"
    compiler_line = suggested_phase_compiler_artifact if suggested_phase_compiler_artifact else "NONE"
    phase_ids_line = ",".join(suggested_phase_ids) if suggested_phase_ids else "NONE"
    required_intake_skills = ", ".join(
        f".cursor/skills/{skill_id}/SKILL.md" for skill_id in INTAKE_REQUIRED_SKILLS
    )
    return (
        "Runtime Package Context:\n"
        f"- Package zip path: {package_path.as_posix()}\n"
        f"- Extracted package root: {extracted_root.as_posix()}\n"
        f"- Package manifest path: {manifest_path.as_posix()}\n"
        f"- Suggested primary document: {primary_line}\n"
        f"- Suggested phase compiler artifact: {compiler_line}\n"
        f"- Suggested phase ids: {phase_ids_line}\n"
        f"- Required technical intake skills: {required_intake_skills}\n"
        f"- Mode hint: {mode}\n"
    )


def run_codex(
    *,
    repo_root: Path,
    prompt: str,
    profile: str,
    output_path: Path,
    model: str | None = None,
) -> int:
    codex_bin = shutil.which("codex")
    if codex_bin is None:
        print(
            "error: `codex` executable was not found in PATH. "
            "Install Codex CLI first and authenticate with your ChatGPT account.",
            file=sys.stderr,
        )
        return 127

    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        codex_bin,
        "exec",
        "-C",
        str(repo_root),
        "--output-last-message",
        str(output_path),
    ]
    if profile.strip():
        cmd.extend(["-p", profile.strip()])
    if model and model.strip():
        cmd.extend(["-m", model.strip()])
    cmd.append("-")
    completed = subprocess.run(
        cmd,
        input=prompt,
        text=True,
        encoding="utf-8",
        cwd=repo_root,
        check=False,
    )
    return int(completed.returncode)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch Codex from a zip package.")
    parser.add_argument("package_path", nargs="?", help="Path to the zip package.")
    parser.add_argument(
        "--mode",
        default="auto",
        choices=sorted(ALLOWED_MODES),
        help="Autopilot mode hint passed to Codex.",
    )
    parser.add_argument(
        "--profile",
        default="",
        help="Codex profile name.",
    )
    parser.add_argument(
        "--prompt-file",
        default=str(DEFAULT_PROMPT),
        help="Repo-relative path to the package entry prompt file.",
    )
    parser.add_argument(
        "--inbox",
        default=str(DEFAULT_INBOX),
        help="Repo-relative inbox used when package_path is omitted.",
    )
    parser.add_argument(
        "--artifact-root",
        default=str(DEFAULT_ARTIFACT_ROOT),
        help="Repo-relative artifact root for unpacked packages and manifests.",
    )
    parser.add_argument(
        "--output-last-message",
        default=str(DEFAULT_OUTPUT),
        help="Repo-relative path for Codex's last-message capture.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare the package intake artifacts but do not invoke Codex.",
    )
    parser.add_argument(
        "--acceptance-only",
        action="store_true",
        help="Run only sequential intake acceptance gates (product + technical) and skip materialization.",
    )
    return parser.parse_args(argv)


def resolve_path(repo_root: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    repo_root = resolve_repo_root()

    if args.package_path:
        package_path = resolve_path(repo_root, args.package_path)
    else:
        inbox = resolve_path(repo_root, args.inbox)
        latest = choose_latest_package(inbox)
        if latest is None:
            print(f"error: no zip packages found in inbox: {inbox}", file=sys.stderr)
            return 2
        package_path = latest.resolve()

    try:
        ensure_zip_package(package_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    prompt_path = resolve_path(repo_root, args.prompt_file)
    if not prompt_path.exists():
        print(f"error: prompt file not found: {prompt_path}", file=sys.stderr)
        return 2

    artifact_root = resolve_path(repo_root, args.artifact_root)
    run_root = artifact_root / f"{utc_stamp()}-{slugify(package_path.stem)}"
    extracted_root = run_root / "extracted"
    manifest_path = run_root / "manifest.md"
    manifest_json_path = run_root / "manifest.json"

    try:
        safe_extract_zip(package_path, extracted_root)
    except (ValueError, zipfile.BadZipFile) as exc:
        print(f"error: failed to extract package: {exc}", file=sys.stderr)
        return 2

    candidates = collect_document_candidates(extracted_root)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        render_manifest(
            package_path=package_path,
            extracted_root=extracted_root,
            candidates=candidates,
        ),
        encoding="utf-8",
    )

    suggested_primary = None
    suggested_phase_compiler_artifact: Path | None = None
    suggested_phase_ids: list[str] = []
    if candidates:
        suggested_primary_path = (extracted_root / candidates[0].rel_path).resolve()
        suggested_primary = suggested_primary_path.as_posix()
        compiled_plan = compile_phase_plan(suggested_primary_path)
        if compiled_plan.phases:
            suggested_phase_compiler_artifact = run_root / "suggested-phase-plan.json"
            suggested_phase_ids = [phase.phase_id for phase in compiled_plan.phases]
            suggested_phase_compiler_artifact.write_text(
                json.dumps(phase_plan_payload(compiled_plan), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

    print(f"package zip: {package_path.as_posix()}")
    print(f"extracted root: {extracted_root.as_posix()}")
    print(f"manifest: {manifest_path.as_posix()}")
    print(f"suggested primary document: {suggested_primary or 'NONE'}")
    print(
        "suggested phase compiler artifact: "
        f"{suggested_phase_compiler_artifact.as_posix() if suggested_phase_compiler_artifact else 'NONE'}"
    )
    print(f"suggested phase ids: {','.join(suggested_phase_ids) if suggested_phase_ids else 'NONE'}")

    write_manifest_json(
        path=manifest_json_path,
        package_path=package_path,
        extracted_root=extracted_root,
        candidates=candidates,
        compiler_artifact=suggested_phase_compiler_artifact,
        compiler_phase_ids=suggested_phase_ids,
    )

    if args.dry_run:
        print("dry run: Codex invocation skipped")
        return 0

    output_path = resolve_path(repo_root, args.output_last_message)
    runtime_context = build_prompt(
        prompt_path=prompt_path,
        package_path=package_path,
        extracted_root=extracted_root,
        manifest_path=manifest_path,
        suggested_primary=suggested_primary,
        suggested_phase_compiler_artifact=(
            suggested_phase_compiler_artifact.as_posix() if suggested_phase_compiler_artifact else None
        ),
        suggested_phase_ids=suggested_phase_ids,
        mode=args.mode,
    )
    lane_outputs = {
        "product_intake": run_root / "product-intake-last-message.txt",
        "technical_intake": run_root / "technical-intake-last-message.txt",
    }
    lane_prompts = {
        lane: build_intake_lane_prompt(
            runtime_context=runtime_context,
            policy_prompt_path=prompt_path,
            lane=lane,
        )
        for lane in lane_outputs
    }
    lane_exit_codes: dict[str, int] = {}
    for lane in LANE_SEQUENCE:
        print(f"running intake lane: {lane}")
        lane_exit_codes[lane] = run_codex(
            repo_root=repo_root,
            prompt=lane_prompts[lane],
            profile=args.profile,
            output_path=lane_outputs[lane],
            model=lane_model_override(lane),
        )
        print(f"{lane} exit code: {lane_exit_codes[lane]}")
        if lane_exit_codes[lane] != 0:
            print(f"error: intake lane execution failed: {lane}", file=sys.stderr)
            return lane_exit_codes[lane]

    lane_payloads: dict[str, dict[str, Any]] = {}
    for lane in LANE_SEQUENCE:
        lane_output = lane_outputs[lane]
        if not lane_output.exists():
            print(f"error: missing lane output artifact for {lane}: {lane_output.as_posix()}", file=sys.stderr)
            return INTAKE_BLOCKED_EXIT
        try:
            lane_text = lane_output.read_text(encoding="utf-8")
            lane_payloads[lane] = extract_lane_payload_from_text(lane_text, lane=lane)
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            print(f"error: invalid {lane} payload: {exc}", file=sys.stderr)
            return INTAKE_BLOCKED_EXIT
        lane_payload_path = run_root / f"{lane}.json"
        lane_payload_path.write_text(json.dumps(lane_payloads[lane], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"{lane} artifact: {lane_payload_path.as_posix()}")

    intake_gate = evaluate_intake_gate_payload(
        {
            "technical_intake": lane_payloads["technical_intake"],
            "product_intake": lane_payloads["product_intake"],
            "combined_gate": {"decision": "PASS"},
        }
    )
    intake_gate_json = run_root / "intake-gate.json"
    intake_gate_md = run_root / "intake-gate.md"
    intake_gate_json.write_text(json.dumps(intake_gate, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    intake_gate_md.write_text(render_intake_gate_markdown(intake_gate), encoding="utf-8")
    intake_human_summary = build_intake_human_summary(intake_gate)
    intake_human_summary_json = run_root / "intake-human-summary.json"
    intake_human_summary_md = run_root / "intake-human-summary.md"
    intake_human_summary_md_text = render_intake_human_summary_markdown(intake_human_summary)
    intake_human_summary_json.write_text(
        json.dumps(intake_human_summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    intake_human_summary_md.write_text(intake_human_summary_md_text, encoding="utf-8")
    intake_handoff = build_intake_handoff(
        package_path=package_path,
        extracted_root=extracted_root,
        manifest_path=manifest_path,
        suggested_primary=suggested_primary,
        suggested_phase_compiler_artifact=(
            suggested_phase_compiler_artifact.as_posix() if suggested_phase_compiler_artifact else None
        ),
        suggested_phase_ids=suggested_phase_ids,
        lane_payloads=lane_payloads,
        intake_gate=intake_gate,
        intake_human_summary=intake_human_summary,
    )
    intake_handoff_json = run_root / "intake-handoff.json"
    intake_handoff_md = run_root / "intake-handoff.md"
    intake_handoff_json.write_text(json.dumps(intake_handoff, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    intake_handoff_md.write_text(render_intake_handoff_markdown(intake_handoff), encoding="utf-8")
    print(f"intake gate artifact: {intake_gate_json.as_posix()}")
    print(f"intake gate report: {intake_gate_md.as_posix()}")
    print(f"intake human summary artifact: {intake_human_summary_json.as_posix()}")
    print(f"intake human summary report: {intake_human_summary_md.as_posix()}")
    print(f"intake handoff artifact: {intake_handoff_json.as_posix()}")
    print(f"intake handoff report: {intake_handoff_md.as_posix()}")
    print(f"intake gate decision: {intake_gate['combined_gate']['decision']}")
    print(INTAKE_HUMAN_SUMMARY_BEGIN)
    print(intake_human_summary_md_text.rstrip())
    print(INTAKE_HUMAN_SUMMARY_END)
    if intake_gate["combined_gate"]["decision"] != "PASS":
        print("intake gate blocked: resolve P0/P1 blockers and rerun package intake")
        return INTAKE_BLOCKED_EXIT
    if args.acceptance_only:
        print("acceptance-only mode: materialization skipped after PASS")
        return 0

    materialization_prompt = build_materialization_prompt(
        runtime_context=runtime_context,
        policy_prompt_path=prompt_path,
        intake_handoff_path=intake_handoff_json,
        intake_gate_path=intake_gate_json,
        intake_handoff=intake_handoff,
    )
    materialization_exit_code = run_codex(
        repo_root=repo_root,
        prompt=materialization_prompt,
        profile=args.profile,
        output_path=output_path,
    )
    if materialization_exit_code != 0:
        return materialization_exit_code
    if not output_path.exists():
        print(
            f"error: Codex did not produce output-last-message artifact: {output_path.as_posix()}",
            file=sys.stderr,
        )
        return INTAKE_BLOCKED_EXIT
    try:
        materialization_text = output_path.read_text(encoding="utf-8")
        materialization_result_raw = extract_materialization_result_from_text(materialization_text)
        materialization_result = evaluate_materialization_result(
            result=materialization_result_raw,
            handoff=intake_handoff,
        )
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        print(f"error: invalid materialization result payload: {exc}", file=sys.stderr)
        return INTAKE_BLOCKED_EXIT

    materialization_result_json = run_root / "materialization-result.json"
    materialization_result_md = run_root / "materialization-result.md"
    materialization_result_json.write_text(
        json.dumps(materialization_result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    materialization_result_md.write_text(
        render_materialization_result_markdown(materialization_result),
        encoding="utf-8",
    )
    print(f"materialization output: {output_path.as_posix()}")
    print(f"materialization result artifact: {materialization_result_json.as_posix()}")
    print(f"materialization result report: {materialization_result_md.as_posix()}")
    print(f"materialization decision: {materialization_result['decision']}")
    if materialization_result["decision"] != "PASS":
        print("materialization blocked: resolve blockers and rerun package intake")
        return INTAKE_BLOCKED_EXIT
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
