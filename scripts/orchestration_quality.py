from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from result_quality import (
    normalize_result_quality_payload,
    result_quality_to_dict,
    unscored_result_quality_summary,
)


ROLLING_WINDOW_SIZE = 20
QUALITY_BAND_THRESHOLDS = (
    (90, "excellent"),
    (80, "strong"),
    (65, "watch"),
    (50, "fragile"),
    (0, "critical"),
)
BLOCKER_CATEGORY_HINTS = {
    "acceptance_blocker": "Make acceptor findings more structured so blocker classes can be routed earlier.",
    "assumption": "Strengthen worker handoffs so unresolved assumptions are blocked before acceptance.",
    "deferred": "Split critical work earlier so deferred closure does not survive into acceptance.",
    "doc_context_gap": "Require full source and materialized documentation context whenever remediation edits docs.",
    "environment_blocker": "Add environment and host preflight checks so operational failures stop earlier and classify cleanly.",
    "evidence_gap": "Tighten evidence contracts around exact commands, artifacts, and owned-surface coverage.",
    "fallback": "Turn fallback paths into explicit contract decisions instead of allowing silent implementation shortcuts.",
    "missing_acceptance_artifact": "Persist acceptance artifacts for every attempt so scoring and release evidence stay trustworthy.",
    "pass_with_blockers": "Keep verdict semantics strict: PASS must mean zero unresolved blockers.",
    "prohibited_finding": "Move prohibited conditions into preflight and worker guardrails so acceptance is not the first detector.",
    "skip": "Shift required checks earlier or reduce scope until reruns can execute the full evidence set.",
    "uncategorized": "Add structured blocker categories and lenses so repeated failures become machine-aggregatable.",
    "worker_doc_edit": "Keep documentation closure inside remediation and pass a richer docs handoff package.",
}
POLICY_CATEGORY_WEIGHTS = {
    "acceptance_blocker": 6,
    "assumption": 12,
    "deferred": 14,
    "doc_context_gap": 18,
    "environment_blocker": 8,
    "evidence_gap": 10,
    "fallback": 16,
    "missing_acceptance_artifact": 18,
    "pass_with_blockers": 20,
    "prohibited_finding": 16,
    "skip": 14,
    "uncategorized": 6,
    "worker_doc_edit": 18,
}
ENVIRONMENT_TERMS = (
    "permissionerror",
    "command line is too long",
    "temp-directory",
    "host-level",
    "index.lock",
    "lock timeout",
    "runtime root not found",
)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _safe_ratio(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _clamp_score(value: float) -> int:
    return max(0, min(100, int(round(value))))


def _score_label(score: int) -> str:
    for threshold, label in QUALITY_BAND_THRESHOLDS:
        if score >= threshold:
            return label
    return "critical"


def _resolve_report_path(repo_root: Path, raw: str | Path | None) -> Path | None:
    if raw is None:
        return None
    path = Path(str(raw))
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _state_sort_key(payload: dict[str, Any], path: Path) -> datetime:
    updated_at = _parse_iso_datetime(str(payload.get("updated_at", "")))
    if updated_at is not None:
        return updated_at
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def _normalize_policy_category(raw: str) -> str:
    normalized = str(raw or "").strip().lower()
    return normalized if normalized in BLOCKER_CATEGORY_HINTS else "uncategorized"


def _categorize_blocker(blocker: dict[str, Any]) -> str:
    blocker_id = str(blocker.get("id", "")).strip().upper()
    if blocker_id.startswith("P-"):
        parts = blocker_id.split("-")
        if len(parts) >= 3:
            category = "_".join(parts[1:-1]).lower()
            return _normalize_policy_category(category)

    detail = " ".join(
        str(blocker.get(field, "")).strip().lower() for field in ("title", "why", "remediation")
    ).strip()
    if any(term in detail for term in ENVIRONMENT_TERMS):
        return "environment_blocker"
    if "evidence" in detail:
        return "evidence_gap"
    if "fallback" in detail:
        return "fallback"
    if "skip" in detail:
        return "skip"
    if "assumption" in detail:
        return "assumption"
    if "document" in detail and "context" in detail:
        return "doc_context_gap"
    return "acceptance_blocker"


def _ranked_categories(counter: Counter[str]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for category, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        ranked.append(
            {
                "category": category,
                "count": count,
                "suggestion": BLOCKER_CATEGORY_HINTS.get(category, BLOCKER_CATEGORY_HINTS["uncategorized"]),
            }
        )
    return ranked


def _unique_lines(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def summarize_orchestration_run(
    payload: dict[str, Any],
    *,
    repo_root: Path,
) -> dict[str, Any]:
    attempts = payload.get("attempts", [])
    if not isinstance(attempts, list):
        attempts = []

    attempts_total = len(attempts)
    remediation_attempts = 0
    blocked_attempts = 0
    evidence_gaps_total = 0
    prohibited_findings_total = 0
    policy_blockers_total = 0
    acceptance_blockers_total = 0
    missing_acceptance_artifacts = 0
    category_counter: Counter[str] = Counter()

    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        if str(attempt.get("kind", "")).strip().lower() == "remediation":
            remediation_attempts += 1
        if str(attempt.get("verdict", "")).strip().upper() != "PASS":
            blocked_attempts += 1

        acceptance_path = _resolve_report_path(repo_root, attempt.get("acceptance_json_path"))
        if acceptance_path is None or not acceptance_path.exists():
            missing_acceptance_artifacts += 1
            category_counter["missing_acceptance_artifact"] += 1
            continue

        acceptance_payload = _load_json(acceptance_path)
        if acceptance_payload is None:
            missing_acceptance_artifacts += 1
            category_counter["missing_acceptance_artifact"] += 1
            continue

        evidence_gaps = acceptance_payload.get("evidence_gaps", [])
        prohibited_findings = acceptance_payload.get("prohibited_findings", [])
        blockers = [item for item in acceptance_payload.get("blockers", []) if isinstance(item, dict)]
        policy_blockers = [item for item in acceptance_payload.get("policy_blockers", []) if isinstance(item, dict)]

        evidence_gaps_total += len(evidence_gaps) if isinstance(evidence_gaps, list) else 0
        prohibited_findings_total += len(prohibited_findings) if isinstance(prohibited_findings, list) else 0
        policy_blockers_total += len(policy_blockers)

        policy_ids = {str(item.get("id", "")).strip() for item in policy_blockers}
        for blocker in policy_blockers:
            category_counter[_categorize_blocker(blocker)] += 1

        acceptance_only = 0
        for blocker in blockers:
            blocker_id = str(blocker.get("id", "")).strip()
            if blocker_id in policy_ids:
                continue
            acceptance_only += 1
            category_counter[_categorize_blocker(blocker)] += 1
        acceptance_blockers_total += acceptance_only

    final_status = str(payload.get("final_status", "")).strip().lower() or "unknown"
    first_pass_passed = (
        final_status == "accepted"
        and attempts_total == 1
        and remediation_attempts == 0
        and missing_acceptance_artifacts == 0
        and policy_blockers_total == 0
        and evidence_gaps_total == 0
        and prohibited_findings_total == 0
    )

    progression_penalty = max(attempts_total - 1, 0) * 15
    progression_penalty += remediation_attempts * 10
    progression_penalty += blocked_attempts * 5
    if final_status != "accepted":
        progression_penalty += 35
    progression_score = _clamp_score(100 - progression_penalty)

    evidence_penalty = evidence_gaps_total * 14
    evidence_penalty += prohibited_findings_total * 15
    evidence_penalty += acceptance_blockers_total * 6
    evidence_penalty += missing_acceptance_artifacts * 20
    evidence_score = _clamp_score(100 - evidence_penalty)

    policy_penalty = 0
    for category, count in category_counter.items():
        policy_penalty += POLICY_CATEGORY_WEIGHTS.get(category, POLICY_CATEGORY_WEIGHTS["uncategorized"]) * count
    policy_score = _clamp_score(100 - policy_penalty)

    orchestration_score = _clamp_score(
        progression_score * 0.40 + evidence_score * 0.35 + policy_score * 0.25
    )
    top_categories = _ranked_categories(category_counter)

    score_rationale: list[str] = []
    if first_pass_passed:
        score_rationale.append("Accepted on the first pass with no remediation loop.")
    else:
        if remediation_attempts:
            score_rationale.append("Remediation was required before the phase could move forward.")
        if final_status != "accepted":
            score_rationale.append("The phase remained blocked at the end of the governed cycle.")
    if evidence_gaps_total:
        score_rationale.append("Acceptance recorded unresolved evidence gaps.")
    if policy_blockers_total:
        score_rationale.append("Orchestrator policy raised blockers that reduced result confidence.")
    if missing_acceptance_artifacts:
        score_rationale.append("Some acceptance artifacts were missing or unreadable.")
    score_rationale = _unique_lines(score_rationale)

    expansion_points: list[str] = []
    if attempts_total > 1:
        expansion_points.append(
            "Raise first-pass quality by pushing blocker-specific checks into preflight or worker handoff."
        )
    if final_status != "accepted":
        expansion_points.append(
            "Escalate repeated blocker patterns earlier instead of spending the full retry budget on the same class."
        )
    for item in top_categories[:3]:
        expansion_points.append(item["suggestion"])
    expansion_points = _unique_lines(expansion_points)

    return {
        "orchestration_score": orchestration_score,
        "score_label": _score_label(orchestration_score),
        "component_scores": {
            "progression": progression_score,
            "evidence": evidence_score,
            "policy": policy_score,
        },
        "attempts_total": attempts_total,
        "blocked_attempts": blocked_attempts,
        "remediation_attempts": remediation_attempts,
        "first_pass_passed": first_pass_passed,
        "final_status": final_status,
        "acceptance_blockers_total": acceptance_blockers_total,
        "policy_blockers_total": policy_blockers_total,
        "evidence_gaps_total": evidence_gaps_total,
        "prohibited_findings_total": prohibited_findings_total,
        "missing_acceptance_artifacts_total": missing_acceptance_artifacts,
        "top_blocker_categories": top_categories,
        "quality_expansion_points": expansion_points,
        "score_rationale": score_rationale,
    }


def extract_result_quality_summary(payload: dict[str, Any], *, repo_root: Path) -> dict[str, Any]:
    attempts = payload.get("attempts", [])
    if not isinstance(attempts, list) or not attempts:
        return result_quality_to_dict(
            unscored_result_quality_summary("No acceptance attempt is recorded for this run.")
        ) or {}

    latest_attempt = attempts[-1]
    if not isinstance(latest_attempt, dict):
        return result_quality_to_dict(
            unscored_result_quality_summary("Latest attempt payload is malformed.")
        ) or {}

    acceptance_path = _resolve_report_path(repo_root, latest_attempt.get("acceptance_json_path"))
    if acceptance_path is None or not acceptance_path.exists():
        return result_quality_to_dict(
            unscored_result_quality_summary("Latest acceptance artifact is missing.")
        ) or {}

    acceptance_payload = _load_json(acceptance_path)
    if acceptance_payload is None:
        return result_quality_to_dict(
            unscored_result_quality_summary("Latest acceptance artifact could not be read.")
        ) or {}

    try:
        summary = normalize_result_quality_payload(acceptance_payload.get("result_quality"))
    except ValueError as exc:
        return result_quality_to_dict(
            unscored_result_quality_summary(f"Latest result-quality payload is invalid: {exc}")
        ) or {}
    if summary is None:
        return result_quality_to_dict(
            unscored_result_quality_summary("Acceptor did not provide a result-quality evaluation.")
        ) or {}
    return result_quality_to_dict(summary) or {}


def extract_worker_self_quality_summary(payload: dict[str, Any], *, repo_root: Path) -> dict[str, Any]:
    attempts = payload.get("attempts", [])
    if not isinstance(attempts, list) or not attempts:
        return result_quality_to_dict(
            unscored_result_quality_summary("No worker attempt is recorded for this run.")
        ) or {}

    latest_attempt = attempts[-1]
    if not isinstance(latest_attempt, dict):
        return result_quality_to_dict(
            unscored_result_quality_summary("Latest worker attempt payload is malformed.")
        ) or {}

    worker_report_path = _resolve_report_path(repo_root, latest_attempt.get("worker_report_path"))
    if worker_report_path is None or not worker_report_path.exists():
        return result_quality_to_dict(
            unscored_result_quality_summary("Latest worker report artifact is missing.")
        ) or {}

    worker_report_payload = _load_json(worker_report_path)
    if worker_report_payload is None:
        return result_quality_to_dict(
            unscored_result_quality_summary("Latest worker report artifact could not be read.")
        ) or {}

    try:
        summary = normalize_result_quality_payload(worker_report_payload.get("worker_self_quality"))
    except ValueError as exc:
        return result_quality_to_dict(
            unscored_result_quality_summary(f"Latest worker self-quality payload is invalid: {exc}")
        ) or {}
    if summary is None:
        return result_quality_to_dict(
            unscored_result_quality_summary("Worker did not provide a self-quality evaluation.")
        ) or {}
    return result_quality_to_dict(summary) or {}


def build_worker_acceptor_delta_summary(
    *,
    worker_self_quality_summary: dict[str, Any],
    result_quality_summary: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(worker_self_quality_summary, dict) or worker_self_quality_summary.get("status") != "scored":
        return {
            "status": "unscored",
            "reason": "Worker self-quality summary is unavailable.",
        }
    if not isinstance(result_quality_summary, dict) or result_quality_summary.get("status") != "scored":
        return {
            "status": "unscored",
            "reason": "Acceptor result-quality summary is unavailable.",
        }

    worker_score = worker_self_quality_summary.get("overall_score")
    acceptor_score = result_quality_summary.get("overall_score")
    if not isinstance(worker_score, int) or not isinstance(acceptor_score, int):
        return {
            "status": "unscored",
            "reason": "Worker or acceptor score is not numeric.",
        }

    signed_delta = worker_score - acceptor_score
    absolute_delta = abs(signed_delta)
    if absolute_delta <= 5:
        calibration = "aligned"
        interpretation = "Worker self-assessment is closely aligned with independent acceptance."
    elif signed_delta > 5:
        calibration = "overconfident"
        interpretation = "Worker self-assessment is materially higher than acceptance's result-quality verdict."
    else:
        calibration = "underconfident"
        interpretation = "Worker self-assessment is materially lower than acceptance's result-quality verdict."

    return {
        "status": "scored",
        "worker_self_score": worker_score,
        "acceptor_result_score": acceptor_score,
        "signed_delta": signed_delta,
        "absolute_delta": absolute_delta,
        "calibration": calibration,
        "interpretation": interpretation,
    }


def attach_quality_summaries(payload: dict[str, Any], *, repo_root: Path) -> dict[str, Any]:
    enriched = dict(payload)
    enriched["result_quality_summary"] = extract_result_quality_summary(enriched, repo_root=repo_root)
    enriched["worker_self_quality_summary"] = extract_worker_self_quality_summary(enriched, repo_root=repo_root)
    enriched["worker_acceptor_delta_summary"] = build_worker_acceptor_delta_summary(
        worker_self_quality_summary=enriched["worker_self_quality_summary"],
        result_quality_summary=enriched["result_quality_summary"],
    )
    enriched["orchestration_quality_summary"] = summarize_orchestration_run(enriched, repo_root=repo_root)
    return enriched


def load_orchestration_states(artifact_root: Path) -> list[dict[str, Any]]:
    if not artifact_root.exists():
        return []

    records: list[dict[str, Any]] = []
    for state_path in artifact_root.rglob("state.json"):
        payload = _load_json(state_path)
        if payload is None:
            continue
        records.append(
            {
                "state_path": state_path.resolve(),
                "payload": payload,
                "sort_key": _state_sort_key(payload, state_path),
            }
        )
    records.sort(key=lambda item: item["sort_key"], reverse=True)
    return records


def compute_orchestration_rollup(
    *,
    artifact_root: Path,
    repo_root: Path,
    window_size: int = ROLLING_WINDOW_SIZE,
) -> dict[str, Any]:
    states = load_orchestration_states(artifact_root)
    window = states[:window_size] if window_size > 0 else states

    run_summaries: list[dict[str, Any]] = []
    category_counter: Counter[str] = Counter()
    delta_absolute_values: list[int] = []
    calibration_runs = 0
    overconfidence_runs = 0
    underconfidence_runs = 0
    for item in window:
        enriched = attach_quality_summaries(item["payload"], repo_root=repo_root)
        summary = enriched["orchestration_quality_summary"]
        run_summaries.append(summary)
        for category in summary["top_blocker_categories"]:
            category_counter[category["category"]] += int(category["count"])
        delta_summary = enriched.get("worker_acceptor_delta_summary", {})
        if isinstance(delta_summary, dict) and delta_summary.get("status") == "scored":
            calibration_runs += 1
            delta_absolute_values.append(int(delta_summary.get("absolute_delta", 0)))
            calibration = str(delta_summary.get("calibration", "")).strip().lower()
            if calibration == "overconfident":
                overconfidence_runs += 1
            elif calibration == "underconfident":
                underconfidence_runs += 1

    scores = [int(item["orchestration_score"]) for item in run_summaries]
    accepted_runs = sum(1 for item in run_summaries if item["final_status"] == "accepted")
    blocked_runs = sum(1 for item in run_summaries if item["final_status"] != "accepted")
    first_pass_runs = sum(1 for item in run_summaries if item["first_pass_passed"])
    remediation_runs = sum(1 for item in run_summaries if int(item["remediation_attempts"]) > 0)
    score_bands = Counter(str(item["score_label"]) for item in run_summaries)

    top_categories = _ranked_categories(category_counter)
    quality_expansion_points: list[str] = []
    if window:
        if _safe_ratio(first_pass_runs, len(window)) < 0.7:
            quality_expansion_points.append(
                "First-pass acceptance is too low; strengthen worker handoff contracts and preflight checks."
            )
        if _safe_ratio(remediation_runs, len(window)) > 0.3:
            quality_expansion_points.append(
                "Remediation is activating often; move recurring blocker classes into earlier route decisions."
            )
        if _safe_ratio(blocked_runs, len(window)) > 0.2:
            quality_expansion_points.append(
                "Blocked final states are too frequent; escalate repeated blocker patterns sooner."
            )
    for item in top_categories[:3]:
        quality_expansion_points.append(item["suggestion"])
    quality_expansion_points = _unique_lines(quality_expansion_points)

    return {
        "runs_total": len(states),
        "window_runs_count": len(window),
        "burn_in_complete": len(window) >= max(window_size, 1),
        "accepted_runs": accepted_runs,
        "blocked_runs": blocked_runs,
        "first_pass_acceptance_rate": _safe_ratio(first_pass_runs, len(window)),
        "remediation_rate": _safe_ratio(remediation_runs, len(window)),
        "average_orchestration_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
        "median_orchestration_score": float(median(scores)) if scores else 0.0,
        "latest_orchestration_score": scores[0] if scores else 0,
        "latest_score_label": run_summaries[0]["score_label"] if run_summaries else "unscored",
        "calibration_runs": calibration_runs,
        "average_worker_acceptor_absolute_delta": (
            round(sum(delta_absolute_values) / len(delta_absolute_values), 2) if delta_absolute_values else 0.0
        ),
        "worker_overconfidence_rate": _safe_ratio(overconfidence_runs, calibration_runs),
        "worker_underconfidence_rate": _safe_ratio(underconfidence_runs, calibration_runs),
        "score_bands": dict(sorted(score_bands.items())),
        "top_blocker_categories": top_categories,
        "quality_expansion_points": quality_expansion_points,
        "latest_run": run_summaries[0] if run_summaries else None,
    }


def render_rollup_markdown(rollup: dict[str, Any]) -> str:
    lines = [
        "# Orchestration Quality Rollup",
        "",
        f"- runs_total: {rollup.get('runs_total', 0)}",
        f"- window_runs_count: {rollup.get('window_runs_count', 0)}",
        f"- average_orchestration_score: {float(rollup.get('average_orchestration_score', 0.0)):.2f}",
        f"- median_orchestration_score: {float(rollup.get('median_orchestration_score', 0.0)):.2f}",
        f"- average_worker_acceptor_absolute_delta: {float(rollup.get('average_worker_acceptor_absolute_delta', 0.0)):.2f}",
        f"- latest_score_label: {rollup.get('latest_score_label', 'unscored')}",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| `first_pass_acceptance_rate` | {float(rollup.get('first_pass_acceptance_rate', 0.0)):.2f} |",
        f"| `remediation_rate` | {float(rollup.get('remediation_rate', 0.0)):.2f} |",
        f"| `blocked_runs` | {int(rollup.get('blocked_runs', 0))} |",
        "",
        "## Top Blocker Categories",
    ]
    top_categories = rollup.get("top_blocker_categories", [])
    if not isinstance(top_categories, list) or not top_categories:
        lines.append("- none")
    else:
        for item in top_categories[:5]:
            if not isinstance(item, dict):
                continue
            lines.append(f"- {item.get('category')}: {item.get('count')}")
    lines.extend(["", "## Quality Expansion Points"])
    expansion_points = rollup.get("quality_expansion_points", [])
    if not isinstance(expansion_points, list) or not expansion_points:
        lines.append("- none")
    else:
        for item in expansion_points:
            lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Roll up governed orchestration quality from run artifacts.")
    parser.add_argument("--artifact-root", default="artifacts/codex/orchestration")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--window-size", type=int, default=ROLLING_WINDOW_SIZE)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()

    rollup = compute_orchestration_rollup(
        artifact_root=Path(args.artifact_root),
        repo_root=Path(args.repo_root).resolve(),
        window_size=args.window_size,
    )
    if args.format == "markdown":
        print(render_rollup_markdown(rollup).rstrip())
    else:
        print(json.dumps(rollup, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
