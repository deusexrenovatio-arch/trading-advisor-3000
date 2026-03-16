from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
import sys
from pathlib import Path
from typing import Any

from agent_process_telemetry import compute_process_rollup, load_task_outcomes


TERMINAL_STATUSES = {"completed", "partial", "blocked"}

METRIC_TARGETS: dict[str, tuple[str, float]] = {
    "correct_first_time_pct": (">=", 0.80),
    "start_match_pct": (">=", 0.90),
    "context_expansion_rate": ("<=", 0.20),
    "repeat_error_rate": ("<=", 0.00),
    "environment_blocker_rate": ("<=", 0.05),
}


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _norm_text(value: Any, *, default: str, lowercase: bool = True) -> str:
    text = str(value or "").strip()
    if not text:
        text = default
    return text.lower() if lowercase else text


def _norm_positive_int(value: Any, *, default: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _window_records(payload: dict[str, Any], window_size: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw in payload.get("items", []):
        if not isinstance(raw, dict):
            continue
        closed_at = _parse_iso_datetime(raw.get("closed_at"))
        outcome = _norm_text(raw.get("outcome_status"), default="in_progress")
        if outcome not in TERMINAL_STATUSES:
            continue
        if closed_at is None:
            continue
        records.append(
            {
                "task_id": _norm_text(raw.get("task_id"), default="unknown-task", lowercase=False),
                "closed_at": closed_at,
                "outcome_status": outcome,
                "route_match": _norm_text(raw.get("route_match"), default="pending"),
                "decision_quality": _norm_text(raw.get("decision_quality"), default="pending"),
                "incident_signature": _norm_text(raw.get("incident_signature"), default="none"),
                "same_path_attempts": _norm_positive_int(raw.get("same_path_attempts"), default=1),
                "primary_rework_cause": _norm_text(raw.get("primary_rework_cause"), default="none"),
                "improvement_action": _norm_text(raw.get("improvement_action"), default="none"),
                "improvement_artifact": _norm_text(
                    raw.get("improvement_artifact"),
                    default="none",
                    lowercase=False,
                ),
            }
        )
    records.sort(key=lambda item: item["closed_at"])
    if window_size > 0:
        return records[-window_size:]
    return records


def _counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: int(value) for key, value in sorted(counter.items())}


def _build_patterns(window: list[dict[str, Any]]) -> dict[str, Any]:
    decision_counts = Counter(row["decision_quality"] for row in window)
    outcome_counts = Counter(row["outcome_status"] for row in window)
    rework_counts = Counter(
        row["primary_rework_cause"] for row in window if row["primary_rework_cause"] != "none"
    )
    improvement_counts = Counter(
        row["improvement_action"] for row in window if row["improvement_action"] != "none"
    )
    signature_counts = Counter(
        row["incident_signature"] for row in window if row["incident_signature"] != "none"
    )
    repeated_signatures = {
        key: value for key, value in sorted(signature_counts.items()) if value > 1
    }
    retry_rows = [row for row in window if int(row["same_path_attempts"]) >= 2]
    partial_or_blocked = [
        row for row in window if row["outcome_status"] in {"partial", "blocked"}
    ]
    missing_improvement = [
        row for row in partial_or_blocked if row["improvement_action"] in {"none", "n/a", "pending"}
    ]
    return {
        "decision_quality_counts": _counter_dict(decision_counts),
        "outcome_status_counts": _counter_dict(outcome_counts),
        "rework_cause_counts": _counter_dict(rework_counts),
        "improvement_action_counts": _counter_dict(improvement_counts),
        "repeat_incident_signatures": repeated_signatures,
        "same_path_retry_tasks": [row["task_id"] for row in retry_rows],
        "same_path_retry_count": len(retry_rows),
        "partial_or_blocked_count": len(partial_or_blocked),
        "missing_improvement_actions_count": len(missing_improvement),
    }


def _metric_target_text(metric_name: str) -> str:
    operator, target = METRIC_TARGETS.get(metric_name, (">=", 0.0))
    return f"{operator} {target:.2f}"


def _metric_status(metric_name: str, value: float) -> str:
    operator, target = METRIC_TARGETS.get(metric_name, (">=", 0.0))
    if operator == ">=":
        return "OK" if value >= target else "ACTION_REQUIRED"
    return "OK" if value <= target else "ACTION_REQUIRED"


def _append_item(items: list[dict[str, str]], **kwargs: str) -> None:
    items.append(
        {
            "id": kwargs["id"],
            "priority": kwargs["priority"],
            "trigger": kwargs["trigger"],
            "action": kwargs["action"],
            "owner": kwargs["owner"],
            "due": kwargs["due"],
        }
    )


def _build_action_items(
    *,
    rollup: dict[str, Any],
    patterns: dict[str, Any],
    window_size: int,
) -> list[dict[str, str]]:
    metrics = rollup.get("current_metrics", {})
    items: list[dict[str, str]] = []

    correct_first_time = float(metrics.get("correct_first_time_pct", 0.0))
    if correct_first_time < 0.80:
        _append_item(
            items,
            id="PROC-QUALITY-001",
            priority="P1",
            trigger=(
                f"correct_first_time_pct={correct_first_time:.2f} "
                f"(target {_metric_target_text('correct_first_time_pct')})"
            ),
            action=(
                "Run a first-time-right retro for recent non-first-time tasks, "
                "then update first-time-right checklist before next PR closeout."
            ),
            owner="process-owner",
            due="before next PR gate",
        )

    if int(patterns.get("partial_or_blocked_count", 0)) > 0:
        _append_item(
            items,
            id="PROC-CLOSEOUT-002",
            priority="P1",
            trigger=f"partial_or_blocked_count={patterns.get('partial_or_blocked_count', 0)}",
            action=(
                "For each partial/blocked outcome, add explicit remediation note and follow-up task link "
                "to keep lifecycle evidence complete."
            ),
            owner="task-owner",
            due="in current session closeout",
        )

    if int(patterns.get("same_path_retry_count", 0)) > 0:
        _append_item(
            items,
            id="PROC-RETRY-003",
            priority="P2",
            trigger=f"same_path_retry_count={patterns.get('same_path_retry_count', 0)}",
            action=(
                "Apply two-failure stop rule for repeated same-path attempts and require replan note "
                "before the third attempt."
            ),
            owner="loop-owner",
            due="before next repeated attempt",
        )

    if int(patterns.get("missing_improvement_actions_count", 0)) > 0:
        _append_item(
            items,
            id="PROC-TRACE-004",
            priority="P2",
            trigger=(
                f"missing_improvement_actions_count={patterns.get('missing_improvement_actions_count', 0)}"
            ),
            action=(
                "Backfill missing improvement_action fields for non-completed outcomes to preserve "
                "actionability of telemetry and postmortems."
            ),
            owner="process-owner",
            due="before nightly lane",
        )

    repeat_error_rate = float(metrics.get("repeat_error_rate", 0.0))
    if repeat_error_rate > 0.00:
        _append_item(
            items,
            id="PROC-INCIDENT-005",
            priority="P1",
            trigger=(
                f"repeat_error_rate={repeat_error_rate:.2f} "
                f"(target {_metric_target_text('repeat_error_rate')})"
            ),
            action=(
                "Promote repeated incident signatures into memory/incidents with owner and preventive gate."
            ),
            owner="incident-owner",
            due="before next nightly lane",
        )

    environment_blocker_rate = float(metrics.get("environment_blocker_rate", 0.0))
    if environment_blocker_rate > 0.05:
        _append_item(
            items,
            id="PROC-ENV-006",
            priority="P2",
            trigger=(
                f"environment_blocker_rate={environment_blocker_rate:.2f} "
                f"(target {_metric_target_text('environment_blocker_rate')})"
            ),
            action=(
                "Document environment blocker fallback path and ensure lane-level skip policy is explicit."
            ),
            owner="platform-owner",
            due="before next lane execution",
        )

    if not items:
        if not bool(rollup.get("burn_in_complete", False)):
            _append_item(
                items,
                id="PROC-BURNIN-007",
                priority="P3",
                trigger=(
                    "burn_in_incomplete "
                    f"(window_tasks_count={rollup.get('window_tasks_count', 0)}/target={window_size})"
                ),
                action=(
                    "Collect additional completed tasks until burn-in window is full, then recalculate thresholds."
                ),
                owner="process-owner",
                due="next dashboard refresh",
            )
        else:
            _append_item(
                items,
                id="PROC-MAINT-008",
                priority="P3",
                trigger="all signal metrics in acceptable range",
                action="Keep weekly calibration review and preserve current gate thresholds.",
                owner="governance-owner",
                due="weekly",
            )
    return items


def _counter_to_text(counter_payload: dict[str, int]) -> str:
    if not counter_payload:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in counter_payload.items())


def _build_report_payload(
    *,
    payload: dict[str, Any],
    window_size: int,
) -> dict[str, Any]:
    rollup = compute_process_rollup(payload, window_size=window_size)
    window = _window_records(payload, window_size)
    patterns = _build_patterns(window)
    action_items = _build_action_items(rollup=rollup, patterns=patterns, window_size=window_size)
    return {
        "generated_at": date.today().isoformat(),
        "window_size": int(window_size),
        "rollup": rollup,
        "patterns": patterns,
        "action_items": action_items,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    rollup = report.get("rollup", {})
    metrics = rollup.get("current_metrics", {})
    patterns = report.get("patterns", {})
    action_items = report.get("action_items", [])

    lines = [
        "# Process Improvement Report",
        "",
        f"- generated_at: {report.get('generated_at')}",
        f"- completed_tasks_count: {rollup.get('completed_tasks_count', 0)}",
        f"- window_tasks_count: {rollup.get('window_tasks_count', 0)}",
        f"- window_size: {report.get('window_size', 0)}",
        f"- burn_in_complete: {rollup.get('burn_in_complete', False)}",
        "",
        "## Signal Snapshot",
        "",
        "| Metric | Current | Target | Status |",
        "| --- | --- | --- | --- |",
    ]
    for metric_name in (
        "correct_first_time_pct",
        "start_match_pct",
        "context_expansion_rate",
        "repeat_error_rate",
        "environment_blocker_rate",
    ):
        value = float(metrics.get(metric_name, 0.0))
        lines.append(
            "| "
            f"`{metric_name}` | {value:.2f} | {_metric_target_text(metric_name)} | "
            f"{_metric_status(metric_name, value)} |"
        )

    lines.extend(
        [
            "",
            "## Observed Patterns",
            "",
            f"- decision_quality_counts: {_counter_to_text(patterns.get('decision_quality_counts', {}))}",
            f"- outcome_status_counts: {_counter_to_text(patterns.get('outcome_status_counts', {}))}",
            f"- rework_cause_counts: {_counter_to_text(patterns.get('rework_cause_counts', {}))}",
            f"- improvement_action_counts: {_counter_to_text(patterns.get('improvement_action_counts', {}))}",
            f"- repeat_incident_signatures: {_counter_to_text(patterns.get('repeat_incident_signatures', {}))}",
            f"- same_path_retry_count: {patterns.get('same_path_retry_count', 0)}",
            "",
            "## Action Items",
            "",
            "| ID | Priority | Trigger | Action | Owner | Due |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in action_items:
        lines.append(
            "| "
            f"{item.get('id', '')} | {item.get('priority', '')} | {item.get('trigger', '')} | "
            f"{item.get('action', '')} | {item.get('owner', '')} | {item.get('due', '')} |"
        )
    lines.append("")
    return "\n".join(lines)


def run(
    *,
    task_outcomes_path: Path,
    output: Path,
    summary_file: Path | None,
    output_format: str,
    window_size: int,
) -> int:
    payload = load_task_outcomes(task_outcomes_path)
    report_payload = _build_report_payload(payload=payload, window_size=window_size)
    if output_format == "json":
        rendered = json.dumps(report_payload, ensure_ascii=False, indent=2) + "\n"
    else:
        rendered = _render_markdown(report_payload)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(f"process improvement report written: {output.as_posix()}")

    if summary_file is not None:
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        with summary_file.open("a", encoding="utf-8") as handle:
            handle.write(
                "Process improvement report written "
                f"(format={output_format}, action_items={len(report_payload.get('action_items', []))}).\n"
            )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Build process-improvement report from task outcomes.")
    parser.add_argument("--task-outcomes-path", default="memory/task_outcomes.yaml")
    parser.add_argument("--output", default="artifacts/process-improvement-report.md")
    parser.add_argument("--summary-file", default=None)
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--window-size", type=int, default=20)
    args = parser.parse_args()
    summary_path = Path(args.summary_file) if args.summary_file else None
    sys.exit(
        run(
            task_outcomes_path=Path(args.task_outcomes_path),
            output=Path(args.output),
            summary_file=summary_path,
            output_format=args.format,
            window_size=args.window_size,
        )
    )


if __name__ == "__main__":
    main()
