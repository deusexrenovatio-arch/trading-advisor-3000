from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


ROLLING_WINDOW_SIZE = 20


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


def load_task_outcomes(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": None, "items": []}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {"version": 1, "updated_at": None, "items": []}
    items = payload.get("items")
    if not isinstance(items, list):
        payload["items"] = []
    payload.setdefault("version", 1)
    payload.setdefault("updated_at", None)
    return payload


def _normalize_record(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": str(raw.get("task_id", "")).strip(),
        "closed_at": raw.get("closed_at"),
        "route_match": str(raw.get("route_match", "pending")).strip().lower() or "pending",
        "decision_quality": str(raw.get("decision_quality", "pending")).strip().lower() or "pending",
        "incident_signature": str(raw.get("incident_signature", "none")).strip() or "none",
        "same_path_attempts": int(raw.get("same_path_attempts", 1) or 1),
        "outcome_status": str(raw.get("outcome_status", "in_progress")).strip().lower() or "in_progress",
    }


def completed_task_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items", [])
    records: list[dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        record = _normalize_record(raw)
        if record["outcome_status"] not in {"completed", "partial", "blocked"}:
            continue
        if not record.get("closed_at"):
            continue
        records.append(record)
    records.sort(
        key=lambda item: _parse_iso_datetime(str(item.get("closed_at", "")))
        or datetime.min.replace(tzinfo=timezone.utc)
    )
    return records


def compute_process_rollup(
    payload: dict[str, Any],
    window_size: int = ROLLING_WINDOW_SIZE,
    burn_in_min_completed_tasks: int | None = None,
) -> dict[str, Any]:
    records = completed_task_records(payload)
    window = records[-window_size:] if window_size > 0 else records
    signatures_seen: Counter[str] = Counter()
    repeat_errors = 0
    for record in window:
        signature = record.get("incident_signature", "")
        if signature and signature != "none":
            signatures_seen[signature] += 1
            if signatures_seen[signature] > 1:
                repeat_errors += 1

    total = len(window)
    matched = sum(1 for row in window if row["route_match"] == "matched")
    expanded = sum(1 for row in window if row["route_match"] == "expanded")
    correct_first_time = sum(1 for row in window if row["decision_quality"] == "correct_first_time")
    env_blocked = sum(1 for row in window if row["decision_quality"] == "environment_blocked")

    metrics = {
        "correct_first_time_pct": _safe_ratio(correct_first_time, total),
        "start_match_pct": _safe_ratio(matched, total),
        "context_expansion_rate": _safe_ratio(expanded, total),
        "repeat_error_rate": _safe_ratio(repeat_errors, total),
        "environment_blocker_rate": _safe_ratio(env_blocked, total),
    }
    burn_in_target = max(window_size, burn_in_min_completed_tasks or window_size, 1)
    return {
        "completed_tasks_count": len(records),
        "window_size": window_size,
        "burn_in_min_completed_tasks": burn_in_target,
        "window_tasks_count": total,
        "burn_in_complete": len(records) >= burn_in_target,
        "current_metrics": metrics,
    }


def render_rollup_markdown(rollup: dict[str, Any]) -> str:
    metrics = rollup.get("current_metrics", {})
    lines = [
        "# Process Rollup",
        "",
        f"- completed_tasks_count: {rollup.get('completed_tasks_count', 0)}",
        f"- window_tasks_count: {rollup.get('window_tasks_count', 0)}",
        f"- burn_in_min_completed_tasks: {rollup.get('burn_in_min_completed_tasks', 0)}",
        f"- burn_in_complete: {rollup.get('burn_in_complete', False)}",
        "",
        "| Metric | Value |",
        "| --- | --- |",
    ]
    for key in (
        "correct_first_time_pct",
        "start_match_pct",
        "context_expansion_rate",
        "repeat_error_rate",
        "environment_blocker_rate",
    ):
        value = float(metrics.get(key, 0.0))
        lines.append(f"| `{key}` | {value:.2f} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Roll up process telemetry from memory/task_outcomes.yaml.")
    parser.add_argument("--task-outcomes-path", default="memory/task_outcomes.yaml")
    parser.add_argument("--window-size", type=int, default=ROLLING_WINDOW_SIZE)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()

    payload = load_task_outcomes(Path(args.task_outcomes_path))
    rollup = compute_process_rollup(payload, window_size=args.window_size)
    if args.format == "markdown":
        print(render_rollup_markdown(rollup).rstrip())
    else:
        print(json.dumps(rollup, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
