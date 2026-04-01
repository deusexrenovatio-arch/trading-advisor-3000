#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ALLOW_VERDICT = "ALLOW_RELEASE_READINESS"
DENY_VERDICT = "DENY_RELEASE_READINESS"
ROUTE_SIGNAL = "release-decision:governed-h4"
ACCEPTANCE_ROUTE_PREFIX = "acceptance:"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def _section_lines(path: Path, heading: str) -> list[str]:
    lines = _read_lines(path)
    start = -1
    for idx, raw in enumerate(lines):
        if raw.strip() == heading:
            start = idx
            break
    if start < 0:
        raise ValueError(f"{path.as_posix()}: missing heading `{heading}`")
    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end = idx
            break
    return lines[start + 1 : end]


def _parse_bullet_map(lines: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in lines:
        stripped = raw.strip()
        if not stripped.startswith("- ") or ":" not in stripped:
            continue
        key, value = stripped[2:].split(":", 1)
        result[key.strip()] = value.strip()
    return result


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"missing required json artifact: {path.as_posix()}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json artifact: {path.as_posix()} ({exc})") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"json artifact root must be object: {path.as_posix()}")
    return payload


def _parse_summary_markers(path: Path) -> tuple[str, str]:
    text = path.read_text(encoding="utf-8")
    snapshot_match = re.search(r"- Snapshot mode:\s*`([^`]+)`", text)
    profile_match = re.search(r"- Profile:\s*`([^`]+)`", text)
    if not snapshot_match or not profile_match:
        raise ValueError(f"{path.as_posix()}: summary is missing snapshot/profile markers")
    snapshot_mode = snapshot_match.group(1).strip().lower()
    profile = profile_match.group(1).strip()
    if not snapshot_mode:
        raise ValueError(f"{path.as_posix()}: summary snapshot marker is empty")
    if not profile:
        raise ValueError(f"{path.as_posix()}: summary profile marker is empty")
    return snapshot_mode, profile


def _load_mutation_events(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as exc:
        raise ValueError(f"missing mutation events artifact: {path.as_posix()}") from exc
    events: list[dict[str, Any]] = []
    for raw in lines:
        if not raw.strip():
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path.as_posix()}: invalid jsonl event ({exc})") from exc
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _decision_value(raw: str) -> str:
    text = str(raw or "").strip().lower()
    if text == "allow":
        return ALLOW_VERDICT
    if text == "deny":
        return DENY_VERDICT
    raise ValueError("decision must be one of auto|allow|deny")


def _load_acceptance_evidence(path: Path) -> tuple[str, str]:
    payload = _load_json(path)
    route_signal = str(payload.get("route_signal", "")).strip()
    verdict = str(payload.get("verdict", "")).strip().upper()
    if not route_signal.startswith(ACCEPTANCE_ROUTE_PREFIX):
        raise ValueError(
            f"{path.as_posix()}: acceptance route signal must start with `{ACCEPTANCE_ROUTE_PREFIX}`, "
            f"got `{route_signal or 'missing'}`."
        )
    if verdict not in {"PASS", "BLOCKED"}:
        raise ValueError(
            f"{path.as_posix()}: acceptance verdict must be PASS or BLOCKED, got `{verdict or 'missing'}`."
        )
    return route_signal, verdict


def _contract_truth(execution_contract_path: Path, phase_brief_path: Path) -> tuple[str, str, str]:
    target = _parse_bullet_map(_section_lines(execution_contract_path, "## Release Target Contract"))
    target_decision = target.get("Target Decision", "").strip().upper()
    if target_decision not in {ALLOW_VERDICT, DENY_VERDICT}:
        raise ValueError(
            f"{execution_contract_path.as_posix()}: unsupported or missing `Target Decision` "
            f"(got `{target_decision or 'missing'}`)."
        )

    gate = _parse_bullet_map(_section_lines(phase_brief_path, "## Release Gate Impact"))
    min_proof = gate.get("Minimum Proof Class", "").strip().lower()
    accepted_state = gate.get("Accepted State Label", "").strip().lower()
    ownership = _parse_bullet_map(_section_lines(phase_brief_path, "## Release Surface Ownership"))
    delivered_proof = ownership.get("Delivered Proof Class", "").strip().lower()

    if accepted_state != "release_decision":
        raise ValueError(
            f"{phase_brief_path.as_posix()}: accepted state must be `release_decision`, got `{accepted_state}`."
        )
    if min_proof != "live-real":
        raise ValueError(
            f"{phase_brief_path.as_posix()}: minimum proof class must be `live-real`, got `{min_proof}`."
        )
    if delivered_proof != "live-real":
        raise ValueError(
            f"{phase_brief_path.as_posix()}: delivered proof class must be `live-real`, got `{delivered_proof}`."
        )
    return target_decision, min_proof, delivered_proof


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit governed release decision package for H4 enforcement.")
    parser.add_argument("--execution-contract", required=True)
    parser.add_argument("--phase-brief", required=True)
    parser.add_argument("--acceptance-json", required=True)
    parser.add_argument("--route-state", required=True)
    parser.add_argument("--loop-summary", required=True)
    parser.add_argument("--pr-summary", required=True)
    parser.add_argument("--mutation-events", required=True)
    parser.add_argument("--artifact-path", action="append", default=[])
    parser.add_argument("--decision", choices=("auto", "allow", "deny"), default="auto")
    parser.add_argument("--reason", action="append", default=[])
    parser.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv or sys.argv[1:])

    execution_contract_path = Path(args.execution_contract).resolve()
    phase_brief_path = Path(args.phase_brief).resolve()
    acceptance_json_path = Path(args.acceptance_json).resolve()
    route_state_path = Path(args.route_state).resolve()
    loop_summary_path = Path(args.loop_summary).resolve()
    pr_summary_path = Path(args.pr_summary).resolve()
    mutation_events_path = Path(args.mutation_events).resolve()
    output_path = Path(args.output).resolve()
    artifact_paths = [Path(item).resolve() for item in list(args.artifact_path)]

    try:
        target_decision, minimum_proof_class, delivered_proof_class = _contract_truth(
            execution_contract_path,
            phase_brief_path,
        )
        acceptance_route_signal, acceptance_verdict = _load_acceptance_evidence(acceptance_json_path)
        route_state = _load_json(route_state_path)
        route_snapshot = str(route_state.get("snapshot_mode", "")).strip().lower()
        route_profile = str(route_state.get("profile", "")).strip()
        if not route_snapshot or not route_profile:
            raise ValueError(
                f"{route_state_path.as_posix()}: route-state must include explicit snapshot_mode and profile markers."
            )
        loop_snapshot, loop_profile = _parse_summary_markers(loop_summary_path)
        pr_snapshot, pr_profile = _parse_summary_markers(pr_summary_path)
        mutation_events = _load_mutation_events(mutation_events_path)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    blockers: list[str] = []
    if route_snapshot != loop_snapshot or route_snapshot != pr_snapshot:
        blockers.append(
            "snapshot marker mismatch across route-state/loop/pr evidence"
        )
    if route_profile != loop_profile or route_profile != pr_profile:
        blockers.append(
            "profile marker mismatch across route-state/loop/pr evidence"
        )
    if acceptance_verdict != "PASS":
        blockers.append("acceptance verdict is BLOCKED; release readiness must remain DENY")

    acquired_count = sum(1 for event in mutation_events if str(event.get("event", "")).strip() == "acquired")
    released_count = sum(1 for event in mutation_events if str(event.get("event", "")).strip() == "released")
    if acquired_count == 0 or released_count == 0:
        blockers.append("mutation lock evidence is incomplete (expected acquired + released events)")

    missing_artifacts = [path.as_posix() for path in artifact_paths if not path.exists()]
    if missing_artifacts:
        blockers.append("artifact bundle contains missing paths: " + ", ".join(missing_artifacts))

    if args.decision == "deny":
        verdict = DENY_VERDICT
    elif args.decision == "allow":
        verdict = ALLOW_VERDICT if not blockers else DENY_VERDICT
    else:
        verdict = ALLOW_VERDICT if not blockers else DENY_VERDICT

    payload = {
        "version": 1,
        "generated_at": _utc_now(),
        "route_signal": ROUTE_SIGNAL,
        "decision_mode": args.decision,
        "target_decision": target_decision,
        "verdict": verdict,
        "proof_class": delivered_proof_class,
        "minimum_proof_class": minimum_proof_class,
        "truth_sources": {
            "execution_contract": execution_contract_path.as_posix(),
            "phase_brief": phase_brief_path.as_posix(),
            "acceptance_json": acceptance_json_path.as_posix(),
            "route_state": route_state_path.as_posix(),
            "loop_summary": loop_summary_path.as_posix(),
            "pr_summary": pr_summary_path.as_posix(),
            "mutation_events": mutation_events_path.as_posix(),
        },
        "acceptance_binding": {
            "route_signal": acceptance_route_signal,
            "verdict": acceptance_verdict,
        },
        "marker_bindings": {
            "route_state": {"snapshot_mode": route_snapshot, "profile": route_profile},
            "loop_gate": {"snapshot_mode": loop_snapshot, "profile": loop_profile},
            "pr_gate": {"snapshot_mode": pr_snapshot, "profile": pr_profile},
            "consistent": not blockers
            or not any("marker mismatch" in blocker for blocker in blockers),
        },
        "mutation_lock_evidence": {
            "events_path": mutation_events_path.as_posix(),
            "acquired_events": acquired_count,
            "released_events": released_count,
        },
        "artifact_bundle": [path.as_posix() for path in artifact_paths],
        "blockers": blockers,
        "reasons": [str(item).strip() for item in list(args.reason) if str(item).strip()],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"release decision: {verdict} (blockers={len(blockers)})")
    print(f"output: {output_path.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
