#!/usr/bin/env python3
"""Build and validate truth recomposition reports for stacked follow-up continuation."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class TruthRecompositionError(RuntimeError):
    """Raised when recomposition helper or validator input is invalid."""


@dataclass(frozen=True)
class FollowupSurfaceContract:
    allowed_to_carry_forward: list[str]
    temporary_downgrade_surfaces: list[str]
    predecessor_merged: bool


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_list(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for raw in values:
        item = str(raw).strip()
        if item and item not in normalized:
            normalized.append(item)
    return normalized


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise TruthRecompositionError(f"missing file: {path.as_posix()}") from exc
    except json.JSONDecodeError as exc:
        raise TruthRecompositionError(f"invalid json in {path.as_posix()}: {exc}") from exc
    if not isinstance(payload, dict):
        raise TruthRecompositionError(f"json root must be an object: {path.as_posix()}")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_followup_contract(path: Path) -> FollowupSurfaceContract:
    payload = read_json(path)
    surface_contract = payload.get("surface_contract")
    if not isinstance(surface_contract, dict):
        raise TruthRecompositionError(
            "follow-up contract is missing `surface_contract`; expected stacked-followup contract payload"
        )
    predecessor_merge_context = payload.get("predecessor_merge_context")
    if not isinstance(predecessor_merge_context, dict):
        raise TruthRecompositionError(
            "follow-up contract is missing `predecessor_merge_context`; merged predecessor binding is required"
        )
    allowed = normalize_list(surface_contract.get("allowed_to_carry_forward", []))
    temporary = normalize_list(surface_contract.get("temporary_downgrade_surfaces", []))
    predecessor_merged = bool(predecessor_merge_context.get("merged_into_new_base", False))
    if not allowed:
        raise TruthRecompositionError(
            "follow-up contract has no allowed carry-forward surfaces; cannot compute recomposition delta"
        )
    return FollowupSurfaceContract(
        allowed_to_carry_forward=allowed,
        temporary_downgrade_surfaces=temporary,
        predecessor_merged=predecessor_merged,
    )


def build_report(
    *,
    followup_contract: Path,
    merged_surfaces: list[str],
    candidate_surfaces: list[str],
    output_path: Path,
) -> dict[str, Any]:
    contract = parse_followup_contract(followup_contract)
    merged = normalize_list(merged_surfaces)
    candidate = normalize_list(candidate_surfaces)
    if not merged:
        raise TruthRecompositionError("recomposition build requires at least one --merged-surface")
    if not candidate:
        raise TruthRecompositionError("recomposition build requires at least one --candidate-surface")

    merged_set = set(merged)
    candidate_set = set(candidate)
    allowed_set = set(contract.allowed_to_carry_forward)
    temporary_set = set(contract.temporary_downgrade_surfaces)

    remaining_deltas = sorted(item for item in candidate_set if item not in merged_set)
    restored_truth_surfaces = sorted(item for item in temporary_set if item in merged_set)
    lingering_temporary_downgrades = sorted(item for item in remaining_deltas if item in temporary_set)
    out_of_contract_surfaces = sorted(item for item in remaining_deltas if item not in allowed_set)
    status = (
        "ready"
        if contract.predecessor_merged and not lingering_temporary_downgrades and not out_of_contract_surfaces
        else "blocked"
    )

    payload = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "route_signal": "recomposition:report",
        "status": status,
        "inputs": {
            "followup_contract": followup_contract.as_posix(),
            "merged_surfaces": merged,
            "candidate_surfaces": candidate,
        },
        "contract": {
            "allowed_to_carry_forward": contract.allowed_to_carry_forward,
            "temporary_downgrade_surfaces": contract.temporary_downgrade_surfaces,
            "predecessor_merged": contract.predecessor_merged,
        },
        "analysis": {
            "restored_truth_surfaces": restored_truth_surfaces,
            "remaining_deltas": remaining_deltas,
            "lingering_temporary_downgrades": lingering_temporary_downgrades,
            "out_of_contract_surfaces": out_of_contract_surfaces,
        },
    }
    write_json(output_path, payload)
    return payload


def validate_report(report_path: Path) -> tuple[bool, list[str]]:
    payload = read_json(report_path)
    errors: list[str] = []
    status = str(payload.get("status", "")).strip().lower()
    if status != "ready":
        errors.append("recomposition report status is not `ready`")

    contract = payload.get("contract")
    if not isinstance(contract, dict) or not bool(contract.get("predecessor_merged", False)):
        errors.append("recomposition report does not confirm merged predecessor context")

    analysis = payload.get("analysis")
    if not isinstance(analysis, dict):
        errors.append("recomposition report missing `analysis` section")
        return False, errors

    lingering = normalize_list(analysis.get("lingering_temporary_downgrades", []))
    if lingering:
        errors.append(
            "temporary downgrade surfaces still remain in recomposed delta: " + ", ".join(lingering)
        )

    out_of_contract = normalize_list(analysis.get("out_of_contract_surfaces", []))
    if out_of_contract:
        errors.append("remaining deltas include out-of-contract surfaces: " + ", ".join(out_of_contract))

    return (not errors), errors


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Truth recomposition helper and validator.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="Build recomposition report from stacked-followup contract and surfaces.")
    build.add_argument("--followup-contract", required=True)
    build.add_argument("--merged-surface", action="append", default=[])
    build.add_argument("--candidate-surface", action="append", default=[])
    build.add_argument("--output", required=True)

    validate = subparsers.add_parser("validate", help="Validate recomposition report fail-closed.")
    validate.add_argument("--report", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv or sys.argv[1:])
    repo_root = resolve_repo_root()

    try:
        if args.command == "build":
            followup_contract = resolve_path(repo_root, args.followup_contract)
            output_path = resolve_path(repo_root, args.output)
            payload = build_report(
                followup_contract=followup_contract,
                merged_surfaces=list(args.merged_surface or []),
                candidate_surfaces=list(args.candidate_surface or []),
                output_path=output_path,
            )
            print(f"status: {payload['status']}")
            print(f"report: {output_path.as_posix()}")
            return 0

        report_path = resolve_path(repo_root, args.report)
        ok, errors = validate_report(report_path)
        if ok:
            print("validation_status: PASS")
            print(f"report: {report_path.as_posix()}")
            return 0
        print("validation_status: BLOCKED", file=sys.stderr)
        for item in errors:
            print(f"- {item}", file=sys.stderr)
        return 2
    except TruthRecompositionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
