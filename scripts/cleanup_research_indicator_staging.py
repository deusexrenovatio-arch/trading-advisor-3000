from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Iterable

ALLOWED_DELTA_TAILS = frozenset(
    {
        "research_indicator_frames.delta",
        "cf_indicator_input_frame.delta",
        "continuous_front_indicator_frames.delta",
        "indicator_roll_rules.delta",
        "continuous_front_indicator_qc_observations.delta",
        "continuous_front_indicator_run_manifest.delta",
        "continuous_front_indicator_acceptance_report.delta",
    }
)
ALLOWED_FILE_SUFFIXES = (
    ".status.json",
    ".manifest.json",
    ".lock.json",
)
PROTECTED_PARTS = frozenset({"current", "canonical", "raw", "baseline"})
REQUIRED_ROOT_PARTS = frozenset({"staging", "verification"})


def _normalized_parts(path: Path) -> tuple[str, ...]:
    return tuple(part.lower() for part in path.parts)


def _is_under_staging_or_verification(path: Path) -> bool:
    return bool(REQUIRED_ROOT_PARTS & set(_normalized_parts(path)))


def _is_protected(path: Path) -> bool:
    parts = set(_normalized_parts(path))
    return bool(PROTECTED_PARTS & parts)


def _is_allowed_tail(path: Path) -> bool:
    name = path.name
    if name in ALLOWED_DELTA_TAILS and path.is_dir():
        return True
    return path.is_file() and name.endswith(ALLOWED_FILE_SUFFIXES)


def _iter_cleanup_candidates(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if _is_allowed_tail(path):
            yield path


def build_cleanup_inventory(root: Path) -> list[dict[str, str]]:
    resolved_root = root.resolve()
    if not resolved_root.exists():
        return []
    if not _is_under_staging_or_verification(resolved_root):
        raise ValueError(
            f"cleanup root must be under a staging or verification path: {resolved_root.as_posix()}"
        )
    if _is_protected(resolved_root):
        raise ValueError(f"cleanup root is protected: {resolved_root.as_posix()}")

    inventory: list[dict[str, str]] = []
    for candidate in sorted(_iter_cleanup_candidates(resolved_root)):
        resolved_candidate = candidate.resolve()
        if resolved_root not in (resolved_candidate, *resolved_candidate.parents):
            continue
        if _is_protected(resolved_candidate):
            continue
        inventory.append(
            {
                "path": resolved_candidate.as_posix(),
                "relative_path": resolved_candidate.relative_to(resolved_root).as_posix(),
                "kind": "directory" if resolved_candidate.is_dir() else "file",
                "reason": "phase4_research_indicator_staging_tail",
            }
        )
    return inventory


def _cleanup_item_path(root: Path, item: dict[str, str]) -> Path:
    resolved_root = root.resolve()
    raw_path = Path(item["path"])
    candidate = raw_path if raw_path.is_absolute() else resolved_root / raw_path

    if candidate.is_symlink():
        resolved_parent = candidate.parent.resolve()
        if not resolved_parent.is_relative_to(resolved_root):
            raise ValueError(f"cleanup inventory path is outside root: {candidate.as_posix()}")
        if candidate.resolve() == resolved_root:
            raise ValueError(f"cleanup inventory path is root: {candidate.as_posix()}")
        return candidate

    resolved_candidate = candidate.resolve()
    if not resolved_candidate.is_relative_to(resolved_root):
        raise ValueError(f"cleanup inventory path is outside root: {resolved_candidate.as_posix()}")
    if resolved_candidate == resolved_root:
        raise ValueError(f"cleanup inventory path is root: {resolved_candidate.as_posix()}")
    if _is_protected(resolved_candidate):
        raise ValueError(f"cleanup inventory path is protected: {resolved_candidate.as_posix()}")
    return resolved_candidate


def delete_cleanup_inventory(inventory: Iterable[dict[str, str]], *, root: Path) -> int:
    deleted = 0
    for item in inventory:
        path = _cleanup_item_path(root, item)
        if not path.exists():
            continue
        if path.is_symlink():
            path.unlink()
            deleted += 1
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        deleted += 1
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inventory or delete allowlisted research indicator staging tails."
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    inventory = build_cleanup_inventory(args.root)
    deleted = delete_cleanup_inventory(inventory, root=args.root) if args.apply else 0
    print(
        json.dumps(
            {
                "root": args.root.resolve().as_posix(),
                "apply": bool(args.apply),
                "candidate_count": len(inventory),
                "deleted_count": deleted,
                "inventory": inventory,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
