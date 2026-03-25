from __future__ import annotations

import argparse
import sys
from pathlib import Path

from critical_contours import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_SESSION_HANDOFF_PATH,
    REQUIRED_INTENT_FIELDS,
    SOLUTION_CLASSES,
    extract_solution_intent,
    load_critical_contours,
    match_critical_contours,
    normalize_text,
    read_task_note,
)
from gate_common import collect_changed_files


PLACEHOLDER_CLOSURE_VALUES = {
    "",
    "none",
    "pending",
    "tbd",
    "<what proves closure>",
}


def run(
    path: Path,
    *,
    config_path: Path = DEFAULT_CONFIG_PATH,
    base_sha: str | None = None,
    head_sha: str | None = None,
    git_ref: str | None = None,
    from_git: bool = False,
    changed_files_override: list[str] | None = None,
) -> int:
    changed_files = collect_changed_files(
        base_ref=base_sha,
        head_ref=head_sha,
        git_ref=git_ref,
        from_git=from_git,
        changed_files=list(changed_files_override or []),
        from_stdin=False,
    )

    try:
        contours = load_critical_contours(config_path)
    except ValueError as exc:
        print(f"solution intent validation failed: {exc}")
        return 1

    matched = match_critical_contours(changed_files, contours)
    if not matched:
        print(
            "solution intent validation: OK "
            f"(critical_contours=0 changed_files={len(changed_files)})"
        )
        return 0
    if len(matched) > 1:
        contour_ids = ", ".join(contour.contour_id for contour in matched)
        print("solution intent validation failed:")
        print(f"- multiple critical contours triggered in one patch: {contour_ids}")
        print("- remediation: split the patch or reduce the change surface to one contour")
        return 1

    contour = matched[0]
    if not path.exists():
        print(f"solution intent validation failed: missing {path.as_posix()}")
        return 1
    note_path, lines, pointer_mode = read_task_note(path)
    fields = extract_solution_intent(lines)

    errors: list[str] = []
    for field_name in REQUIRED_INTENT_FIELDS:
        raw_value = fields.get(field_name, "")
        if field_name not in fields or not normalize_text(raw_value):
            errors.append(f"missing Solution Intent field: {field_name.replace('_', ' ')}")

    solution_class = normalize_text(fields.get("solution_class", ""))
    critical_contour = normalize_text(fields.get("critical_contour", ""))
    closure_evidence = normalize_text(fields.get("closure_evidence", ""))
    shortcut_waiver = normalize_text(fields.get("shortcut_waiver", ""))

    if solution_class and solution_class not in SOLUTION_CLASSES:
        errors.append(f"invalid Solution Class: {fields.get('solution_class', '').strip()}")
    if critical_contour and critical_contour != contour.contour_id:
        errors.append(
            f"Critical Contour mismatch: declared `{fields.get('critical_contour', '').strip()}` "
            f"but diff matches `{contour.contour_id}`"
        )
    if solution_class == "target" and closure_evidence in PLACEHOLDER_CLOSURE_VALUES:
        errors.append("target Solution Class requires explicit Closure Evidence")
    if solution_class == "fallback" and shortcut_waiver in {"", "none"}:
        errors.append("fallback Solution Class requires a non-empty Shortcut Waiver")

    if errors:
        print("solution intent validation failed:")
        for item in errors:
            print(f"- {item}")
        print(
            "remediation: add `## Solution Intent` to the active task note before coding "
            "for critical contour work"
        )
        return 1

    print(
        "solution intent validation: OK "
        f"(contour={contour.contour_id} solution_class={solution_class} "
        f"source={note_path.as_posix()} pointer_mode={pointer_mode})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Solution Intent for critical contour tasks.")
    parser.add_argument("--path", default=str(DEFAULT_SESSION_HANDOFF_PATH))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--head-sha", default=None)
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    args = parser.parse_args()

    changed_files_override: list[str] | None = None
    if args.base_sha and args.head_sha:
        changed_files_override = None
    elif args.stdin or args.changed_files:
        stdin_items = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()] if args.stdin else []
        changed_files_override = [*list(args.changed_files), *stdin_items]

    sys.exit(
        run(
            Path(args.path),
            config_path=Path(args.config),
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            git_ref=args.git_ref,
            from_git=bool(args.from_git),
            changed_files_override=changed_files_override,
        )
    )


if __name__ == "__main__":
    main()
