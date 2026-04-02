from __future__ import annotations

import argparse
import sys
from pathlib import Path

from critical_contours import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_SESSION_HANDOFF_PATH,
    extract_solution_intent,
    load_critical_contours,
    match_critical_contours,
    normalize_text,
    read_task_note,
    split_csv_field,
)
from gate_common import collect_changed_files


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    normalized = normalize_text(text)
    return any(marker in normalized for marker in markers)


def _multi_contour_declaration_ok(declared: str, contour_ids: list[str]) -> bool:
    normalized = normalize_text(declared)
    if not normalized:
        return False
    expected = ",".join(sorted(contour_ids))
    allowed = {
        "multi",
        "multi-contour",
        "multiple",
        expected,
        f"multi:{expected}",
    }
    return normalized in allowed


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
        print(f"critical contour closure validation failed: {exc}")
        return 1

    matched = match_critical_contours(changed_files, contours)
    if not matched:
        print(
            "critical contour closure validation: OK "
            f"(critical_contours=0 changed_files={len(changed_files)})"
        )
        return 0
    if not path.exists():
        print(f"critical contour closure validation failed: missing {path.as_posix()}")
        return 1
    note_path, lines, pointer_mode = read_task_note(path)
    fields = extract_solution_intent(lines)
    contour_ids = [contour.contour_id for contour in matched]
    multi_contour = len(matched) > 1
    contour_label = ", ".join(contour_ids)
    contour = matched[0]

    if multi_contour and not _multi_contour_declaration_ok(fields.get("critical_contour", ""), contour_ids):
        print("critical contour closure validation failed:")
        print(f"- multiple critical contours triggered in one patch: {contour_label}")
        print(
            "- remediation: declare `Critical Contour: multi-contour` (or an explicit sorted contour id list) "
            "in the Solution Intent block"
        )
        return 1

    if multi_contour:
        all_required_markers = tuple(
            marker for item in matched for marker in item.required_evidence_markers
        )
        all_forbidden_markers = tuple(
            marker for item in matched for marker in item.forbidden_shortcut_markers
        )
        all_staged_markers = tuple(
            marker for item in matched for marker in item.allowed_staged_markers
        )
    else:
        all_required_markers = contour.required_evidence_markers
        all_forbidden_markers = contour.forbidden_shortcut_markers
        all_staged_markers = contour.allowed_staged_markers

    solution_class = normalize_text(fields.get("solution_class", ""))
    closure_evidence = normalize_text(fields.get("closure_evidence", ""))
    shortcut_waiver = normalize_text(fields.get("shortcut_waiver", ""))
    declared_shortcuts = split_csv_field(fields.get("forbidden_shortcuts", ""))
    note_text = normalize_text("\n".join(lines))

    errors: list[str] = []
    required_fields = ("solution_class", "critical_contour", "forbidden_shortcuts", "closure_evidence", "shortcut_waiver")
    for field_name in required_fields:
        raw_value = fields.get(field_name, "")
        if field_name not in fields or not normalize_text(raw_value):
            errors.append(
                "Solution Intent is incomplete; run solution intent remediation first"
            )
            break

    if solution_class == "target":
        if multi_contour:
            missing_required = [
                item.contour_id
                for item in matched
                if not _contains_any(closure_evidence, item.required_evidence_markers)
            ]
            if missing_required:
                errors.append(
                    "target claim does not cite required contour evidence for: "
                    + ", ".join(missing_required)
                )
        elif not _contains_any(closure_evidence, all_required_markers):
            errors.append(
                f"target claim for `{contour_label}` does not cite required contour evidence"
            )
        if shortcut_waiver not in {"", "none"}:
            errors.append("target claim cannot carry a Shortcut Waiver")
    elif solution_class == "staged":
        if multi_contour:
            missing_staged_markers = [
                item.contour_id
                for item in matched
                if not _contains_any(note_text, item.allowed_staged_markers)
            ]
            if missing_staged_markers:
                errors.append(
                    "staged claim must use explicit staged wording for: "
                    + ", ".join(missing_staged_markers)
                )
            missing_required = [
                item.contour_id
                for item in matched
                if not _contains_any(closure_evidence, item.required_evidence_markers)
            ]
            if missing_required:
                errors.append(
                    "staged claim still needs contour evidence markers for: "
                    + ", ".join(missing_required)
                )
        else:
            if not _contains_any(note_text, all_staged_markers):
                errors.append(
                    f"staged claim for `{contour_label}` must use explicit staged wording"
                )
            if not _contains_any(closure_evidence, all_required_markers):
                errors.append(
                    f"staged claim for `{contour_label}` still needs contour evidence markers"
                )
    elif solution_class == "fallback":
        if shortcut_waiver in {"", "none"}:
            errors.append("fallback claim requires a non-empty Shortcut Waiver")

    if _contains_any(closure_evidence, all_forbidden_markers):
        errors.append(
            f"closure evidence for `{contour_label}` contains a forbidden shortcut marker"
        )

    if declared_shortcuts and declared_shortcuts != ["none"]:
        unknown_shortcuts = [
            item
            for item in declared_shortcuts
            if not any(item in marker or marker in item for marker in all_forbidden_markers)
        ]
        if unknown_shortcuts:
            errors.append(
                "Forbidden Shortcuts lists values outside the configured contour markers: "
                + ", ".join(unknown_shortcuts)
            )

    if errors:
        print("critical contour closure validation failed:")
        for item in errors:
            print(f"- {item}")
        print(
            "remediation: replace scaffold/sample/synthetic closure claims with contour-specific evidence "
            "or downgrade the declared solution class explicitly"
        )
        return 1

    print(
        "critical contour closure validation: OK "
        f"(contour={contour_label} solution_class={solution_class} "
        f"source={note_path.as_posix()} pointer_mode={pointer_mode})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate closure evidence for critical contour tasks.")
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
