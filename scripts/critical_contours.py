from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from handoff_resolver import read_task_note_lines


DEFAULT_CONFIG_PATH = Path("configs/critical_contours.yaml")
DEFAULT_SESSION_HANDOFF_PATH = Path("docs/session_handoff.md")
SOLUTION_INTENT_HEADING = "## Solution Intent"
SOLUTION_CLASSES = {"target", "staged", "fallback"}
REQUIRED_INTENT_FIELDS = (
    "solution_class",
    "critical_contour",
    "forbidden_shortcuts",
    "closure_evidence",
    "shortcut_waiver",
)
SPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
NON_ID_RE = re.compile(r"[^a-z0-9-]+")


@dataclass(frozen=True)
class CriticalContour:
    contour_id: str
    summary: str
    trigger_paths: tuple[str, ...]
    trigger_patterns: tuple[str, ...]
    default_solution_class: str
    forbidden_shortcut_markers: tuple[str, ...]
    required_evidence_markers: tuple[str, ...]
    allowed_staged_markers: tuple[str, ...]
    reacceptance_trigger_markers: tuple[str, ...]


def normalize_path(path_text: str) -> str:
    return path_text.replace("\\", "/").strip().lower()


def normalize_text(value: str) -> str:
    return SPACE_RE.sub(" ", str(value).strip().lower())


def normalize_key(value: str) -> str:
    return NON_ALNUM_RE.sub("_", str(value).strip().lower()).strip("_")


def normalize_identifier(value: str) -> str:
    normalized = str(value).strip().lower().replace("_", "-")
    normalized = NON_ID_RE.sub("-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized.strip("-")


def _normalize_list(raw: Any, *, mode: str) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise ValueError(f"expected list for {mode}")
    values: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = normalize_path(str(item)) if mode == "path" else normalize_text(str(item))
        if not text or text in seen:
            continue
        seen.add(text)
        values.append(text)
    return tuple(values)


def load_critical_contours(config_path: Path = DEFAULT_CONFIG_PATH) -> list[CriticalContour]:
    if not config_path.exists():
        raise ValueError(f"missing critical contour config: {config_path.as_posix()}")

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("critical contour config must be a YAML object")

    raw_contours = payload.get("contours")
    if not isinstance(raw_contours, list) or not raw_contours:
        raise ValueError("critical contour config must contain a non-empty `contours` list")

    contours: list[CriticalContour] = []
    seen_ids: set[str] = set()
    for index, raw_item in enumerate(raw_contours, start=1):
        if not isinstance(raw_item, dict):
            raise ValueError(f"contours[{index}] must be an object")

        contour_id = normalize_identifier(str(raw_item.get("id", "")))
        if not contour_id:
            raise ValueError(f"contours[{index}] missing `id`")
        if contour_id in seen_ids:
            raise ValueError(f"duplicate critical contour id: {contour_id}")
        seen_ids.add(contour_id)

        default_solution_class = normalize_text(str(raw_item.get("default_solution_class", "")))
        if default_solution_class not in SOLUTION_CLASSES:
            raise ValueError(
                f"critical contour `{contour_id}` has invalid default_solution_class "
                f"{default_solution_class!r}"
            )

        trigger_paths = _normalize_list(raw_item.get("trigger_paths", []), mode="path")
        trigger_patterns = _normalize_list(raw_item.get("trigger_patterns", []), mode="text")
        if not trigger_paths and not trigger_patterns:
            raise ValueError(f"critical contour `{contour_id}` must define trigger paths or patterns")

        forbidden_shortcut_markers = _normalize_list(
            raw_item.get("forbidden_shortcut_markers", []),
            mode="text",
        )
        required_evidence_markers = _normalize_list(
            raw_item.get("required_evidence_markers", []),
            mode="text",
        )
        allowed_staged_markers = _normalize_list(
            raw_item.get("allowed_staged_markers", []),
            mode="text",
        )
        reacceptance_trigger_markers = _normalize_list(
            raw_item.get("reacceptance_trigger_markers", []),
            mode="text",
        )
        if not forbidden_shortcut_markers:
            raise ValueError(f"critical contour `{contour_id}` must define forbidden shortcut markers")
        if not required_evidence_markers:
            raise ValueError(f"critical contour `{contour_id}` must define required evidence markers")
        if not allowed_staged_markers:
            raise ValueError(f"critical contour `{contour_id}` must define allowed staged markers")
        if not reacceptance_trigger_markers:
            raise ValueError(f"critical contour `{contour_id}` must define re-acceptance markers")

        contours.append(
            CriticalContour(
                contour_id=contour_id,
                summary=str(raw_item.get("summary", "")).strip(),
                trigger_paths=trigger_paths,
                trigger_patterns=trigger_patterns,
                default_solution_class=default_solution_class,
                forbidden_shortcut_markers=forbidden_shortcut_markers,
                required_evidence_markers=required_evidence_markers,
                allowed_staged_markers=allowed_staged_markers,
                reacceptance_trigger_markers=reacceptance_trigger_markers,
            )
        )
    return contours


def match_critical_contours(
    changed_files: list[str],
    contours: list[CriticalContour],
) -> list[CriticalContour]:
    normalized_files = [normalize_path(path_text) for path_text in changed_files if normalize_path(path_text)]
    matched: list[CriticalContour] = []
    seen: set[str] = set()
    for contour in contours:
        for path_text in normalized_files:
            path_hit = any(
                path_text == trigger_path or path_text.startswith(f"{trigger_path.rstrip('/')}/")
                for trigger_path in contour.trigger_paths
            )
            pattern_hit = any(pattern in path_text for pattern in contour.trigger_patterns)
            if path_hit or pattern_hit:
                if contour.contour_id not in seen:
                    seen.add(contour.contour_id)
                    matched.append(contour)
                break
    return matched


def section_lines(lines: list[str], heading: str) -> list[str]:
    start = -1
    for index, raw in enumerate(lines):
        if raw.strip() == heading:
            start = index
            break
    if start < 0:
        return []
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].strip().startswith("## "):
            end = index
            break
    return lines[start + 1 : end]


def extract_solution_intent(lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for raw in section_lines(lines, SOLUTION_INTENT_HEADING):
        stripped = raw.strip()
        if not stripped.startswith("- "):
            continue
        body = stripped[2:]
        if ":" not in body:
            continue
        key, value = body.split(":", 1)
        fields[normalize_key(key)] = value.strip()
    return fields


def split_csv_field(value: str) -> list[str]:
    return [normalize_text(part) for part in str(value).split(",") if normalize_text(part)]


def read_task_note(path: Path = DEFAULT_SESSION_HANDOFF_PATH) -> tuple[Path, list[str], bool]:
    note_path, lines, pointer_mode = read_task_note_lines(path)
    return note_path, lines, pointer_mode
