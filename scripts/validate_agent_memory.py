from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Any

import yaml


SECTION_KEYS = ("decisions", "incidents", "patterns")
REQUIRED_FIELDS = {
    "decisions": ("id", "date", "title", "context", "decision", "impact"),
    "incidents": ("id", "date", "title", "symptom", "root_cause", "remediation", "remediation_type"),
    "patterns": ("id", "date", "title", "pattern", "when_to_use"),
}
REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("agent memory must be a YAML object")
    return payload


def _resolve_index_item_path(index_path: Path, item_path: str) -> Path:
    candidate = Path(item_path)
    if candidate.is_absolute():
        return candidate
    repo_root = index_path.parent.parent.parent
    search_roots = (repo_root, index_path.parent.parent, index_path.parent)
    for root in search_roots:
        resolved = (root / candidate).resolve()
        if resolved.exists():
            return resolved
    return (repo_root / candidate).resolve()


def _load_section_from_index(index_path: Path) -> list[dict[str, Any]]:
    payload = _load_yaml(index_path)
    rows = payload.get("items")
    if not isinstance(rows, list):
        return []
    section_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item_path = row.get("path")
        if not isinstance(item_path, str) or not item_path.strip():
            continue
        candidate = _resolve_index_item_path(index_path, item_path)
        if not candidate.exists():
            continue
        item = _load_yaml(candidate)
        if isinstance(item, dict) and item:
            section_rows.append(item)
    return section_rows


def _load_layout_memory(memory_root: Path) -> tuple[dict[str, Any] | None, Path | None]:
    index_paths = {section: memory_root / section / "index.yaml" for section in SECTION_KEYS}
    if not any(path.exists() for path in index_paths.values()):
        return None, None
    payload: dict[str, Any] = {"version": 1, "updated_at": date.today().isoformat()}
    newest_updated_at = date.today().isoformat()
    for section, index_path in index_paths.items():
        payload[section] = _load_section_from_index(index_path) if index_path.exists() else []
        if index_path.exists():
            index_payload = _load_yaml(index_path)
            raw_updated = str(index_payload.get("updated_at", "")).strip()
            if raw_updated and raw_updated > newest_updated_at:
                newest_updated_at = raw_updated
    payload["updated_at"] = newest_updated_at
    return payload, memory_root


def _parse_date(value: Any, label: str, errors: list[str]) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        errors.append(f"{label}: invalid ISO date '{text}'")
        return None


def run(path: Path) -> int:
    if not path.exists():
        print(f"agent memory validation failed: file missing {path.as_posix()}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    layout_payload, source_root = _load_layout_memory(path.parent)
    payload = layout_payload if layout_payload is not None else _load_yaml(path)
    errors: list[str] = []
    if payload.get("version") != 1:
        errors.append(f"unsupported version: {payload.get('version')!r} (expected 1)")

    updated_at = str(payload.get("updated_at", "")).strip()
    if not updated_at:
        errors.append("missing top-level field 'updated_at'")
    else:
        _parse_date(updated_at, "updated_at", errors)

    seen_ids: set[str] = set()
    total_entries = 0
    for section in SECTION_KEYS:
        rows = payload.get(section)
        if not isinstance(rows, list):
            errors.append(f"section '{section}' must be a list")
            continue
        for idx, row in enumerate(rows):
            label = f"{section}[{idx}]"
            if not isinstance(row, dict):
                errors.append(f"{label} must be an object")
                continue
            total_entries += 1
            for field in REQUIRED_FIELDS[section]:
                value = str(row.get(field, "")).strip()
                if not value:
                    errors.append(f"{label} missing required field '{field}'")
            entry_id = str(row.get("id", "")).strip()
            if entry_id:
                if entry_id in seen_ids:
                    errors.append(f"duplicate entry id: {entry_id}")
                seen_ids.add(entry_id)
            row_date = str(row.get("date", "")).strip()
            if row_date:
                _parse_date(row_date, f"{label}.date", errors)
            links = row.get("links")
            if links is not None:
                if not isinstance(links, list):
                    errors.append(f"{label}.links must be a list when present")
                else:
                    for link in links:
                        text = str(link).strip()
                        if not text:
                            errors.append(f"{label}.links contains empty value")

    if total_entries == 0:
        errors.append("agent memory must contain at least one entry")

    if errors:
        print("agent memory validation failed:")
        for item in errors:
            print(f"- {item}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    print(
        "agent memory validation: OK "
        f"(source={source_root.as_posix() if source_root else path.as_posix()} entries={total_entries} updated_at={updated_at})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate agent operational memory registry.")
    parser.add_argument("--path", default="memory/agent_memory.yaml")
    args = parser.parse_args()
    sys.exit(run(Path(args.path)))


if __name__ == "__main__":
    main()
