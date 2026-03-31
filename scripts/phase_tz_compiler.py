from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path


PHASE_HEADING_RE = re.compile(
    r"^###\s+Phase\s+(?P<phase_id>[A-Za-z0-9][A-Za-z0-9-]*)\s*[—–-]\s*(?P<title>.+?)\s*$",
    re.IGNORECASE,
)
BOLD_SECTION_RE = re.compile(r"^\*\*(?P<name>[^*]+?)\*\*\s*:?\s*(?P<rest>.*)$")


@dataclass(frozen=True)
class CompiledPhase:
    phase_id: str
    title: str
    objective: str
    acceptance_gate: tuple[str, ...]
    disprover: tuple[str, ...]


@dataclass(frozen=True)
class CompiledPhasePlan:
    source_path: str
    phases: tuple[CompiledPhase, ...]


def _collapse_ws(value: str) -> str:
    return " ".join(value.replace("\t", " ").split())


def _normalize_section_name(value: str) -> str:
    return _collapse_ws(value).strip().rstrip(":").lower()


def _extract_entries(lines: list[str]) -> list[str]:
    entries: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith("- "):
            entries.append(stripped[2:].strip())
            continue
        numbered = re.match(r"^\d+\.\s+(.*)$", stripped)
        if numbered:
            entries.append(numbered.group(1).strip())
            continue
        if entries and raw.startswith("  "):
            entries[-1] = f"{entries[-1]} {_collapse_ws(stripped)}".strip()
            continue
        entries.append(stripped)
    return [_collapse_ws(item) for item in entries if _collapse_ws(item)]


def _section_map(lines: list[str]) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current: str | None = None
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue
        heading = BOLD_SECTION_RE.match(stripped)
        if heading:
            current = _normalize_section_name(heading.group("name"))
            sections.setdefault(current, [])
            rest = _collapse_ws((heading.group("rest") or "").strip())
            if rest:
                sections[current].append(rest)
            continue
        if current:
            sections.setdefault(current, []).append(raw.rstrip())
    return sections


def _phase_block(lines: list[str], start: int, end: int) -> CompiledPhase:
    header = lines[start].strip()
    match = PHASE_HEADING_RE.match(header)
    if not match:
        raise ValueError(f"invalid phase heading: {header}")
    phase_id = match.group("phase_id").strip()
    title = _collapse_ws(match.group("title").strip())
    sections = _section_map(lines[start + 1 : end])
    objective_entries = _extract_entries(sections.get("objective", []))
    acceptance_entries = _extract_entries(sections.get("acceptance gate", []))
    disprover_entries = _extract_entries(sections.get("disprover", []))
    objective = objective_entries[0] if objective_entries else ""
    return CompiledPhase(
        phase_id=phase_id,
        title=title,
        objective=objective,
        acceptance_gate=tuple(acceptance_entries),
        disprover=tuple(disprover_entries),
    )


def compile_phase_plan(path: Path) -> CompiledPhasePlan:
    lines = path.read_text(encoding="utf-8").splitlines()
    phase_starts: list[int] = []
    for idx, raw in enumerate(lines):
        if PHASE_HEADING_RE.match(raw.strip()):
            phase_starts.append(idx)

    phases: list[CompiledPhase] = []
    for index, start in enumerate(phase_starts):
        end = phase_starts[index + 1] if index + 1 < len(phase_starts) else len(lines)
        phases.append(_phase_block(lines, start, end))

    return CompiledPhasePlan(
        source_path=path.resolve().as_posix(),
        phases=tuple(phases),
    )


def phase_plan_payload(plan: CompiledPhasePlan) -> dict[str, object]:
    return {
        "source_path": plan.source_path,
        "phase_count": len(plan.phases),
        "phase_ids": [phase.phase_id for phase in plan.phases],
        "phases": [asdict(phase) for phase in plan.phases],
    }


def run(*, source: Path, output: Path | None) -> int:
    plan = compile_phase_plan(source)
    payload = phase_plan_payload(plan)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if output is None:
        print(text, end="")
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8")
    print(f"phase tz compiler: wrote {output.as_posix()} ({payload['phase_count']} phases)")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Compile a phase-driven TZ markdown file into deterministic phase IR.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    raise SystemExit(
        run(
            source=Path(args.source).resolve(),
            output=Path(args.output).resolve() if args.output else None,
        )
    )


if __name__ == "__main__":
    main()
