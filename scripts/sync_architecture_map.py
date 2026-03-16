from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path


GENERATED_HEADER = "<!-- generated-by: scripts/sync_architecture_map.py -->"


def _collect_headings(path: Path, *, prefix: str = "## ") -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        if stripped.startswith(prefix):
            value = stripped[len(prefix) :].strip()
            if value:
                out.append(value)
    return out


def _collect_entity_names(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    entities: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        if not stripped.startswith("### "):
            continue
        name = stripped[4:].strip()
        if name:
            entities.append(name)
    return entities


def _render_map(*, layers: list[str], entities: list[str]) -> str:
    layer_nodes = [f"L{i}" for i in range(1, len(layers) + 1)]
    lines = [
        GENERATED_HEADER,
        f"<!-- generated-at: {date.today().isoformat()} -->",
        "# Architecture Map v2",
        "",
        "```mermaid",
        "flowchart TB",
    ]
    for idx, layer in enumerate(layers):
        node = layer_nodes[idx]
        lines.append(f'  {node}["{layer}"]')
        if idx > 0:
            lines.append(f"  {layer_nodes[idx - 1]} --> {node}")

    if entities:
        lines.append('  E0["Entity Registry"]')
        for idx, entity in enumerate(entities[:8], start=1):
            lines.append(f'  E{idx}["{entity}"]')
            lines.append(f"  E0 --> E{idx}")
        if layer_nodes:
            lines.append(f"  {layer_nodes[min(1, len(layer_nodes) - 1)]} --> E0")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def run(*, layers_doc: Path, entities_doc: Path, output: Path) -> int:
    if not layers_doc.exists():
        print(f"sync architecture map: missing layers doc {layers_doc.as_posix()}")
        return 1
    if not entities_doc.exists():
        print(f"sync architecture map: missing entities doc {entities_doc.as_posix()}")
        return 1

    layers = _collect_headings(layers_doc)
    entities = _collect_entity_names(entities_doc)
    if not layers:
        print("sync architecture map: no layers found in layers doc")
        return 1
    if not entities:
        print("sync architecture map: no entities found in entities doc")
        return 1

    content = _render_map(layers=layers, entities=entities)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    print(f"architecture map synced: {output.as_posix()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Regenerate architecture-map-v2 from source docs.")
    parser.add_argument("--layers-doc", default="docs/architecture/layers-v2.md")
    parser.add_argument("--entities-doc", default="docs/architecture/entities-v2.md")
    parser.add_argument("--output", default="docs/architecture/architecture-map-v2.md")
    args = parser.parse_args()
    raise SystemExit(
        run(
            layers_doc=Path(args.layers_doc),
            entities_doc=Path(args.entities_doc),
            output=Path(args.output),
        )
    )


if __name__ == "__main__":
    main()
