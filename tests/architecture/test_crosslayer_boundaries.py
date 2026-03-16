from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _python_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(candidate for candidate in path.rglob("*.py") if candidate.is_file())


def test_app_layer_does_not_import_control_plane_modules() -> None:
    offenders: list[str] = []
    for path in _python_files(ROOT / "src" / "trading_advisor_3000"):
        text = path.read_text(encoding="utf-8")
        lowered = text.lower()
        if "import scripts." in lowered or "from scripts." in lowered:
            offenders.append(path.relative_to(ROOT).as_posix())
    assert not offenders, f"crosslayer boundary violation: {offenders}"


def test_architecture_v2_docs_exist_and_generated_map_marker_present() -> None:
    required = (
        "docs/architecture/trading-advisor-3000.md",
        "docs/architecture/layers-v2.md",
        "docs/architecture/entities-v2.md",
        "docs/architecture/architecture-map-v2.md",
    )
    for rel in required:
        assert (ROOT / rel).exists(), f"missing architecture v2 doc: {rel}"
    map_text = (ROOT / "docs/architecture/architecture-map-v2.md").read_text(encoding="utf-8")
    assert "<!-- generated-by: scripts/sync_architecture_map.py -->" in map_text
    assert "```mermaid" in map_text
