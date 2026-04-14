from __future__ import annotations

from pathlib import Path


FORBIDDEN_TOKENS = (
    "moex",
    "strategy",
    "signals",
    "orderbook",
)

ALLOWED_DOMAIN_PATH_PREFIXES = (
    "src/trading_advisor_3000/product_plane/",
    "src/trading_advisor_3000/dagster_defs/",
)


def _python_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def test_placeholder_package_has_no_domain_tokens() -> None:
    root = Path("src/trading_advisor_3000")
    offenders: list[str] = []
    for path in _python_files(root):
        rel = path.as_posix()
        if any(rel.startswith(prefix) for prefix in ALLOWED_DOMAIN_PATH_PREFIXES):
            continue
        text = path.read_text(encoding="utf-8").lower()
        for token in FORBIDDEN_TOKENS:
            if token in text:
                offenders.append(f"{rel}:{token}")
    assert not offenders, f"domain_boundary_violation:{offenders}"
