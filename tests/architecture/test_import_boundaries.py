from __future__ import annotations

from pathlib import Path


FORBIDDEN_TOKENS = (
    "moex",
    "strategy",
    "signals",
    "orderbook",
)


def _python_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def test_placeholder_package_has_no_domain_tokens() -> None:
    root = Path("src/trading_advisor_3000")
    offenders: list[str] = []
    for path in _python_files(root):
        text = path.read_text(encoding="utf-8").lower()
        for token in FORBIDDEN_TOKENS:
            if token in text:
                offenders.append(f"{path.as_posix()}:{token}")
    assert not offenders, f"domain_boundary_violation:{offenders}"
