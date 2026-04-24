from __future__ import annotations

from pathlib import Path


FORBIDDEN_TOKENS = (
    "moex",
    "strategy",
    "signals",
    "orderbook",
)

ALLOWED_DOMAIN_PATH_PREFIXES = (
    "src/trading_advisor_3000/dagster_defs/",
    "src/trading_advisor_3000/product_plane/",
    "src/trading_advisor_3000/spark_jobs/",
)

SHELL_SENSITIVE_ROOTS = (
    Path("shell"),
    Path("codex_ai_delivery_shell_package"),
)


def _python_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def test_repository_surface_map_declares_runtime_package_as_product_plane() -> None:
    surface_map = Path("docs/architecture/repository-surfaces.md").read_text(encoding="utf-8")
    assert "| `src/trading_advisor_3000/*` | product-plane |" in surface_map


def test_shell_sensitive_roots_have_no_domain_tokens() -> None:
    offenders: list[str] = []
    for root in SHELL_SENSITIVE_ROOTS:
        for path in _python_files(root):
            rel = path.as_posix()
            text = path.read_text(encoding="utf-8").lower()
            for token in FORBIDDEN_TOKENS:
                if token in text:
                    offenders.append(f"{rel}:{token}")
    assert not offenders, f"domain_boundary_violation:{offenders}"
