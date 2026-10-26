"""Read-only native input facts for staging admission; the existing owner validates them."""

from pathlib import Path

__all__ = ["registered_history_refresh_kwargs"]


def registered_history_refresh_kwargs(root: Path) -> dict:
    from .native_history_refresh import registered_history_refresh_kwargs as native_facts

    return native_facts(root)
