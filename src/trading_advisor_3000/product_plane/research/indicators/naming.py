from __future__ import annotations


def _normalize_token(value: object) -> str:
    text = str(value).strip().lower()
    if not text:
        raise ValueError("indicator naming token must be non-empty")
    return text.replace(".", "_").replace("-", "_")


def indicator_column_name(prefix: str, *parts: object) -> str:
    tokens = [_normalize_token(prefix)]
    tokens.extend(_normalize_token(part) for part in parts)
    return "_".join(tokens)

