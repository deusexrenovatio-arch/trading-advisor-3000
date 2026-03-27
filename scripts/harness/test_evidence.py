from __future__ import annotations

import re
from typing import Any, Iterable


ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
WHITESPACE_RE = re.compile(r"\s+")
STATUS_PREFIX_RE = re.compile(
    r"^(?:\[[^\]]+\]\s*)?(?:PASSED|FAILED|ERROR|XFAILED|XFAIL|XPASSED|XPASS|SKIPPED|PASS|FAIL|OK|SUCCESS)\s*[:|-]?\s*",
    re.IGNORECASE,
)
FAIL_PREFIX_RE = re.compile(
    r"^(?:\[[^\]]+\]\s*)?(?:FAILED|FAIL|ERROR|XFAILED|XFAIL)\b",
    re.IGNORECASE,
)
PASS_PREFIX_RE = re.compile(
    r"^(?:\[[^\]]+\]\s*)?(?:PASSED|PASS|OK|SUCCESS|XPASSED|XPASS)\b",
    re.IGNORECASE,
)
NODE_ID_RE = re.compile(r"([A-Za-z0-9_./\\-]+\.py::[A-Za-z0-9_./\\\-:\[\]]+)")
PY_FILE_RE = re.compile(r"([A-Za-z0-9_./\\-]+\.py)")
PYTEST_CMD_RE = re.compile(r"(?:^| )(?:python -m )?pytest(?:\.exe)?(?: |$)", re.IGNORECASE)


def _clean_text(raw: str) -> str:
    text = ANSI_ESCAPE_RE.sub("", str(raw))
    text = text.replace("\\", "/").strip()
    return WHITESPACE_RE.sub(" ", text).strip()


def _dedupe(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def coerce_string_entries(value: Any) -> list[str]:
    def _collect(item: Any) -> list[str]:
        if item is None:
            return []
        if isinstance(item, str):
            text = item.strip()
            if not text:
                return []
            if "\n" not in text:
                return [text]
            return [line.strip() for line in text.splitlines() if line.strip()]
        if isinstance(item, dict):
            preferred_keys = (
                "test_id",
                "test",
                "nodeid",
                "name",
                "id",
                "check",
                "path",
                "file",
                "value",
            )
            candidates = [
                str(item[key]).strip()
                for key in preferred_keys
                if key in item and str(item[key]).strip()
            ]
            status_raw = item.get("status") or item.get("result") or item.get("outcome")
            status_text = str(status_raw).strip() if status_raw is not None else ""
            if status_text and candidates:
                candidates.insert(0, f"{status_text} {candidates[0]}")
            if candidates:
                return candidates
            return [
                str(raw).strip()
                for raw in item.values()
                if isinstance(raw, str) and str(raw).strip()
            ]
        if isinstance(item, (list, tuple, set)):
            nested: list[str] = []
            for nested_item in item:
                nested.extend(_collect(nested_item))
            return nested
        text = str(item).strip()
        return [text] if text else []

    return _dedupe(_collect(value))


def extract_test_identifier(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""

    candidate = STATUS_PREFIX_RE.sub("", cleaned, count=1).strip("`'\",; ")
    node_match = NODE_ID_RE.search(candidate)
    if node_match:
        return _clean_text(node_match.group(1)).strip("`'\",; ")

    if PYTEST_CMD_RE.search(candidate):
        for token in candidate.split(" "):
            probe = token.strip("`'\",; ")
            if not probe or probe.startswith("-"):
                continue
            node_token = NODE_ID_RE.search(probe)
            if node_token:
                return _clean_text(node_token.group(1)).strip("`'\",; ")
            py_file_token = PY_FILE_RE.search(probe)
            if py_file_token:
                return _clean_text(py_file_token.group(1)).strip("`'\",; ")

    file_match = PY_FILE_RE.search(candidate)
    if file_match:
        return _clean_text(file_match.group(1)).strip("`'\",; ")

    return candidate


def infer_test_status(value: str) -> str | None:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    if FAIL_PREFIX_RE.match(cleaned):
        return "failed"
    if PASS_PREFIX_RE.match(cleaned):
        return "passed"
    return None


def normalize_test_entries(items: Any) -> list[str]:
    normalized = [extract_test_identifier(item) for item in coerce_string_entries(items)]
    return _dedupe([item for item in normalized if item])


def build_test_evidence(
    *,
    checks_run: Any,
    passed_tests: Any,
    failed_tests: Any,
) -> tuple[list[str], list[str], list[str]]:
    raw_checks = coerce_string_entries(checks_run)
    passed_from_checks: list[str] = []
    failed_from_checks: list[str] = []

    for raw in raw_checks:
        identifier = extract_test_identifier(raw)
        if not identifier:
            continue
        status = infer_test_status(raw)
        if status == "passed":
            passed_from_checks.append(identifier)
        elif status == "failed":
            failed_from_checks.append(identifier)

    normalized_passed = normalize_test_entries([*coerce_string_entries(passed_tests), *passed_from_checks])
    normalized_failed = normalize_test_entries([*coerce_string_entries(failed_tests), *failed_from_checks])
    normalized_checks = normalize_test_entries([*raw_checks, *normalized_passed, *normalized_failed])
    return normalized_checks, normalized_passed, normalized_failed


def _normalize_path(path: str) -> str:
    cleaned = _clean_text(path).lower().strip("/")
    if cleaned.startswith("./"):
        cleaned = cleaned[2:]
    return cleaned


def _split_node_id(identifier: str) -> tuple[str, str | None]:
    normalized = _clean_text(identifier).lower()
    if "::" in normalized:
        path, node = normalized.split("::", 1)
        return _normalize_path(path), node.strip()
    return _normalize_path(normalized), None


def _path_suffix_match(left: str, right: str) -> bool:
    if left == right:
        return True
    left_parts = [part for part in left.split("/") if part]
    right_parts = [part for part in right.split("/") if part]
    if not left_parts or not right_parts:
        return False
    if len(left_parts) >= len(right_parts) and left_parts[-len(right_parts) :] == right_parts:
        return True
    if len(right_parts) >= len(left_parts) and right_parts[-len(left_parts) :] == left_parts:
        return True
    return False


def identifiers_match(required: str, observed: str) -> bool:
    required_path, required_node = _split_node_id(required)
    observed_path, observed_node = _split_node_id(observed)
    if not required_path or not observed_path:
        return False

    if required_node and observed_node:
        return required_node == observed_node and _path_suffix_match(required_path, observed_path)
    if required_node and not observed_node:
        return False
    if not required_node and observed_node:
        return _path_suffix_match(required_path, observed_path)
    return _path_suffix_match(required_path, observed_path)


def find_missing_required_tests(
    *,
    required_tests: Any,
    checks_run: Any,
    passed_tests: Any,
    failed_tests: Any,
) -> list[str]:
    required = normalize_test_entries(required_tests)
    normalized_checks, normalized_passed, normalized_failed = build_test_evidence(
        checks_run=checks_run,
        passed_tests=passed_tests,
        failed_tests=failed_tests,
    )
    observed = normalize_test_entries([*normalized_checks, *normalized_passed, *normalized_failed])

    missing: list[str] = []
    for required_test in required:
        if any(identifiers_match(required_test, observed_test) for observed_test in observed):
            continue
        missing.append(required_test)
    return _dedupe(missing)

