from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


REQUIRED_TOKENS = (
    "AI_SHELL_EMERGENCY_MAIN_PUSH",
    "AI_SHELL_EMERGENCY_MAIN_PUSH_REASON",
)
FORBIDDEN_TOKENS = (
    "MOEX_CARRY_ALLOW_MAIN_PUSH",
    "MOEX_CARRY_EMERGENCY_MAIN_PUSH",
    "MOEX_CARRY_EMERGENCY_MAIN_PUSH_REASON",
)
REQUIRED_GITHUB_RULE_TYPES = (
    "pull_request",
    "required_status_checks",
    "non_fast_forward",
)
REQUIRED_STATUS_CHECKS = (
    "loop-lane",
    "pr-lane",
)
GITHUB_API_ROOT = "https://api.github.com"
GITHUB_API_VERSION = "2026-03-10"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse_github_repo_slug(remote_url: str) -> str | None:
    value = remote_url.strip()
    if not value:
        return None

    if value.startswith("git@github.com:"):
        candidate = value.split(":", 1)[1]
    elif value.startswith("https://github.com/"):
        candidate = urlparse(value).path.lstrip("/")
    elif value.startswith("ssh://git@github.com/"):
        candidate = urlparse(value).path.lstrip("/")
    else:
        return None

    if candidate.endswith(".git"):
        candidate = candidate[:-4]
    parts = [item for item in candidate.split("/") if item]
    if len(parts) != 2:
        return None
    owner, repo = parts
    return f"{owner}/{repo}"


def _resolve_github_repo_slug(repo_root: Path, explicit_repo: str | None) -> str:
    if explicit_repo:
        slug = explicit_repo.strip()
        if slug.count("/") != 1:
            raise RuntimeError(f"invalid GitHub repo slug `{slug}`; expected `owner/repo`")
        return slug

    completed = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"unable to resolve `origin` remote for GitHub policy validation: {detail or 'git remote lookup failed'}")

    slug = _parse_github_repo_slug(completed.stdout)
    if slug is None:
        raise RuntimeError("`origin` remote is not a supported GitHub URL; cannot validate server-side main protection")
    return slug


def _github_token() -> str | None:
    for name in ("GH_TOKEN", "GITHUB_TOKEN", "GITHUB_PERSONAL_ACCESS_TOKEN"):
        value = os.getenv(name, "").strip()
        if value:
            return value
    return None


def _github_api_get_json(url: str, token: str | None) -> Any:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "trading-advisor-3000/validate_pr_only_policy",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=15) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        hint = ""
        if exc.code in {401, 403} and not token:
            hint = " (public repositories can be read anonymously; private repos require GH_TOKEN/GITHUB_TOKEN)"
        raise RuntimeError(f"GitHub API request failed for {url}: HTTP {exc.code}{hint}; {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"GitHub API request failed for {url}: {exc.reason}") from exc

    return json.loads(payload)


def _fetch_branch_rules(*, repo_slug: str, branch: str, token: str | None) -> list[dict[str, Any]]:
    branch_ref = quote(branch, safe="")
    payload = _github_api_get_json(f"{GITHUB_API_ROOT}/repos/{repo_slug}/rules/branches/{branch_ref}", token)
    if not isinstance(payload, list):
        raise RuntimeError(f"unexpected GitHub branch rules payload for `{repo_slug}` `{branch}`")
    rules: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            rules.append(item)
    return rules


def _validate_github_branch_rules(*, repo_slug: str, branch: str, token: str | None) -> list[str]:
    errors: list[str] = []
    rules = _fetch_branch_rules(repo_slug=repo_slug, branch=branch, token=token)
    rule_types = {str(item.get("type")) for item in rules if item.get("type")}

    if not rules:
        errors.append(f"GitHub branch `{branch}` has no applied rules; main protection is not enforced server-side")
        return errors

    for rule_type in REQUIRED_GITHUB_RULE_TYPES:
        if rule_type not in rule_types:
            errors.append(f"GitHub branch `{branch}` is missing required `{rule_type}` rule")

    status_rule = next((item for item in rules if item.get("type") == "required_status_checks"), None)
    if status_rule is None:
        return errors

    parameters = status_rule.get("parameters")
    if not isinstance(parameters, dict):
        errors.append("GitHub required status checks rule is missing parameters")
        return errors

    if parameters.get("strict_required_status_checks_policy") is not True:
        errors.append("GitHub required status checks rule must enable strict head/base freshness")

    contexts: set[str] = set()
    raw_checks = parameters.get("required_status_checks")
    if isinstance(raw_checks, list):
        for item in raw_checks:
            if isinstance(item, dict):
                context = str(item.get("context") or "").strip()
                if context:
                    contexts.add(context)
    missing_contexts = [context for context in REQUIRED_STATUS_CHECKS if context not in contexts]
    if missing_contexts:
        rendered = ", ".join(missing_contexts)
        errors.append(f"GitHub required status checks are missing: {rendered}")

    return errors


def run(
    repo_root: Path,
    *,
    github_repo: str | None = None,
    github_branch: str = "main",
) -> int:
    files = [
        repo_root / "AGENTS.md",
        repo_root / "README.md",
        repo_root / "docs" / "DEV_WORKFLOW.md",
        repo_root / ".githooks" / "pre-push",
    ]
    errors: list[str] = []
    for path in files:
        if not path.exists():
            errors.append(f"missing required policy file: {path.as_posix()}")
            continue
        text = _read(path)
        for token in REQUIRED_TOKENS:
            if token not in text:
                errors.append(f"{path.as_posix()}: missing required token `{token}`")
        for token in FORBIDDEN_TOKENS:
            if token in text:
                errors.append(f"{path.as_posix()}: forbidden legacy token `{token}`")

    hook_path = repo_root / ".githooks" / "pre-push"
    if hook_path.exists():
        hook_text = _read(hook_path)
        if "run_loop_gate.py" not in hook_text:
            errors.append(".githooks/pre-push must call run_loop_gate.py")
        if "refs/heads/main" not in hook_text:
            errors.append(".githooks/pre-push must protect refs/heads/main")

    try:
        repo_slug = _resolve_github_repo_slug(repo_root, github_repo)
        errors.extend(
            _validate_github_branch_rules(
                repo_slug=repo_slug,
                branch=github_branch,
                token=_github_token(),
            )
        )
    except RuntimeError as exc:
        errors.append(str(exc))

    if errors:
        print("pr-only policy validation failed:")
        for item in errors:
            print(f"- {item}")
        print("remediation: see docs/runbooks/governance-remediation.md")
        return 1

    print("pr-only policy validation: OK")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate PR-only main policy surfaces.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--github-repo", default="", help="Optional GitHub repo slug override, e.g. owner/repo")
    parser.add_argument("--github-branch", default="main", help="GitHub branch to validate, defaults to `main`")
    args = parser.parse_args()
    sys.exit(
        run(
            Path(args.repo_root).resolve(),
            github_repo=args.github_repo or None,
            github_branch=args.github_branch,
        )
    )


if __name__ == "__main__":
    main()
