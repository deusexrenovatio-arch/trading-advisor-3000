from __future__ import annotations

from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

from scripts import validate_pr_only_policy


def _write_policy_files(repo_root: Path) -> None:
    (repo_root / "docs").mkdir(parents=True, exist_ok=True)
    (repo_root / ".githooks").mkdir(parents=True, exist_ok=True)
    content = "\n".join(
        [
            "AI_SHELL_EMERGENCY_MAIN_PUSH",
            "AI_SHELL_EMERGENCY_MAIN_PUSH_REASON",
            "run_loop_gate.py",
            "refs/heads/main",
        ]
    )
    (repo_root / "AGENTS.md").write_text(content + "\n", encoding="utf-8")
    (repo_root / "README.md").write_text(content + "\n", encoding="utf-8")
    (repo_root / "docs" / "DEV_WORKFLOW.md").write_text(content + "\n", encoding="utf-8")
    (repo_root / ".githooks" / "pre-push").write_text(content + "\n", encoding="utf-8")


def test_parse_github_repo_slug_supports_https_and_ssh() -> None:
    assert (
        validate_pr_only_policy._parse_github_repo_slug(
            "https://github.com/deusexrenovatio-arch/trading-advisor-3000.git"
        )
        == "deusexrenovatio-arch/trading-advisor-3000"
    )
    assert (
        validate_pr_only_policy._parse_github_repo_slug(
            "git@github.com:deusexrenovatio-arch/trading-advisor-3000.git"
        )
        == "deusexrenovatio-arch/trading-advisor-3000"
    )


def test_validate_pr_only_policy_passes_with_required_github_rules(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_policy_files(tmp_path)

    def _fake_fetch_branch_rules(
        *, repo_slug: str, branch: str, token: str | None
    ) -> list[dict[str, object]]:
        assert repo_slug == "deusexrenovatio-arch/trading-advisor-3000"
        assert branch == "main"
        return [
            {"type": "pull_request"},
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": True,
                    "required_status_checks": [
                        {"context": "loop-lane"},
                        {"context": "pr-lane"},
                    ],
                },
            },
            {"type": "non_fast_forward"},
        ]

    monkeypatch.setattr(validate_pr_only_policy, "_fetch_branch_rules", _fake_fetch_branch_rules)
    assert (
        validate_pr_only_policy.run(
            tmp_path,
            github_repo="deusexrenovatio-arch/trading-advisor-3000",
        )
        == 0
    )


def test_validate_pr_only_policy_fails_closed_when_required_check_is_missing(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    _write_policy_files(tmp_path)

    def _fake_fetch_branch_rules(
        *, repo_slug: str, branch: str, token: str | None
    ) -> list[dict[str, object]]:
        return [
            {"type": "pull_request"},
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": True,
                    "required_status_checks": [
                        {"context": "loop-lane"},
                    ],
                },
            },
            {"type": "non_fast_forward"},
        ]

    monkeypatch.setattr(validate_pr_only_policy, "_fetch_branch_rules", _fake_fetch_branch_rules)
    result = validate_pr_only_policy.run(
        tmp_path,
        github_repo="deusexrenovatio-arch/trading-advisor-3000",
    )

    captured = capsys.readouterr()
    assert result == 1
    assert "GitHub required status checks are missing: pr-lane" in captured.out


def test_github_api_get_json_retries_transient_url_error(monkeypatch) -> None:
    calls = 0

    class _Response:
        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'[{"type": "pull_request"}]'

    def _urlopen(_request: object, *, timeout: int) -> _Response:
        nonlocal calls
        assert timeout == validate_pr_only_policy.GITHUB_API_TIMEOUT_SECONDS
        calls += 1
        if calls == 1:
            raise URLError(TimeoutError("simulated SSL handshake timeout"))
        return _Response()

    monkeypatch.setattr(validate_pr_only_policy, "urlopen", _urlopen)
    monkeypatch.setattr(validate_pr_only_policy.time, "sleep", lambda _seconds: None)

    payload = validate_pr_only_policy._github_api_get_json("https://example.test", token=None)

    assert payload == [{"type": "pull_request"}]
    assert calls == 2


def test_github_api_get_json_retries_transient_http_error(monkeypatch) -> None:
    calls = 0
    sleeps: list[float] = []

    class _Response:
        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'[{"type": "required_status_checks"}]'

    def _urlopen(_request: object, *, timeout: int) -> _Response:
        nonlocal calls
        assert timeout == validate_pr_only_policy.GITHUB_API_TIMEOUT_SECONDS
        calls += 1
        if calls == 1:
            raise HTTPError(
                "https://example.test",
                503,
                "temporary service unavailable",
                {},
                BytesIO(b"temporary"),
            )
        return _Response()

    monkeypatch.setattr(validate_pr_only_policy, "urlopen", _urlopen)
    monkeypatch.setattr(validate_pr_only_policy.time, "sleep", sleeps.append)

    payload = validate_pr_only_policy._github_api_get_json("https://example.test", token=None)

    assert payload == [{"type": "required_status_checks"}]
    assert calls == 2
    assert sleeps == [validate_pr_only_policy.GITHUB_API_RETRY_DELAY_SECONDS]


def test_github_api_get_json_retries_rate_limited_403(monkeypatch) -> None:
    calls = 0
    sleeps: list[float] = []

    class _Response:
        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'[{"type": "required_status_checks"}]'

    def _urlopen(_request: object, *, timeout: int) -> _Response:
        nonlocal calls
        assert timeout == validate_pr_only_policy.GITHUB_API_TIMEOUT_SECONDS
        calls += 1
        if calls == 1:
            raise HTTPError(
                "https://example.test",
                403,
                "rate limited",
                {"X-RateLimit-Reset": "120.0"},
                BytesIO(b"rate limited"),
            )
        return _Response()

    monkeypatch.setattr(validate_pr_only_policy, "urlopen", _urlopen)
    monkeypatch.setattr(validate_pr_only_policy.time, "time", lambda: 100.0)
    monkeypatch.setattr(validate_pr_only_policy.time, "sleep", sleeps.append)

    payload = validate_pr_only_policy._github_api_get_json("https://example.test", token=None)

    assert payload == [{"type": "required_status_checks"}]
    assert calls == 2
    assert sleeps == [20.0]


def test_github_api_get_json_preserves_exhausted_http_error_detail(monkeypatch) -> None:
    calls = 0

    def _urlopen(_request: object, *, timeout: int) -> object:
        nonlocal calls
        assert timeout == validate_pr_only_policy.GITHUB_API_TIMEOUT_SECONDS
        calls += 1
        raise HTTPError(
            "https://example.test",
            503,
            "temporary service unavailable",
            {},
            BytesIO(f"temporary-{calls}".encode("utf-8")),
        )

    monkeypatch.setattr(validate_pr_only_policy, "urlopen", _urlopen)
    monkeypatch.setattr(validate_pr_only_policy.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="temporary-3"):
        validate_pr_only_policy._github_api_get_json("https://example.test", token=None)

    assert calls == validate_pr_only_policy.GITHUB_API_ATTEMPTS
