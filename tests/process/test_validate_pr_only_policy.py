from __future__ import annotations

from pathlib import Path

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

    def _fake_fetch_branch_rules(*, repo_slug: str, branch: str, token: str | None) -> list[dict[str, object]]:
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

    def _fake_fetch_branch_rules(*, repo_slug: str, branch: str, token: str | None) -> list[dict[str, object]]:
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
