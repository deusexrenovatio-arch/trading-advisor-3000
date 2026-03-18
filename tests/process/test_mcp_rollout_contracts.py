from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from mcp_preflight_smoke import run_preflight  # noqa: E402
from validate_mcp_config import validate as validate_mcp_config  # noqa: E402
from validate_no_tracked_secrets import validate as validate_no_tracked_secrets  # noqa: E402


def test_mcp_config_contract_passes_for_repository_template() -> None:
    errors, report = validate_mcp_config(
        config_path=ROOT / "deployment" / "mcp" / "config.template.toml",
        matrix_path=ROOT / "deployment" / "mcp" / "mcp-rollout-matrix.yaml",
        manifest_path=ROOT / "deployment" / "mcp" / "rollout-manifest.yaml",
        runbook_path=ROOT / "docs" / "runbooks" / "app" / "mcp-wave-rollout-runbook.md",
    )
    assert errors == []
    assert report["errors_total"] == 0
    assert len(report["required_server_ids"]) == 6


def test_mcp_config_validation_fails_when_required_server_is_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    matrix_path = tmp_path / "matrix.yaml"
    runbook_path = tmp_path / "runbook.md"
    runbook_path.write_text("github\nopenai_docs\n", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "[project]",
                "require_trusted = true",
                "",
                "[profiles.base]",
                'servers = ["github"]',
                "[profiles.ops]",
                'servers = ["github"]',
                "[profiles.data_readonly]",
                'servers = ["github"]',
                "",
                "[mcp_servers.github]",
                'command = "python"',
                "args = []",
                "required_env = []",
                'health_probe_args = ["--version"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    matrix_path.write_text(
        "\n".join(
            [
                "servers:",
                "  github:",
                "    required_env: []",
                "  openai_docs:",
                "    required_env: []",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    errors, _report = validate_mcp_config(
        config_path=config_path,
        matrix_path=matrix_path,
        runbook_path=runbook_path,
    )
    assert any("missing mcp_servers.openai_docs" in item for item in errors)


def test_mcp_preflight_fails_on_missing_env_in_strict_mode(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    matrix_path = tmp_path / "matrix.yaml"
    config_path.write_text(
        "\n".join(
            [
                "[profiles.ops]",
                'servers = ["github"]',
                "",
                "[mcp_servers.github]",
                'command = "python"',
                'args = ["-c", "print(1)"]',
                'required_env = ["TA3000_TEST_TOKEN"]',
                "health_probe_args = []",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    matrix_path.write_text(
        "\n".join(
            [
                "servers:",
                "  github:",
                '    required_env: ["TA3000_TEST_TOKEN"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code, payload = run_preflight(
        config_path=config_path,
        matrix_path=matrix_path,
        profile="ops",
        env={},
        strict_env_check=True,
        probe_commands=False,
        command_timeout_sec=1.0,
    )
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert any("missing env vars" in row for row in payload["errors"])


def test_mcp_preflight_fails_when_server_command_is_not_available(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    matrix_path = tmp_path / "matrix.yaml"
    config_path.write_text(
        "\n".join(
            [
                "[profiles.ops]",
                'servers = ["github"]',
                "",
                "[mcp_servers.github]",
                'command = "definitely-not-a-real-command"',
                "args = []",
                "required_env = []",
                "health_probe_args = []",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    matrix_path.write_text(
        "\n".join(
            [
                "servers:",
                "  github:",
                "    required_env: []",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code, payload = run_preflight(
        config_path=config_path,
        matrix_path=matrix_path,
        profile="ops",
        env={},
        strict_env_check=False,
        probe_commands=False,
        command_timeout_sec=1.0,
    )
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert any("command not found in PATH" in row for row in payload["errors"])


def test_tracked_secret_validator_detects_hardcoded_token_pattern(tmp_path: Path) -> None:
    suspicious = tmp_path / "suspicious.env"
    token = "ghp_" + "ABCDEFGHIJKLMNO" + "PQRSTUVWXYZ12345"
    suspicious.write_text(f"GITHUB_TOKEN={token}\n", encoding="utf-8")

    findings, report = validate_no_tracked_secrets(root=tmp_path, paths=[suspicious.name])
    assert report["status"] == "failed"
    assert report["findings_total"] == 1
    assert findings[0]["pattern"] == "github_classic_pat"
