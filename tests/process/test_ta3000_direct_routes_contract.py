from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ensure_ta3000_direct_routes.ps1"
TASK_INSTALLER = ROOT / "scripts" / "install_ta3000_direct_route_task.ps1"
RUNBOOK = ROOT / "docs" / "runbooks" / "app" / "ta3000-direct-egress-runbook.md"
RUNBOOK_INDEX = ROOT / "docs" / "runbooks" / "app" / "README.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_ta3000_direct_routes_script_contract() -> None:
    text = _read(SCRIPT)

    assert "ValidateSet('Check', 'Apply', 'Remove')" in text
    assert "$DestinationPrefix = '85.118.181.0/24'" in text
    assert "$ProbeHost = 'iss.moex.com'" in text
    assert "$ProbeUrl = 'https://iss.moex.com/iss/engines.json'" in text
    assert "[switch]$Replace" in text

    for token in (
        "New-NetRoute",
        "Find-NetRoute",
        "PersistentStore",
        "route.exe",
        "'-p'",
        "'MASK'",
        "Get-NetRoute",
        "Resolve-DnsName",
        "Invoke-WebRequest",
        "ConvertFrom-Json",
        "Content-Type",
        "json_has_expected_root",
        "statusCode -eq 200",
        "removal incomplete",
        "-ErrorAction Stop",
        "$LASTEXITCODE -ne 0",
    ):
        assert token in text

    for status in ("PASS", "NEEDS_ROUTE", "BLOCKED", "REMOVED"):
        assert status in text

    for excluded_alias in ("outline", "nord", "vpn", "tap", "tun", "vEthernet", "Loopback"):
        assert excluded_alias in text

    assert "D:\\TA3000-data" not in text
    assert "D:/TA3000-data" not in text


def test_ta3000_direct_route_task_installer_contract() -> None:
    text = _read(TASK_INSTALLER)

    assert "ValidateSet('Check', 'Install', 'Remove')" in text
    assert "ensure_ta3000_direct_routes.ps1" in text
    assert "$DestinationPrefix = '85.118.181.0/24'" in text
    assert "$ProbeHost = 'iss.moex.com'" in text
    assert "$ProbeUrl = 'https://iss.moex.com/iss/engines.json'" in text
    assert "$RepetitionMinutes = 5" in text

    for token in (
        "New-ScheduledTaskAction",
        "New-ScheduledTaskTrigger -AtStartup",
        "New-ScheduledTaskTrigger -AtLogOn",
        "RepetitionInterval",
        "MSFT_TaskEventTrigger",
        "Microsoft-Windows-NetworkProfile/Operational",
        "EventID=10000",
        "New-ScheduledTaskPrincipal",
        "-UserId 'SYSTEM'",
        "-LogonType ServiceAccount",
        "-RunLevel Highest",
        "Register-ScheduledTask",
        "Start-ScheduledTask",
        "Unregister-ScheduledTask",
        "administrator PowerShell is required to prove the SYSTEM scheduled task state",
        "-Mode Apply",
        "-Replace",
    ):
        assert token in text

    for status in ("PASS", "MISSING", "BLOCKED", "INSTALLED", "REMOVED"):
        assert status in text

    assert "D:\\TA3000-data" not in text
    assert "D:/TA3000-data" not in text


def test_ta3000_direct_routes_script_parses_when_powershell_is_available() -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is not available in this environment")

    for script in (SCRIPT, TASK_INSTALLER):
        result = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-Command",
                (
                    "$errors=$null; "
                    "$null=[System.Management.Automation.Language.Parser]::ParseFile("
                    f"'{script}', [ref]$null, [ref]$errors); "
                    "if ($errors.Count -gt 0) { $errors | ConvertTo-Json -Depth 5; exit 1 }"
                ),
            ],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr


def test_ta3000_direct_routes_runbook_contract() -> None:
    text = _read(RUNBOOK)
    index_text = _read(RUNBOOK_INDEX)

    assert "direct Wi-Fi egress" in text
    assert "85.118.181.0/24" in text
    assert "iss.moex.com" in text
    assert "https://iss.moex.com/iss/engines.json" in text
    assert "administrator PowerShell" in text
    assert "network runtime layer" in text
    assert "not a data-plane change" in text
    assert "does not modify `D:/TA3000-data`" in text
    assert "install_ta3000_direct_route_task.ps1" in text
    assert "Scheduled Task" in text
    assert "SYSTEM" in text
    assert "task `Check` require administrator PowerShell" in text
    assert "startup" in text
    assert "logon" in text
    assert "network profile connection event" in text
    assert "every five minutes" in text

    for mode in ("-Mode Check", "-Mode Apply", "-Mode Remove"):
        assert mode in text
    for mode in ("-Mode Install", "-Mode Check", "-Mode Remove"):
        assert mode in text

    assert "-Replace" in text
    assert "docs/runbooks/app/ta3000-direct-egress-runbook.md" in index_text
