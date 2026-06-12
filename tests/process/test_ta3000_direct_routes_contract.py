from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ensure_ta3000_direct_routes.ps1"
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


def test_ta3000_direct_routes_script_parses_when_powershell_is_available() -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is not available in this environment")

    result = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-Command",
            (
                "$errors=$null; "
                "$null=[System.Management.Automation.Language.Parser]::ParseFile("
                f"'{SCRIPT}', [ref]$null, [ref]$errors); "
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

    for mode in ("-Mode Check", "-Mode Apply", "-Mode Remove"):
        assert mode in text

    assert "-Replace" in text
    assert "docs/runbooks/app/ta3000-direct-egress-runbook.md" in index_text
