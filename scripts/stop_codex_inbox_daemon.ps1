$ErrorActionPreference = "Stop"

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$pidPath = Join-Path $repoRoot ".runlogs/codex-inbox-daemon/pid.json"

if (-not (Test-Path $pidPath)) {
    Write-Output "daemon pid file not found"
    exit 0
}

$payload = Get-Content $pidPath -Raw -Encoding UTF8 | ConvertFrom-Json
$daemonPid = [int]$payload.pid

try {
    Stop-Process -Id $daemonPid -Force -ErrorAction Stop
    Write-Output ("stopped codex inbox daemon pid=" + $daemonPid)
} catch {
    Write-Output ("daemon process already stopped pid=" + $daemonPid)
}

Remove-Item -Force $pidPath -ErrorAction SilentlyContinue
