param(
    [string]$RepoRoot = "",
    [string]$InboxRelative = "docs/codex/packages/inbox",
    [int]$PollSeconds = 5,
    [int]$StabilitySeconds = 5,
    [string]$Mode = "auto",
    [string]$Profile = "",
    [string]$Backend = "codex-cli",
    [string]$WorkerModel = "",
    [string]$AcceptorModel = "",
    [string]$RemediationModel = ""
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot.Trim()) {
    $RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
}

$daemonScript = Join-Path $PSScriptRoot "codex_inbox_daemon.ps1"
$runlogsRoot = Join-Path $RepoRoot ".runlogs/codex-inbox-daemon"
if (-not (Test-Path $runlogsRoot)) {
    New-Item -ItemType Directory -Force -Path $runlogsRoot | Out-Null
}
$pidPath = Join-Path $runlogsRoot "pid.json"

$args = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $daemonScript,
    "-RepoRoot", $RepoRoot,
    "-InboxRelative", $InboxRelative,
    "-PollSeconds", $PollSeconds,
    "-StabilitySeconds", $StabilitySeconds,
    "-Mode", $Mode,
    "-Profile", $Profile,
    "-Backend", $Backend
)
if ($WorkerModel -and $WorkerModel.Trim()) {
    $args += @("-WorkerModel", $WorkerModel)
}
if ($AcceptorModel -and $AcceptorModel.Trim()) {
    $args += @("-AcceptorModel", $AcceptorModel)
}
if ($RemediationModel -and $RemediationModel.Trim()) {
    $args += @("-RemediationModel", $RemediationModel)
}

$proc = Start-Process -FilePath "powershell.exe" -ArgumentList $args -PassThru -WindowStyle Hidden
$payload = @{
    pid = $proc.Id
    started_at = ([datetime]::UtcNow).ToString("o")
    repo_root = $RepoRoot
    inbox_relative = $InboxRelative
    mode = $Mode
    backend = $Backend
}
($payload | ConvertTo-Json -Depth 4) | Set-Content -Encoding UTF8 $pidPath
Write-Output ("started codex inbox daemon pid=" + $proc.Id)
