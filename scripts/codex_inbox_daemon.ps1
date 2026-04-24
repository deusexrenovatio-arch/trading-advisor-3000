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
    [string]$RemediationModel = "",
    [switch]$Once
)

$ErrorActionPreference = "Stop"

function Resolve-RepoRoot {
    param([string]$Root)
    if ($Root -and $Root.Trim()) {
        return [System.IO.Path]::GetFullPath($Root)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
}

function Ensure-Dir {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Force -Path $Path | Out-Null
    }
}

function ConvertTo-NativeObject {
    param($Value)

    if ($null -eq $Value) {
        return $null
    }
    if ($Value -is [System.Collections.IDictionary]) {
        $result = @{}
        foreach ($key in $Value.Keys) {
            $result[$key] = ConvertTo-NativeObject -Value $Value[$key]
        }
        return $result
    }
    if ($Value -is [pscustomobject]) {
        $result = @{}
        foreach ($property in $Value.PSObject.Properties) {
            $result[$property.Name] = ConvertTo-NativeObject -Value $property.Value
        }
        return $result
    }
    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        $items = @()
        foreach ($item in $Value) {
            $items += ,(ConvertTo-NativeObject -Value $item)
        }
        return $items
    }
    return $Value
}

function Load-State {
    param([string]$StatePath)
    if (-not (Test-Path $StatePath)) {
        return @{ processed = @{}; attempts = @() }
    }
    try {
        $raw = Get-Content $StatePath -Raw -Encoding UTF8
        $parsed = ConvertTo-NativeObject -Value ($raw | ConvertFrom-Json)
        if (-not $parsed.processed) { $parsed.processed = @{} }
        if (-not $parsed.attempts) { $parsed.attempts = @() }
        return $parsed
    } catch {
        return @{ processed = @{}; attempts = @() }
    }
}

function Save-State {
    param(
        [string]$StatePath,
        [hashtable]$State
    )
    $State.updated_at = ([datetime]::UtcNow).ToString("o")
    ($State | ConvertTo-Json -Depth 8) | Set-Content -Encoding UTF8 $StatePath
}

function Invoke-PackageRun {
    param(
        [string]$RepoRoot,
        [string]$PackagePath,
        [string]$Mode,
        [string]$Profile,
        [string]$Backend,
        [string]$WorkerModel,
        [string]$AcceptorModel,
        [string]$RemediationModel,
        [string]$LogPath
    )

    $python = (Get-Command python).Source
    $script = Join-Path $RepoRoot "scripts/codex_governed_bootstrap.py"
    $request = "process dropped package " + [System.IO.Path]::GetFileName($PackagePath)
    $arguments = @(
        $script,
        "--request", $request,
        "--route", "package",
        "--package-path", $PackagePath,
        "--mode", $Mode,
        "--backend", $Backend
    )
    if ($Profile -and $Profile.Trim()) {
        $arguments += @("--profile", $Profile)
    }
    if ($WorkerModel -and $WorkerModel.Trim()) {
        $arguments += @("--worker-model", $WorkerModel)
    }
    if ($AcceptorModel -and $AcceptorModel.Trim()) {
        $arguments += @("--acceptor-model", $AcceptorModel)
    }
    if ($RemediationModel -and $RemediationModel.Trim()) {
        $arguments += @("--remediation-model", $RemediationModel)
    }

    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $python @arguments 2>&1 | Tee-Object -FilePath $LogPath | Out-Host
        return [int]$LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
}

$repoRoot = Resolve-RepoRoot -Root $RepoRoot
$inbox = Join-Path $repoRoot $InboxRelative
$runlogsRoot = Join-Path $repoRoot ".runlogs/codex-inbox-daemon"
$logsRoot = Join-Path $runlogsRoot "logs"
$statePath = Join-Path $runlogsRoot "state.json"

Ensure-Dir -Path $inbox
Ensure-Dir -Path $logsRoot

while ($true) {
    $state = Load-State -StatePath $statePath
    $packages = Get-ChildItem -Path $inbox -Filter *.zip -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTimeUtc -Descending

    foreach ($package in $packages) {
        $ageSeconds = ([datetime]::UtcNow - $package.LastWriteTimeUtc).TotalSeconds
        if ($ageSeconds -lt $StabilitySeconds) {
            continue
        }

        $key = ($package.FullName.ToLowerInvariant() + "|" + $package.LastWriteTimeUtc.Ticks)
        if ($state.processed.ContainsKey($key)) {
            continue
        }

        $stamp = [datetime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
        $slug = [System.IO.Path]::GetFileNameWithoutExtension($package.Name) -replace '[^A-Za-z0-9._-]', '-'
        $logPath = Join-Path $logsRoot ($stamp + "-" + $slug + ".log")
        $exitCode = 1
        try {
            $exitCode = Invoke-PackageRun `
                -RepoRoot $repoRoot `
                -PackagePath $package.FullName `
                -Mode $Mode `
                -Profile $Profile `
                -Backend $Backend `
                -WorkerModel $WorkerModel `
                -AcceptorModel $AcceptorModel `
                -RemediationModel $RemediationModel `
                -LogPath $logPath
        } catch {
            $_ | Out-String | Add-Content -Encoding UTF8 $logPath
            $exitCode = 1
        }

        $record = @{
            package_path = $package.FullName
            package_name = $package.Name
            last_write_time_utc = $package.LastWriteTimeUtc.ToString("o")
            processed_at = ([datetime]::UtcNow).ToString("o")
            mode = $Mode
            backend = $Backend
            profile = $Profile
            exit_code = $exitCode
            status = $(if ($exitCode -eq 0) { "succeeded" } else { "failed" })
            log_path = $logPath
            result_path = (Join-Path $repoRoot "artifacts/codex/from-package-last-message.txt")
        }
        $state.processed[$key] = $record
        $state.attempts += $record
        Save-State -StatePath $statePath -State $state
    }

    if ($Once) {
        break
    }
    Start-Sleep -Seconds ([Math]::Max($PollSeconds, 1))
}
