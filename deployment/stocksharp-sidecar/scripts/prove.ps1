param(
    [string]$Configuration = "Release",
    [string]$PublishDir = "",
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 18091,
    [string]$SmokeOutput = "",
    [string]$DotnetExecutable = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($DotnetExecutable)) {
    $DotnetExecutable = if ([string]::IsNullOrWhiteSpace($env:TA3000_DOTNET_BIN)) { "dotnet" } else { $env:TA3000_DOTNET_BIN }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path

if ([string]::IsNullOrWhiteSpace($PublishDir)) {
    $PublishDir = Join-Path $repoRoot "artifacts\dotnet-sidecar\stocksharp-sidecar\publish"
}
if ([string]::IsNullOrWhiteSpace($SmokeOutput)) {
    $SmokeOutput = Join-Path $repoRoot "artifacts\dotnet-sidecar\stocksharp-sidecar\python-smoke.json"
}

& (Join-Path $PSScriptRoot "build.ps1") -Configuration $Configuration -DotnetExecutable $DotnetExecutable
& (Join-Path $PSScriptRoot "test.ps1") -Configuration $Configuration -DotnetExecutable $DotnetExecutable
& (Join-Path $PSScriptRoot "publish.ps1") -Configuration $Configuration -OutputDir $PublishDir -DotnetExecutable $DotnetExecutable

$binary = Join-Path $PublishDir "TradingAdvisor3000.StockSharpSidecar.dll"
if (-not (Test-Path $binary)) {
    throw "Published sidecar binary not found: $binary"
}

$smokeScript = (Resolve-Path (Join-Path $repoRoot "scripts\smoke_stocksharp_sidecar_binary.py")).Path
Write-Host "[phase08] python $smokeScript --sidecar-binary $binary --dotnet $DotnetExecutable --host $BindHost --port $Port"
python $smokeScript --sidecar-binary $binary --dotnet $DotnetExecutable --host $BindHost --port $Port --output $SmokeOutput
if ($LASTEXITCODE -ne 0) {
    throw "python sidecar smoke failed with exit code $LASTEXITCODE"
}
