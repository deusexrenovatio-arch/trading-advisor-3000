param(
    [string]$Configuration = "Release",
    [string]$OutputDir = "",
    [string]$DotnetExecutable = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($DotnetExecutable)) {
    $DotnetExecutable = if ([string]::IsNullOrWhiteSpace($env:TA3000_DOTNET_BIN)) { "dotnet" } else { $env:TA3000_DOTNET_BIN }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$project = (Resolve-Path (Join-Path $PSScriptRoot "..\src\TradingAdvisor3000.StockSharpSidecar\TradingAdvisor3000.StockSharpSidecar.csproj")).Path

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $repoRoot "artifacts\dotnet-sidecar\stocksharp-sidecar\publish"
}

Write-Host "[phase08] $DotnetExecutable publish $project -c $Configuration -o $OutputDir"
& $DotnetExecutable publish $project -c $Configuration -o $OutputDir
if ($LASTEXITCODE -ne 0) {
    throw "dotnet publish failed with exit code $LASTEXITCODE"
}
