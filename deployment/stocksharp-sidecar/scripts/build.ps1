param(
    [string]$Configuration = "Release",
    [string]$DotnetExecutable = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($DotnetExecutable)) {
    $DotnetExecutable = if ([string]::IsNullOrWhiteSpace($env:TA3000_DOTNET_BIN)) { "dotnet" } else { $env:TA3000_DOTNET_BIN }
}

$solution = (Resolve-Path (Join-Path $PSScriptRoot "..\TradingAdvisor3000.StockSharpSidecar.sln")).Path
Write-Host "[phase08] $DotnetExecutable build $solution -c $Configuration"
& $DotnetExecutable build $solution -c $Configuration
if ($LASTEXITCODE -ne 0) {
    throw "dotnet build failed with exit code $LASTEXITCODE"
}
