param(
    [string]$Configuration = "Release",
    [string]$DotnetExecutable = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($DotnetExecutable)) {
    $DotnetExecutable = if ([string]::IsNullOrWhiteSpace($env:TA3000_DOTNET_BIN)) { "dotnet" } else { $env:TA3000_DOTNET_BIN }
}

$solution = (Resolve-Path (Join-Path $PSScriptRoot "..\TradingAdvisor3000.StockSharpSidecar.sln")).Path
Write-Host "[phase08] $DotnetExecutable test $solution -c $Configuration"
& $DotnetExecutable test $solution -c $Configuration
if ($LASTEXITCODE -ne 0) {
    throw "dotnet test failed with exit code $LASTEXITCODE"
}
