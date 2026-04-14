param(
    [Parameter(Mandatory = $true)]
    [string]$RunId,
    [string]$NightlyRoot = 'artifacts/codex/moex-nightly',
    [string]$Phase01Root = 'artifacts/codex/moex-phase01',
    [string]$Phase02Root = 'artifacts/codex/moex-phase02',
    [string]$DataRoot = 'D:\TA3000-data\trading-advisor-3000-nightly',
    [string]$BaselineId = 'moex-baseline-4y-current'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-FullPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue
    )
    return [System.IO.Path]::GetFullPath($PathValue)
}

function Assert-PathExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetPath,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )
    if (-not (Test-Path -LiteralPath $TargetPath)) {
        throw "$Label not found: $TargetPath"
    }
}

function Replace-DirectoryWithMaterializedCopy {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DestinationPath,
        [Parameter(Mandatory = $true)]
        [string]$SourcePath
    )

    $resolvedDestination = Resolve-FullPath -PathValue $DestinationPath
    $resolvedSource = Resolve-FullPath -PathValue $SourcePath
    $parent = Split-Path -Path $resolvedDestination -Parent
    $tmpCopyPath = "$resolvedDestination.__materialize__"

    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    if (Test-Path -LiteralPath $tmpCopyPath) {
        Remove-Item -LiteralPath $tmpCopyPath -Recurse -Force
    }
    New-Item -ItemType Directory -Path $tmpCopyPath -Force | Out-Null

    $null = robocopy $resolvedSource $tmpCopyPath /E /COPY:DAT /R:2 /W:2 /NFL /NDL /NJH /NJS /NP
    if ($LASTEXITCODE -gt 7) {
        throw "robocopy failed while materializing baseline directory: $resolvedSource -> $resolvedDestination (exit=$LASTEXITCODE)"
    }
    if (-not (Test-Path -LiteralPath (Join-Path $tmpCopyPath '_delta_log'))) {
        throw "materialized Delta directory is missing _delta_log: $tmpCopyPath"
    }

    if (Test-Path -LiteralPath $resolvedDestination) {
        Remove-Item -LiteralPath $resolvedDestination -Recurse -Force
    }
    Move-Item -LiteralPath $tmpCopyPath -Destination $resolvedDestination
}

$resolvedNightlyRoot = Resolve-FullPath -PathValue $NightlyRoot
$resolvedPhase01Root = Resolve-FullPath -PathValue $Phase01Root
$resolvedPhase02Root = Resolve-FullPath -PathValue $Phase02Root
$resolvedDataRoot = Resolve-FullPath -PathValue $DataRoot
$resolvedRawBaselineRoot = Join-Path $resolvedDataRoot "raw\moex\$BaselineId"
$resolvedCanonicalBaselineRoot = Join-Path $resolvedDataRoot "canonical\moex\$BaselineId"
$resolvedDerivedRoot = Join-Path $resolvedDataRoot 'derived\moex'

$nightlyRunRoot = Join-Path $resolvedNightlyRoot $RunId
$phase01RunRoot = Join-Path $resolvedPhase01Root $RunId
$phase02RunRoot = Join-Path $resolvedPhase02Root $RunId

$nightlyReportPath = Join-Path $nightlyRunRoot 'nightly-backfill-report.json'
$phase01ReportPath = Join-Path $phase01RunRoot 'phase01-foundation-report.json'
$phase02ReportPath = Join-Path $phase02RunRoot 'phase02-canonical-report.json'
$rawTableSourcePath = Join-Path $phase01RunRoot 'delta\raw_moex_history.delta'
$canonicalBarsSourcePath = Join-Path $phase02RunRoot 'delta\canonical_bars.delta'
$canonicalProvenanceSourcePath = Join-Path $phase02RunRoot 'delta\canonical_bar_provenance.delta'

Assert-PathExists -TargetPath $nightlyReportPath -Label 'nightly report'
Assert-PathExists -TargetPath $phase01ReportPath -Label 'phase-01 report'
Assert-PathExists -TargetPath $phase02ReportPath -Label 'phase-02 report'
Assert-PathExists -TargetPath $rawTableSourcePath -Label 'raw table'
Assert-PathExists -TargetPath $canonicalBarsSourcePath -Label 'canonical bars table'
Assert-PathExists -TargetPath $canonicalProvenanceSourcePath -Label 'canonical provenance table'

$nightlyReport = Get-Content -LiteralPath $nightlyReportPath -Raw | ConvertFrom-Json
$phase01Report = Get-Content -LiteralPath $phase01ReportPath -Raw | ConvertFrom-Json
$phase02Report = Get-Content -LiteralPath $phase02ReportPath -Raw | ConvertFrom-Json

if ($nightlyReport.status -ne 'PASS') {
    throw "cannot pin baseline from non-PASS nightly run: $RunId (status=$($nightlyReport.status))"
}

if ($phase02Report.publish_decision -ne 'publish') {
    throw "cannot pin baseline from non-publish phase-02 run: $RunId (publish_decision=$($phase02Report.publish_decision))"
}

New-Item -ItemType Directory -Path $resolvedRawBaselineRoot -Force | Out-Null
New-Item -ItemType Directory -Path $resolvedCanonicalBaselineRoot -Force | Out-Null
New-Item -ItemType Directory -Path $resolvedDerivedRoot -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $resolvedDerivedRoot 'features') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $resolvedDerivedRoot 'indicators') -Force | Out-Null

$baselineReportsRoot = Join-Path $resolvedCanonicalBaselineRoot 'reports'
New-Item -ItemType Directory -Path $baselineReportsRoot -Force | Out-Null

$baselineRawPath = Join-Path $resolvedRawBaselineRoot 'raw_moex_history.delta'
$baselineCanonicalBarsPath = Join-Path $resolvedCanonicalBaselineRoot 'canonical_bars.delta'
$baselineCanonicalProvenancePath = Join-Path $resolvedCanonicalBaselineRoot 'canonical_bar_provenance.delta'

Replace-DirectoryWithMaterializedCopy -DestinationPath $baselineRawPath -SourcePath $rawTableSourcePath
Replace-DirectoryWithMaterializedCopy -DestinationPath $baselineCanonicalBarsPath -SourcePath $canonicalBarsSourcePath
Replace-DirectoryWithMaterializedCopy -DestinationPath $baselineCanonicalProvenancePath -SourcePath $canonicalProvenanceSourcePath

Copy-Item -LiteralPath $nightlyReportPath -Destination (Join-Path $baselineReportsRoot 'nightly-backfill-report.json') -Force
Copy-Item -LiteralPath $phase02ReportPath -Destination (Join-Path $baselineReportsRoot 'phase02-canonical-report.json') -Force

$promotedAtUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
$manifest = [ordered]@{
    baseline_id = $BaselineId
    baseline_status = 'accepted'
    promoted_at_utc = $promotedAtUtc
    selected_run_id = $RunId
    authoritative_storage_root = $resolvedCanonicalBaselineRoot
    data_root = $resolvedDataRoot
    storage_mode = 'materialized-autonomous'
    policy = [ordered]@{
        authoritative_rule = 'Use the canonical baseline root for downstream canonical reads and the paired raw baseline root for authoritative raw history.'
        rerun_rule = 'Later reruns are non-authoritative until they are explicitly pinned into the baseline root.'
        retention_rule = 'Keep the autonomous baseline data roots and remove superseded failed or diagnostic runs once they are no longer needed.'
    }
    storage_layout = [ordered]@{
        raw_root = $resolvedRawBaselineRoot
        canonical_root = $resolvedCanonicalBaselineRoot
        derived_root = $resolvedDerivedRoot
        features_root = (Join-Path $resolvedDerivedRoot 'features')
        indicators_root = (Join-Path $resolvedDerivedRoot 'indicators')
    }
    source_run_roots = [ordered]@{
        nightly_root = $nightlyRunRoot
        phase01_root = $phase01RunRoot
        phase02_root = $phase02RunRoot
    }
    baseline_paths = [ordered]@{
        raw_table = $baselineRawPath
        canonical_bars = $baselineCanonicalBarsPath
        canonical_bar_provenance = $baselineCanonicalProvenancePath
        nightly_report = (Join-Path $baselineReportsRoot 'nightly-backfill-report.json')
        phase02_report = (Join-Path $baselineReportsRoot 'phase02-canonical-report.json')
        manifest = (Join-Path $resolvedCanonicalBaselineRoot 'baseline-manifest.json')
        readme = (Join-Path $resolvedCanonicalBaselineRoot 'README.md')
    }
    source_artifacts = [ordered]@{
        nightly_report = $nightlyReportPath
        phase01_report = $phase01ReportPath
        phase02_report = $phase02ReportPath
        raw_table_source = $rawTableSourcePath
        canonical_bars_source = $canonicalBarsSourcePath
        canonical_bar_provenance_source = $canonicalProvenanceSourcePath
    }
    source_retention_status = 'historical source run folders may be purged after baseline materialization because raw/canonical baseline paths are self-contained'
    summary = [ordered]@{
        bootstrap_window_days = $phase01Report.bootstrap_window_days
        source_rows = $nightlyReport.phase01_summary.source_rows
        incremental_rows = $nightlyReport.phase01_summary.incremental_rows
        deduplicated_rows = $nightlyReport.phase01_summary.deduplicated_rows
        canonical_rows = $nightlyReport.canonical_summary.canonical_rows
        source_timeframes = @($nightlyReport.phase01_summary.source_timeframes)
        source_intervals = @($nightlyReport.phase01_summary.source_intervals)
        target_timeframes = @($nightlyReport.canonical_summary.target_timeframes)
        qc_status = $phase02Report.qc_report.status
        publish_decision = $phase02Report.publish_decision
    }
}

$readmeLines = @(
    '# MOEX 4Y Baseline Storage',
    '',
    "Pinned run: $RunId",
    "Promoted at UTC: $promotedAtUtc",
    'Storage mode: autonomous materialized baseline',
    '',
    'Authoritative rule:',
    'Use the canonical baseline root for downstream canonical reads and the paired raw baseline root for authoritative raw history. Do not point consumers at ad hoc rerun folders.',
    '',
    'Baseline storage layout:',
    "- raw root: $resolvedRawBaselineRoot",
    "- canonical root: $resolvedCanonicalBaselineRoot",
    "- derived root: $resolvedDerivedRoot",
    '',
    'Stable data paths:',
    "- raw: $baselineRawPath",
    "- canonical bars: $baselineCanonicalBarsPath",
    "- canonical provenance: $baselineCanonicalProvenancePath",
    '',
    'Derived placeholders:',
    "- features: $(Join-Path $resolvedDerivedRoot 'features')",
    "- indicators: $(Join-Path $resolvedDerivedRoot 'indicators')",
    '',
    'Historical pinned source run roots:',
    "- nightly: $nightlyRunRoot",
    "- phase01: $phase01RunRoot",
    "- phase02: $phase02RunRoot",
    '',
    'Retention note:',
    'The stable baseline paths above are materialized inside the data root and do not depend on historical source run folders for reads.'
)

(Join-Path $resolvedCanonicalBaselineRoot 'baseline-manifest.json') | ForEach-Object {
    [System.IO.File]::WriteAllText($_, ($manifest | ConvertTo-Json -Depth 100) + [Environment]::NewLine, [System.Text.Encoding]::UTF8)
}
(Join-Path $resolvedCanonicalBaselineRoot 'README.md') | ForEach-Object {
    [System.IO.File]::WriteAllText($_, ($readmeLines -join [Environment]::NewLine) + [Environment]::NewLine, [System.Text.Encoding]::UTF8)
}

$result = [ordered]@{
    baseline_id = $BaselineId
    run_id = $RunId
    data_root = $resolvedDataRoot
    baseline_root = $resolvedCanonicalBaselineRoot
    raw_root = $resolvedRawBaselineRoot
    raw_table = $baselineRawPath
    canonical_bars = $baselineCanonicalBarsPath
    canonical_bar_provenance = $baselineCanonicalProvenancePath
    promoted_at_utc = $promotedAtUtc
}
$result | ConvertTo-Json -Depth 20
