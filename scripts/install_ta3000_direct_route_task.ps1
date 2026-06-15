param(
    [ValidateSet('Check', 'Install', 'Remove')]
    [string]$Mode = 'Check',
    [string]$TaskPath = '\TA3000\',
    [string]$TaskName = 'MOEX Direct Wi-Fi Route',
    [string]$RouteScriptPath = (Join-Path $PSScriptRoot 'ensure_ta3000_direct_routes.ps1'),
    [string]$DestinationPrefix = '85.118.181.0/24',
    [string]$ProbeHost = 'iss.moex.com',
    [string]$ProbeUrl = 'https://iss.moex.com/iss/engines.json',
    [int]$RepetitionMinutes = 5
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Normalize-TaskPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not $Path.StartsWith('\')) {
        $Path = "\$Path"
    }
    if (-not $Path.EndsWith('\')) {
        $Path = "$Path\"
    }
    return $Path
}

function ConvertTo-CommandLineArgument {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    if ($Value -notmatch '[\s"]') {
        return $Value
    }

    return '"' + ($Value -replace '"', '\"') + '"'
}

function Resolve-RouteScriptPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "route script not found: $Path"
    }
    return (Resolve-Path -LiteralPath $Path).Path
}

function New-RouteTaskArguments {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ResolvedRouteScriptPath
    )

    $argumentParts = @(
        '-NoProfile',
        '-ExecutionPolicy',
        'Bypass',
        '-File',
        (ConvertTo-CommandLineArgument -Value $ResolvedRouteScriptPath),
        '-Mode',
        'Apply',
        '-DestinationPrefix',
        (ConvertTo-CommandLineArgument -Value $DestinationPrefix),
        '-ProbeHost',
        (ConvertTo-CommandLineArgument -Value $ProbeHost),
        '-ProbeUrl',
        (ConvertTo-CommandLineArgument -Value $ProbeUrl),
        '-Replace'
    )

    return ($argumentParts -join ' ')
}

function New-NetworkProfileEventTrigger {
    try {
        $triggerClass = Get-CimClass `
            -Namespace 'Root/Microsoft/Windows/TaskScheduler' `
            -ClassName 'MSFT_TaskEventTrigger' `
            -ErrorAction Stop
        $trigger = New-CimInstance -CimClass $triggerClass -ClientOnly
        $trigger.Enabled = $true
        $trigger.Subscription = '<QueryList><Query Id="0" Path="Microsoft-Windows-NetworkProfile/Operational"><Select Path="Microsoft-Windows-NetworkProfile/Operational">*[System[(EventID=10000)]]</Select></Query></QueryList>'
        return $trigger
    }
    catch {
        return $null
    }
}

function Get-RouteTask {
    param(
        [Parameter(Mandatory = $true)]
        [string]$NormalizedTaskPath
    )

    return Get-ScheduledTask `
        -TaskPath $NormalizedTaskPath `
        -TaskName $TaskName `
        -ErrorAction SilentlyContinue
}

function Test-TaskContract {
    param(
        [AllowNull()]
        [object]$Task,
        [Parameter(Mandatory = $true)]
        [string]$ResolvedRouteScriptPath
    )

    if ($null -eq $Task) {
        return [ordered]@{
            ok = $false
            reason = 'scheduled task is not installed'
        }
    }

    $action = @($Task.Actions) | Select-Object -First 1
    $argumentText = if ($null -eq $action) { '' } else { [string]$action.Arguments }
    $argumentTextLower = $argumentText.ToLowerInvariant()
    $scriptPathLower = $ResolvedRouteScriptPath.ToLowerInvariant()
    $executeText = if ($null -eq $action) { '' } else { [string]$action.Execute }
    $principal = $Task.Principal
    $principalUser = if ($null -eq $principal) { '' } else { [string]$principal.UserId }
    $principalLogonType = if ($null -eq $principal) { '' } else { [string]$principal.LogonType }
    $principalRunLevel = if ($null -eq $principal) { '' } else { [string]$principal.RunLevel }
    $triggers = @($Task.Triggers)
    $problems = @()

    if ($executeText -notmatch '(?i)(powershell|pwsh)(\.exe)?$') {
        $problems += 'task action does not execute PowerShell'
    }
    if (-not $argumentTextLower.Contains($scriptPathLower)) {
        $problems += 'task action does not target the direct route script'
    }
    if (-not $argumentText.Contains('-Mode Apply')) {
        $problems += 'task action does not run Apply mode'
    }
    if (-not $argumentText.Contains('-Replace')) {
        $problems += 'task action does not replace stale routes'
    }
    if ($principalLogonType -notmatch 'ServiceAccount') {
        $problems += 'task principal is not a service account'
    }
    if ($principalRunLevel -notmatch 'Highest') {
        $problems += 'task run level is not Highest'
    }
    if ($triggers.Count -eq 0) {
        $problems += 'task has no triggers'
    }

    return [ordered]@{
        ok = ($problems.Count -eq 0)
        reason = if ($problems.Count -eq 0) { 'scheduled task matches the direct route contract' } else { $problems -join '; ' }
    }
}

function Convert-TaskForJson {
    param(
        [AllowNull()]
        [object]$Task
    )

    if ($null -eq $Task) {
        return $null
    }

    $actions = @()
    foreach ($action in @($Task.Actions)) {
        $actions += [ordered]@{
            execute = $action.Execute
            arguments = $action.Arguments
        }
    }

    $triggers = @()
    foreach ($trigger in @($Task.Triggers)) {
        $triggers += [ordered]@{
            type = $trigger.CimClass.CimClassName
            enabled = $trigger.Enabled
        }
    }

    return [ordered]@{
        task_name = $Task.TaskName
        task_path = $Task.TaskPath
        state = $Task.State
        principal_user_id = $Task.Principal.UserId
        principal_logon_type = $Task.Principal.LogonType
        principal_run_level = $Task.Principal.RunLevel
        actions = $actions
        triggers = $triggers
    }
}

function New-StatusPayload {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Status,
        [Parameter(Mandatory = $true)]
        [string]$Reason,
        [AllowNull()]
        [object]$Task,
        [AllowNull()]
        [string]$ResolvedRouteScriptPath,
        [AllowNull()]
        [object]$ErrorDetail
    )

    return [ordered]@{
        status = $Status
        mode = $Mode
        reason = $Reason
        task_name = $TaskName
        task_path = $TaskPath
        route_script_path = $ResolvedRouteScriptPath
        destination_prefix = $DestinationPrefix
        probe_host = $ProbeHost
        probe_url = $ProbeUrl
        repetition_minutes = $RepetitionMinutes
        checked_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        task = Convert-TaskForJson -Task $Task
        error = $ErrorDetail
    }
}

function Write-JsonAndExit {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Payload,
        [Parameter(Mandatory = $true)]
        [int]$ExitCode
    )

    $Payload | ConvertTo-Json -Depth 20
    exit $ExitCode
}

try {
    $TaskPath = Normalize-TaskPath -Path $TaskPath
    $resolvedRouteScriptPath = Resolve-RouteScriptPath -Path $RouteScriptPath
    $isAdministrator = Test-IsAdministrator
    $requiresAdministrator = $Mode -eq 'Install' -or $Mode -eq 'Remove'
    if ($requiresAdministrator -and -not $isAdministrator) {
        $payload = New-StatusPayload `
            -Status 'BLOCKED' `
            -Reason 'administrator PowerShell is required for Install or Remove' `
            -Task $null `
            -ResolvedRouteScriptPath $resolvedRouteScriptPath `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 2
    }

    if ($Mode -eq 'Remove') {
        $existingTask = Get-RouteTask -NormalizedTaskPath $TaskPath
        if ($null -ne $existingTask) {
            Unregister-ScheduledTask `
                -TaskPath $TaskPath `
                -TaskName $TaskName `
                -Confirm:$false `
                -ErrorAction Stop
        }
        $payload = New-StatusPayload `
            -Status 'REMOVED' `
            -Reason 'direct route scheduled task removed when present' `
            -Task (Get-RouteTask -NormalizedTaskPath $TaskPath) `
            -ResolvedRouteScriptPath $resolvedRouteScriptPath `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 0
    }

    if ($Mode -eq 'Install') {
        if ($RepetitionMinutes -lt 1) {
            throw "RepetitionMinutes must be at least 1"
        }

        $actionArguments = New-RouteTaskArguments -ResolvedRouteScriptPath $resolvedRouteScriptPath
        $action = New-ScheduledTaskAction `
            -Execute 'powershell.exe' `
            -Argument $actionArguments
        $triggers = @()
        $triggers += New-ScheduledTaskTrigger -AtStartup
        $triggers += New-ScheduledTaskTrigger -AtLogOn
        $triggers += New-ScheduledTaskTrigger `
            -Once `
            -At (Get-Date).Date.AddMinutes(1) `
            -RepetitionInterval (New-TimeSpan -Minutes $RepetitionMinutes) `
            -RepetitionDuration (New-TimeSpan -Days 3650)
        $networkProfileTrigger = New-NetworkProfileEventTrigger
        if ($null -ne $networkProfileTrigger) {
            $triggers += $networkProfileTrigger
        }

        $principal = New-ScheduledTaskPrincipal `
            -UserId 'SYSTEM' `
            -LogonType ServiceAccount `
            -RunLevel Highest
        $settings = New-ScheduledTaskSettingsSet `
            -StartWhenAvailable `
            -MultipleInstances IgnoreNew `
            -ExecutionTimeLimit (New-TimeSpan -Minutes 2) `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries

        Register-ScheduledTask `
            -TaskPath $TaskPath `
            -TaskName $TaskName `
            -Description 'Keeps TA3000 MOEX ISS direct egress pinned to the current Wi-Fi gateway.' `
            -Action $action `
            -Trigger $triggers `
            -Principal $principal `
            -Settings $settings `
            -Force | Out-Null

        Start-ScheduledTask `
            -TaskPath $TaskPath `
            -TaskName $TaskName `
            -ErrorAction Stop

        $task = Get-RouteTask -NormalizedTaskPath $TaskPath
        $contract = Test-TaskContract -Task $task -ResolvedRouteScriptPath $resolvedRouteScriptPath
        if (-not $contract.ok) {
            $payload = New-StatusPayload `
                -Status 'BLOCKED' `
                -Reason $contract.reason `
                -Task $task `
                -ResolvedRouteScriptPath $resolvedRouteScriptPath `
                -ErrorDetail $null
            Write-JsonAndExit -Payload $payload -ExitCode 2
        }

        $payload = New-StatusPayload `
            -Status 'INSTALLED' `
            -Reason 'direct route scheduled task installed and started' `
            -Task $task `
            -ResolvedRouteScriptPath $resolvedRouteScriptPath `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 0
    }

    $task = Get-RouteTask -NormalizedTaskPath $TaskPath
    if ($null -eq $task -and -not $isAdministrator) {
        $payload = New-StatusPayload `
            -Status 'BLOCKED' `
            -Reason 'administrator PowerShell is required to prove the SYSTEM scheduled task state' `
            -Task $null `
            -ResolvedRouteScriptPath $resolvedRouteScriptPath `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 2
    }
    $contract = Test-TaskContract -Task $task -ResolvedRouteScriptPath $resolvedRouteScriptPath
    $status = if ($null -eq $task) { 'MISSING' } elseif ($contract.ok) { 'PASS' } else { 'BLOCKED' }
    $exitCode = if ($status -eq 'BLOCKED') { 2 } else { 0 }
    $payload = New-StatusPayload `
        -Status $status `
        -Reason $contract.reason `
        -Task $task `
        -ResolvedRouteScriptPath $resolvedRouteScriptPath `
        -ErrorDetail $null
    Write-JsonAndExit -Payload $payload -ExitCode $exitCode
}
catch {
    $payload = New-StatusPayload `
        -Status 'BLOCKED' `
        -Reason 'direct route scheduled task operation failed' `
        -Task $null `
        -ResolvedRouteScriptPath $null `
        -ErrorDetail ([ordered]@{
            message = $_.Exception.Message
            type = $_.Exception.GetType().FullName
            line = $_.InvocationInfo.ScriptLineNumber
            command = $_.InvocationInfo.Line
        })
    Write-JsonAndExit -Payload $payload -ExitCode 2
}
