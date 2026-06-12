param(
    [ValidateSet('Check', 'Apply', 'Remove')]
    [string]$Mode = 'Check',
    [string]$DestinationPrefix = '85.118.181.0/24',
    [string]$ProbeHost = 'iss.moex.com',
    [string]$ProbeUrl = 'https://iss.moex.com/iss/engines.json',
    [switch]$Replace
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ExcludedInterfaceAliasPattern = '(?i)(outline|nord|vpn|tap|tun|vEthernet|Loopback)'

function Convert-IPv4ToUInt32 {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Address
    )

    $bytes = [System.Net.IPAddress]::Parse($Address).GetAddressBytes()
    [Array]::Reverse($bytes)
    return [BitConverter]::ToUInt32($bytes, 0)
}

function Test-IPv4InCidr {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Address,
        [Parameter(Mandatory = $true)]
        [string]$Prefix
    )

    $parts = $Prefix.Split('/')
    if ($parts.Count -ne 2) {
        throw "invalid IPv4 prefix: $Prefix"
    }

    $network = Convert-IPv4ToUInt32 -Address $parts[0]
    $candidate = Convert-IPv4ToUInt32 -Address $Address
    $prefixLength = [int]$parts[1]
    if ($prefixLength -lt 0 -or $prefixLength -gt 32) {
        throw "invalid IPv4 prefix length: $Prefix"
    }

    if ($prefixLength -eq 0) {
        return $true
    }

    $hostBits = 32 - $prefixLength
    $maskValue = [uint32]([math]::Pow(2, 32) - [math]::Pow(2, $hostBits))
    return (($candidate -band $maskValue) -eq ($network -band $maskValue))
}

function Convert-PrefixLengthToSubnetMask {
    param(
        [Parameter(Mandatory = $true)]
        [int]$PrefixLength
    )

    if ($PrefixLength -lt 0 -or $PrefixLength -gt 32) {
        throw "invalid IPv4 prefix length: $PrefixLength"
    }

    $bitString = ('1' * $PrefixLength).PadRight(32, '0')
    $octets = @()
    for ($offset = 0; $offset -lt 32; $offset += 8) {
        $octets += [convert]::ToInt32($bitString.Substring($offset, 8), 2)
    }

    return ($octets -join '.')
}

function Convert-CidrToRouteExeTarget {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prefix
    )

    $parts = $Prefix.Split('/')
    if ($parts.Count -ne 2) {
        throw "invalid IPv4 prefix: $Prefix"
    }

    return [ordered]@{
        network = $parts[0]
        mask = Convert-PrefixLengthToSubnetMask -PrefixLength ([int]$parts[1])
    }
}

function Convert-RouteForJson {
    param(
        [AllowNull()]
        [object]$Route
    )

    if ($null -eq $Route) {
        return $null
    }

    $policyStore = $null
    if ($Route.PSObject.Properties['PolicyStore']) {
        $policyStore = $Route.PolicyStore
    }
    elseif ($Route.PSObject.Properties['Store']) {
        $policyStore = $Route.Store
    }

    return [ordered]@{
        destination_prefix = $Route.DestinationPrefix
        interface_alias = $Route.InterfaceAlias
        interface_index = $Route.InterfaceIndex
        next_hop = $Route.NextHop
        route_metric = $Route.RouteMetric
        policy_store = $policyStore
    }
}

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-IsExcludedInterface {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InterfaceAlias
    )

    return $InterfaceAlias -match $ExcludedInterfaceAliasPattern
}

function Test-IsWifiInterface {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InterfaceAlias,
        [AllowNull()]
        [object]$Adapter
    )

    if ($InterfaceAlias -match '(?i)(wi-?fi|wireless|wlan|802\.11)') {
        return $true
    }

    if ($null -eq $Adapter) {
        return $false
    }

    $adapterText = @(
        $Adapter.Name
        $Adapter.InterfaceDescription
        $Adapter.NdisPhysicalMedium
        $Adapter.MediaType
    ) -join ' '

    return $adapterText -match '(?i)(wi-?fi|wireless|wlan|802\.11|native 802)'
}

function Get-DefaultWifiRoute {
    $routes = @(Get-NetRoute -DestinationPrefix '0.0.0.0/0' -AddressFamily IPv4 -ErrorAction SilentlyContinue)
    $candidates = @()

    foreach ($route in $routes) {
        if ([string]::IsNullOrWhiteSpace([string]$route.NextHop) -or $route.NextHop -eq '0.0.0.0') {
            continue
        }
        if (Test-IsExcludedInterface -InterfaceAlias $route.InterfaceAlias) {
            continue
        }

        $adapter = Get-NetAdapter -InterfaceIndex $route.InterfaceIndex -ErrorAction SilentlyContinue
        if (-not (Test-IsWifiInterface -InterfaceAlias $route.InterfaceAlias -Adapter $adapter)) {
            continue
        }

        $ipInterface = Get-NetIPInterface -InterfaceIndex $route.InterfaceIndex -AddressFamily IPv4 -ErrorAction SilentlyContinue
        $interfaceMetric = 0
        if ($null -ne $ipInterface) {
            $interfaceMetric = [int]$ipInterface.InterfaceMetric
        }

        $candidates += [pscustomobject]@{
            Route = $route
            CombinedMetric = ([int]$route.RouteMetric + $interfaceMetric)
        }
    }

    return @($candidates | Sort-Object -Property CombinedMetric | Select-Object -First 1).Route
}

function Resolve-ProbeIPAddress {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HostName,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedPrefix
    )

    $records = @(Resolve-DnsName -Name $HostName -Type A -ErrorAction Stop)
    $addresses = @(
        $records |
            Where-Object { $_.IPAddress -match '^\d{1,3}(\.\d{1,3}){3}$' } |
            ForEach-Object { $_.IPAddress }
    )
    $matching = @($addresses | Where-Object { Test-IPv4InCidr -Address $_ -Prefix $ExpectedPrefix })
    $selectedAddress = $null
    if ($matching.Count -gt 0) {
        $selectedAddress = $matching[0]
    }

    return [ordered]@{
        addresses = $addresses
        matching_addresses = $matching
        selected_address = $selectedAddress
    }
}

function Get-ExactDestinationRoutes {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prefix
    )

    $routes = @()
    foreach ($policyStore in @('ActiveStore', 'PersistentStore')) {
        $routes += @(Get-NetRoute `
            -DestinationPrefix $Prefix `
            -AddressFamily IPv4 `
            -PolicyStore $policyStore `
            -ErrorAction SilentlyContinue)
    }
    return $routes
}

function Get-PersistentDestinationRoutes {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prefix
    )

    return @(Get-NetRoute `
        -DestinationPrefix $Prefix `
        -AddressFamily IPv4 `
        -PolicyStore PersistentStore `
        -ErrorAction SilentlyContinue)
}

function Get-ActiveRoute {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RemoteIPAddress
    )

    $result = @(Find-NetRoute -RemoteIPAddress $RemoteIPAddress -ErrorAction Stop)
    $route = $result |
        Where-Object { $_.PSObject.Properties['DestinationPrefix'] -and $_.PSObject.Properties['NextHop'] } |
        Select-Object -First 1
    return $route
}

function Test-RouteUsesGateway {
    param(
        [AllowNull()]
        [object]$Route,
        [Parameter(Mandatory = $true)]
        [object]$GatewayRoute
    )

    if ($null -eq $Route) {
        return $false
    }

    return ([int]$Route.InterfaceIndex -eq [int]$GatewayRoute.InterfaceIndex -and [string]$Route.NextHop -eq [string]$GatewayRoute.NextHop)
}

function Test-HttpProbe {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )

    $requestArgs = @{
        Uri = $Url
        Method = 'Get'
        TimeoutSec = 20
        ErrorAction = 'Stop'
    }
    if ((Get-Command Invoke-WebRequest).Parameters.ContainsKey('UseBasicParsing')) {
        $requestArgs.UseBasicParsing = $true
    }

    $response = Invoke-WebRequest @requestArgs
    return [ordered]@{
        status_code = [int]$response.StatusCode
        ok = ([int]$response.StatusCode -ge 200 -and [int]$response.StatusCode -lt 400)
    }
}

function New-StatusPayload {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Status,
        [Parameter(Mandatory = $true)]
        [string]$Reason,
        [AllowNull()]
        [object]$WifiRoute,
        [AllowNull()]
        [object]$ActiveRoute,
        [AllowNull()]
        [object[]]$ExactRoutes,
        [AllowNull()]
        [object]$DnsResult,
        [AllowNull()]
        [object]$HttpProbe,
        [AllowNull()]
        [object]$ErrorDetail
    )

    $convertedExactRoutes = @()
    foreach ($route in @($ExactRoutes)) {
        if ($null -ne $route) {
            $convertedExactRoutes += Convert-RouteForJson -Route $route
        }
    }

    return [ordered]@{
        status = $Status
        mode = $Mode
        reason = $Reason
        destination_prefix = $DestinationPrefix
        probe_host = $ProbeHost
        probe_url = $ProbeUrl
        replace = [bool]$Replace
        checked_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        wifi_gateway = if ($null -eq $WifiRoute) { $null } else { $WifiRoute.NextHop }
        wifi_interface_alias = if ($null -eq $WifiRoute) { $null } else { $WifiRoute.InterfaceAlias }
        wifi_interface_index = if ($null -eq $WifiRoute) { $null } else { $WifiRoute.InterfaceIndex }
        dns = $DnsResult
        active_route = Convert-RouteForJson -Route $ActiveRoute
        exact_routes = $convertedExactRoutes
        http_probe = $HttpProbe
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

function Remove-DestinationRoutes {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prefix
    )

    $routes = Get-ExactDestinationRoutes -Prefix $Prefix
    $routeTarget = Convert-CidrToRouteExeTarget -Prefix $Prefix
    foreach ($route in $routes) {
        foreach ($policyStore in @('ActiveStore', 'PersistentStore')) {
            Remove-NetRoute `
                -DestinationPrefix $route.DestinationPrefix `
                -InterfaceIndex $route.InterfaceIndex `
                -NextHop $route.NextHop `
                -PolicyStore $policyStore `
                -Confirm:$false `
                -ErrorAction SilentlyContinue
        }
        & route.exe DELETE $routeTarget.network MASK $routeTarget.mask $route.NextHop | Out-Null
    }
}

function Add-PersistentDestinationRoute {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prefix,
        [Parameter(Mandatory = $true)]
        [object]$GatewayRoute
    )

    try {
        New-NetRoute `
            -DestinationPrefix $Prefix `
            -InterfaceIndex $GatewayRoute.InterfaceIndex `
            -NextHop $GatewayRoute.NextHop `
            -PolicyStore PersistentStore `
            -RouteMetric 1 | Out-Null
        return
    }
    catch {
        $netRouteError = $_.Exception.Message
        $routeTarget = Convert-CidrToRouteExeTarget -Prefix $Prefix
        $routeArgs = @(
            '-p',
            'ADD',
            $routeTarget.network,
            'MASK',
            $routeTarget.mask,
            [string]$GatewayRoute.NextHop,
            'METRIC',
            '1',
            'IF',
            [string]$GatewayRoute.InterfaceIndex
        )

        & route.exe @routeArgs | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "New-NetRoute failed ($netRouteError); route.exe persistent ADD also failed for $Prefix via $($GatewayRoute.NextHop) on interface $($GatewayRoute.InterfaceIndex) (exit=$LASTEXITCODE)"
        }
    }
}

try {
    $isAdministrator = Test-IsAdministrator
    if (($Mode -eq 'Apply' -or $Mode -eq 'Remove') -and -not $isAdministrator) {
        $payload = New-StatusPayload `
            -Status 'BLOCKED' `
            -Reason 'administrator PowerShell is required for Apply or Remove' `
            -WifiRoute $null `
            -ActiveRoute $null `
            -ExactRoutes @() `
            -DnsResult $null `
            -HttpProbe $null `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 2
    }

    $wifiRoute = Get-DefaultWifiRoute
    if ($null -eq $wifiRoute -and $Mode -ne 'Remove') {
        $payload = New-StatusPayload `
            -Status 'BLOCKED' `
            -Reason 'no non-VPN Wi-Fi default gateway was found' `
            -WifiRoute $null `
            -ActiveRoute $null `
            -ExactRoutes (Get-ExactDestinationRoutes -Prefix $DestinationPrefix) `
            -DnsResult $null `
            -HttpProbe $null `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 2
    }

    if ($Mode -eq 'Remove') {
        Remove-DestinationRoutes -Prefix $DestinationPrefix
        $payload = New-StatusPayload `
            -Status 'REMOVED' `
            -Reason 'destination route removed when present' `
            -WifiRoute $wifiRoute `
            -ActiveRoute $null `
            -ExactRoutes (Get-ExactDestinationRoutes -Prefix $DestinationPrefix) `
            -DnsResult $null `
            -HttpProbe $null `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 0
    }

    $dnsResult = Resolve-ProbeIPAddress -HostName $ProbeHost -ExpectedPrefix $DestinationPrefix
    if ($null -eq $dnsResult.selected_address) {
        $payload = New-StatusPayload `
            -Status 'BLOCKED' `
            -Reason 'probe host DNS did not resolve into the expected destination prefix' `
            -WifiRoute $wifiRoute `
            -ActiveRoute $null `
            -ExactRoutes (Get-ExactDestinationRoutes -Prefix $DestinationPrefix) `
            -DnsResult $dnsResult `
            -HttpProbe $null `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 2
    }

    $exactRoutes = Get-ExactDestinationRoutes -Prefix $DestinationPrefix
    $matchingRoutes = @($exactRoutes | Where-Object { Test-RouteUsesGateway -Route $_ -GatewayRoute $wifiRoute })
    $conflictingRoutes = @($exactRoutes | Where-Object { -not (Test-RouteUsesGateway -Route $_ -GatewayRoute $wifiRoute) })

    if ($Mode -eq 'Apply') {
        if ($conflictingRoutes.Count -gt 0 -and -not $Replace) {
            $payload = New-StatusPayload `
                -Status 'BLOCKED' `
                -Reason 'conflicting destination route exists; rerun with -Replace to replace it' `
                -WifiRoute $wifiRoute `
                -ActiveRoute (Get-ActiveRoute -RemoteIPAddress $dnsResult.selected_address) `
                -ExactRoutes $exactRoutes `
                -DnsResult $dnsResult `
                -HttpProbe $null `
                -ErrorDetail $null
            Write-JsonAndExit -Payload $payload -ExitCode 2
        }

        if ($conflictingRoutes.Count -gt 0 -and $Replace) {
            Remove-DestinationRoutes -Prefix $DestinationPrefix
            $exactRoutes = @()
            $matchingRoutes = @()
        }

        $persistentRoutes = Get-PersistentDestinationRoutes -Prefix $DestinationPrefix
        $matchingPersistentRoutes = @($persistentRoutes | Where-Object { Test-RouteUsesGateway -Route $_ -GatewayRoute $wifiRoute })

        if ($matchingPersistentRoutes.Count -eq 0) {
            Add-PersistentDestinationRoute -Prefix $DestinationPrefix -GatewayRoute $wifiRoute
        }

        $exactRoutes = Get-ExactDestinationRoutes -Prefix $DestinationPrefix
        $matchingRoutes = @($exactRoutes | Where-Object { Test-RouteUsesGateway -Route $_ -GatewayRoute $wifiRoute })
    }

    $activeRoute = Get-ActiveRoute -RemoteIPAddress $dnsResult.selected_address
    $activeRouteUsesWifi = Test-RouteUsesGateway -Route $activeRoute -GatewayRoute $wifiRoute

    if ($matchingRoutes.Count -eq 0 -or -not $activeRouteUsesWifi) {
        $payload = New-StatusPayload `
            -Status 'NEEDS_ROUTE' `
            -Reason 'destination route is absent or the active route does not use the Wi-Fi gateway' `
            -WifiRoute $wifiRoute `
            -ActiveRoute $activeRoute `
            -ExactRoutes $exactRoutes `
            -DnsResult $dnsResult `
            -HttpProbe $null `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 0
    }

    $httpProbe = Test-HttpProbe -Url $ProbeUrl
    if (-not $httpProbe.ok) {
        $payload = New-StatusPayload `
            -Status 'BLOCKED' `
            -Reason 'MOEX HTTP probe returned a non-success status' `
            -WifiRoute $wifiRoute `
            -ActiveRoute $activeRoute `
            -ExactRoutes $exactRoutes `
            -DnsResult $dnsResult `
            -HttpProbe $httpProbe `
            -ErrorDetail $null
        Write-JsonAndExit -Payload $payload -ExitCode 2
    }

    $payload = New-StatusPayload `
        -Status 'PASS' `
        -Reason 'destination route is active through the Wi-Fi gateway and MOEX probe answered' `
        -WifiRoute $wifiRoute `
        -ActiveRoute $activeRoute `
        -ExactRoutes $exactRoutes `
        -DnsResult $dnsResult `
        -HttpProbe $httpProbe `
        -ErrorDetail $null
    Write-JsonAndExit -Payload $payload -ExitCode 0
}
catch {
    $payload = New-StatusPayload `
        -Status 'BLOCKED' `
        -Reason 'direct egress route check failed' `
        -WifiRoute $null `
        -ActiveRoute $null `
        -ExactRoutes @() `
        -DnsResult $null `
        -HttpProbe $null `
        -ErrorDetail ([ordered]@{
            message = $_.Exception.Message
            type = $_.Exception.GetType().FullName
            line = $_.InvocationInfo.ScriptLineNumber
            command = $_.InvocationInfo.Line
        })
    Write-JsonAndExit -Payload $payload -ExitCode 2
}
