# TA3000 Direct Egress Runbook

## Purpose
TA3000 MOEX automation must reach MOEX ISS through direct Wi-Fi egress when a
local VPN, Outline tunnel, or similar virtual route is active.

This runbook covers only the Windows network runtime layer. It is not a data-plane change, does not modify `D:/TA3000-data`, and does not change the
Python MOEX client, coverage mode, nightly scheduler, or production branch.

## Current MOEX Route Target
- Destination prefix: `85.118.181.0/24`
- Probe host: `iss.moex.com`
- Probe URL: `https://iss.moex.com/iss/engines.json`

The script discovers the current Wi-Fi default gateway dynamically from the
local `0.0.0.0/0` route and excludes common VPN or virtual interfaces such as
Outline, Nord, TAP/TUN, vEthernet, and Loopback aliases.

## Commands
Run from the repository root.

Check the current route:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ensure_ta3000_direct_routes.ps1 -Mode Check
```

Apply a persistent direct route:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ensure_ta3000_direct_routes.ps1 -Mode Apply
```

Remove the direct route:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ensure_ta3000_direct_routes.ps1 -Mode Remove
```

`Apply` and `Remove` require administrator PowerShell because they change the
Windows route table. `Check` is safe to run without elevation.

Optional explicit override:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ensure_ta3000_direct_routes.ps1 `
  -Mode Apply `
  -DestinationPrefix 85.118.181.0/24 `
  -ProbeHost iss.moex.com `
  -ProbeUrl https://iss.moex.com/iss/engines.json `
  -Replace
```

Use `-Replace` only when the JSON output reports a conflicting route and the
operator has confirmed that the existing route is stale or VPN-bound.

## JSON Status Contract
- `PASS`: the destination route is active through the Wi-Fi gateway and the
  MOEX probe answered.
- `NEEDS_ROUTE`: the destination route is absent, or the active route still
  resolves through a non-Wi-Fi/VPN path.
- `BLOCKED`: the script cannot safely prove or apply the direct route. Typical
  causes are missing administrator rights for `Apply` or `Remove`, no Wi-Fi
  gateway, DNS outside `85.118.181.0/24`, a conflicting route without
  `-Replace`, or a failed HTTP probe.
- `REMOVED`: the direct route was removed when present.

The script prints JSON for every outcome so operators can save the exact
runtime state in issue notes or PR evidence.

## Smoke Validation
After `Apply`, verify that Windows selects the Wi-Fi interface for MOEX:

```powershell
Find-NetRoute -RemoteIPAddress 85.118.181.24
```

The result must point to the Wi-Fi interface and gateway, not an Outline,
VPN, TAP/TUN, vEthernet, or Loopback alias.

Then verify the MOEX ISS probe:

```powershell
curl https://iss.moex.com/iss/engines.json
```

The expected result is `200 OK` with JSON content. If this succeeds, run the
original MOEX history endpoint used by the operational workflow and confirm it
returns JSON rows.

## Boundary
This is a host routing control for MOEX ISS connectivity only. Broker/live
execution endpoints are out of scope and should be added later through the same
explicit route mechanism when a separate endpoint contract exists.
