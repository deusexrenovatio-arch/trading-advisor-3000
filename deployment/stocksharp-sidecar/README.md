# StockSharp Sidecar (.NET) — Phase 08 Slice

This directory now contains a real in-repo `.NET 8` sidecar project that implements the v1 wire contract used by the Python execution transport.

## What Is Implemented
- `TradingAdvisor3000.StockSharpSidecar.sln`
- ASP.NET Core sidecar service in `src/TradingAdvisor3000.StockSharpSidecar/`
- xUnit test project in `tests/TradingAdvisor3000.StockSharpSidecar.Tests/`
- phase scripts for build/test/publish/prove in `scripts/`

## Wire Contract Scope
Implemented endpoints:
- `GET /health`
- `GET /ready`
- `GET /metrics`
- `POST /v1/intents/submit`
- `POST /v1/intents/{intent_id}/cancel`
- `POST /v1/intents/{intent_id}/replace`
- `GET /v1/stream/updates`
- `GET /v1/stream/fills`
- `POST /v1/admin/kill-switch`

Contract reference: `docs/architecture/app/sidecar-wire-api-v1.md`.

## Build / Test / Publish
From repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/build.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/test.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/publish.ps1
```

Optional override when global `dotnet` is unavailable:
- set `TA3000_DOTNET_BIN=<absolute-path-to-dotnet.exe>`

## External Connector Binding
- The sidecar gateway now fails closed without an external connector endpoint.
- Required runtime env:
  - `TA3000_BROKER_CONNECTOR_BASE_URL`
  - `TA3000_BROKER_CONNECTOR_AUTH_TOKEN` (or `TA3000_STOCKSHARP_API_KEY`)
- `/health` must expose session-bound connector fields:
  - `connector_session_id`
  - `connector_binding_source`
  - `connector_last_heartbeat`

## Compiled-Binary Python Smoke
Run all phase checks in one command:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/prove.ps1
```

This executes:
1. `dotnet build`
2. `dotnet test`
3. `dotnet publish`
4. `python scripts/smoke_stocksharp_sidecar_binary.py` against the compiled `.dll` (including `/metrics` and admin kill-switch behavior checks)

Smoke output is written to:
- `artifacts/phase8/stocksharp-sidecar/python-smoke.json`

## F1-D Immutable Evidence Chain
To produce commit-linked immutable sidecar evidence with hash validation and disprovers:

```powershell
python scripts/run_f1d_sidecar_immutable_evidence.py --output-root artifacts/f1/phase04/sidecar-immutable
```

This command writes phase artifacts for environment/build/test/publish/smoke/hashes plus a manifest with recorded step exit codes.

## Non-Goals In This Slice
- No production broker rollout claim.
- No claim of full release readiness.
- No strategy/business trading logic in shell control-plane paths.
