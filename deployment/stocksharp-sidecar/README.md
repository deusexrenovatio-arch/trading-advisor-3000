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

## Non-Goals In This Slice
- No production broker rollout claim.
- No claim of full release readiness.
- No strategy/business trading logic in shell control-plane paths.
