# .NET Sidecar .NET Sidecar Proving Runbook

## Purpose
Provide deterministic local proving for the in-repo `.NET 8` StockSharp sidecar contract process.

## Preconditions
1. `.NET 8 SDK` is installed and available via `dotnet`.
2. Python environment can import `trading_advisor_3000` from `src/`.
3. No process is already bound to smoke port `18091` (or pass another port).
4. If `dotnet` is not on `PATH`, set `TA3000_DOTNET_BIN=<absolute-path-to-dotnet.exe>`.

## Canonical Proving Sequence
From repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/prove.ps1
```

This executes, in order:
1. `dotnet build`
2. `dotnet test`
3. `dotnet publish`
4. Python transport smoke against compiled sidecar binary (`/metrics`, admin kill-switch toggle, readiness/submit kill-switch behavior)

## Manual Step-By-Step
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/build.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/test.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/publish.ps1
python scripts/smoke_stocksharp_sidecar_binary.py --sidecar-binary artifacts/dotnet-sidecar/stocksharp-sidecar/publish/TradingAdvisor3000.StockSharpSidecar.dll --port 18091
```

## Expected Evidence
- publish output directory:
  - `artifacts/dotnet-sidecar/stocksharp-sidecar/publish/`
- smoke report:
  - `artifacts/dotnet-sidecar/stocksharp-sidecar/python-smoke.json`

## Failure Triage
1. SDK missing:
   - symptom: `No .NET SDKs were found`.
   - action: install `.NET 8 SDK`, then rerun proving sequence.
2. Port in use:
   - symptom: sidecar boot timeout in smoke script.
   - action: rerun smoke with `--port <free-port>`.
3. Contract mismatch:
   - symptom: smoke failure on submit/replace/cancel or stream states.
   - action: inspect sidecar logs printed by smoke script tail and compare with `docs/architecture/product-plane/sidecar-wire-api-v1.md`.

## Scope Guard
This runbook validates contract-level sidecar closure only.
It does not declare production broker rollout or release readiness.
