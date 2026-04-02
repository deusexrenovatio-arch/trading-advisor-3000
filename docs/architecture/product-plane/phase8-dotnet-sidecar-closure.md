# Phase 08 - Real .NET Sidecar Closure (E1)

## Objective
Land a real in-repo `.NET 8` sidecar process that implements the v1 wire contract and can be proved via compiled-binary smoke from Python.

## Scope Boundaries
In scope:
- `deployment/stocksharp-sidecar/` project and tests
- build/test/publish scripts for sidecar
- Python smoke against compiled binary

Out of scope:
- production broker rollout claims
- release-wide acceptance claims

## Implemented Artifacts
- `deployment/stocksharp-sidecar/TradingAdvisor3000.StockSharpSidecar.sln`
- `deployment/stocksharp-sidecar/src/TradingAdvisor3000.StockSharpSidecar/`
- `deployment/stocksharp-sidecar/tests/TradingAdvisor3000.StockSharpSidecar.Tests/`
- `deployment/stocksharp-sidecar/scripts/build.ps1`
- `deployment/stocksharp-sidecar/scripts/test.ps1`
- `deployment/stocksharp-sidecar/scripts/publish.ps1`
- `deployment/stocksharp-sidecar/scripts/prove.ps1`
- `scripts/smoke_stocksharp_sidecar_binary.py`

## Contract Endpoints Covered
- `/health`, `/ready`, `/metrics`
- `/v1/intents/submit`
- `/v1/intents/{intent_id}/cancel`
- `/v1/intents/{intent_id}/replace`
- `/v1/stream/updates`
- `/v1/stream/fills`
- `/v1/admin/kill-switch`

Compiled smoke explicitly validates:
- `/metrics` availability plus kill-switch metric toggle (`0 -> 1 -> 0`)
- readiness status under kill-switch (`503`, `reason=kill_switch_active`)
- submit rejection under kill-switch and recovery after disable

## Phase Evidence Commands
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/build.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/test.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/publish.ps1
python scripts/smoke_stocksharp_sidecar_binary.py --sidecar-binary artifacts/phase8/stocksharp-sidecar/publish/TradingAdvisor3000.StockSharpSidecar.dll
```

Or single orchestrated command:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/prove.ps1
```

## Evidence Outputs
- `artifacts/phase8/stocksharp-sidecar/publish/`
- `artifacts/phase8/stocksharp-sidecar/python-smoke.json`

## F1-D Immutable Evidence Hardening
For governed `F1-D` closure, run the immutable replay chain:

```powershell
python scripts/run_f1d_sidecar_immutable_evidence.py --output-root artifacts/f1/phase04/sidecar-immutable
```

This produces commit-linked evidence with:
- machine-recorded step exit codes for build/test/publish/smoke;
- immutable artifact hashes;
- disprovers for broken binary path, kill-switch readiness failure, and hash mismatch.

## Constraints Reminder
This phase closes contract-level sidecar implementation proof only.
It does not declare full production readiness.
