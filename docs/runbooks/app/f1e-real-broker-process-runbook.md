# F1-E Real Broker Process Runbook

## Purpose
Run the governed `F1-E` staging-real broker contour in fail-closed mode and collect evidence only when real Finam session bindings and operator-managed secrets are present.

## Current Phase State
- `real_broker_process` is currently `planned` (phase `blocked`) for the bounded `staging-real` contour.
- The phase no longer accepts external gateway `/health` as closure proof.
- Finam-native preflight (`/v1/sessions/details`) is the canonical binding check for this scope.
- Readonly or accountless Finam sessions are rejected for closure evidence.
- Session-only Finam mode cannot be treated as real lifecycle transport closure.
- This runbook does not claim production readiness or release-decision closure.

## Preconditions
1. `.NET 8 SDK` is installed (`dotnet --info` succeeds).
2. Python environment can import `trading_advisor_3000` from `src/`.
3. Required secrets are exported in environment:
   - `TA3000_STOCKSHARP_API_KEY`
   - `TA3000_FINAM_API_TOKEN`
4. Finam API binding is exported in environment:
   - `TA3000_FINAM_API_BASE_URL`
   - value must be absolute `http/https` and must not be `localhost`/loopback.
5. Broker profile contract exists and is current:
   - `configs/broker_staging_connector_profile.v1.json`

## Canonical Command
From repository root:

```powershell
python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process --dotnet "C:\Program Files\dotnet\dotnet.exe"
```

## What This Command Proves
1. Finam-native session preflight verifies `TA3000_FINAM_API_BASE_URL + /v1/sessions/details` with real JWT before runtime smoke starts.
2. Sidecar build/test/publish against current commit.
3. Runtime smoke for boot, readiness, and kill-switch; lifecycle steps are accepted as closure evidence only when they traverse a non-synthetic external connector boundary.
4. Staging rollout path with reconciliation (`connectivity -> canary -> batch`) on compiled sidecar, with fail-closed rejection for synthetic/stub connector markers.
5. Sidecar connector health/session fields remain present and valid for real contour proof.
6. Negative disprover for miswired/unavailable transport plus successful recovery replay.
7. Immutable hash manifest for generated proof artifacts.

If Finam session bindings are absent or invalid, the route must fail and the run must be rejected as phase evidence.

## Evidence Artifacts
Each run writes:
- `artifacts/f1/phase05/real-broker-process/<run-id>/failure.json` (fail-closed report for early validation/preflight stops)
- `artifacts/f1/phase05/real-broker-process/<run-id>/environment.json`
- `artifacts/f1/phase05/real-broker-process/<run-id>/connector-preflight.json` (Finam session preflight result)
- `artifacts/f1/phase05/real-broker-process/<run-id>/smoke.json`
- `artifacts/f1/phase05/real-broker-process/<run-id>/rollout.json`
- `artifacts/f1/phase05/real-broker-process/<run-id>/recovery-validation.json`
- `artifacts/f1/phase05/real-broker-process/<run-id>/negative-tests.json`
- `artifacts/f1/phase05/real-broker-process/<run-id>/hashes.json`
- `artifacts/f1/phase05/real-broker-process/<run-id>/manifest.json`

Reference successful staging-real bundle (for acceptance traceability):
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/connector-preflight.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/rollout.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/recovery-validation.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/negative-tests.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/hashes.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/manifest.json`

## Failure Triage
1. Missing secret env vars:
   - export required secrets and rerun;
   - command fails closed and writes `failure.json` with `failure_stage=secret_validation`.
2. Placeholder or synthetic secret values:
   - replace with operator-managed real staging credentials;
   - do not use placeholder/test/demo values in governed runs.
3. Finam session preflight fails:
   - verify `TA3000_FINAM_API_BASE_URL` and `TA3000_FINAM_API_TOKEN`;
   - verify endpoint `/v1/sessions/details` returns contract payload;
   - rerun after session preflight is valid.
4. Miswire disprover unexpectedly passes:
   - verify isolated port is truly unavailable;
   - treat as blocker because fail-closed path was not enforced.
5. Rollout or reconciliation fails:
   - inspect `logs/rollout-recovery.stderr.log`;
   - inspect `rollout.json` for failed stage and details;
   - keep kill-switch behavior active until recovery replay passes.
6. Hash validation fails:
   - treat as integrity failure;
   - do not reuse the run as phase evidence.
