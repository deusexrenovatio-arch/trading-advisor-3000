# F1-E Real Broker Process Closure

## Objective
Move the release-blocking broker contour from planned to implemented for a governed staging-real profile using Finam-native session proof.

## Current Closure State
0. Phase state is `blocked` for `staging-real` closure.
1. Compiled sidecar process is built/tested/published in-repo (`deployment/stocksharp-sidecar`) and exercised through governed rollout.
2. Connector + secrets requirements are versioned through:
   - `configs/broker_staging_connector_profile.v1.json`
   - `src/trading_advisor_3000/product_plane/contracts/schemas/broker_staging_connector_profile.v1.json`
3. Governed route remains:
   - `python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process ...`
4. Finam-native preflight (`/v1/sessions/details`) is enforced as a prerequisite, not as lifecycle closure.
5. Fail-closed controls are explicit:
   - synthetic/stub connector markers are rejected in rollout validation,
   - executable Finam session binding requires `readonly=false` and non-empty `account_ids`,
   - Finam session-only mode cannot masquerade submit/cancel/replace/updates/fills as real broker transport.

## Closure Rule
The contour is promoted to `implemented` only after governed runs prove Finam-native session binding with operator-managed secrets and replayable artifacts.

Observed governed evidence (insufficient for closure on its own):
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/connector-preflight.json` (`status=ok`, Finam-native `/v1/sessions/details`, required session fields present)
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/build.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/test.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/publish.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/smoke.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/rollout.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/recovery-validation.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/negative-tests.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/hashes.json`
- `artifacts/f1/phase05/real-broker-process/20260331T184215Z-7a1dc827e46e/manifest.json`
- `artifacts/f1/phase05/real-broker-process/20260331T204717Z-2cfa7ca773f5/connector-preflight.json`
- `artifacts/f1/phase05/real-broker-process/20260331T204717Z-2cfa7ca773f5/rollout.json`
- `artifacts/f1/phase05/real-broker-process/20260331T204717Z-2cfa7ca773f5/recovery-validation.json`
- `artifacts/f1/phase05/real-broker-process/20260331T204717Z-2cfa7ca773f5/negative-tests.json`
- `artifacts/f1/phase05/real-broker-process/20260331T204717Z-2cfa7ca773f5/hashes.json`
- `artifacts/f1/phase05/real-broker-process/20260331T204717Z-2cfa7ca773f5/manifest.json`

Fail-closed disprover evidence (still required, not closure by itself):
- `artifacts/f1/phase05/real-broker-process/20260331T184828Z-7a1dc827e46e/connector-preflight.json` (Finam-native `/v1/sessions/details` rejects invalid JWT and keeps the phase fail-closed)
- `artifacts/f1/phase05/real-broker-process/20260331T184828Z-7a1dc827e46e/failure.json` (records `failure_stage=finam_session_preflight` for governed traceability)
- `artifacts/codex/orchestration/20260331T204848Z-f1-full-closure-phase-05/attempt-01/acceptance.json` (documents current blocked verdict and remaining lifecycle boundary gap)

## Contract Surfaces
- `src/trading_advisor_3000/product_plane/contracts/schemas/broker_staging_connector_profile.v1.json`
- `src/trading_advisor_3000/product_plane/contracts/schemas/staging_rollout_report.v1.json`
- `src/trading_advisor_3000/product_plane/contracts/schemas/runtime_operational_snapshot.v1.json`
- `src/trading_advisor_3000/product_plane/contracts/schemas/real_broker_process_report.v1.json`

## Non-Goals
1. This is not a final production rollout claim.
2. This does not prove `ALLOW_RELEASE_READINESS`.
3. This does not override the phase-06 operational readiness gate.
