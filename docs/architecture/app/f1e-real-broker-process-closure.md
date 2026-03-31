# F1-E Real Broker Process Closure

## Objective
Move the release-blocking broker contour from planned to implemented for a governed staging-real profile using Finam-native session proof.

## Current Closure State
0. Phase state is currently `blocked` and not promoted to implemented.
1. Compiled sidecar process is built/tested/published in-repo (`deployment/stocksharp-sidecar`) and exercised through governed rollout on the compiled contour.
2. Connector + secrets requirements are versioned through:
   - `configs/broker_staging_connector_profile.v1.json`
   - `contracts/broker_staging_connector_profile.v1.json`
3. Governed proof route:
   - `python scripts/run_f1e_real_broker_process.py --output-root artifacts/f1/phase05/real-broker-process ...`
4. Finam-native preflight requires real JWT/session details payload (`/v1/sessions/details`) before rollout starts.
5. Sidecar and rollout checks enforce explicit runtime connector fields:
   - sidecar `/health` includes `connector_mode`, `connector_backend`, `connector_ready`, `connector_session_id`, `connector_binding_source`, `connector_last_heartbeat`,
   - governed runner rejects local loopback Finam API base URLs,
   - staging rollout evidence is produced from compiled sidecar runtime through Finam-bound transport profile.
6. The route remains fail-closed for invalid bindings:
   - smoke lifecycle (`boot`, `readiness`, `kill-switch`, `submit`, `replace`, `cancel`, `updates`),
   - staging rollout with reconciliation,
   - miswire/unavailable transport disprover and recovery replay,
   - immutable hash manifest and commit-linked run manifest.

## Closure Rule
The contour is promoted to `implemented` only after governed runs prove Finam-native session binding with operator-managed secrets and replayable artifacts.

Successful governed evidence bundle (staging-real contour candidate):
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

Fail-closed disprover evidence (still required, not closure by itself):
- `artifacts/f1/phase05/real-broker-process/20260331T184828Z-7a1dc827e46e/connector-preflight.json` (Finam-native `/v1/sessions/details` rejects invalid JWT and keeps the phase fail-closed)
- `artifacts/f1/phase05/real-broker-process/20260331T184828Z-7a1dc827e46e/failure.json` (records `failure_stage=finam_session_preflight` for governed traceability)

## Contract Surfaces
- `contracts/broker_staging_connector_profile.v1.json`
- `contracts/staging_rollout_report.v1.json`
- `contracts/runtime_operational_snapshot.v1.json`
- `contracts/real_broker_process_report.v1.json`

## Non-Goals
1. This is not a final production rollout claim.
2. This does not prove `ALLOW_RELEASE_READINESS`.
3. This does not override the phase-06 operational readiness gate.
