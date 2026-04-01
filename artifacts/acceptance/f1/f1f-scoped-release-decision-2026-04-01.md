# F1-F Scoped Release Decision (2026-04-01)

## Verdict
`DENY_RELEASE_READINESS`

## Scope Of This Iteration
- Run the F1/F1-F test contour in governed mode.
- Exclude broker execution profile tests on purpose for this cycle.
- Confirm that non-broker contours remain stable before next acceptance pass.

## Evidence Summary
- Test run date: 2026-04-01
- Result: `294 passed`
- Runtime: `47.24s`
- Environment: local governed branch `codex/product-f1-finam-split`

## Explicit Exclusions
- `tests/process/test_run_f1e_real_broker_process.py`
- `tests/app/contracts/test_phase5_real_broker_process_contracts.py`
- `tests/app/integration/test_real_execution_staging_rollout.py`
- `tests/app/integration/test_phase4_live_execution_controlled.py`
- `tests/app/integration/test_phase6_operational_hardening.py`
- `tests/app/unit/test_real_execution_http_transport.py`
- `tests/app/unit/test_real_execution_runtime_profile_ops.py`
- `tests/app/unit/test_real_execution_staging_gateway_deployment.py`
- `tests/app/unit/test_phase4_broker_sync.py`
- `tests/app/unit/test_phase4_live_bridge.py`
- `tests/app/unit/test_phase6_live_bridge_hardening.py`
- `tests/app/unit/test_phase6_production_profile_deployment.py`
- `tests/app/unit/test_phase6_runtime_profile_ops.py`

## Why Verdict Stays Deny
- F1-F requires terminal evidence for real broker execution contour.
- Broker execution profile is intentionally deferred in this iteration.
- Therefore `ALLOW_RELEASE_READINESS` remains forbidden by contract.

## Next Governed Step
- Keep F1-F unaccepted and release readiness denied.
- Resume broker execution profile closure and evidence package completion.
- Re-run full F1-F acceptance with broker contour included.
