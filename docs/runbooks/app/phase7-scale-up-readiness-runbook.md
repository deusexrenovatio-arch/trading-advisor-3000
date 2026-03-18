# Phase 7 Scale-up Readiness Runbook

## Purpose
Validate that expansion to new providers/adapters can be delivered without core refactor:
- provider onboarding uses registry contracts,
- fundamentals/news runtime path uses context provider seam,
- execution adapters are registered via adapter catalog.

## Validation Procedure
1. Run phase-specific seam tests:
   - `python -m pytest tests/app/unit/test_phase7_execution_adapter_catalog.py -q`
   - `python -m pytest tests/app/unit/test_phase7_provider_extension_seams.py -q`
2. Run full app regression:
   - `python -m pytest tests/app -q`
3. Run gates:
   - `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
   - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Expansion Readiness Checklist
1. New adapter can be added through `ExecutionAdapterCatalog.register` only.
2. New data feed can be added through `DataProviderRegistry.register` with declared provider kind.
3. Fundamentals/news runtime payload enters decision path through `ContextProviderRegistry`.
4. No mandatory blocking refactor appears in runtime pipeline to add one new provider or one new adapter.

## Incident Pattern During Expansion
- Symptom: new provider requires direct changes in core runtime/execution classes.
- Meaning: seam contract is bypassed.
- Action: move provider/adapter-specific logic into registry/provider implementation and keep core orchestration neutral.
