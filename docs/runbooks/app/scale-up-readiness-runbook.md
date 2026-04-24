# Scale-Up Readiness Scale-up Readiness Runbook

## Purpose
Validate that expansion to new providers/adapters can be delivered without core refactor:
- provider onboarding uses registry contracts,
- fundamentals/news runtime path uses context provider seam,
- execution adapters are registered via adapter catalog.

## Validation Procedure
1. Run phase-specific seam tests:
   - `python -m pytest tests/product-plane/unit/test_execution_adapter_catalog.py -q`
   - `python -m pytest tests/product-plane/unit/test_provider_extension_seams.py -q`
   - `python -m pytest tests/product-plane/unit/test_runtime_context_orchestration.py -q`
2. Run full app regression:
   - `python -m pytest tests/product-plane -q`
3. Run gates:
   - `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
   - `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Expansion Readiness Checklist
1. New adapter can be added through `ExecutionAdapterCatalog.register` only.
2. Adapter operations are dispatched through adapter-bound transports; no sidecar-only hardcoded submit path.
3. Fundamentals/news runtime payload enters decision replay through `ContextProviderRegistry` from pipeline wiring.
4. New data feed can be added through `DataProviderRegistry.register` with declared provider kind.
5. No mandatory blocking refactor appears in runtime pipeline to add one new provider or one new adapter.

## Incident Pattern During Expansion
- Symptom: new provider requires direct changes in core runtime/execution classes.
- Meaning: seam contract is bypassed.
- Action: move provider/adapter-specific logic into registry/provider implementation and keep core orchestration neutral.
