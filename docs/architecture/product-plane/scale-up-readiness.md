# Scale-Up Readiness - Scale-up Readiness

## Goal
Prepare architecture and core contracts for expansion without blocking refactors:
- extension seams for new assets/providers,
- explicit runtime path for fundamentals/news context,
- extension seam for additional execution adapters,
- performance notes and prioritized backlog for next wave.

## Deliverables
- `src/trading_advisor_3000/product_plane/execution/adapters/catalog.py`
- `src/trading_advisor_3000/product_plane/execution/adapters/transport.py`
- `src/trading_advisor_3000/product_plane/data_plane/providers/registry.py`
- `src/trading_advisor_3000/product_plane/runtime/context/providers.py`
- `tests/product-plane/unit/test_execution_adapter_catalog.py`
- `tests/product-plane/unit/test_provider_extension_seams.py`
- `tests/product-plane/unit/test_runtime_context_orchestration.py`
- `docs/architecture/adr/0002-scale-up-extension-seams.md`
- `docs/architecture/product-plane/scale-up-readiness.md`
- archived checklist batch: `docs/archive/product-plane-acceptance-checklists/2026-05-06/README.md`

## Design Decisions
1. Adapter/provider extensibility is registry-first: core flow uses catalogs/registries, not hardcoded if-else branches.
2. Fundamentals/news path is wired into runtime orchestration: `build_runtime_stack*` injects `ContextProviderRegistry` into `SignalRuntimeEngine`, and replay persists context slices as signal events.
3. Execution adapter growth uses `ExecutionAdapterCatalog` with mode contracts (`supports_live`, `supports_paper`) plus adapter-to-transport bindings, so submit/cancel/replace dispatch is adapter-driven (not hardcoded sidecar-only).
4. Scale-up readiness is accepted only with explicit non-blocking architecture path and validated seam tests.

## Performance Notes
- Current replay path is CPU-light and file-contract friendly, but not tuned for high-cardinality provider fan-out.
- Runtime context fan-out should remain bounded per signal decision window.
- Execution adapter dispatch is catalog + transport lookup and O(1) per operation; broker stream drain cost grows with number of bound transports.

## Performance Backlog (next wave)
1. Add bounded cache/TTL layer for fundamentals/news context requests by (`contract_id`, `as_of_ts`, `provider_id`).
2. Add provider latency/error SLO metrics by provider kind and ID.
3. Add replay benchmark fixture for multi-provider context fan-out.
4. Add adapter capability matrix contract tests when second live adapter is onboarded.

## Acceptance Commands
- `python -m pytest tests/product-plane/unit/test_execution_adapter_catalog.py -q`
- `python -m pytest tests/product-plane/unit/test_provider_extension_seams.py -q`
- `python -m pytest tests/product-plane/unit/test_runtime_context_orchestration.py -q`
- `python -m pytest tests/product-plane -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD --skip-session-check`

## Out of Scope
- onboarding real external fundamentals/news data sources,
- implementing direct additional live broker transport in production,
- performance optimization of all expansion paths in this phase.
