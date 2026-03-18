# ADR 0002: Scale-up Extension Seams (Phase 7)

Status: accepted

## Context
The next expansion wave requires adding:
- new asset classes and data providers,
- fundamentals/news context in runtime decisions,
- additional execution adapters beyond the current sidecar path.

Without explicit extension seams, these changes would push adapter/provider branching into core runtime code and create blocking refactors.

## Decision
Use seam-first registries/contracts before adding new integrations:
1. `ExecutionAdapterCatalog` is the single registration point for execution adapters and mode support.
2. `DataProviderRegistry` is the single registration point for market/fundamentals/news providers.
3. `ContextProviderRegistry` is the single runtime seam for fundamentals/news context fetch.
4. Core runtime and execution flow remain transport/provider-neutral and consume these seams via contracts.

## Consequences
- New adapters/providers can be introduced by registration and targeted tests, without rewriting core orchestration.
- Fundamentals/news onboarding has an explicit runtime boundary before introducing heavy model logic.
- Architecture keeps control-plane and app-plane boundaries clean while reducing future refactor risk.

## Rejected alternatives
- Hardcode additional adapters/providers directly in runtime/execution classes.
- Introduce asset/provider branching inside strategy decision logic.
- Delay seam formalization until after first expansion integrations.
