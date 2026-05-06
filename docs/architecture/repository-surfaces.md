# Repository Surfaces

## Intent
This map makes repository navigation explicit for two coordinated surfaces:
- Delivery Shell (control plane)
- Product Plane (application plane)

## Canonical surface terms
Use these exact terms in task notes and PRs:
- `shell`
- `product-plane`
- `mixed`

## Path map

| Path zone | Surface | Why it exists |
| --- | --- | --- |
| `AGENTS.md`, `docs/agent/*`, `docs/DEV_WORKFLOW.md` | shell | execution policy and run discipline |
| `scripts/*`, `configs/*` | shell | lifecycle and validation runtime |
| `plans/*`, `memory/*` | shell | durable process state |
| `docs/checklists/*`, `docs/workflows/*`, `docs/runbooks/*` | shell | governance contracts and operations |
| `docs/agent/native-runtime-selection.md` | shell | routing shim that points agents to the product-plane runtime ownership architecture |
| `src/trading_advisor_3000/*` | product-plane | application runtime, contracts, and modules |
| `src/trading_advisor_3000/dagster_defs/*`, `src/trading_advisor_3000/spark_jobs/*` | product-plane | data-plane execution definitions and Spark jobs; domain tokens are allowed here because these modules are product execution surfaces |
| `tests/product-plane/*` | product-plane | product tests |
| `docs/architecture/product-plane/*`, `docs/runbooks/app/*`, `docs/workflows/app/*` | product-plane | product architecture and operational evidence |
| `docs/checklists/app/README.md` | product-plane | pointer to archived product-plane acceptance checklist history |
| `deployment/*` | product-plane | deploy and transport surfaces |
| `docs/architecture/*` (outside `product-plane/`) | shared | architecture bridge between shell and product-plane |

## Naming decision for this sweep
1. Canonical docs root is now `docs/architecture/product-plane/`.
2. Legacy app-path docs are archived under `docs/archive/legacy-app-docs/2026-05-06/`.
3. Runtime and tests physical renames remain deferred to their dedicated phases.

Migration specification reference:
- `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md`

## Phase 2 Compatibility Bridge Status
- Runtime namespace bridge: `src/trading_advisor_3000/product_plane/__init__.py` proxies to the current legacy app package namespace.
- Docs namespace pointer: `docs/architecture/product-plane/README.md` points contributors to the current canonical docs root during bridge mode.
- Guardrail validator: `scripts/validate_legacy_namespace_growth.py` blocks new legacy namespace references in changed files outside explicit migration allowlist paths.

## Controlled rename candidates and status
- Completed in this phase: legacy app-path docs -> `docs/architecture/product-plane`, with retired app-path docs archived.
- Completed in this phase: product tests namespace now runs from `tests/product-plane/`.
- Completed in this phase: product runtime namespace now runs from `src/trading_advisor_3000/product_plane/`.

Remaining migration work is deferred to governance selector finalization and legacy cleanup phases.

## Mixed change policy
If a patch is `mixed`:
1. State why both surfaces are required.
2. Keep boundaries explicit in the patch summary.
3. Prefer split order for high-risk paths: `contracts -> code -> docs`.

## Native runtime ownership
Product-plane runtime ownership is architectural truth, not only an agent
preference. The canonical matrix is
`docs/architecture/product-plane/native-runtime-ownership.md`.

Agent route shims may reference it, but they must not become the only place
where Spark, Delta Lake, Dagster, pandas-ta-classic, vectorbt, Optuna, DuckDB,
or Python ownership is defined.
