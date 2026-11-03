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

## Product-plane module structure

The product plane is internally organized as deep modules, not as one flat app
package:

```text
Contracts
  -> Market Data Foundation
  -> Research Data Factory
  -> Strategy Factory
  -> Runtime Plane
  -> Execution Plane
```

This module structure is architectural ownership, not a directory rename plan by
itself. Current source paths remain mapped in
`docs/architecture/product-plane/product-plane-module-charters.md`, and public
communication rules are defined in
`docs/architecture/product-plane/product-plane-module-apis.md`.

Cross-module imports should go through Contracts or explicit public module APIs.
Direct imports of another product module's private storage helpers, jobs,
`_common` modules, runtime-private wiring, or data layout mechanics are not a
stable architecture surface.

## Naming decision for this sweep
1. Canonical docs root is now `docs/architecture/product-plane/`.
2. Legacy app-path docs are archived under `docs/archive/legacy-app-docs/2026-05-06/`.
3. Runtime and tests physical renames are complete: implementation lives under `src/trading_advisor_3000/product_plane/`, with product tests under `tests/product-plane/`.

Migration specification reference:
- `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md`

## Namespace cutover status
- Runtime namespace cutover is complete. `src/trading_advisor_3000/product_plane/__init__.py` is the product package entrypoint, not a proxy to a legacy app package; implementations live in its product-plane modules.
- Docs namespace pointer: `docs/architecture/product-plane/README.md` points contributors to the current canonical docs root.
- Guardrail validator: `scripts/validate_legacy_namespace_growth.py` blocks new legacy namespace references in changed files outside explicit migration allowlist paths.

## Controlled rename candidates and status
- Completed in this phase: legacy app-path docs -> `docs/architecture/product-plane`, with retired app-path docs archived.
- Completed in this phase: product tests namespace now runs from `tests/product-plane/`.
- Completed in this phase: product runtime namespace now runs from `src/trading_advisor_3000/product_plane/`.

The migration specification retains historical planning and provenance; it does
not defer the completed runtime/test namespace cutover.

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


## Repository navigation and Serena retirement

Decision date: 2026-11-04. Change surface: shell.
Decision status: accepted for this scoped delivery by the user.
Implementation status: implemented in this change; focused checks and isolated Git
checkout proof establish the repository behavior. No production deployment is claimed.
Owner: Delivery Shell navigation and worktree bootstrap.

The main baseline `92160367` mandates Serena for code discovery, enables it in
base MCP policy and registers new checkouts through a post-checkout hook. This
adds tool setup and machine-local writes to the ordinary development path. The
product runtime does not depend on Serena.

The repository retires its Serena configuration, bootstrap entrypoint and
post-checkout registration hook. Context routing, bounded symbol/caller searches
with `rg`, source inspection and the existing loop/PR gates remain the accepted
engineering path. An optional navigation tool is not a prerequisite for it.
Machine-local MCP configuration remains ignored; its default base server list is
empty. This change neither edits global/user Serena configuration nor uninstalls
software or deletes user memories, caches or product data.

Keeping the mandatory tool and registration writer was rejected because the
existing direct route already supplies source and reference evidence. Merely
relabelling Serena optional while retaining automatic registration would leave
the unwanted writer active. No replacement service, index or skill dependency
is introduced.

Migration removes only repository-owned files and active policy references;
dated historical evidence remains intact. Workflow invariants reject the
retired bootstrap/policy route, and context-router tests exercise the remaining
route. A real isolated Git checkout/worktree proof verifies that repository
bootstrap no longer creates Serena state while context routing still works.
Rollback is a normal PR revert of these repository files and policy. It does not
require runtime restart or data migration. Revisit only if a demonstrated source
navigation requirement cannot be met by the accepted direct route.

Sources: the user's scoped delivery instruction of 2026-11-04; `AGENTS.md`,
`docs/agent/runtime.md`, `scripts/context_router.py` and the retired hook/bootstrap
in baseline `92160367`. The delivery PR records independent review and checks;
this record does not assert a production deployment.
