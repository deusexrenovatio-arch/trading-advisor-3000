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
| `src/trading_advisor_3000/*` | product-plane | application runtime, contracts, and modules |
| `tests/app/*` | product-plane | product tests |
| `docs/architecture/app/*`, `docs/runbooks/app/*`, `docs/checklists/app/*`, `docs/workflows/app/*` | product-plane | product architecture and operational evidence |
| `deployment/*` | product-plane | deploy and transport surfaces |
| `docs/architecture/*` (outside `app/`) | shared | architecture bridge between shell and product-plane |

## Naming decision for this sweep
1. Keep existing filesystem paths for compatibility and validator stability.
2. Clarify intent through explicit terms, hubs, ownership, and PR templates.
3. Defer physical folder renames to a dedicated migration once import/test/link impact is pre-validated.

Migration specification reference:
- `docs/architecture/dual-surface-safe-rename-migration-technical-specification.md`

## Phase 2 Compatibility Bridge Status
- Runtime namespace bridge: `src/trading_advisor_3000/product_plane/__init__.py` proxies to the current legacy app package namespace.
- Docs namespace pointer: `docs/architecture/product-plane/README.md` points contributors to the current canonical docs root during bridge mode.
- Guardrail validator: `scripts/validate_legacy_namespace_growth.py` blocks new legacy namespace references in changed files outside explicit migration allowlist paths.

## Controlled rename candidates (deferred)
- `docs/architecture/app/` -> `docs/architecture/product-plane/`
- `tests/app/` -> `tests/product-plane/`
- `src/trading_advisor_3000/app/` -> `src/trading_advisor_3000/product_plane/`

These are intentionally deferred to avoid hidden breakage in imports, tests, scripts, and historical references.

## Mixed change policy
If a patch is `mixed`:
1. State why both surfaces are required.
2. Keep boundaries explicit in the patch summary.
3. Prefer split order for high-risk paths: `contracts -> code -> docs`.
