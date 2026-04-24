---
name: module-scaffold
description: Scaffold new modules with architecture placement, baseline contracts, tests, and governance hooks.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-ARCHITECTURE
scope: module initialization with architecture placement and governance defaults
routing_triggers:
  - "module scaffold"
  - "new module"
  - "bounded context"
  - "scaffold"
---

# Module Scaffold

## Purpose
Scaffold new modules with baseline contracts, tests, and governance hooks.

## Start Here In This Repository
- Read the first whole-system map listed in `docs/architecture/README.md` to place the new module in the correct architectural plane.
- Read `docs/architecture/repository-surfaces.md` to verify whether the module belongs to `shell`, `product-plane`, or a justified `mixed` change.
- Read `docs/architecture/product-plane/STATUS.md` when the new module extends implemented product capabilities instead of only target design.

## Workflow
1. Confirm the requested module outcome and declare the change surface before writing files.
2. Decide the module's plane and responsibility:
   - shell governance/process,
   - product data/research/runtime/execution,
   - or a justified bridge.
3. Scaffold contracts, tests, and documentation in the same architectural zone.
4. If the new module changes the reader-facing architecture picture, update the canonical map or the nearest architecture index.
5. Keep changes small, deterministic, and review-friendly.
6. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`

## Boundaries

This skill should NOT:
- create product runtime logic inside shell-owned paths.
- add a new module without a clear ownership story and matching tests.
- treat target-phase documents as proof that the module is already implemented.
