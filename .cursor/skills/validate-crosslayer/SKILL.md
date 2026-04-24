---
name: validate-crosslayer
description: Validate cross-layer consistency and prevent boundary drift between subsystems with implemented-reality checks.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-ARCHITECTURE
scope: cross-layer consistency and boundary validation with implemented-reality checks
routing_triggers:
  - "crosslayer"
  - "boundary validation"
  - "consistency checks"
  - "layer contract"
---

# Validate Crosslayer

## Purpose
Validate cross-layer consistency and prevent boundary drift between subsystems.

## Start Here In This Repository
- Read the first whole-system map listed in `docs/architecture/README.md`.
- Read `docs/architecture/repository-surfaces.md` for shell/product-plane ownership.
- Read `docs/architecture/product-plane/STATUS.md` when the validation depends on what is implemented now.
- Read `docs/architecture/product-plane/CONTRACT_SURFACES.md` when the crossing touches release-blocking product interfaces.

## Workflow
1. Confirm which layers or surfaces are being crossed.
2. Check whether the crossing is:
   - shell to product-plane,
   - product layer to product layer,
   - or contract to implementation.
3. Validate that the boundary is explicit in code, docs, and tests.
4. When documents disagree, prefer implemented-reality and contract-truth sources over orientation text.
5. Keep changes small, deterministic, and review-friendly.
6. Record assumptions and residual risks in task artifacts.

## Validation
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`

## Boundaries

This skill should NOT:
- treat a cross-layer hop as acceptable just because it is convenient in one patch.
- infer implementation truth from roadmap or target-state documents alone.
- skip contract validation when the crossing touches release-blocking boundaries.
