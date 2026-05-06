# Product Plane Overlay - Trading Advisor 3000

This file extends root `AGENTS.md` for product-plane work. Root shell policy
remains the source of truth for governance and delivery rules.

## Read First
1. Root `AGENTS.md`
2. `docs/agent/entrypoint.md`
3. `docs/agent/domains.md`
4. `docs/agent/checks.md`
5. `docs/agent/runtime.md`
6. `docs/DEV_WORKFLOW.md`
7. `docs/project-map/current-truth-map-2026-05-05.md`
8. `docs/architecture/product-plane/README.md`
9. `docs/architecture/product-plane/STATUS.md`
10. `docs/architecture/product-plane/CONTRACT_SURFACES.md`
11. `docs/architecture/product-plane/stack-conformance-baseline.md`

Archived documents under `docs/archive/` are off-route by default. Open them
only when a current truth document asks for historical provenance or when the
task is explicitly an archive audit.

## Scope And Boundaries
- Product code lives under `src/trading_advisor_3000/product_plane/*`.
- Product tests live under `tests/product-plane/*`.
- Product docs live under `docs/architecture/product-plane/*`,
  `docs/runbooks/app/*`, and `docs/workflows/app/*`.
- Historical product-plane acceptance checklists are archived under
  `docs/archive/product-plane-acceptance-checklists/2026-05-06/`.
- Deployment artifacts live under `deployment/*`.

## Prohibitions
1. Do not change shell runtime/contracts for product trading logic.
2. Do not place product runtime config in root `configs/*` without a separate
   governance decision.
3. Do not bypass loop/pr/nightly gates.
4. Do not mix shell-sensitive and product patch sets without explicit need.

## Delivery Discipline
- Current implementation claims come from `STATUS.md`, contract docs, code,
  tests, runbooks, and accepted data/runtime evidence.
- Historical bootstrap specs and old capability notes are provenance, not
  active proof.
- For high-risk changes, keep the order `contracts/policy -> code -> docs`.

## Baseline Checks
- `python -m pytest tests/product-plane -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
