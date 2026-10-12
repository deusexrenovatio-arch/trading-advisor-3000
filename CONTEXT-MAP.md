# Context Map

TA3000 uses a multi-context map. This file routes work to existing source-of-truth docs; it does not replace `AGENTS.md`, hot docs, context cards, or Serena/code inspection.

## Shell / Governance

Use when the task concerns process, gates, PR policy, routing, skills, shell contracts, or validation.

Read first:
- `AGENTS.md`
- `docs/agent/entrypoint.md`
- `docs/agent/domains.md`
- `docs/agent/checks.md`
- `docs/agent/runtime.md`
- `docs/DEV_WORKFLOW.md`

## Product Plane

Use when the task concerns app/product code, product-plane status, contracts, runtime ownership, or isolated app operations.

Read first:
- `product-plane/README.md`
- `docs/architecture/product-plane/STATUS.md`
- `docs/architecture/product-plane/CONTRACT_SURFACES.md`
- `docs/runbooks/app/bootstrap.md`

## Data / Research / Compute

Use when the task concerns market data, canonical storage, Spark/Delta/Dagster, indicators, derived indicators, vectorbt, Optuna, backtests, validation, or signal delivery.

Read first:
- `docs/architecture/product-plane/native-runtime-ownership.md`
- `docs/agent/native-runtime-selection.md`

## Architecture / Boundaries

Use when the task asks where concepts live, whether a boundary moved, or how shell/product/data/runtime ownership should be shaped.

Read first:
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/repository-surfaces.md`
- `docs/architecture/README.md`

## Navigation Rule

Before broad repo reading on a non-trivial task, run:

```bash
python scripts/context_router.py --from-git --format text
```

Then read the primary context card in `docs/agent-contexts/` and follow `navigation_order` only as far as the task requires.
