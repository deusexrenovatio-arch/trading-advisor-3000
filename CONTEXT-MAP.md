# Context Map

TA3000 uses a multi-context map. This file routes Matt Pocock skills to the existing source-of-truth docs; it does not replace `AGENTS.md`, hot docs, context cards, or Serena/code inspection.

## Shell / Governance

Use when the task concerns process, gates, PR policy, routing, skills, shell contracts, or validation.

Read first:
- `AGENTS.md`
- `docs/agent/entrypoint.md`
- `docs/agent/domains.md`
- `docs/agent/checks.md`
- `docs/agent/runtime.md`
- `docs/agent/skills-routing.md`
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
- `docs/agent/skills-routing.md`
- `.codex/skills/ta3000-data-plane-proof/SKILL.md` when storage proof is needed
- `.codex/skills/ta3000-quant-compute-methodology/SKILL.md` when compute implementation is needed
- `.codex/skills/ta3000-strategy-research-methodology/SKILL.md` when strategy intent is being shaped
- `.codex/skills/ta3000-technical-analysis-system-design/SKILL.md` when TA rules are being shaped
- `.codex/skills/ta3000-backtest-validation-and-overfit-control/SKILL.md` when validation or overfit risk is in scope
- `.codex/skills/ta3000-signal-to-action-lifecycle/SKILL.md` when output signals or delivery chains are in scope

## Architecture / Boundaries

Use when the task asks where concepts live, whether a boundary moved, or how shell/product/data/runtime ownership should be shaped.

Read first:
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/repository-surfaces.md`
- `docs/architecture/README.md`
- `docs/agent/skills-routing.md`

## Navigation Rule

Before broad repo reading on a non-trivial task, run:

```bash
python scripts/context_router.py --from-git --format text
```

Then read the primary context card in `docs/agent-contexts/` and follow `navigation_order` only as far as the task requires.
