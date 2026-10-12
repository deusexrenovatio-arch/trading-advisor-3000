# Skills Routing Policy

## Runtime Model
- Repo-local descriptors under `.codex/skills/*/` are only for TA3000-specific trading, product-plane data/research, or compute-runtime knowledge.
- `.cursor/skills/*/` is retired and is not an active skill location.
- `docs/agent/skills-catalog.md` is generated only from `.codex/skills/*/SKILL.md`.
- Repo-local skills stay cold-by-default and are opened only for a matching TA3000 trigger.

## TA3000 Skill Triggers
- TA3000 active product-surface naming, phase/debug labels, capability naming, and active/archive/provenance separation: repo-local `ta3000-product-surface-naming-cleanup`.
- TA3000 data-plane proof on `D:/TA3000-data`, Delta `_delta_log`, row counts, report JSON, canonical tail alignment, or real production-route materialization: repo-local `ta3000-data-plane-proof`.
- TA3000 futures contract economics, MOEX money math, margin/tick/step value, research `execution_*` propagation, vectorbt-vs-ledger money truth, fees/slippage/PnL, or risk sizing: repo-local `ta3000-futures-money-math-and-execution-economics`.
- TA3000 vectorbt, pandas-ta-classic, signal matrices, indicator/derived compute, or research backtest integration: repo-local `ta3000-quant-compute-methodology`.
- TA3000 strategy hypothesis, trading intent, market regimes, research protocol, acceptance, or rejection: repo-local `ta3000-strategy-research-methodology`.
- TA3000 technical-analysis design, trend, momentum, mean reversion, breakout, volatility, volume, divergence, or multi-timeframe logic: repo-local `ta3000-technical-analysis-system-design`.
- TA3000 backtest validation, walk-forward, out-of-sample evidence, robustness, overfitting, costs, slippage, lookahead, or survivorship risk: repo-local `ta3000-backtest-validation-and-overfit-control`.
- TA3000 signal delivery, Telegram/advisory alerts, webhook payloads, paper trading, live mode, robot lifecycle, or signal-to-action chains: repo-local `ta3000-signal-to-action-lifecycle`.

## Product-Plane Research Routing
- For new or revised trading ideas, start with `ta3000-strategy-research-methodology`; add `ta3000-technical-analysis-system-design` for indicators, chart structure, or technical-analysis regimes.
- For indicators, derived indicators, signal matrices, vectorbt, pandas-ta-classic, Optuna, or research materialization, add `ta3000-quant-compute-methodology` before implementation.
- For futures money math, margin, fees/slippage/PnL, or execution-economics propagation, add `ta3000-futures-money-math-and-execution-economics`.
- For strategy testing or promotion claims, add `ta3000-backtest-validation-and-overfit-control`.
- For user-facing signals, paper routing, semi-auto approval, or live execution, add `ta3000-signal-to-action-lifecycle` and keep the output mode explicit.
- Repo-local skills do not replace executable evidence or PR hygiene.

## Class Policy

| Class | Baseline runtime | Policy |
| --- | --- | --- |
| `KEEP_CORE` | allowed | repo-local active skill |
| `KEEP_OPTIONAL` | blocked | separate phase gate |
| `DEFER_STACK` | blocked | stack activation gate |
| `EXCLUDE_DOMAIN_INITIAL` | blocked | non-baseline by default |

## Lifecycle Rules

### Add Skill
1. Create a new skill folder under `.codex/skills/` with complete metadata.
2. Confirm it is TA3000-specific and product-plane/data/research/compute owned.
3. Run `python scripts/sync_skills_catalog.py`.
4. Update routing policy only if class or routing behavior changed.
5. Run strict validators and skill tests.

### Change Skill
1. Edit skill metadata/body.
2. Regenerate the catalog.
3. Update routing docs only when routing metadata changed.
4. Run `python scripts/skill_update_decision.py --strict ...`.

### Remove Or Rename Skill
1. Remove or rename the directory and frontmatter together.
2. Regenerate the catalog.
3. Update routing references or roadmap when placement changed.
4. Run strict decision and precommit gates.

## Validation Commands
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/validate_skills.py --strict`
- `python scripts/skill_update_decision.py --strict --from-git --git-ref HEAD`
- `python scripts/skill_precommit_gate.py --from-git --git-ref HEAD`
