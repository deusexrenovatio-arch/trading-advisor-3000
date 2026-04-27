# Skills Catalog

<!-- generated-by: scripts/sync_skills_catalog.py -->
<!-- source-of-truth: .codex/skills/*/SKILL.md -->
<!-- generated-contract: do not edit manually; run python scripts/sync_skills_catalog.py -->
<!-- catalog-sha256: 06a3397fa721e75ca3dc6d496cac31a8602c52bbae5b439ffd37c3945b186ed3 -->

| skill_id | classification | wave | status | scope | owner_surface | routing_triggers | source | hot_context_policy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ta3000-backtest-validation-and-overfit-control` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 backtest validation, robustness, and overfitting control | `CTX-RESEARCH` | backtest validation; overfit; overfitting; walk-forward; out-of-sample; robustness; slippage; transaction costs; lookahead; survivorship; strategy testing | `repo_local` | `cold-by-default` |
| `ta3000-data-plane-proof` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 authoritative data-plane proof and materialization evidence | `CTX-DATA` | data-plane proof; D:/TA3000-data; Delta proof; _delta_log; row counts; canonical bars; research materialization; real prod tables | `repo_local` | `cold-by-default` |
| `ta3000-governed-package-intake` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 governed package intake and route integrity | `CTX-ORCHESTRATION` | governed package intake; governed flow; package intake; technical intake; product intake; continue route; intake gate | `repo_local` | `cold-by-default` |
| `ta3000-product-surface-naming-cleanup` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 active product-surface naming cleanup | `CTX-DOMAIN` | product surface naming; active surface; phase labels; debug labels; capability names; naming cleanup | `repo_local` | `cold-by-default` |
| `ta3000-quant-compute-methodology` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 vectorbt and pandas-ta-classic methodology for research compute integration | `CTX-COMPUTE` | vectorbt; pandas-ta-classic; pandas_ta_classic; signal matrix; from_signals; from_order_func; technical indicators; derived indicators; research backtest; strategy execution | `repo_local` | `cold-by-default` |
| `ta3000-signal-to-action-lifecycle` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 strategy output delivery from signal to action | `CTX-COMPUTE` | signal delivery; Telegram signal; trading signal; webhook; alert delivery; paper trading; live trading; robot lifecycle; execution chain; signal-to-action | `repo_local` | `cold-by-default` |
| `ta3000-strategy-research-methodology` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 strategy hypothesis design and research protocol methodology | `CTX-RESEARCH` | strategy research; trading hypothesis; strategy intent; market regime; alpha idea; research protocol; strategy acceptance; reject strategy | `repo_local` | `cold-by-default` |
| `ta3000-technical-analysis-system-design` | `KEEP_CORE` | `WAVE_1` | `ACTIVE` | TA3000 technical-analysis signal taxonomy and system design | `CTX-RESEARCH` | technical analysis; trend strategy; momentum strategy; mean reversion; breakout; volatility squeeze; volume confirmation; divergence; market structure; multi-timeframe | `repo_local` | `cold-by-default` |

## Generation
- Generate: `python scripts/sync_skills_catalog.py`
- Check drift: `python scripts/sync_skills_catalog.py --check`
