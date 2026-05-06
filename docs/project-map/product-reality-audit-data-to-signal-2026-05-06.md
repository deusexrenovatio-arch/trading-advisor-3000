# Product Reality Audit - Data To Signal - 2026-05-06

## Purpose

This audit answers the current product question:

> What is real today in the chain from data to research to actionable Telegram
> signals?

It follows the product frame agreed during the reset:

```text
Data Plane
  -> Research Factory
  -> validated strategy set
  -> Signal-to-Action lifecycle
  -> Telegram advisory signals
  -> manual repetition by the user
  -> later paper / semi-auto / live robot modes
```

This is not a task-note inventory. Old task notes, package-intake artifacts, and
target-shape specs are treated as historical evidence only.

## Evidence Boundary

Evidence used:

- current `origin/main` after PR #97 was merged;
- current product-plane status and research runbooks;
- current code surfaces for data materialization, research campaigns, vectorbt /
  Optuna backtests, candidate projection, runtime replay, Telegram publication,
  and durable signal state;
- authoritative storage root:
  `D:/TA3000-data/trading-advisor-3000-nightly`;
- direct Delta table reads, `_delta_log` checks, and row counts.

Not used as truth:

- old `docs/tasks/**` state;
- archived product specs;
- stale worktree notes;
- previous audit statements that are contradicted by current code or data.

## Executive Status

| Layer | Current state | Product meaning |
| --- | --- | --- |
| Data Plane | Real and materialized | The product has a usable research data foundation. |
| Research data prep | Real and materialized | The Research Factory can build working frames from canonical data. |
| Strategy registry / search | Implemented and populated | Strategy-family machinery exists; it is not just docs. |
| Vectorbt / Optuna backtest path | Implemented and materially exercised | Current code has an active vectorbt and Optuna path. |
| Strategy validation | Partly implemented | There are rankings and gates, but no accepted capital-allocation standard yet. |
| Signal candidate projection | Implemented in code, not current on disk | This is the main break before runtime usefulness. |
| Runtime signal lifecycle | Implemented | Candidate replay, Telegram publication lifecycle, and durable state exist. |
| Telegram advisory product loop | Not proven | No current real candidate feed and no accepted live Telegram delivery proof. |
| Paper / semi-auto / live execution | Downstream | Correctly remains after validated strategies and advisory proof. |

The short version:

TA3000 is no longer only scaffolding. The data and research factory are real.
The runtime/Telegram chain is real as a technical surface. The missing product
bridge is a current, materialized `research_signal_candidates` output plus an
accepted advisory signal contract and live delivery proof.

## Physical Data Evidence

Authoritative root:

```text
D:/TA3000-data/trading-advisor-3000-nightly
```

Current root families exist:

- `raw/moex/baseline-4y-current`;
- `canonical/moex/baseline-4y-current`;
- `research/gold/current`;
- `research/registry/current`;
- `research/runs`.

Canonical tables:

| Table | Rows | Version |
| --- | ---: | ---: |
| `canonical_bars.delta` | 7,240,768 | 28 |
| `canonical_bar_provenance.delta` | 7,240,768 | 28 |
| `canonical_session_calendar.delta` | 80,902 | 4 |
| `canonical_roll_map.delta` | 18,769 | 4 |

Research gold current tables:

| Table | Rows | Version |
| --- | ---: | ---: |
| `research_datasets.delta` | 2 | 11 |
| `research_instrument_tree.delta` | 17 | 11 |
| `research_bar_views.delta` | 2,031,445 | 11 |
| `research_indicator_frames.delta` | 1,026,585 | 22 |
| `research_derived_indicator_frames.delta` | 1,026,585 | 564 |
| `continuous_front_bars.delta` | 1,026,585 | 0 |
| `continuous_front_indicator_frames.delta` | 1,024,973 | 1 |
| `continuous_front_derived_indicator_frames.delta` | 1,024,973 | 1 |

Research registry current tables:

| Table | Rows | Version |
| --- | ---: | ---: |
| `research_strategy_families.delta` | 10 | 197 |
| `research_strategy_templates.delta` | 10 | 197 |
| `research_strategy_template_modules.delta` | 30 | 197 |
| `research_campaigns.delta` | 13 | 31 |
| `research_campaign_runs.delta` | 22 | 34 |
| `research_rankings_index.delta` | 29,258 | 7 |
| `research_run_stats_index.delta` | 37,322 | 7 |
| `research_strategy_notes.delta` | 22 | 21 |

Important residue:

- `research_feature_frames.delta` still exists physically under
  `research/gold/current` with 2,543,712 rows.
- Do not treat it as an active product route unless a current consumer and
  current architecture document explicitly promote it.

## Research Factory Reality

Current research docs and code point to this primary route:

```text
canonical data
  -> research_datasets / research_bar_views
  -> indicators
  -> derived indicators
  -> strategy registry
  -> StrategyFamilySearchSpec
  -> MTF input resolver
  -> vectorbt SignalFactory.from_choice_func
  -> vectorbt Portfolio.from_signals
  -> optional Optuna study/trial provenance
  -> rankings / findings
  -> projection candidates
  -> runtime
```

Correction to the older 2026-05-05 reset audit:

Current code does have active Optuna/vectorbt research surfaces. The older note
that no active Optuna code was found is superseded by the current `origin/main`
state.

Latest meaningful sampled run:

```text
D:/TA3000-data/trading-advisor-3000-nightly/research/runs/
  moex_approved_subset_optuna_15m_trend_mtf_pullback_research/
  20260430T124414366266Z/run-summary.json
```

Observed status:

| Field | Value |
| --- | ---: |
| Target stage | `backtest` |
| Status | `success` |
| Optimizer trials | 21,120 |
| Vectorbt param results | 6,135 |
| Backtest runs | 6,135 |
| Strategy rankings | 3,703 |
| Trade records | 443,028 |
| Order records | 885,565 |
| Projection-qualified ranking rows | 184 |

This proves a serious research/backtest path. It does not prove current runtime
candidate availability, because the run selected backtest assets and did not
materialize `research_signal_candidates`.

## Signal Candidate Gap

Direct storage search found:

```text
research_signal_candidates.delta directories under D:/TA3000-data: 0
```

Interpretation:

- Candidate projection exists in code and tests.
- Some backtest summaries contain `projection_qualified_count`.
- No current physical `research_signal_candidates.delta` table exists under the
  authoritative data root.

This is the main product gap between Research Factory and Telegram advisory
delivery.

The next useful proof is not another broad audit. It is a projection proof that
materializes candidate rows from current ranked research output and verifies:

- output path;
- `_delta_log`;
- row count;
- schema;
- candidate payload fields required by runtime;
- explicit reason when zero rows qualify.

## Runtime And Telegram Reality

What is implemented:

- candidate replay through the runtime decision engine;
- stateful signal lifecycle: publish, update/edit, close, cancel, expire;
- idempotent publication by signal identity;
- signal-event history;
- Telegram publication engine;
- real Telegram Bot API transport when a bot token is configured;
- durable `PostgresSignalStore` for staging/production profiles;
- runtime health/readiness API smoke coverage.

Operational binding rules already exist:

- `TA3000_TELEGRAM_CHANNEL` is mandatory for runtime publication binding;
- `TA3000_TELEGRAM_BOT_TOKEN` is required for live-real Telegram evidence;
- staging/production runtime profiles must use durable Postgres state;
- publication evidence must fail closed when live binding is not real.

What is not proven as a product loop:

- no current physical signal-candidate feed;
- no accepted Telegram message template for manual trading;
- no current real Telegram bot/channel evidence tied to fresh candidates;
- no schedule or silence monitor for periodic signal scans;
- no manual feedback loop for followed, skipped, modified, or closed signals;
- no accepted capital-risk standard for promoting strategies beyond research.

Runtime is therefore implemented, but not yet the user's working advisory
product.

## Strategy Acceptance Gap

The current research system can rank and filter strategy candidates, but the
product still lacks a clear acceptance packet for risking capital.

A promoted strategy should require at least:

- written hypothesis and invalidation logic;
- closed-bar causal data proof;
- in-sample / out-of-sample or walk-forward evidence;
- cost, slippage, turnover, and liquidity sensitivity;
- per-instrument and per-regime breakdown;
- parameter stability evidence;
- examples of trades and non-trades;
- explicit decision: reject, research-only, paper-signal, paper-trade,
  live-candidate, or live-enabled.

Until this exists, high-ranking research output should be treated as a candidate
for investigation, not as a trade recommendation.

## Live Decision Boundary

Historical and batch data are research/backfill inputs. They are not valid as
intraday live decision data.

The strict live boundary remains:

```text
StockSharp -> QUIK -> broker/Finam -> live market state -> live decision
```

If broker live data is unavailable or unconfirmed, live decision publication
must remain fail-closed.

Near-term Telegram advisory mode can still exist before live execution, but it
must be explicit about its mode, timing, data freshness, validity window, and
manual nature.

## Current Product Gaps

| Gap | Why it matters | Next proof |
| --- | --- | --- |
| No current `research_signal_candidates.delta` | Runtime has no current research-backed input. | Materialize projection asset from latest ranked output. |
| No advisory signal contract accepted | Telegram messages may be technically delivered but not useful for manual trading. | Define entry/exit/invalidate/hold payload and lifecycle. |
| No live Telegram evidence tied to candidates | Adapter exists, but product channel is not proven. | Send or dry-run with fail-closed live binding proof after candidate materialization. |
| Strategy validation not capital-ready | Rankings are not enough to risk capital. | Create repeatable validation packet and promotion labels. |
| Legacy feature table residue | Old data may confuse active route reading. | Classify as archive/residue unless current consumers require it. |
| Calendar spread / volume profile not active | Important future strategy families need more data-plane work. | Treat as future capability slices, not current readiness. |

## Recommended Next Sequence

1. Projection proof:
   materialize `research_signal_candidates.delta` from current ranking output or
   record why zero candidates qualify.
2. Advisory signal contract:
   define the Telegram-facing signal payload for watch, enter, exit, reduce,
   invalidate, and no-trade states.
3. Runtime replay proof:
   replay materialized candidates through the current runtime in advisory or
   shadow mode and persist publication/state evidence.
4. Telegram live binding proof:
   verify real bot/channel binding only after the message contract and candidate
   feed are in place.
5. Strategy validation packet:
   define the evidence threshold for `research-only`, `paper-signal`,
   `paper-trade`, and later `live-candidate`.
6. Future execution:
   only after the advisory loop is stable, decide which parts move to paper,
   semi-auto, and live robot modes.

## What Not To Do Next

- Do not return to old task-note archaeology as the main status source.
- Do not treat `projection_qualified_count` in a backtest digest as a persisted
  runtime candidate feed.
- Do not promote Telegram adapter existence into product readiness.
- Do not start live execution work before strategy validation and advisory
  delivery are proven.
- Do not treat archived specs or obsolete acceptance checklists as current
  product state.
