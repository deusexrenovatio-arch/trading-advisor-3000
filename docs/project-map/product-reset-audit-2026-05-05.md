# TA3000 Product Reset Audit - 2026-05-05

## Product Frame

This audit uses the reset frame:

```text
Trading Advisor 3000 =
  Research Factory
  -> validated strategy set
  -> Signal-to-Action lifecycle
  -> Telegram advisory signals
  -> manual repetition by the user
  -> later paper / semi-auto / live robot modes
```

Out of scope for the current product direction:

- news and fundamental analysis;
- broad advisor cockpit UI;
- HFT;
- live execution before a credible strategy layer exists.

The near-term business goal is capital growth through technically grounded,
repeatable strategies that can emit actionable entry/exit lifecycle signals.

## Audit Basis

This is an audit of the current `origin/main` product surface plus authoritative
runtime data roots. Stale worktrees are treated as candidates, not project truth.

Evidence used:

- current product-plane docs;
- current tracked code and tests;
- Serena symbol overview for key data/research/runtime/execution modules;
- `D:/TA3000-data` Delta roots and row counts;
- focused smoke test run.

Focused smoke:

```text
tests/product-plane/unit/test_delta_runtime.py
tests/product-plane/unit/test_moex_canonical_route.py
tests/product-plane/unit/test_research_campaign_runner.py
tests/product-plane/unit/test_research_ranking_projection.py
tests/product-plane/unit/test_runtime_components.py
tests/product-plane/integration/test_runtime_lifecycle.py

Result: 38 passed in 4.22s
```

Tracked surface counts:

| Surface | Tracked files |
| --- | ---: |
| data plane | 30 |
| research plane | 61 |
| runtime plane | 26 |
| execution plane | 20 |
| contracts | 69 |
| product-plane tests | 170 |
| Dagster definitions | 5 |
| Spark jobs | 5 |

## Readiness Scale

| Score | Meaning |
| ---: | --- |
| 0 | absent |
| 1 | contracts/docs only |
| 2 | code and unit tests exist |
| 3 | materialized or integration proof exists |
| 4 | usable for strategy/product work with known limits |
| 5 | operationally routine and production-ready for the current goal |

## Executive Verdict

| Layer | Score | Verdict |
| --- | ---: | --- |
| Data plane | 3.5 | Real and materialized, but still needs a strategy-readiness QC definition. |
| Research data prep | 4 | Strongest layer: real Delta outputs exist and are reusable. |
| Strategy research factory | 3 | Registry/backtest/ranking/projection exist, but strategy hypothesis discipline is incomplete. |
| Backtest validation | 2.5 | Policy scoring exists, but robustness and capital-risk acceptance are not yet enough. |
| Signal-to-Action runtime | 2.5 | Contracts, replay engine, store, Telegram adapter exist; operational Telegram loop is not live-product ready. |
| Telegram manual advisory mode | 2 | Shape exists, but no proved scheduled real-signal delivery workflow. |
| Paper / semi-auto / live robot | 1.5 | Scaffolding exists; correctly should remain downstream. |

Overall:

TA3000 already has a credible technical foundation for `Research Factory`, but
not yet a credible product loop for capital growth. The next work should not be
generic cleanup. It should close the gap between correct data, tested strategies,
and Telegram-delivered trade lifecycle signals.

## Data Plane Audit

What exists:

- MOEX data-plane code exists under `product_plane/data_plane`.
- Historical canonical route includes QC gates, contract compatibility,
  runtime decoupling, selected source interval logic, canonical bars,
  provenance, session calendar sidecars, and roll-map sidecars.
- Delta runtime helper supports read/write/append/delete/count/schema operations.
- `D:/TA3000-data` is the active authoritative root.

Authoritative Delta evidence:

| Table | Rows | Version |
| --- | ---: | ---: |
| `canonical_bars.delta` | 7,224,656 | 24 |
| `canonical_session_calendar.delta` | 80,804 | 2 |
| `canonical_roll_map.delta` | 18,753 | 2 |
| `research_datasets.delta` | 2 | 9 |
| `research_bar_views.delta` | 2,029,833 | 9 |
| `research_indicator_frames.delta` | 1,024,973 | 12 |
| `research_derived_indicator_frames.delta` | 1,024,973 | 554 |
| `continuous_front_bars.delta` | 1,024,973 | 0 |
| `continuous_front_indicator_frames.delta` | 1,024,973 | 1 |

Gaps:

- There is not yet a compact product-level definition of "data is strategy-ready".
- Calendar spread data is absent in current main: `calendar_spread` has zero source hits.
- Volume profile is absent in current main: `volume_profile` has zero source hits. A candidate exists in the fresh `codex/volume-plan` worktree, but it is not accepted code.
- The data plane is strong for MOEX/continuous-front research, but not yet complete for the broader strategy set the user described: calendar futures spread/arbitrage, position trading, and imbalance-style strategies.

Verdict:

Data plane is real, not just scaffolding. But it is not done until strategy-readiness
QC covers the instruments and strategy families actually intended for research.

## Research Factory Audit

What exists:

- Campaign runner exists: `run_campaign`.
- Campaign config exists: `fut_br_base_15m.explore.yaml`.
- Data prep, strategy registry refresh, backtest, ranking, projection stages are represented in code.
- Strategy family modules exist:
  - breakout;
  - MA cross;
  - mean reversion;
  - MTF pullback;
  - squeeze release.
- Backtest engine has signal generation, vectorbt portfolio execution, trades,
  orders, drawdowns, rankings, and candidate projection surfaces.
- Registry/current has real strategy artifacts:
  - `research_strategy_families.delta`: 10 rows;
  - `research_strategy_templates.delta`: 10 rows;
  - `research_strategy_template_modules.delta`: 30 rows;
  - `research_campaign_runs.delta`: 22 rows;
  - `research_rankings_index.delta`: 29,258 rows;
  - `research_run_stats_index.delta`: 37,322 rows.
- Research runs root has 289 Delta tables, including backtest, trade, ranking,
  optimizer-study, optimizer-trial, and candidate-like outputs.

Gaps:

- Current source has no active `optuna`, `optimizer`, `study`, or `trial` code hits under source/campaign/test paths, despite existing Optuna-named persisted benchmark artifacts. This needs reconciliation before claiming Optuna is an active current-code capability.
- Strategy families exist more as technical implementations than as falsifiable strategy hypotheses tied to capital-growth goals.
- Strategy specs do not yet look like a strong research backlog with explicit market behavior, regime, invalidation, expected failure modes, and acceptance evidence.
- Calendar spread/arbitrage strategies are not present in current main.
- Volume-profile/POC/cluster features are not present in current main.

Verdict:

Research Factory exists structurally. It can run campaigns and persist evidence.
The missing piece is not more scaffolding; it is stronger strategy research protocol
and a narrowed backlog of strategies worth validating.

## Validation Audit

What exists:

- Ranking and projection policies exist.
- Campaign configs include:
  - out-of-sample requirement;
  - stress slippage;
  - minimum slippage score;
  - projection selection policy.
- Code includes walk-forward / out-of-sample concepts in campaign and dataset surfaces.
- Tests cover ranking/projection and campaign runner behavior.

Gaps:

- Validation is still not enough to decide "I should risk capital on this".
- No single acceptance packet is defined for a promoted strategy.
- Missing or underdeveloped product-level gates:
  - walk-forward stability;
  - parameter stability;
  - slippage/commission sensitivity by instrument and timeframe;
  - drawdown and capital-at-risk limits;
  - liquidity and churn filters;
  - regime split reporting;
  - rejected-strategy reports.
- Strategy promotion events exist in persisted runs, but observed rows include zero promotion events in sampled runs. That is acceptable for smoke runs, but not enough for product readiness.

Verdict:

Validation exists as machinery, not yet as a capital-allocation standard.

## Signal-To-Action Audit

What exists:

- Runtime contracts exist:
  - `signal_candidate.v1`;
  - `runtime_signal.v1`;
  - `signal_event.v1`;
  - `decision_publication.v1`;
  - `telegram_operation.v1`;
  - `order_intent.v1`;
  - broker and position snapshots.
- Runtime components exist:
  - `SignalRuntimeEngine`;
  - `InMemorySignalStore`;
  - `TelegramPublicationEngine`;
  - runtime stack builder;
  - runtime API smoke and lifecycle tests.
- Runtime lifecycle design includes:
  - strategy activation;
  - explicit signal state;
  - idempotent Telegram create by `signal_id`;
  - deterministic replay order;
  - close/cancel signal flows.
- Focused runtime lifecycle tests passed.

Gaps:

- Current runtime docs explicitly keep external Telegram API network calls out of MVP scope.
- There is no proved scheduled path from real current research candidates into a real Telegram bot/channel.
- Signal content is not yet product-shaped enough for manual trading:
  - entry zone;
  - invalidation;
  - stop;
  - target;
  - time stop;
  - validity window;
  - "hold/reduce/exit/invalidated" updates;
  - outcome summary.
- Runtime is replay-capable, not yet an operator-grade daily signal service.

Verdict:

Signal-to-Action has a good skeleton. It is not yet the product loop.

## Telegram Manual Advisory Audit

What exists:

- Telegram operation contracts.
- Telegram publication engine with publish/edit/close/cancel methods.
- Idempotency model.
- Runtime integration tests around lifecycle.

Gaps:

- No live Telegram delivery proof in the current audit.
- No message template accepted for the user's actual manual execution workflow.
- No daily/periodic schedule policy.
- No silence monitor: "expected signal scan did not run" or "no signals because filters blocked everything".
- No manual-action feedback loop: whether the user followed the signal, skipped it, modified it, or closed differently.

Verdict:

Telegram is implemented as a technical adapter surface, not yet as the user's main
product interface.

## Execution / Robot Audit

What exists:

- Paper broker engine.
- Controlled live execution engine.
- Live execution bridge with feature flags, adapter checks, retries, telemetry,
  and health.
- Reconciliation module for positions, orders, fills.
- Execution-related contract fixtures and tests.

Gaps:

- This is intentionally not the next product priority.
- Execution should not advance until at least one strategy has a credible
  research/validation package and the Telegram advisory lifecycle works.
- Live execution requires risk gates, kill switch, stale data gate, duplicate
  order gate, and broker-specific operational evidence.

Verdict:

Execution scaffolding exists. Keep it downstream.

## Biggest Mismatches

1. Product goal is clearer than the current docs.
   The repo still reads like a broad product-plane platform. The actual goal is
   narrower: research factory leading to Telegram-managed manual trades.

2. Data and research are ahead of product delivery.
   There are real Delta roots and research artifacts, but not yet a daily signal
   loop that helps the user trade.

3. Optuna state is inconsistent.
   Persisted benchmark artifacts mention Optuna, but current source scan found
   no active optimizer/Optuna code. This needs a focused reconciliation before
   optimizer capability is treated as current.

4. Strategy layer lacks product-grade hypotheses.
   Strategy family code exists, but the next value comes from explicit hypotheses,
   rejection criteria, and validation packages.

5. Telegram exists technically, but not as the product UX.
   The important missing part is not the adapter; it is the lifecycle message and
   operator loop.

6. Documentation noise now hides product reality.
   Old TZs, package-intake artifacts, active task notes, target-shape specs, and
   partial implementation records can make obsolete intent look current. The
   cleanup policy is captured in
   `docs/project-map/documentation-reality-audit-2026-05-05.md`.

## Recommended Next Work

### Slice 0: Documentation Reality Cleanup

Before large new planning work, label and reduce the documentation surfaces that
distort the current product state. Do not start with mass deletion; first classify
what is current truth, historical evidence, superseded intent, or safe delete.

### Slice 1: Product Reset Document

Write the new product truth:

- TA3000 is Research Factory -> Signal-to-Action.
- Telegram manual advisory mode is the first delivery mode.
- Cockpit, news, fundamentals, and live robot are out of current scope.
- Define the first strategy-ready data criteria.

### Slice 2: Data Plane Strategy-Readiness Gate

Create a small acceptance catalog for:

- MOEX canonical bars;
- session calendar;
- roll map;
- continuous-front bars;
- volume/open-interest availability;
- current missing areas: volume profile and calendar spread.

Done means the strategy layer can ask: "can I research this strategy family on this instrument/timeframe?"

### Slice 3: Strategy Research Backlog

Create 3-5 strategy hypotheses, not implementations:

- trend / MTF pullback;
- mean reversion;
- breakout / squeeze release;
- volume-profile or POC reaction after the volume-profile candidate is accepted;
- calendar-spread/arbitrage only after data exists.

Each must define intent, regime, required columns, entry/exit, invalidation,
risk concept, and rejection test.

### Slice 4: Validation Packet

Define one standard promotion packet:

- metrics;
- OOS / walk-forward;
- costs;
- drawdown;
- stability;
- regime split;
- examples of accepted and rejected trades;
- reason for Telegram eligibility.

### Slice 5: Telegram Signal Contract

Turn `signal_candidate` into a user-facing trade lifecycle:

- setup detected;
- entry triggered;
- position active;
- hold/reduce/exit;
- invalidated;
- closed with outcome.

This should happen before any live robot work.

## Current Product Readiness Statement

TA3000 is not at "ready to trade capital" yet.

It is at:

```text
data/research platform exists
  -> research factory partly exists
  -> validation standard incomplete
  -> Telegram lifecycle skeleton exists
  -> live manual advisory product not yet proven
```

The next valuable work is not repo cleanup. It is to connect the existing data
and research machinery to a small number of explicit, falsifiable strategy
hypotheses and then to a Telegram lifecycle that can be followed manually.
