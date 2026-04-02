# 06. Phases and Acceptance Gates

## 1. Правило фаз

Каждая фаза должна быть:
- изолирована,
- проверяема,
- не смешивать shell и product без необходимости,
- завершаться явным acceptance gate.

## 2. Общая gate модель

### Local gate
- task session started
- relevant unit/contract/integration tests green
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`

### PR gate
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
- все phase deliverables выполнены
- docs updated
- нет незакрытых critical TODO

### Hygiene gate
- `python scripts/run_nightly_gate.py --from-git --git-ref HEAD` when applicable
- dashboard refresh if changed shell reporting or metrics artifacts

## 3. Фазы

## Phase 0 — Shell alignment and repo landing

### Цель
Зафиксировать, как product development встраивается в существующий AI shell.

### Scope
- docs package added;
- AGENTS overlay defined;
- product path policy fixed;
- decision on PR #1 baseline documented;
- no app code yet beyond skeleton if needed.

### Deliverables
- merged/aligned docs;
- repo structure decision;
- phase plan;
- acceptance checklist.

### Acceptance
- root shell docs referenced correctly;
- phase package committed;
- no uncontrolled edits to shell-sensitive paths;
- loop gate green.

---

## Phase 1 — Contracts and scaffolding

### Цель
Заморозить базовые product contracts и skeleton package layout.

### Deliverables
- contracts package;
- migrations skeleton;
- fixture payloads;
- `src/trading_advisor_3000/AGENTS.md`;
- `tests/product-plane/contracts/*`;
- base app package structure.

### Acceptance
- contract tests green;
- loop gate green;
- PR gate green;
- docs updated.

### Parallelization after close
Track A/B/C/D may start.

---

## Phase 2A — Data plane MVP

### Deliverables
- ingestion;
- canonical builder;
- Delta schemas;
- initial Dagster assets;
- minimal Spark job skeleton.

### Acceptance
- sample backfill works;
- canonical bars built for whitelist instruments;
- data quality tests green;
- loop gate green.

---

## Phase 2B — Research plane MVP

### Deliverables
- feature engine;
- feature store contract;
- backtest engine;
- sample strategy implementations;
- candidate outputs.

### Acceptance
- backtest reproducible;
- point-in-time tests green;
- research outputs written to Delta;
- loop gate green.

---

## Phase 2C — Runtime MVP

### Deliverables
- strategy registry;
- signal runtime;
- signal store;
- Telegram publish/edit/close;
- runtime APIs.

### Acceptance
- advisory signal lifecycle works end-to-end on replay;
- runtime tests green;
- idempotent publication confirmed;
- loop gate green.

---

## Phase 2D — Execution MVP

### Deliverables
- execution contracts;
- paper broker mode;
- StockSharp sidecar stub;
- broker event log;
- reconciliation skeleton.

### Acceptance
- paper mode works from OrderIntent to PositionSnapshot;
- contract tests green;
- loop gate green.

---

## Phase 3 — Shadow-forward and system integration

### Deliverables
- shadow-forward engine;
- integrated replay scenario:
  market data -> signal -> publication -> forward outcome;
- first system runbook.

### Acceptance
- end-to-end replay green;
- analytics outcome produced;
- PR gate green.

---

## Phase 4 — Live execution integration (controlled)

### Deliverables
- StockSharp <-> Python bridge;
- QUIK/Finam integration under feature flags;
- live sync/reconciliation;
- incident handling runbook.

### Acceptance
- test/live-sim environment connected;
- broker order/fill sync proven;
- reconciliation incidents surfaced correctly;
- PR gate green.

---

## Phase 5 — Review, analytics, observability

### Deliverables
- MFE/MAE/R metrics;
- strategy/instrument dashboards;
- latency metrics;
- Prometheus/Grafana/Loki plumbing;
- app runbooks.

### Acceptance
- dashboards render;
- metrics exported;
- observability smoke tests green.

---

## Phase 6 — Operational hardening

### Deliverables
- failure recovery paths;
- retry/idempotency hardening;
- security and secrets handling;
- DR notes;
- production-like compose profile.

### Acceptance
- failure scenarios replayed;
- secrets policy documented;
- PR gate green.

---

## Phase 7 — Scale-up readiness

### Deliverables
- extension seams for new assets/providers;
- documented path for fundamentals/news;
- documented path for additional adapters;
- performance notes and backlog.

### Acceptance
- ADRs updated;
- architecture docs updated;
- no blocking refactor identified for next expansion wave.

## 4. Фазные чек-листы для Codex

### Перед началом фазы
- открыть task session;
- перечитать active phase;
- определить change surface;
- разбить mixed patch sets.

### Перед сдачей фазы
- обновить docs;
- прогнать local gate;
- прогнать phase-specific tests;
- прогнать PR gate;
- сформировать PR summary с phase acceptance evidence.

## 5. Особое правило по shell-sensitive фазам

Если фаза затрагивает:
- `.cursor/skills/*`,
- `docs/agent/*`,
- `scripts/*`,
- root `configs/*`,
- `.github/workflows/*`,

то она оформляется как отдельный governance patch.
Нельзя смешивать её с основной продуктовой реализацией.
