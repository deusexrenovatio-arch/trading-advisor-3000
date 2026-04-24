# 05. Modules DoD and Parallelization

## 1. Модули

| Модуль | Основной путь | Зависимости | Можно параллелить с |
| --- | --- | --- | --- |
| Foundation contracts | `src/.../contracts` | none | repo scaffolding |
| Data ingestion | `src/.../data_plane/ingestion` | contracts | sidecar stub, docs |
| Canonical builder | `src/.../data_plane/canonical` | contracts, ingestion | feature engine prep |
| Feature engine | `src/.../research/features` | canonical builder | Telegram templates |
| Backtest engine | `src/.../research/backtest` | feature engine | runtime scaffolding |
| Strategy registry | `src/.../runtime/config` | contracts | runtime scaffolding |
| Signal runtime | `src/.../runtime/decision` | canonical, strategy registry | publishing |
| Publishing engine | `src/.../interfaces/telegram` | signal runtime contracts | analytics scaffolding |
| Forward engine | `src/.../research/forward` | signal contracts, canonical | sidecar stub |
| Execution contracts | `src/.../execution/intents` | contracts | StockSharp sidecar |
| Sidecar bridge | `deployment/stocksharp-sidecar` + adapter | execution contracts | analytics |
| Reconciliation | `src/.../execution/reconciliation` | sidecar bridge | review analytics |
| Review/analytics | `src/.../runtime/analytics` | signal events, fills | docs/runbooks |
| Dagster definitions | `src/.../dagster_defs` | data/research modules | infra |
| Spark jobs | `src/.../spark_jobs` | canonical contracts | runtime |

## 2. DoD по модулям

## M0. Bootstrap / shell alignment
### DoD
- подтверждён baseline shell;
- подтверждено, merged/backported ли PR-hardening;
- согласованы product paths;
- добавлены product docs и overlay guidance;
- нет конфликта между shell и product ownership.

---

## M1. Foundation contracts
### DoD
- зафиксированы domain contracts:
  - market data,
  - features,
  - strategy,
  - signal,
  - execution;
- подготовлены contract tests;
- подготовлены initial migrations;
- fixtures и sample payloads существуют;
- docs обновлены.

---

## M2. Data ingestion
### DoD
- historical backfill работает;
- incremental load работает;
- raw append-only semantics соблюдаются;
- ingestion идемпотентен;
- health/logging реализованы;
- integration tests есть.

---

## M3. Canonical data builder
### DoD
- построены `instruments`, `contracts`, `session_calendar`, `bars`, `roll_map`;
- дубли и пропуски валидируются;
- timezone/session rules корректны;
- повторный прогон воспроизводим;
- есть data quality checks.

---

## M4. Feature engine
### DoD
- feature catalog зафиксирован;
- point-in-time correctness доказана тестами;
- versioned feature set реализован;
- feature snapshots доступны в Delta.

---

## M5. Backtest engine
### DoD
- walk-forward pipeline работает;
- комиссии/slippage/session filters учтены;
- output -> `research.signal_candidates`;
- результаты воспроизводимы;
- метрики стратегии считаются.

---

## M6. Strategy registry
### DoD
- есть strategy entities и versions;
- есть risk templates;
- есть lifecycle `draft/shadow/active/deprecated`;
- runtime читает только активные версии.

---

## M7. Signal runtime
### DoD
- signal lifecycle работает;
- stop/target/expiry/validity window выставляются;
- dedup/cooldown работает;
- blackouts around roll/expiry поддержаны;
- event log и snapshot согласованы.

---

## M8. Publishing engine
### DoD
- Telegram create/edit/close/cancel работает;
- idempotency for publications соблюдена;
- message templates versioned;
- failures не ломают signal store.

---

## M9. Forward engine
### DoD
- работает на тех же strategy contracts, что и runtime;
- создаёт forward observations;
- считает MFE/MAE/R;
- не зависит от реального order execution.

---

## M10. Execution contracts
### DoD
- `OrderIntent`, `BrokerOrder`, `BrokerFill`, `PositionSnapshot`, `RiskSnapshot`, `BrokerEvent` формализованы;
- unit/contract tests есть;
- paper/live режимы используют одинаковые contracts.

---

## M11. StockSharp sidecar bridge
### DoD
- Python core может submit/cancel/replace intents;
- broker order updates и fills приходят назад;
- raw broker event log сохраняется;
- sidecar health/readiness endpoints есть;
- secrets не текут в repo.

---

## M12. Reconciliation
### DoD
- расхождения order/fill/position state детектируются;
- incident model определён;
- recovery path описан;
- reconciliation tests есть.

---

## M13. Review / analytics
### DoD
- outcomes считаются по закрытым сигналам;
- есть отчёты по strategy/instrument/timeframe;
- latency report есть;
- metrics export для observability есть.

---

## M14. Dagster / orchestration
### DoD
- data/research assets описаны;
- schedules/sensors есть;
- asset checks включены;
- локальный dev run воспроизводим.

---

## M15. Spark jobs
### DoD
- heavy recompute jobs работают;
- contracts согласованы с Delta layer;
- локальный/CI smoke run предусмотрен;
- runtime от Spark не зависит.

## 3. Параллелизация

### До contracts freeze
Параллелизация ограничена.
Разрешено:
- shell/product docs alignment,
- sidecar spike,
- deployment skeleton,
- observability skeleton.

### После contracts freeze
Можно запустить 4 независимых трека:

#### Track A — Data
- ingestion
- canonical
- Spark
- Dagster data assets

#### Track B — Research
- features
- backtest
- shadow-forward

#### Track C — Runtime
- strategy registry
- signal runtime
- Telegram publishing
- analytics

#### Track D — Execution
- execution contracts
- sidecar bridge
- reconciliation
- paper/live mode

## 4. Последовательность запуска треков

1. Bootstrap - shell alignment
2. Contracts and scaffolding - repo scaffold + boundary freeze
3. Capability bootstrap - Track A/B/C/D can start in parallel with controlled dependencies
4. Shadow replay - integration and system replay
5. Live execution hardening - paper/live hardening
