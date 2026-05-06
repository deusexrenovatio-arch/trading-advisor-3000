# 09. Constraints, Risks and NFRs

## 1. Ограничения

### Архитектурные
- верхнеуровневую DFD не менять;
- control plane и application plane не сливать;
- Spark не встраивать в live decision path;
- StockSharp не использовать для strategy logic;
- live execution — только на реальных контрактах.

### Процессные
- task lifecycle обязателен;
- loop/pr/nightly gates обязательны;
- shell-sensitive patches выделяются отдельно;
- product docs не заменяют shell hot docs.

### Данные
- point-in-time correctness обязателен;
- raw layer append-only;
- signal/execution layer хранит snapshot + events;
- broker is source of truth in live mode.

## 2. Основные риски

| Риск | Проявление | Снижение |
| --- | --- | --- |
| Shell/app contamination | product logic в shell paths | строгие ownership boundaries |
| Root configs misuse | конфликт с CTX-CONTRACTS | app config держать вне root `configs/` |
| Roll/expiry mistakes | некорректные live signals | `roll_map` + blackout rules |
| Look-ahead in research | переоценка стратегий | PIT tests + frozen dataset versions |
| Broker/runtime mismatch | неверный local position state | reconciliation + broker event log |
| Sidecar drift | разные модели между Python и sidecar | transport-neutral contracts + sync tests |
| Delta over-complexity | indicator incompatibility | conservative table capability policy |
| CI/gate bypass | неустойчивый delivery process | mandatory shell gate flow |
| Secrets leakage | компрометация broker/API access | secret manager + no repo secrets |

## 3. NFRs

### Maintainability
- читаемая модульная структура;
- изоляция planes;
- ADR-driven changes on key architecture surfaces;
- минимизация shell coupling.

### Scalability
- data plane масштабируется отдельно от runtime;
- новые активы/стратегии добавляются расширением, а не переписыванием;
- sidecar можно заменить при сохранении execution contracts.

### Reliability
- idempotent processing;
- replayable system flows;
- explicit reconciliation incidents;
- observability from signal to fill.

### Auditability
- event logs для сигналов и исполнения;
- reproducible backtests;
- PR evidence and phase acceptance records;
- documented runbooks.

### Security
- secrets outside repo;
- least-privilege for MCP and DB access;
- read-only MCP for data stores by default in dev/stage.

### Performance
- bar-based intraday latency acceptable for advisory/paper/live MVP;
- heavy recompute вынесен в Spark jobs;
- local debug должен работать на single-node tooling.

## 4. Негативный список

На текущем этапе запрещено:
- писать собственный full trading engine с нуля;
- строить live path на continuous contracts;
- сливать product planning with `plans/*` business artefacts;
- использовать shell memory as runtime store;
- подменять broker confirmations локальными догадками.
