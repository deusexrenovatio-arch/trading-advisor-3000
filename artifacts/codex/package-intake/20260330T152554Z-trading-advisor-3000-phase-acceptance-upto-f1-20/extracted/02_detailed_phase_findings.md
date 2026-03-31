# Детальные findings по фазам до F1

Дата: 2026-03-30

## 1. Cross-phase verdict
Позитивная часть remediation реально landed:
- physical Delta runtime появился;
- Spark path стал исполняемым, а не plan-string only;
- Dagster перестал быть только asset metadata;
- runtime bootstrap для `staging/production` больше не может тихо упасть в in-memory;
- FastAPI surface реально существует;
- in-repo .NET sidecar больше не является пустым placeholder.

Проблемная часть remediation — governance truth model:
- `completed` / `accepted` в module/route surfaces не гарантируют, что stack truth sources реально согласованы;
- phase-10 / F1 docs делают claims, которые не подтверждаются registry/spec/ADR set;
- validator и CI уже есть, но они не ловят весь класс расхождений.

## 2. Подробно по фазам

### G0 — Claim Freeze and Checklist Repair
**Заявленный контракт фазы:** все reviewed docs должны перестать overclaim-ить, а `STATUS.md`, README-level docs, phase docs и checklists должны стать mutually consistent.

**Что реально есть:**
- введён ограниченный словарь acceptance wording;
- есть baseline doc, который правильно декларирует hierarchy и запрещает переписывать truth source;
- historical acceptance docs действительно были partially downgraded.

**Почему фаза не принимается сейчас:**
1. `stack-conformance-baseline.md` сам пишет, что truth source — это `STATUS.md`, но дальше заявляет, что durable runtime, service/API, Telegram adapter и real sidecar closure «не part of this branch baseline and remain unresolved here». Это конфликтует с текущим `STATUS.md`, где durable runtime и service/API уже `implemented`, а live execution transport baseline тоже `implemented`.
2. `phase10-stack-conformance-reacceptance-report.md` пишет, что `polars`, `duckdb`, `vectorbt`, `alembic`, `opentelemetry`, `aiogram` являются `removed by ADR`. Но registry этого не говорит, stack spec продолжает держать их `chosen`, а ADR set не содержит removal ADRs для этих технологий.

**Вывод:** G0 надо считать reopened. Документы в репо ещё не mutually consistent.

### G1 — Machine-Verifiable Stack-Conformance Gate
**Заявленный контракт фазы:** registry + validator + tests + CI должны fail-closed ловить ghost technologies, unsupported closure claims и drift между docs/runtime/spec/ADR.

**Что реально есть:**
- `registry/stack_conformance.yaml` появился;
- `scripts/validate_stack_conformance.py` реально существует;
- есть targeted tests validator-а;
- validator wired в checks/CI.

**Почему фаза не принимается сейчас:**
1. Coverage validator-а недостаточна для claim surfaces, которые реально влияют на acceptance reading. Он сканирует `README`, `STATUS`, `stack-conformance-baseline`, phase acceptance docs и app checklists, но не защищает от unsupported claims в `phase10-stack-conformance-reacceptance-report.md`, red-team result и module briefs.
2. Из-за этого validator может быть зелёным одновременно с тем, что phase-10 report заявляет ADR-backed removals, которых нет в registry/spec/ADR set.

**Вывод:** G1 landed как механизм, но не закрыт как fail-closed governance phase.

### D1 — Physical Delta Closure
**Почему принимается:**
- runtime code реально пишет/читает Delta через `deltalake`;
- integration proof проверяет существование `_delta_log` и реальные readbacks;
- disprover удаляет physical parquet while metadata remains, и proof правильно ломается.

**Граница принятия:** это не full-system Delta closure; это честно bounded phase closure по agreed phase2 slice.

### D2 — Spark Execution Closure
**Почему принимается:**
- Spark job реально поднимает `SparkSession`, использует `delta-spark`, пишет Delta outputs и валидирует output contract;
- phase brief честно ограничивает scope proof-profile;
- registry продолжает держать Spark как `partial`, что не masquerades it as full-system closure.

**Граница принятия:** phase scope accepted; distributed/release-wide Spark closure вне этой фазы.

### D3 — Dagster Execution Closure
**Почему принимается:**
- в repo есть executable Dagster definitions, proof script и integration test surfaces;
- phase contract ограничен agreed slice и не притворяется platform-wide orchestration closure.

**Граница принятия:** bounded Dagster materialization contour accepted; broader orchestration remains open.

### R1 — Durable Runtime Default and Service Closure
**Почему принимается:**
- `staging/production` runtime path требует `postgres` и валится без DSN;
- in-memory fallback для durable profiles explicitly blocked;
- FastAPI ASGI surface реально существует и wired в тот же profile-aware bootstrap;
- `STATUS.md` и registry по этому срезу согласованы.

**Residual debt:** это не полноценная production API perimeter hardening и не HA-ready Postgres topology.

### R2 — Telegram Adapter Closure
**Почему не принимается:**
- phase contract требует два честных исхода: либо real `aiogram`, либо ADR-backed removal + aligned stack updates;
- фактический runtime использует custom Telegram publication engine;
- `src/.../interfaces/telegram/` по сути пустой;
- stack spec всё ещё говорит `aiogram | chosen`;
- registry говорит `aiogram | planned`;
- route report уже пометил фазу как accepted.

**Вывод:** это textbook example ложного closure. Фаза должна быть reopened.

### E1 — Real .NET Sidecar Closure
**Почему принимается с оговорками:**
- real in-repo `.sln` / `.csproj` / test project / `Program.cs` действительно landed;
- scripts cover build/test/publish/prove path;
- есть compiled-binary Python smoke artifact с успешным boot/health/readiness/metrics/kill-switch/submit/replace/cancel.

**Почему остаётся evidence debt:**
- в открытом artifact tree я вижу только `python-smoke.json`;
- immutable build/test/publish outputs не surfaced рядом с артефактом;
- обычный CI workflow не выполняет dotnet lane, поэтому ongoing reproducibility phase closure не подтверждается автоматически.

**Вывод:** phase functionality landed; для F1 нужно harden evidence discipline.

### S1 — Replaceable Stack Decisions
**Почему не принимается:**
- phase contract требует, чтобы каждый replaceable tech получил один из двух исходов: executable proof или ADR-backed removal;
- stack spec всё ещё держит `Polars`, `DuckDB`, `vectorbt`, `aiogram`, `Alembic`, `OpenTelemetry` как `chosen`;
- registry держит их как `planned`, а не `removed`;
- ADR set не показывает explicit removal ADRs;
- одновременно phase-10 report и red-team review утверждают ADR-backed removal.

**Вывод:** S1 в текущем tree не закрыт. Здесь главный риск самообмана — phase-report declared success without truth-source convergence.

## 3. Самые опасные системные расхождения

### B-G0-01 — baseline ↔ STATUS drift
`stack-conformance-baseline.md` и `STATUS.md` по-разному описывают уже landed surfaces. Это бьёт по самому смыслу truth-source hierarchy.

### B-G0-02 / B-S1-02 — phase10/F1 report ↔ registry/spec/ADR drift
F1 report и red-team report заявляют ADR removals, которых текущий stack spec и ADR catalog не подтверждают.

### B-G1-01 — validator surface coverage gap
Validator fail-closed только на части docs corpus. Acceptance-report level claims могут уйти мимо него.

### B-R2-01 — Telegram stack mismatch
Declared stack и runtime reality расходятся, а phase accepted.

### B-E1-01 — sidecar proof immutability gap
Есть smoke evidence, но нет достаточно жёсткого и постоянно воспроизводимого build/test/publish evidence chain.

## 4. Decision
В текущем tree нельзя считать remediation fully accepted up to F1.
Корректный reading:
- product/runtime foundation materially improved;
- governance/reacceptance foundation ещё не заслуживает доверия как release gate.
