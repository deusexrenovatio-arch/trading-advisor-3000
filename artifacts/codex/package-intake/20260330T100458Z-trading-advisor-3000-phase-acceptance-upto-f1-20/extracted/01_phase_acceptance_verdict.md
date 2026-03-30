# Приёмка фаз до F1

Дата: 2026-03-30

## Итоговый вердикт

Я принимаю фазы не как narrative, а как проверяемые bounded closures. По этому стандарту результат такой:

- **G0 — не принят (reopened)**. В repo есть baseline/vocabulary docs, но truth-source контур сейчас не взаимоcогласован: baseline конфликтует с STATUS, а phase10/F1 документы содержат unsupported claims про ADR-removal технологий.
- **G1 — не принят**. Registry/validator/tests/CI landed, но gate не fail-closed на все claim-carrying surfaces: phase10/red-team can overclaim while validator still passes.
- **D1 — принят в границах фазы**. Physical Delta runtime landed: code writes/reads Delta tables and disprover removes parquet data while metadata remains.
- **D2 — принят в границах фазы**. Executable Spark job landed for canonical bars proof slice; broader distributed/orchestrated closure remains outside accepted contour.
- **D3 — принят в границах фазы**. Dagster definitions/proof/test surfaces exist for bounded materialization contour; broader orchestration closure remains partial.
- **R1 — принят**. Staging/production profiles now fail-closed to Postgres, and FastAPI ASGI runtime is real and smoke-covered.
- **R2 — не принят**. Aiogram mismatch remains unresolved: custom Telegram engine exists, aiogram still chosen in spec, registry says planned, yet phase route says accepted.
- **E1 — принят с оговорками**. Real in-repo .NET sidecar and compiled-binary Python smoke are present, but immutable build/test/publish evidence is not durably surfaced in the repo/CI.
- **S1 — не принят**. Ghost chosen technologies remain: spec still marks several tools chosen, registry marks them planned, and no ADR-backed removals are visible in the stack ADR set.

## Общая картина

Репозиторий теперь намного ближе к честной архитектурной базе, чем в исходной приёмке: появились реальный Delta runtime, реальный Spark path, Dagster proof contour, fail-closed bootstrap для Postgres/FastAPI и реальный in-repo .NET sidecar. Но процесс доказательства всё ещё дырявый: phase briefs и route reports говорят `completed/accepted`, в то время как live truth sources продолжают содержать `planned/partial/not accepted` либо прямо расходятся между собой.

## Разбор по фазам

### G0 — не принят (reopened)
**Что обещала фаза:** claim freeze and checklist repair.
**Решение:** `not_accepted`.
**Почему:** В repo есть baseline/vocabulary docs, но truth-source контур сейчас не взаимоcогласован: baseline конфликтует с STATUS, а phase10/F1 документы содержат unsupported claims про ADR-removal технологий.
**Ключевые блокеры:** B-G0-01; B-G0-02.
**Влияние на F1:** critical.

### G1 — не принят
**Что обещала фаза:** machine-verifiable stack-conformance gate.
**Решение:** `not_accepted`.
**Почему:** Registry/validator/tests/CI landed, но gate не fail-closed на все claim-carrying surfaces: phase10/red-team can overclaim while validator still passes.
**Ключевые блокеры:** B-G1-01; B-G1-02.
**Влияние на F1:** critical.

### D1 — принят в границах фазы
**Что обещала фаза:** physical Delta closure for agreed slice.
**Решение:** `accepted_bounded`.
**Почему:** Physical Delta runtime landed: code writes/reads Delta tables and disprover removes parquet data while metadata remains.
**Ключевые блокеры:** нет внутри согласованного phase scope; остаётся только residual debt вне границ этой фазы.
**Влияние на F1:** supporting.

### D2 — принят в границах фазы
**Что обещала фаза:** Spark execution closure for agreed slice.
**Решение:** `accepted_bounded`.
**Почему:** Executable Spark job landed for canonical bars proof slice; broader distributed/orchestrated closure remains outside accepted contour.
**Ключевые блокеры:** нет внутри согласованного phase scope; остаётся только residual debt вне границ этой фазы.
**Влияние на F1:** supporting.

### D3 — принят в границах фазы
**Что обещала фаза:** Dagster execution closure for agreed slice.
**Решение:** `accepted_bounded`.
**Почему:** Dagster definitions/proof/test surfaces exist for bounded materialization contour; broader orchestration closure remains partial.
**Ключевые блокеры:** нет внутри согласованного phase scope; остаётся только residual debt вне границ этой фазы.
**Влияние на F1:** supporting.

### R1 — принят
**Что обещала фаза:** durable runtime default and service closure.
**Решение:** `accepted`.
**Почему:** Staging/production profiles now fail-closed to Postgres, and FastAPI ASGI runtime is real and smoke-covered.
**Ключевые блокеры:** нет внутри согласованного phase scope; остаётся только residual debt вне границ этой фазы.
**Влияние на F1:** critical.

### R2 — не принят
**Что обещала фаза:** Telegram adapter closure.
**Решение:** `not_accepted`.
**Почему:** Aiogram mismatch remains unresolved: custom Telegram engine exists, aiogram still chosen in spec, registry says planned, yet phase route says accepted.
**Ключевые блокеры:** B-R2-01; B-R2-02.
**Влияние на F1:** critical.

### E1 — принят с оговорками
**Что обещала фаза:** real .NET sidecar closure.
**Решение:** `accepted_with_conditions`.
**Почему:** Real in-repo .NET sidecar and compiled-binary Python smoke are present, but immutable build/test/publish evidence is not durably surfaced in the repo/CI.
**Ключевые блокеры:** B-E1-01.
**Влияние на F1:** critical.

### S1 — не принят
**Что обещала фаза:** replaceable stack decisions.
**Решение:** `not_accepted`.
**Почему:** Ghost chosen technologies remain: spec still marks several tools chosen, registry marks them planned, and no ADR-backed removals are visible in the stack ADR set.
**Ключевые блокеры:** B-S1-01; B-S1-02.
**Влияние на F1:** critical.

## Что это значит practically

1. Нельзя опираться на `completed` в module briefs или `accepted` в route reports как на конечное доказательство. Нужно смотреть на truth-source bundle: `STATUS.md`, `registry/stack_conformance.yaml`, stack spec, ADR set, runtime code, tests, CI/gates.
2. Фазы `D1/D2/D3/R1` реально двигают систему вперёд и их не нужно откатывать.
3. Фазы `G0/G1/R2/S1` в текущем дереве должны считаться reopened/rejected до выравнивания truth-source и fail-closed проверки.
4. Фаза `E1` функционально landed, но для F1 её надо усилить immutable evidence и CI replay.