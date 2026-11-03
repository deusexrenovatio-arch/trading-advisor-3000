# Реализация, открытые решения и приёмка расширения

Дата: 2026-09-30. Change surface: `product-plane`. Solution Intent: `target`.
Документ задаёт работу и проверочные сценарии; ни один пункт ниже не является
заявлением о выполненной runtime/data проверке.

## Последовательность поставки

| Этап | Результат | Условие перехода |
| --- | --- | --- |
| Контрактная спецификация этапа | Только зависимые grain/method/profile decisions; DTO/JSON schemas, examples, миграционный план | Закрыты Q-решения именно этого этапа; одна schema authority, negative parity cases и compatibility proof |
| Journal + ручной Intake | Доставка локального v1 baseline: формы, факты/заметки, revisions, command receipts и backup | Запись факта без Delta/LLM; duplicate/conflict/restart/restore; source-level fixes Stage1 подтверждены для поставляемого immutable кода |
| Общий ProductDataAPI: family → contract → economics | Один facade над публичными Market/Research readers; dated selection и pinned economics | Default/manual/historical выбор, spot unresolved, as-of/units/availability и source evidence |
| Calculator: первый data pilot | Fixed quantity + ручные native уровни; сохранить calculation bundle в новой Journal revision | Принята method version; независимый money case; missing costs/economics; смена контракта/input hash; источник числа и replay сохранённых inputs |
| Факторный DataContext и сценарии | Отдельный analysis series, EMA50/EMA100/ATR14 и частичные результаты | Физически доказаны coherent versions, warmup/closed bars, compatible price-space mapping и методики; без скрытой материализации |
| Review + AI Intake | Замороженный report profile, LM Studio adapter, corpus eval | Схемы/ссылки проходят; измерено качество/latency; offline inference подтверждён |
| Frontend + lineage | Сквозной workflow и «От числа к источнику» | UI показывает сохранённые версии; каждая ветвь соответствует реальным dependencies |
| Локальная приёмка | Один воспроизводимый end-to-end демонстрационный сценарий | Real published input, clean restart, восстановление запросов, backup/restore и bounded performance |

Frontend read-model fixtures можно готовить раньше, но синтетические примеры не
закрывают приёмку интеграции. Ручной ввод позволяет проверить Journal до выбора
модели; это последовательность разработки, а не отказ от принятого AI Intake.
Восстановление data pipeline не смешивается с UI/Journal patch: ownership и
публикация проверяются существующим data-plane процессом.
Journal/manual slice не зависит от завершения data/AI этапов. Конкретный порядок
и первый сквозной сценарий сохраняет
[план интеграции](docs/architecture/product-plane/trade-workspace/journal-integration-plan.md).

## Состояние реализации на 2026-11-04

Это статический source inventory, не свежая проверка HTTP, PostgreSQL, модели или
данных. Main baseline `92160367` не содержит Workspace API, SQL-миграций или frontend.
Принятый полный target остаётся planned; локальные реализации не объявляются
доставленными, допущенными или развёрнутыми одним этим документом.

| Локальный источник | Что подтверждено чтением выбранного кода | Граница утверждения |
| --- | --- | --- |
| Journal baseline `360c18da416220ef734f02ae00948babd46178f3` | Отдельная исходная реализация manual Journal | Историческая точка отсчёта, не текущий deployment |
| Stage1 `8b617ed117912d2f897cf1aabc94df727c051f5b` | Схемы/DTO и negative parity tests; публичный `check_ready` проверяет packaged migrations и checksums read-only; revision/receipt сохраняются атомарно | Оба code gap от 2026-10-15 устранены в этом локальном source; перенос в main и повторная приёмка отдельно |
| ProductData `3cd217a710f64f8f78b22a1bf95d0106cd875b15` | Typed data adapters, bounded native charts и factual execution markers; ручная позиция, средняя цена и sourced margin | Это не полный Calculator: CalculationBundle, native уровни/риск и сохранение расчётного пакета ещё не приняты |
| Normalized Intake `7918bca127043c0931b9333687f8b5d3cc782e18` | Отдельный нормализованный intake-кандидат | Сравнительная локальная линия; наличие схемы не допускает AI по качеству |
| Voice/native Intake: HEAD `1afc1fef20242aa4f8cdf320f3911f59cbb920eb` + более поздний dirty source | ASR/Intake-код и новый native handoff через обычные create/change Journal, receipt и exact readback | Более поздний native v3 не принадлежит указанному immutable HEAD; pending delivery/revalidation, не отдельный авторитетный writer |

Source locators для повторного чтения:

- `D:/CodexHome/worktrees/journal-stage1/trading advisor 3000`, ветка `codex/journal-contract-readiness-fix`.
- `D:/CodexHome/worktrees/journal-productdata-integration/trading advisor 3000`, ветка `codex/journal-execution-markers`.
- `D:/CodexHome/worktrees/intake-normalized-input/trading advisor 3000`, ветка `codex/intake-normalized-input`.
- `D:/trading advisor 3000/.worktrees/voice-intake`, ветка `codex/voice-intake`; dirty source нужно закрепить отдельно до доставки.

Выбранные source-файлы первых трёх кандидатов сверены с указанными HEAD по
нормализованному тексту. Это не заявление о чистоте всего checkout. Исторические
проверки и текущие test definitions не считаются повторно выполненными тестами.
Review Workspace и его LM Studio interpretation adapter остаются отложенными;
существующий Runtime analytics review — другой модуль. Qwen-кандидат AI Intake
не допущен по семантическому качеству, даже при успешном структурном G0.
Сохранённый ASR replay не доказывает пользовательскую проверку полей и commit.

## Историческое архитектурное ревью 2026-10-15

Октябрьское ревью отдельного Journal baseline обнаружило schema/DTO parity gap
и прямой private SQL readiness read в HTTP. В Stage1 source, указанном выше,
есть conditional constraints/negative parity tests и публичный `check_ready`.
Старые замечания не переносятся как текущие blockers без нового воспроизведения.
Код в этой поставке не меняется; implementation PASS полного Workspace не выдан.

Основа ревью — [AS-01–AS-10](docs/architecture/product-plane/architecture-standard.md).
Data audit 2026-10-14 и прежние service probes — исторические свидетельства,
не нынешняя freshness. До следующих этапов остаются Q-решения ниже и отдельные
schema/producer-to-consumer, restart/restore, data, model и deployment proofs.

## Реестр открытых решений

| ID | Решение | Предложение для обсуждения / что блокирует |
| --- | --- | --- |
| Q-GRAIN | Развитие текущего v1 single-episode/contract/account ограничения до идеи с несколькими эпизодами | V1 сохраняет действующее ограничение; изменение требует решения, версии и миграции. Проверить добор, выход, повторный вход и ролл до расширяющего DDL |
| Q-ACCOUNT | Счета, equity, база риск-бюджета, время оценки капитала | Явно заданный account snapshot с источником; до fixed_risk_budget acceptance |
| Q-FACT | Обязательные поля фактов, комиссия unknown, поздние записи, correction model | Сохранить schema/DTO parity Stage1; fixtures на неполный факт и supersedes для каждого нового контракта; повторная приёмка перед Journal delivery |
| Q-BATCH | Одна заявка с несколькими операциями | Предлагается явно выбранный атомарный business bundle; не молчаливый partial commit |
| Q-STOPS | EMA50/EMA100/ATR, отступы, направление, tick rounding, costs | Полный method profile и ручные контрольные случаи; до Calculator acceptance |
| Q-REVIEW | Состав/порядок факторов, timeframes, состояния, допустимые выводы | Версионированный профиль с примерами supported/contradicted/insufficient; до Review acceptance |
| Q-SOURCES | Источник справочника и наличие prepared inputs | Audit public catalogue и опубликованных таблиц; до разрешения контрактов/расчётов |
| Q-TIME | Свежесть, историческая availability, native/adjusted compatibility | Политика на фактор и economics interval; до point-in-time/replay claims |
| Q-LINEAGE | Гранулярность текущих manifests и field-level dependencies | Проверить путь конкретного числа; недостающие contracts добавляет владелец pipeline |
| Q-RETENTION | Сроки inputs, attached files, Delta history и replay artifacts | Покрыть VACUUM/cleanup/restore; до обещания долговременного replay |
| Q-MODEL | Модель, quantization, контекст, eval thresholds и ресурсы | Измерить на размеченном корпусе и локальном железе; до AI acceptance |
| Q-RUNTIME | Interactive latency, размещение долгих операций, LM Studio topology | Public Journal schema readiness, один владелец запуска Dagster, durable request IDs; до deployment acceptance |
| Q-UI | Frontend dependency versions, численные API/payload/SLO limits | Фиксировать после fixture/prototype measurement; не изобретать SLA заранее |

Открытые решения не переоткрывают принятое разделение модулей, PostgreSQL,
LM Studio или выбранное представление происхождения. Они ограничивают
implementation-ready claim до получения ответов и соответствующих fixtures.

## Матрица приёмки

| Сценарий | Ожидаемое доказательство |
| --- | --- |
| Пользователь пишет заметку без цены/объёма | Сохранена заметка, отсутствующие trade fields не выдуманы |
| Реальное исполнение при недоступной Delta/LM Studio | Факт сохранён с original/evidence; calculation/review status явно недоступен |
| Два контракта подходят под тикер | Intake требует выбора; расчёт не идёт по случайному контракту |
| Повтор команды после сетевого сбоя | Один business commit; возвращается прежний receipt |
| Тот же ID, другой payload | Conflict, никаких вторых/частичных записей |
| Две конкурентные правки | Одна ожидаемая revision принята; вторая получает conflict |
| Правка входа после расчёта | Старый bundle остаётся доступен, но не применяется к новым условиям |
| EMA50 доступна, EMA100 отсутствует | Обе строки видимы; отсутствующая обязательная метрика не позволяет выбрать рассчитанный план |
| Отсутствует FX/стоимость шага/обязательная экономика | Risk/ГО unavailable, не zero; доступные независимые значения сохранены |
| Adjusted factor + native execution price | Явный корректный price-space mapping либо incompatible result |
| MTF bar ещё не закрыт | Такой input исключён; нет будущего факта в контексте |
| Новый published набор после сохранения | Старые расчёт/Review не меняются; новый анализ имеет новый context ID |
| История Delta удалена retention | Старый scalar/result читается; replay/lineage сообщает evidence_expired |
| AI добавил фактор/параметр/несуществующую ссылку | Ответ отвергнут; факт-пакет не переписан |
| AI вернул корректный JSON с неподдержанным выводом | Семантическая оценка/ограничения выявляют дефект; одного schema pass недостаточно |
| Модель недоступна / timeout | Request и inputs сохраняются; bounded retry; явный failed |
| Сбой после Journal commit до завершения заявки | Recovery находит receipt и завершает request без дубликата |
| Остановлен процесс после принятия долгого запроса | Запрос восстанавливается/маркируется retryable; не остаётся вечным running |
| Browser закрыт и открыт | Видны серверные состояния по IDs; процесс не зависит от вкладки |
| Lineage выбранного числа | Совпадают input values, методика, версии, window и source records; user input выделен отдельно |
| Очистка временного Intake | Сохранённые свидетельства и вложения Journal/Review не удалены |
| Restore backup | Восстановлены БД, файлы, hashes, ссылки и выбранный исторический snapshot |

## Проверочные уровни

- Contract tests: schema compatibility, fixtures для каждого state, IDs/hash и
  decimal/time encoding; HTTP/DTO parity и error envelopes.
- Unit tests: независимые контрольные расчёты сценариев, правила состояния,
  invalid/missing inputs; не тесты, просто повторяющие реализацию.
- PostgreSQL integration: транзакционность revision + receipt, unique constraints,
  optimistic conflict, migrations, повтор после рестарта и backup/restore.
- Data integration: реальные опубликованные таблицы, принятые roots/manifests,
  native predicates/projection, coherent versions, price-space и causality.
- AI evaluation: размеченный корпус, повторные прогоны, format + semantics,
  ограничения и измеренная нагрузка; выбранная модель выполняется локально.
- UI acceptance: сквозные пользовательские пути, stale/partial/failure states,
  bounded chart/lineage payload и отсутствие прямых browser storage/model вызовов.

Новые тесты создаются при реализации соответствующих контрактов/поведения.
Текущий архитектурный пакет проверяется doc/link/architecture валидаторами;
он не содержит утверждения, что будущая матрица уже выполнена.

## Локальная эксплуатация

Топология: web UI → FastAPI application → PostgreSQL; отдельный локальный endpoint
LM Studio; data provider читает Delta; долгие governed работы принадлежат Dagster.
При контейнерном backend адрес модели должен быть достижим из контейнера:
его `localhost` не тождественен Windows host. Адреса задаются конфигурацией и
проверяются health/functional probes, не зашиваются в frontend.

Приём запроса и запуск engine не являются одной транзакцией. Приложение сначала
сохраняет request и dispatch intent, затем связывает его с run ID. Idempotent
run key и recovery существующего orchestrator устраняют разрыв. Это состояние
прикладного запроса, не outbox/event publication Journal. Отдельный скрытый scheduler
или неуправляемые fire-and-forget задачи FastAPI не вводятся.

Логи связываются request/correlation/run IDs и отражают latency, retries, statuses,
source versions; приватные поля и секреты исключены из обычных логов. Для запросов
задаются timeouts, payload bounds и concurrency limits; значения принимаются после измерений.
Health процесса и freshness данных отображаются раздельно. Частичная деградация
модели/Delta не выключает запись фактов и чтение сохранённых карточек.

Первая установка обслуживает локального пользователя; endpoints не публикуются
в интернет по умолчанию. Перед сетевым/многопользовательским доступом нужен
отдельный контракт аутентификации/авторизации и ограничения доступа к attachments,
accounts и model endpoint. CORS/origin policy соответствует фактическому локальному UI.

SQL-миграции версионируются и имеют upgrade/restore proof. Процесс не изменяет
схему неожиданно при чтении карточки. Backup выполняется до миграции; irreversible
data transformations требуют отдельной процедуры восстановления.

## Презентационная приёмка

Один реальный сценарий должен показать:
1. Исходный пользовательский материал и нормализованный ввод.
2. Два сценария стопа с источниками входов и явной доступностью.
3. Сохранённую версию Journal и фиксированный Review.
4. Переход от выбранного числа к методике, user input, factor, диапазону свечей и MOEX.
5. Повтор ограниченного детерминированного расчёта на сохранённых входах.
6. Честное отображение missing/expired ветви без выдуманного свидетельства.

Физическое происхождение и корректность расчёта проверяются отдельно от качества
AI-интерпретации. Результат предъявляется вместе с версиями/срезом и не называется
production/live readiness. Закрытие каждого этапа обновляет STATUS только по его доказательствам.
