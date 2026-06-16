# MOEX Money Math Implementation Review

Дата: 2026-06-15; integration refresh: 2026-06-16  
Ветка интеграции: `codex/moex-money-math-integration`  
Поверхность изменения: `product-plane`, data plane + research downstream

## Краткий вывод

Реализация заметно приблизилась к плану: появились raw/canonical economics таблицы, Spark job для расчета контрактной экономики и downstream-поля в `continuous_front_bars` / `research_bar_views`.

## Update 2026-06-15

Дополнительная проверка закрыла оставшиеся regression gaps из исходного Test
Plan:

- `research_bar_views` теперь покрыт Spark regression на propagation одинаковых
  session economics для `1m`, `5m`, `15m`, `1h`, `4h`, `1d`;
- weekly economics покрыт отдельным Spark regression: `1w` использует
  `bar_end_ts` как as-of точку и берет economics последней сессии weekly bar;
- `continuous_front_bars` покрыт Spark regression на roll-смену
  `active_contract_id`: execution step price и margin меняются вместе с
  активным реальным контрактом.

Свежие проверки после этого update:

- targeted Spark money-math/downstream regression: `8 passed`;
- loop gate по changed files: `OK`, включая формат, ruff, compile, process /
  architecture tests и repo validations; внутри boring checks: `241 passed`.
- current real-data Spark proof на сохраненных authoritative raw economics:
  - output:
    `D:/TA3000-data/trading-advisor-3000-nightly/verification/money-math-f224-current-proof-20260615/canonical/economics`;
  - evidence:
    `D:/TA3000-data/trading-advisor-3000-nightly/verification/money-math-f224-current-proof-20260615/evidence/contract-economics-report.json`;
  - status: `PASS`;
  - rows: `canonical_fx_rates=12`,
    `canonical_asset_risk_parameters=187`,
    `canonical_contract_economics=595`;
  - missing required inputs: `0`;
  - affected downstream partitions: `181`;
  - `_delta_log=true` for all three canonical economics tables;
  - sample BR/GOLD/RTS rows use `clearing_type=tc`,
    `economics_session_date=2026-06-15`,
    `effective_session_date=2026-06-16`, and expose
    official `STEPPRICE` / `INITIALMARGIN` beside calculated
    `step_price_rub` / margin estimates.
- stable baseline economics root refreshed from the same authoritative raw side
  tables:
  - output:
    `D:/TA3000-data/trading-advisor-3000-nightly/canonical/economics`;
  - evidence:
    `D:/TA3000-data/trading-advisor-3000-nightly/baseline-update/f224-stable-money-math-20260615/economics-refresh/contract-economics-report.json`;
  - status: `PASS`;
  - rows: `canonical_fx_rates=12`,
    `canonical_asset_risk_parameters=187`,
    `canonical_contract_economics=595`;
  - missing required inputs: `0`;
  - affected downstream partitions: `181`;
  - `_delta_log=true` for all three stable canonical economics tables;
  - stable BR/GOLD/RTS samples use `clearing_type=tc`,
    `economics_session_date=2026-06-15`, and
    `effective_session_date=2026-06-16`.
- current short live ISS diagnostic still fails before raw landing:
  `fetch_futures_contract_securities` returns `MoexRequestError` with
  `_ssl.c:993: The handshake operation timed out`.
- standalone updater now filters `--fx-jsonl` by the requested date window,
  matching contracts/RMS JSONL behavior, so a historical import cannot
  accidentally refresh FX rows outside `--date-from` / `--date-till`.
- regular `moex_baseline_update_job` reports now expose
  `artifact_paths.moex_request_log` and `artifact_paths.moex_request_latest`,
  so operator-facing refresh reports point directly at ISS request evidence.

После исправлений этот review-бэклог в основном закрыт:

- raw refresh пишет валидный JSON и делает scoped replace только по refreshed `trade_date`;
- standalone `bootstrap` и `update` теперь различаются: bootstrap защищен от случайной перезаписи существующего store, update делает scoped raw replace;
- standalone historical JSONL imports filter contracts, FX, and RMS rows by the
  explicit date window;
- regular baseline update publishes MOEX request-log artifacts in the main
  report;
- canonical Spark job пишет merge/upsert по ключам economics tables вместо полной перезаписи;
- `tc` имеет приоритет над `mc` для evening-clearing economics;
- effective session сдвигается на следующую торговую сессию через `canonical_session_calendar`, при отсутствии календаря фиксируется fallback на следующий календарный день;
- downstream join учитывает `effective_from_ts/effective_to_ts`, а weekly использует `bar_end_ts` как as-of точку;
- production/Dagster route включает `execution_economics_required=True`;
- research manifest lineage включает `canonical_contract_economics`;
- buffer policy вынесена в общий список FX/USD-linked asset codes и используется Python helper + Spark job;
- economics tables добавлены в hot Delta inventory.

Проверки:

- focused unit tests: `15 passed`;
- Spark integration money-math: `5 passed`;
- standalone updater retry/evidence tests: `6 passed`;
- real-data Spark proof на сохраненных raw economics:
  - `canonical_fx_rates`: `_delta_log=true`, rows `12`;
  - `canonical_asset_risk_parameters`: `_delta_log=true`, rows `187`;
  - `canonical_contract_economics`: `_delta_log=true`, rows `595`;
  - missing required inputs: `0`;
  - evidence: `D:/TA3000-data/trading-advisor-3000-nightly/verification/money-math-f224-20260615/evidence/existing-raw-contract-economics-report.json`.

Ограничение проверки: live ISS bootstrap в новый isolated root был запущен, но первый MOEX ISS HTTPS запрос завершился `_ssl.c:993 handshake operation timed out` до записи raw tables. Повтор с увеличенным retry budget (`45s`, `5` retries) также завершился тем же сетевым handshake timeout. Диагностический повтор с коротким budget подтвердил, что standalone updater теперь пишет `moex-request-log.jsonl` и `moex-request.latest.json`; latest artifact показал `operation=futures_contract_securities`, `status=fail`, `attempt=2`, `attempt_limit=2`, `error_type=URLError`. Поэтому live network fetch остается внешним сетевым блокером, а Spark/data compatibility подтверждена на уже сохраненных authoritative raw economics.

Оставшиеся риски после исправлений:

- live ISS bootstrap нужно повторить при стабильной сети; standalone updater уже поддерживает настраиваемые timeout/retry и сохраняет request-log evidence при сетевом падении;
- полный 4-летний historical backfill economics требует отдельного источника исторических contract snapshots; standalone updater теперь отказывается делать multi-day backfill через текущий MOEX ISS contract snapshot и требует `--contracts-jsonl`;
- точность session-calendar mapping зависит от полноты `canonical_session_calendar` для нужных instruments.

## P1. Economics не сохраняет историческую глубину

### Проблема

Raw economics refresh берет только одну `trade_date`, а запись raw и canonical economics идет через overwrite. Для V1-плана это критично, потому что baseline рассчитан на историческое покрытие, а не на один текущий снимок.

### Почему это важно

После ночного обновления можно получить canonical economics только для последней даты. Старые бары 4-летнего baseline останутся без корректного as-of покрытия или будут присоединяться к неполной экономике.

### Где видно

- `src/trading_advisor_3000/product_plane/data_plane/moex/baseline_update.py:412`
- `src/trading_advisor_3000/product_plane/data_plane/delta_runtime.py:361`
- `src/trading_advisor_3000/spark_jobs/moex_contract_economics_job.py:86`

### Что нужно исправить

Перевести economics refresh на scoped merge/upsert по ключам `contract_id`, `economics_session_date`, `clearing_type` и не перезаписывать всю историю. Для первичного backfill нужен отдельный режим, который строит историческую economics-глубину.

## P1. As-of семантика может быть сдвинута

### Проблема

План требует: для торговой сессии D использовать параметры, известные после вечернего клиринга предыдущей торговой сессии. Сейчас код выставляет:

- `effective_from_ts = trade_date 19:00:00`;
- `effective_session_date = trade_date`.

Это не то же самое, что "параметры предыдущей сессии применяются к следующей торговой сессии".

### Почему это важно

Интрадей и дневные бары до 19:00 могут получить старую экономику или null. Weekly-бары тоже не имеют явного правила "использовать последнюю торговую сессию недели".

### Где видно

- `src/trading_advisor_3000/spark_jobs/moex_contract_economics_job.py:399`
- `src/trading_advisor_3000/spark_jobs/moex_contract_economics_job.py:437`
- `src/trading_advisor_3000/spark_jobs/research_bar_views_job.py:701`

### Что нужно исправить

Привязать economics к торговой сессии через session calendar, а не только через timestamp бара. Отдельно зафиксировать правила для `1d` и weekly: какая именно session date считается execution economics date.

## P1. `raw_payload_json` не гарантированно является JSON

### Проблема

В raw rows кладется Python dict, но схема объявляет `raw_payload_json` как `string`. Runtime в этом случае делает `str(value)`, а не `json.dumps(value)`.

### Почему это важно

Получается строка вида `{'SECID': '...'}`, которая не является валидным JSON. Spark fallback через `get_json_object` не сможет надежно читать такие payload. Это ломает "source as-is", аудит и запасной путь извлечения полей.

### Где видно

- `src/trading_advisor_3000/product_plane/data_plane/moex/baseline_update.py:448`
- `src/trading_advisor_3000/product_plane/data_plane/delta_runtime.py:87`
- `src/trading_advisor_3000/product_plane/data_plane/schemas/delta.py:74`

### Что нужно исправить

Либо объявить `raw_payload_json` как `json` в schema manifest, либо явно сериализовать payload через `json.dumps(..., ensure_ascii=False, sort_keys=True)` перед записью.

## P1. FX clearing-rate выбирается слишком грубо

### Проблема

Если на дату есть несколько clearing rates, код приоритетно выбирает `mc` перед `tc`. При этом план завязан на economics, известную после вечернего клиринга.

### Почему это важно

При наличии и `mc`, и `tc` можно получить неправильный FX rate для расчета step price и margin. Это особенно опасно для USD-linked и FX-like контрактов.

### Где видно

- `src/trading_advisor_3000/spark_jobs/moex_contract_economics_job.py:339`

### Что нужно исправить

Сделать clearing-aware модель: явно определить, какой clearing type используется для какого effective interval, и покрыть это тестом с одновременным наличием `mc` и `tc` на одну дату.

## P2. Downstream может молча работать без economics

### Проблема

Dagster-обертка уже требует `canonical_contract_economics`, но сами Spark jobs всё ещё принимают `canonical_contract_economics_path=None` и заполняют execution-поля null-ами.

### Почему это важно

Прямой caller может получить "успешный" dataset без money math, хотя для этого режима экономика должна быть обязательной.

### Где видно

- `src/trading_advisor_3000/spark_jobs/research_bar_views_job.py:671`
- `src/trading_advisor_3000/spark_jobs/continuous_front_job.py:943`

### Что нужно исправить

Ввести явный режим `execution_economics_required` или сделать economics обязательной для production/research refresh маршрута. Null-поля допустимы только в явно legacy/test режиме.

## P2. Research lineage не включает economics как источник

### Проблема

`research_bar_views` теперь зависит от `canonical_contract_economics`, но manifest `source_tables`, source versions и source hashes не включают эту таблицу.

### Почему это важно

Изменение margin, step price или FX rate может не менять доказуемый hash research dataset. Это снижает воспроизводимость и делает refresh менее аудируемым.

### Где видно

- `src/trading_advisor_3000/spark_jobs/research_bar_views_job.py:1549`

### Что нужно исправить

Добавить `canonical_contract_economics` в source tables, source delta versions и source hashes для всех контуров, где execution economics попадает в выходной dataset.

## P2. Buffer policy продублирована и расходится с helper-логикой

### Проблема

В helper-логике FX/USD-linked assets получают 5% buffer даже при RUB quote. В Spark job 5% применяется только когда `quote_currency != RUB`.

### Почему это важно

Для части FX-like assetcode можно получить другой margin buffer в Spark canonical economics, чем ожидает доменная helper-логика.

### Где видно

- `src/trading_advisor_3000/product_plane/data_plane/moex/economics.py:94`
- `src/trading_advisor_3000/spark_jobs/moex_contract_economics_job.py:386`

### Что нужно исправить

Синхронизировать Spark policy с helper-правилом или вынести список USD/FX-linked assetcode в общий контракт, который проверяется тестами.

## P2. Economics-таблицы не попали в operational inventory

### Проблема

Новые raw/canonical economics таблицы есть в schema manifest и runbook, но отсутствуют в hot delta table inventory.

### Почему это важно

Операторские проверки, маршрутизация и обзоры "живых" таблиц могут не учитывать economics как часть hot data plane.

### Где видно

- `src/trading_advisor_3000/product_plane/data_plane/hot_delta_tables.py:15`

### Что нужно исправить

Добавить economics tables в operational inventory с явным family/access policy:

- `raw_moex_contract_securities`
- `raw_moex_indicative_fx_rates`
- `raw_moex_rms_limits`
- `raw_moex_rms_staticparams`
- `canonical_fx_rates`
- `canonical_asset_risk_parameters`
- `canonical_contract_economics`

## P2. Тесты пока не доказывают полный baseline-level сценарий

### Проблема

Текущие тесты полезны, но больше проверяют fixture-level поведение. Для приемки плана нужны проверки на уровне жизненного цикла данных.

### Что нужно добавить

- исторический backfill economics на baseline window;
- merge/upsert без потери предыдущих дат;
- валидный JSON в `raw_payload_json`;
- сценарий с `mc` и `tc` на одну дату;
- fail-closed downstream при отсутствии economics;
- daily и weekly as-of по торговой сессии, а не только по timestamp;
- реальный proof на `D:/TA3000-data/trading-advisor-3000-nightly` с `_delta_log`, row counts и покрытием execution-полей.

## Рекомендуемый порядок исправлений

1. Исправить storage lifecycle: historical backfill + scoped merge/upsert вместо overwrite.
2. Зафиксировать session-aware as-of модель для intraday, daily и weekly.
3. Починить `raw_payload_json` как валидный JSON.
4. Сделать execution economics обязательной в production/research refresh.
5. Добавить economics в lineage/manifest и operational inventory.
6. Синхронизировать buffer policy между helper и Spark job.
7. Добавить baseline-level proof и тесты на edge cases.

## Статус проверки

Интеграционный refresh 2026-06-16 перенес изменения на свежий `origin/main` и
перезапустил локальные проверки: focused changed unit slice `165 passed`,
full Spark money-math integration `7 passed`, `git diff --check` без ошибок.
Loop/pr gates фиксируются отдельно перед merge.
