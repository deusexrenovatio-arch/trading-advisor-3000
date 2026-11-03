# Контракты локального рабочего места сделок

Дата: 2026-09-30. Change surface: `product-plane`. Solution Intent: `target`.
Статус: проект контрактов; перечисленные endpoints/DTO не заявлены реализованными.
Владение: [module design](docs/architecture/product-plane/journal-module-design.md).

## Общий конверт и идентичность

| Поле / понятие | Контракт |
| --- | --- |
| schema_version | Версия структуры внешнего/persisted payload; неизвестная версия отклоняется |
| request_id | Идентичность запуска обработки; один retry использует тот же ID |
| command_id | Идентичность команды записи; повтор с иным payload → conflict |
| correlation_id | Связь пользовательского пути и технических запусков; не ключ дедупликации |
| trade_id / revision | Идентичность карточки и точного immutable snapshot |
| submission_id / input_revision | Идентичность и версия нормализуемого ввода |
| expected_revision | Optimistic concurrency для правок существующей карточки |
| contract_id / instrument_id / venue | Конкретный контракт отдельно от семейства инструмента; неоднозначность разрешает Intake |
| family_id / contract_selection | Существующее семейство, выбранный срок, причина/дата default или ручного выбора; исполнение не меняется при новом ролле |
| analysis_reference / series_ref | Отдельный ряд анализа: dataset/contour/series, timeframe и price-space; связь с execution contract задаётся явно |
| event_at / recorded_at / as_of | Время события, регистрации и запрошенного анализа; разные назначения |
| source_available_at / effective_from / effective_to | Доступность источника и интервал применимости; не подменяются временем загрузки |
| value / unit / currency | Число с единицами; денежные/ценовые значения — decimal strings, не binary float |
| evidence_ref | Неизменяемая ссылка на вход/правило/артефакт с версией и типом свидетельства |

Время передаётся ISO 8601 с timezone, нормализуется в UTC; интерфейс показывает
зону пользователя. Для отсутствующих данных используются null + reason/status,
а не нули, пустые строки или выдуманное значение. Типизированные схемы запрещают
неожиданные поля в управляемых результатах AI.

## Внутренние команды и результаты

| Операция | Минимальный вход | Результат |
| --- | --- | --- |
| Intake.submit | SourceSubmission: source_type, original_text/attachment_refs, request_id, необязательный контекст карточки | submission_id, status, input_revision, кандидаты полей, уточнения |
| Intake.clarify | submission_id, expected_input_revision, ответы/исправления | Новая input_revision, нормализованный ввод, unresolved_fields |
| Calculator.calculate | NormalizedTradeInput, mode, scenarios, method_version, DataContextRequest | CalculationBundle, статус каждого сценария и зависимости каждого значения |
| Journal.commit_trade | TradeWriteBundle, command_id | CommitReceipt: trade_id, revision, IDs сохранённых записей/пакетов |
| Journal.apply_change | trade_id, expected_revision, тип операции, artifacts, command_id | Новая revision либо конфликт без частичной перезаписи |
| Journal.get_trade / get_timeline | trade_id, revision или current, cursor/limit | TradeSnapshot / страница истории |
| Review.create_review | trade_id, revision, profile_version, analysis_as_of, request_id | review_id, request status |
| Review.get_review / list_reviews | review_id либо trade_id + filters | ReviewEnvelope / страницы истории |
| ProductDataAPI.list_families / list_contracts | Поиск либо family_id и as_of | Семейства / dated контракты, active default с причиной/датой либо unresolved |
| ProductDataAPI.get_contract_context | Выбранный execution contract, as_of, pinned snapshot ref | ContractEconomicsContext: identity, экономика, units, availability и evidence |
| ProductDataAPI.get_analysis_context | Отдельный series_ref, as_of, factor request, pinned snapshot ref | DataContext с качеством, price-space, значениями и манифестом версий |
| Lineage.get_lineage | result_ref, value_path, cursor, depth, limit | Частичный граф свидетельств, состояние полноты, продолжение |

Прикладной слой разрешает trade_id/revision через Journal read API и передаёт
Review выбранный TradeSnapshot; Review не читает приватные таблицы Journal.
Он же собирает вход повторного расчёта из snapshot и новых параметров.
ProductDataAPI — один общий контракт чтения поверх Market/Research, не отдельный
сетевой сервис или владелец расчётов. resolve_contract — логическая задача его
catalogue reader. Chart/evidence/publication операции описаны в
[плане интеграции](docs/architecture/product-plane/trade-workspace/journal-integration-plan.md).

## Предметные payload

**NormalizedTradeInput:** intent, исходное свидетельство, происхождение полей,
выбранный execution contract и его selection evidence, отдельный analysis_reference
при необходимости, направление, параметры входа, количество или риск-бюджет, контекст счёта,
время события, ссылка на изменяемую карточку. Обязательность зависит от intent:
заметка не требует количества, исполнения не выводятся из расчётного результата.
Многосоставной ввод разбивается на предлагаемые операции и применяется явно.

**CalculationBundle:** calculation_id, input_revision/input_hash, method_version,
mode (`fixed_quantity` / `fixed_risk_budget`), закреплённый DataContext, scenarios[].
В составе закреплённого контекста ContractEconomicsContext и analysis context
различимы. Для ручных native уровней analysis context может отсутствовать с явным
основанием; для факторного правила обязательны series_ref и совместимый price-space.
Spot reference не преобразуется в futures execution contract автоматически.
Для каждого scenario: ID, правило/параметры/таймфрейм, стоп, количество, риск,
оценка ГО, units/currency, допущения/затраты, statuses и evidence refs.
Сохраняется полный список запрошенных сценариев, включая недоступные.

Метрики допускают частичную доступность: например, стоп известен, экономика
недоступна, поэтому риск/ГО — null с причиной. Верхний scenario status объясняет
пригодность сценария для выбора; required_metrics задаёт методика. Нельзя выбрать
как рассчитанный план сценарий с отсутствующей обязательной метрикой.
Сохранить сравнение без выбора или вручную введённый план можно с явным статусом.

**TradeWriteBundle:** подтверждённый ввод, intent/операции, источники, расчётные
пакеты либо причина отсутствия, selected_scenario_id либо null. Journal проверяет
соответствие выбранного сценария input_hash и ожидаемой версии карточки.

**ReviewEnvelope:** выбранная revision, profile_version, analysis_as_of,
report_context_id, факты/состояния, interpretation, completeness, validation,
model/prompt/config refs, timestamps и evidence roots. Время старого плана и нового
анализа показано раздельно. Схема подробнее в
[review-and-local-ai.md](docs/architecture/product-plane/trade-workspace/review-and-local-ai.md).

## HTTP API фронтенда

Префикс проектных маршрутов: `/api/trade-workspace/v1`.
Эти маршруты принадлежат интерфейсу приложения и не заменяют существующий Runtime API.

| Метод и путь относительно префикса | Назначение |
| --- | --- |
| GET /overview | Лёгкая сводка; последние записи/заявки/отчёты, bounded counts |
| GET /contracts?query=... | Публичный справочник; поиск по контексту даты/площадки |
| GET /families; GET /families/{family_id}/contracts?as_of=... | Семейство, dated сроки, источник/дата active default и ручной выбор |
| GET /contracts/{contract_id}/economics?as_of=... | Pinned экономика выбранного контракта, units, availability, model/buffer/source refs |
| POST /submissions | Создать и разобрать исходный ввод |
| GET /submissions/{id} | Поля, уточнения, версии, статус |
| POST /submissions/{id}/clarifications | Уточнить/исправить ввод с expected_input_revision |
| POST /calculations | Рассчитать по вводу/версии/срезу |
| GET /calculations/{id} | Рабочий или сохранённый пакет через владельца |
| POST /trades | Commit нового бизнес-пакета |
| POST /trades/{id}/changes | Типизированная правка с command_id / expected_revision |
| GET /trades | Курсорный список по фильтрам |
| GET /trades/{id}?revision=... | Карточка одной версии; latest разрешается сервером в конкретный номер |
| GET /trades/{id}/timeline | Лента с cursor/limit |
| POST /reviews | Запросить отчёт по сохранённой revision |
| GET /reviews/{id}; GET /trades/{id}/reviews | Статус/отчёт и история |
| GET /requests/{id} | Состояние длительного прикладного запроса и result_ref |
| GET /chart-context | Ограниченные свечи/индикаторы по контракту, срезу, timeframe, диапазону |
| GET /lineage | Происхождение result_ref/value_path, пагинация и глубина |
| GET /evidence/{id} | Проверяемая деталь зарегистрированного свидетельства |
| GET /system-status | Актуальность опубликованных продуктов и доступность сервисов |
| POST /attachments; GET /attachments/{id} | Приём вложения / доступ через проверяемую ссылку |

Синхронный успешный ответ: 200/201. Долгая операция: 202 с request_id и status URL;
поллинг с backoff завершается на terminal state. API не держит PG-транзакцию во
время LLM/Data API вызова. Автоматические пересчёты при GET запрещены.

## Статусы и ошибки

| Область | Состояния |
| --- | --- |
| Прикладной request | queued, running, waiting_for_input, succeeded, failed |
| Intake | received, processing, needs_clarification, ready, failed |
| Scenario | calculated, unavailable_data, invalid_input; per-metric status деталирует частичность |
| Review | processing, completed, insufficient_data, failed |
| Данные | available, missing, stale, unavailable, incompatible |
| Полнота lineage | complete, partial, evidence_expired, unavailable |

`waiting_for_input` не является успехом. `succeeded` прикладного запроса означает
получение результата операции, а не наличие всех факторов: partial calculation
и insufficient_data остаются явными бизнес-результатами.

Ошибки содержат `code`, `message`, `field_errors`, `retryable`, `correlation_id`.
400/422 — невалидный ввод/версия схемы; 404 — неизвестный объект; 409 — устаревшая
revision или ID с другим payload; 503 — недоступная зависимость до принятия запроса.
Ошибка уже принятой асинхронной операции отражается в GET /requests, а не исчезает.

## Повторы и конкурентность

1. ID и canonical payload hash связываются в хранилище владельца. Hash вычисляется
   после нормализации схемой, не от случайного порядка JSON-ключей.
2. Повтор с тем же ID/hash возвращает прежнюю квитанцию; иной hash → 409.
3. Исправление исходных условий увеличивает input_revision и требует нового расчёта.
4. Сбой после Journal commit восстанавливается получением CommitReceipt по command_id.
5. Изменение карточки между расчётом и commit проверяется через expected_revision.
   Устаревший расчёт не прикрепляется молча к новым входам.
6. Для запроса Review revision разрешается и закрепляется при принятии; последующая
   правка сделки не меняет выполняющийся отчёт.
7. При ошибке AI исходный ввод/факты сохраняются. Повтор генерации фиксируется отдельной
   попыткой; завершённый опубликованный отчёт не перезаписывается.

## Версионирование и начало реализации

Версии схем, методики, профиля отчёта, данных, модели и prompt независимы.
До реализации эти DTO переводятся в схемы с fixtures и compatibility tests согласно
[contract-change-policy.md](docs/architecture/product-plane/contract-change-policy.md).
Примеры из макетов не служат fixtures реальных MOEX контрактов.
Автоматическое включение новых схем в release-blocking inventory данным документом
не выполняется; это отдельный согласованный контрактный patch.
