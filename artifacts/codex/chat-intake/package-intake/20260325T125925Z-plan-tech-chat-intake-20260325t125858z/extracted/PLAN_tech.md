# План: Lightweight Anti-Shortcut Governance для AI-Shell

## Summary
Цель: не допускать, чтобы Codex для критичных контуров уходил в локально самый простой путь, если он ухудшает архитектурную форму, повышает стоимость переприемки или подменяет реальное closure scaffold/synthetic-решением.

Подход: не строить тяжелую бюрократию, а встроить в уже существующий harness 4 легких слоя:
- короткая policy-модель классов решения;
- минимальное расширение task contract только для критичных контуров;
- 2 новых fail-closed validator-а в существующие loop/pr/nightly gates;
- pilot-passports для 2 дорогих контуров.

Принципы harness engineering, которые сохраняем:
- никаких новых обязательных lane;
- никаких ручных approval-процессов;
- никаких ADR/waiver-реестров на каждую задачу;
- минимальная дополнительная нагрузка на low-risk задачи;
- deterministic, machine-checkable, fail-closed rules.

## Key Changes
### 1. Policy-модель без лишней бюрократии
Добавить один короткий shell policy document с тремя классами реализации:
- `target`
- `staged`
- `fallback`

Зафиксировать 5 запрещенных shortcut-паттернов:
- synthetic upstream boundary вместо реального output предыдущего шага;
- scaffold-only closure, заявленный как full closure;
- substitute technology/runtime без явного объявления;
- hidden fallback path;
- acceptance по smoke/manifests/sample artifacts вместо required contour evidence.

Не вводить отдельный “совет архитектуры”, scoring model, decision committee или новый approval flow.

### 2. Минимальное расширение task contract
Расширить существующий task note только для задач, попадающих в critical contour:
- `Solution Class: target|staged|fallback`
- `Critical Contour: <id>|none`
- `Forbidden Shortcuts: <comma list>|none`
- `Closure Evidence: <what proves closure>`
- `Shortcut Waiver: none|<one-line reason>`

Не добавлять отдельный большой design document.
Design checkpoint делать inline в task note одной короткой связкой:
- выбранный путь,
- почему не shortcut,
- какой future shape сохраняется.

Для non-critical задач новые поля не обязательны.

### 3. Один config для critical contours
Добавить один machine-readable config, например `configs/critical_contours.yaml`, со следующей схемой:
- contour id
- trigger paths/patterns
- default required class
- forbidden shortcut markers
- required evidence markers
- allowed staged wording
- re-acceptance trigger markers

Стартовый набор contour-ов:
- `data-integration-closure`
- `runtime-publication-closure`

Пока не включать весь repo и все фазы. Сначала pilot only.

### 4. Два новых validator-а и только в существующие gates
Добавить:
- `validate_solution_intent.py`
Проверяет, что для critical contour task note содержит обязательные поля и не заявляет `target` без явного closure evidence.
- `validate_critical_contour_closure.py`
Проверяет, что заявленный класс решения совпадает с доступными доказательствами и не срабатывают forbidden shortcut markers.

Оба validator-а включить в уже существующие:
- local checks
- loop gate
- pr gate
- nightly gate

Не добавлять новый gate lane и не дублировать существующий proving flow.

### 5. Skills и routing без переусложнения
Обновить routing policy так, чтобы для critical contour автоматически подключались:
- architecture context
- QA/acceptance context

Добавить одно простое правило поведения для Codex:
- если задача критичная, сначала зафиксировать `target/staged/fallback`, потом делать код;
- если выбран `fallback`, он должен быть явно назван fallback;
- если решение проще, чем target shape, агент обязан это обозначить.

Не требовать обязательного сравнения двух архитектурных вариантов для каждой задачи.
Сравнение вариантов делать только для `target`-критичных контуров.

### 6. Acceptance passports только для pilot-контуров
Добавить 2 коротких acceptance passport-а:
- data contour: от интеграции данных до downstream research/runtime-ready surface;
- runtime contour: от strategy/runtime output до durable store/publication contour.

В каждом passport:
- что считается `target`;
- что считается `staged`;
- какие green-paths запрещены;
- какие evidence обязательны;
- что триггерит re-acceptance.

Не раскатывать passport-ы на все фазы сразу.

## Test Plan
Добавить только те тесты, которые ловят shortcut-pattern без шумной избыточности:
- unit test на parsing/validation нового task contract;
- unit test на config `critical_contours.yaml`;
- process test: critical contour без `Solution Class` падает в loop gate;
- process test: `target`-claim с scaffold/sample/synthetic evidence падает;
- process test: `staged`-claim с корректным wording и evidence проходит;
- process test: docs-only и low-risk shell diff не требуют новых полей;
- anti-shortcut regression для pilot contour 1: upstream boundary нельзя подменить fixture path;
- anti-shortcut regression для pilot contour 2: runtime closure нельзя принять по synthetic publication path.

Не строить сейчас общий static analyzer “на все случаи”.
Только config-driven checks для pilot contour-ов.

## Rollout
Этап 1:
- policy doc
- task contract extension
- critical contour config
- 2 validator-а
- routing update
- tests
- gate wiring

Этап 2:
- 2 pilot acceptance passport-а
- pilot on `data-integration-closure` and `runtime-publication-closure`

Этап 3:
- 1-2 недели наблюдения
- если false-positive rate низкий, расширить config на:
  - live transport closure
  - durable storage closure

Dashboard-метрики пока ограничить 3 счетчиками:
- `critical tasks with explicit solution class`
- `blocked shortcut claims`
- `staged-vs-target declarations`

Не строить сейчас полноценную KPI-программу и отдельный analytics layer под это.

## Assumptions
- Фокус только на AI-shell; product-plane docs меняются только там, где нужны pilot acceptance passports.
- Новая дисциплина применяется только к critical contours, а не ко всем задачам.
- Inline waiver в task note достаточно; отдельный waiver ledger сейчас не нужен.
- Existing loop/pr/nightly and Phase 8 proving остаются основой harness; мы их усиливаем, а не заменяем.
- Pilot contour-ы по умолчанию: data integration closure и runtime publication closure.
