# Приложение C — риски, допущения и non-goals

## Допущения

1. Новый repo создаётся с нуля или почти с нуля.
2. Цель — ускорить AI-разработку, а не повторить все возможности исходного продукта.
3. Команда готова поддерживать governance как код, а не только как текст.
4. Codex будет использовать patch sets, а не one-shot massive port.

## Главные риски

### 1. Смешение shell и бизнес-логики
Если начать переносить доменные скрипты вместе с process layer, shell потеряет переносимость.

### 2. Избыточный hot context
Если открыть слишком много docs/skills/state по умолчанию, ускорение исчезнет.

### 3. Ложная экономия на validators
Без machine-enforced правил shell быстро деградирует и превращается в набор советов.

### 4. Перенос исторического мусора
Старые task notes, старые decisions и domain incidents будут загрязнять новый repo и агентный контекст.

### 5. Преждевременная абстракция в shared library
Вынесение shell в отдельный пакет до пилотного прогона может усложнить разработку вместо ускорения.

### 6. Невалидируемые skills
Если skills каталог не синхронизируется и не валидируется, он перестаёт быть надёжным источником.

## Non-goals

- построить полный бизнес-функционал Trading Advisor 3000;
- портировать все доменные tests и research scripts;
- сохранить весь исторический контекст старого repo;
- сделать универсальный framework “на все времена” уже в первой итерации;
- превратить governance shell в heavy platform с большим количеством обязательных ручных процедур.

## Критические решения

1. `run_loop_gate.py` — канонический hot-path gate.
2. `docs/session_handoff.md` — pointer shim, не narrative.
3. `plans/items/` — canonical state, `PLANS.yaml` — generated compatibility output.
4. Domain skills — exclude by default.
5. Historical memory — template only.
6. Direct push to `main` — blocked by default.
