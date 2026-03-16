# Runbook для Codex: как внедрять AI Delivery Shell

## 1. Что делать в самом начале

1. Прочитать:
   - `AGENTS.md`
   - `docs/agent/entrypoint.md`
   - `docs/agent/domains.md`
   - `docs/agent/checks.md`
   - `docs/agent/runtime.md`
   - `docs/agent/skills-routing.md`
2. Проверить, что установлены hooks.
3. Создать или активировать task session.
4. Завести task note.
5. Провалидировать task contract.
6. Только после этого менять код.

## 2. Канонический порядок работы

```text
read hot docs
→ task_session begin
→ create/update task note
→ validate_task_request_contract
→ implement minimal patch
→ run_loop_gate
→ update plans/memory/task note
→ run_pr_gate
→ task_session end
```

## 3. Абсолютные запреты

- Не писать в `main` напрямую.
- Не использовать архивную task note как источник истины без closeout evidence.
- Не расширять hot context без необходимости.
- Не переносить domain-specific skills в baseline shell.
- Не делать массовый copy-paste старого repo.
- Не возвращать устаревший `run_lean_gate.py` или аналогичную “короткую дорогу”.

## 4. Как вести patch sets

### Для каждого patch set:
- держать change surface маленьким;
- сначала чинить contracts и docs, затем runtime code;
- не смешивать process shell и продуктовые фичи;
- обновлять task note сразу после изменения решения.

### Что записывать в task note:
- objective;
- scope;
- constraints;
- done evidence;
- chosen context cards;
- validators/tests run;
- closeout notes.

## 5. Когда открывать warm/cold context

Только если hot docs явно отправляют туда или если:
- change surface пересекает несколько контекстов;
- надо обновить architecture docs;
- упёрлись в validator/runbook issue;
- нужно понять durable state conventions.

## 6. Как поступать при красном gate

1. Остановить расширение change surface.
2. Зафиксировать failure в task note.
3. Открыть соответствующий runbook/remediation doc.
4. Исправить governance issue, а не обходить его.
5. Повторить gate.
6. Только после green продолжать работу.

## 7. Как закрывать задачу

Перед `task_session end` должны быть:
- зелёный PR gate;
- обновлённая task note;
- актуализированный plans item (если задача изменила план);
- актуализированный task outcome;
- корректный session handoff pointer;
- отмеченные follow-ups, если они остались.

## 8. Какой результат должен вернуть Codex по фазе

### Обязательный формат ответа по фазе
1. Что было сделано.
2. Какие файлы добавлены/изменены.
3. Какие команды и тесты запускались.
4. Что стало доказательством завершения.
5. Какие follow-ups остаются.
6. Какие риски/ограничения есть у текущего состояния.

## 9. Когда можно подключать приложение

Только после стабилизации phases 1–4.  
Иначе приложение начнёт жить быстрее, чем process shell, и shell снова превратится в “документацию без enforcement”.
