# 08. Codex AI Shell Integration

## 1. Основная идея

Codex должен работать **через существующий AI delivery shell**, а не в обход него.

Это означает:
- root `AGENTS.md` и `docs/agent/*` всегда приоритетнее product docs;
- task lifecycle обязателен;
- shell gates обязательны;
- product docs и product AGENTS — это overlay, а не новый root control plane.

## 2. Стартовая последовательность Codex

1. Read shell hot docs:
   - root `AGENTS.md`
   - `docs/agent/entrypoint.md`
   - `docs/agent/domains.md`
   - `docs/agent/checks.md`
   - `docs/agent/runtime.md`
   - `docs/DEV_WORKFLOW.md`
2. Open task session:
   - `python scripts/task_session.py begin --request "<request>"`
3. Identify change surface:
   - shell-only
   - product-only
   - mixed
4. If mixed:
   - split into ordered patches
5. Read active phase docs
6. Implement minimal phase-complete patch
7. Run loop gate + product checks
8. Prepare PR with acceptance evidence

## 3. Как встроить product AGENTS

### Root level
Root `AGENTS.md` остаётся shell-first.

### Product level
Рекомендуется добавить:
- `src/trading_advisor_3000/AGENTS.md`
- при необходимости вложенные AGENTS для `research/`, `runtime/`, `execution/`

Это хорошо сочетается с Codex discovery chain.

## 4. Пример product-specific AGENTS split

### `src/trading_advisor_3000/AGENTS.md`
- no live side effects
- point-in-time only
- backtest reproducibility required

### `src/trading_advisor_3000/AGENTS.md`
- idempotency
- no broker truth assumptions
- signal event/state duality

### `src/trading_advisor_3000/AGENTS.md`
- broker source of truth
- reconciliation mandatory
- transport-neutral contracts only

## 5. Работа со skills

Поскольку ordinary-chat навыки живут в global Codex root, product-plane-specific skills нужно вводить через repo-local governance path:

1. добавить child directory under `.codex/skills/` with a skill descriptor file;
2. обновить `docs/agent/skills-catalog.md`;
3. при необходимости обновить `docs/agent/skills-routing.md`;
4. прогнать:
   - `python scripts/validate_skills.py`
   - `python scripts/skill_precommit_gate.py --changed-files ...`

### Product skills, которые стоит подготовить
- `delta-contracts`
- `dagster-assets`
- `research-backtest-internal`
- `telegram-lifecycle`
- `stocksharp-sidecar`
- `execution-reconciliation`
- `runtime-replay`

## 6. MCP and Codex config strategy

### User-level config
`~/.codex/config.toml`
- персональные defaults
- shared MCP defaults

### Project-level config
`.codex/config.toml`
- repo-specific MCP and profiles
- trusted project only

### Рекомендуемый проектный профиль
- model / approval settings
- project-root markers
- MCP servers for GitHub, Docs, Dagster, Docker
- optional read-only DB access for dev/stage

## 7. Рекомендуемый `.codex/config.toml` skeleton

```toml
project_root_markers = [".git"]

[mcp_servers.github]
command = "docker"
args = ["run", "--rm", "-i", "ghcr.io/github/github-mcp-server"]

[mcp_servers.openai_docs]
command = "npx"
args = ["-y", "@openai/docs-mcp"]

[mcp_servers.dagster]
command = "uvx"
args = ["dagster", "mcp"]

[mcp_servers.docker]
command = "docker"
args = ["mcp", "gateway", "run"]
```

## 8. Что Codex не должен делать

- создавать shell/runtime coupling через imports;
- писать product runtime config в root `configs/*` без решения через ADR;
- менять gate names;
- игнорировать task lifecycle;
- подменять shell docs локальными ad hoc инструкциями.

## 9. Что Codex должен делать в PR summary

- указать phase/subphase;
- перечислить changed paths;
- отделить shell-sensitive changes от product changes;
- показать acceptance evidence;
- отметить, нужны ли новые MCP servers / skills / secrets.
