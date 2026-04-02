# 10. MCP Deployment Request

## Адресат

Infra / Platform / Repository owners

## Тема

Разворот MCP-контура для Codex в репозитории `trading-advisor-3000` с учётом существующего AI delivery shell и планируемого product plane.

## Цель

Обеспечить Codex и IDE/CLI единым набором MCP servers, достаточным для:
- работы с GitHub PR/issues/repo context,
- чтения официальной документации OpenAI/Codex,
- взаимодействия с Dagster assets and jobs,
- локального/контейнерного dev control plane,
- при необходимости read-only доступа к dev/stage данным.

## Обязательный набор

### 1. GitHub official MCP
**Зачем**
- читать PR, issues, code context;
- сопровождать PR flow;
- использовать repo metadata в Codex.

**Требования**
- auth через GitHub token / GitHub App;
- scope least privilege;
- доступ только к нужным repo/org.

### 2. OpenAI Developer Docs MCP
**Зачем**
- оперативно сверяться с Codex/MCP/AGENTS docs;
- не держать эти знания в repo руками.

### 3. Dagster MCP
**Зачем**
- видеть assets, checks, schedules;
- использовать Codex для data/research orchestration troubleshooting.

### 4. Docker MCP Toolkit / Gateway
**Зачем**
- управлять dev containers;
- запускать локальные интеграционные профили;
- не писать ad hoc shell flows поверх существующего control plane.

## Рекомендуемый набор

### 5. PostgreSQL MCP (read-only; dev/stage only)
**Зачем**
- смотреть runtime state и execution snapshots при отладке.

**Ограничение**
- только read-only;
- не подключать к production by default.

### 6. MotherDuck / DuckDB MCP
**Зачем**
- ad hoc inspection Delta/Parquet outputs;
- быстрая аналитика в dev/stage.

## Не разворачивать по умолчанию

- MCP с write-access к production trading data;
- MCP, имеющий прямой доступ к broker secrets;
- MCP, который обходит shell process/gates.

## Требования к rollout

1. MCP config должен работать и в Codex CLI, и в IDE.
2. Должен быть подготовлен project-scoped `.codex/config.toml`.
3. Repo должен считаться trusted project только после review.
4. Все MCP credentials хранятся вне repo.
5. Добавление MCP server документируется в:
   - `docs/architecture/product-plane/product-plane-spec-v2/08_Codex_AI_Shell_Integration.md`
   - shell instructions / runbooks (если необходимо)

## Предлагаемый порядок внедрения

### Wave 1
- GitHub official MCP
- OpenAI Developer Docs MCP

### Wave 2
- Docker MCP
- Dagster MCP

### Wave 3
- PostgreSQL read-only MCP
- DuckDB/MotherDuck MCP

## Пример project-scoped config

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

## Запрос на действие

Просьба:
1. подтвердить целевой набор MCP servers;
2. развернуть Wave 1 и Wave 2;
3. подготовить `.codex/config.toml` template для repo;
4. отдельно согласовать read-only DB access for Wave 3;
5. вернуть артефакт с:
   - перечнем развёрнутых серверов,
   - auth model,
   - owners,
   - known limitations.
