# Agent Context Map

## Purpose
Keep hot context deterministic by routing work to bounded ownership cards.

## Context Catalog
- `CTX-OPS` -> `docs/agent-contexts/CTX-OPS.md`
- `CTX-CONTRACTS` -> `docs/agent-contexts/CTX-CONTRACTS.md`
- `CTX-ARCHITECTURE` -> `docs/agent-contexts/CTX-ARCHITECTURE.md`
- `CTX-DATA` -> `docs/agent-contexts/CTX-DATA.md`
- `CTX-RESEARCH` -> `docs/agent-contexts/CTX-RESEARCH.md`
- `CTX-ORCHESTRATION` -> `docs/agent-contexts/CTX-ORCHESTRATION.md`
- `CTX-API-UI` -> `docs/agent-contexts/CTX-API-UI.md`
- `CTX-DOMAIN` -> `docs/agent-contexts/CTX-DOMAIN.md`
- `CTX-EXTERNAL-SOURCES` -> `docs/agent-contexts/CTX-EXTERNAL-SOURCES.md`
- `CTX-SKILLS` -> `docs/agent-contexts/CTX-SKILLS.md`

## Rename Freeze
- `CTX-STRATEGY` is migrated as `CTX-DOMAIN`.
- `CTX-NEWS` is migrated as `CTX-EXTERNAL-SOURCES`.

## Routing Script
- `python scripts/context_router.py --from-git --format text`
- `python scripts/validate_agent_contexts.py`

## Rules
1. Prefer one primary context per patch.
2. If multiple contexts are touched, split patch sequence.
3. Keep cold paths (`plans/`, `memory/`, archives, package dump) out of default hot retrieval.
