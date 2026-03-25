# Phase Acceptance Orchestration

This layer runs module phases through an explicit execution loop:

1. worker session implements only the current phase;
2. acceptance session checks only the current phase;
3. if acceptance is `BLOCKED`, the same phase enters remediation;
4. the next phase is unlocked only after acceptance returns `PASS`.

## Why this exists

Plain phase planning is not enough.
The operator wants automatic progression only after an independent acceptance pass, not manual trust that a completed phase is really acceptable.

## Core guarantees

- The worker and acceptor run as separate sessions.
- The acceptor cannot unlock the next phase unless it returns `PASS`.
- The worker cannot advance the parent brief on its own.
- Remediation attempts stay inside the same phase until acceptance passes or the retry limit is reached.
- The orchestrator itself will still force `BLOCKED` if the payload reports:
  - assumptions,
  - skipped checks,
  - fallback paths,
  - deferred critical work,
  - evidence gaps,
  - prohibited findings.

## Acceptance contract

- Human-readable contract: `docs/codex/orchestration/acceptance-contract.md`
- Hard review lens skill: `.cursor/skills/phase-acceptance-governor/SKILL.md`
- Companion skills:
  - `.cursor/skills/architecture-review/SKILL.md`
  - `.cursor/skills/testing-suite/SKILL.md`
  - `.cursor/skills/docs-sync/SKILL.md`

Acceptance is fail-closed.
`PASS` is not allowed when unresolved quality debt is only being "recorded".

## Backends

The orchestrator supports pluggable backends.

### `simulate`

Used for deterministic local testing of the orchestration state machine.

### `codex-cli`

Uses isolated `codex exec` runs for:
- worker
- remediation worker
- acceptor

This is the most practical repo-local native path today because the chat-only `spawn_agent` tool is not directly callable from repository scripts.
In this lab, the standalone CLI was installed through `npm install -g @openai/codex` and exposed through a shell shim so PowerShell resolves the standalone CLI instead of the broken `WindowsApps` alias.

## Main artifacts

For each run, the orchestrator writes under `artifacts/codex/orchestration/<run-id>/`:

- `state.json`
- a human-readable route report artifact
- per-attempt prompt files
- worker last message
- changed-files snapshot
- acceptance json
- acceptance markdown report

## Role defaults

- worker: `gpt-5.3-codex`
- acceptor: `gpt-5.4`
- remediation: defaults to the worker model unless overridden

Role-specific model and profile overrides are available directly on the runner CLI.

## Main entrypoint

```bash
python scripts/codex_governed_entry.py auto
```

```bash
python scripts/codex_phase_orchestrator.py preflight \
  --execution-contract docs/codex/contracts/plan-tech.execution-contract.md \
  --parent-brief docs/codex/modules/plan-tech.parent.md
```

```bash
python scripts/codex_phase_orchestrator.py run-current-phase \
  --execution-contract docs/codex/contracts/plan-tech.execution-contract.md \
  --parent-brief docs/codex/modules/plan-tech.parent.md \
  --backend simulate \
  --simulate-scenario block-then-pass
```

## Unlock rule

The parent brief and execution contract advance only after acceptance `PASS`.
If acceptance is `BLOCKED`, the same phase remains current.

## Resume flow

When a package has already been intake-processed and the module is in progress, use:

- `docs/codex/prompts/entry/resume_current_phase.md`
- `python scripts/codex_governed_entry.py continue --execution-contract <path> --parent-brief <path>`

This keeps the operator flow simple:

1. intake once;
2. continue by current phase pointer;
3. keep the governed route visible in artifacts and summaries.

Manual chat continuation without the governed launcher is a route miss.
