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
- Worker attempts cannot edit documentation surfaces; docs edits are remediation-only.
- Docs remediation must carry full source + materialized context and explicit goal/acceptance preservation evidence.
- The orchestrator itself will still force `BLOCKED` if the payload reports:
  - assumptions,
  - skipped checks,
  - fallback paths,
  - deferred critical work,
  - recurrence risks,
  - operational exceptions,
  - evidence gaps,
  - prohibited findings.

## Acceptance contract

- Human-readable contract: `docs/codex/orchestration/acceptance-contract.md`
- Bounded worker evidence contract: `docs/checklists/phase-evidence-contract.md`
- Route/snapshot/profile vocabulary (H0 declaration): `docs/codex/orchestration/route-snapshot-profile-contract.md`
- CI/proof runtime contract (H2): `docs/codex/orchestration/proof-runtime-contract.md`
- Hard review lens skill: `.cursor/skills/phase-acceptance-governor/SKILL.md`
- Companion skills:
  - `.cursor/skills/architecture-review/SKILL.md`
  - `.cursor/skills/code-reviewer/SKILL.md`
  - `.cursor/skills/testing-suite/SKILL.md`
  - `.cursor/skills/docs-sync/SKILL.md`
  - `.cursor/skills/verification-before-completion/SKILL.md`

Acceptance is fail-closed.
`PASS` is not allowed when unresolved quality debt is only being "recorded".

## H0/H1 contract vocabulary status

- `H0` introduced explicit route/snapshot/profile/session terms as contracts.
- `H1` keeps legacy commands operational as wrappers, and adds explicit dual-mode metadata in runtime outputs:
  - governed entry route state now records `route_mode`, `session_mode`, `snapshot_mode`, and `profile`;
  - loop/pr gate output now includes explicit `snapshot_mode` and `profile` markers.
  - bootstrap reuse must fail closed on session-mode mismatch instead of silently reporting a different mode than the active task session.
- Strong fail-closed marker enforcement remains a later phase concern.

## H2 CI/proof status

- PR lane now resolves contour-aware profile/check plans through `scripts/run_surface_pr_matrix.py`.
- Hosted CI emits contour/profile summary artifacts under `artifacts/ci/`.
- Docker proof runtime normalization is shared through `scripts/proof_runtime_contract.py` and adopted by `scripts/run_phase2a_spark_proof.py`.

## H3 stacked follow-up / recomposition status

- Governed entry supports `--route stacked-followup` with explicit predecessor merge context and new base contract.
- Multi-module ambiguity now emits machine-readable resolution artifact before fail-closed exit:
  - `.runlogs/codex-governed-entry/module-ambiguity-report.json`
- Stacked follow-up continuation contract artifact is emitted by default:
  - `.runlogs/codex-governed-entry/stacked-followup-contract.json`
- Stacked follow-up continuation contract is forwarded into orchestrator preflight/worker execution, recorded in orchestration state metadata, and validated fail-closed against the selected execution contract + parent brief.
- Truth recomposition helper/validator is available:
  - `scripts/truth_recomposition.py build`
  - `scripts/truth_recomposition.py validate`

## H4 enforcement / release-decision status

- Loop/PR policy-critical paths now require explicit markers (`snapshot_mode` + `profile`) and fail closed when missing.
- Governed write paths use repo mutation serialization with lock event evidence:
  - `.runlogs/codex-governed-entry/repo-mutation-events.jsonl`
- Explicit release decision package builder is available:
  - `scripts/build_governed_release_decision.py`
- Final `ALLOW_RELEASE_READINESS` / `DENY_RELEASE_READINESS` emission is acceptance-owned and must bind to acceptance artifacts (`acceptance.json`), not worker-only evidence.
- Release-decision closeout fails closed unless the current attempt provides an explicit phase-scoped route-state artifact/input; mutable global `.runlogs/codex-governed-entry/last-route.json` is not accepted as implicit fallback.
- Decision output is explicit and fail-closed:
  - `ALLOW_RELEASE_READINESS`
  - `DENY_RELEASE_READINESS`

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
- remediation: `gpt-5.3-codex` by default; escalate to `gpt-5.4` on the second remediation attempt unless explicitly overridden

## Current package-intake model policy

- `product_intake`: `gpt-5.4`
- `technical_intake`: `gpt-5.3-codex`
- `materialization`: `gpt-5.3-codex`

Role-specific model and profile overrides are available directly on the runner CLI.

## Draft vNext routing (planned, not yet enforced)

This section records the current working agreement for the next orchestration revision.
It is a target-state contract and is not fully wired into the runtime yet.

### Intake phase split

Intake is treated as a full phase with its own `PASS` / `BLOCKED` gate.

- `INTAKE_TECH` (codex exec stream):
  - purpose: architecture fit and OSS fit evaluation for incoming requirements;
  - core skills: `architecture-review`, `tz-oss-scout`;
  - expected model policy: `gpt-5.4` with `xhigh` reasoning.
- `INTAKE_PRODUCT` (codex exec stream):
  - purpose: use cases, acceptance test cases, and product value framing;
  - core skills: `business-analyst`, `product-owner`;
  - expected model policy: `gpt-5.4` with `xhigh` reasoning.
- `INTAKE_GATE` (codex exec stream):
  - purpose: merge both intake reports and return `PASS` / `BLOCKED`;
  - may return `BLOCKED` when architecture concerns remain open, OSS fit is weak, or product value is not justified enough;
  - on `BLOCKED`, operator input is required before phase execution continues.

### Acceptance findings contract (vNext)

Acceptor findings should be emitted in a structured shape per lens:

- `lens` (for example: route-integrity, test-evidence, docs-coverage, product-value);
- `severity` (`P0`, `P1`, `P2`);
- `size` (`S`, `M`, `L`, `XL`);
- `blast_radius` (`local`, `cross-surface`);
- `summary`;
- `required_evidence`.

In addition, the acceptance payload must report:

- `recurrence_risks`: explicit follow-up risks that would likely recreate the same blocker class;
- `operational_exceptions`: host/env/manual-workaround exceptions that still affect truthful closure.

`architecture-review` is removed from the acceptor default lens set in this vNext design.
Product-value evaluation is embedded into acceptance output instead of a separate product gate.

### Worker vs remediation routing rule (vNext)

Route to worker when all blockers are small/local:

- no `P0`;
- change size is only `S` or `M`;
- blast radius is `local`.

Route to remediation when risk/volume is high:

- any `P0`; or
- any `L` / `XL`; or
- any `cross-surface` blocker; or
- repeated blocker after one worker retry.

### Worker skills (vNext baseline)

- `testing-suite`
- `docs-sync`
- `verification-before-completion`
- plus phase-specific/domain skills required by the active phase brief

### Remediation skills (vNext baseline)

- `repeated-issue-review`
- `testing-suite`
- `docs-sync`
- `verification-before-completion`
- plus the exact skills needed by blocker category (for example security, data, integration, CI)

### Reasoning effort policy (vNext)

Target policy is `xhigh` reasoning effort for all governed codex exec streams:
intake-tech, intake-product, intake-gate, worker, acceptor, and remediation.

## Main entrypoint

```bash
python scripts/codex_governed_bootstrap.py --request "continue governed module" --route auto
```

```bash
python scripts/codex_governed_entry.py auto
```

```bash
python scripts/codex_governed_entry.py --route stacked-followup \
  --execution-contract docs/codex/contracts/plan-tech.execution-contract.md \
  --parent-brief docs/codex/modules/plan-tech.parent.md \
  --predecessor-ref <merged-ref> \
  --source-branch <split-branch> \
  --new-base-ref origin/main \
  --carry-surface <surface>
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
