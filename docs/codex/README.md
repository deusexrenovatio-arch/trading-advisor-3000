# Codex Package Intake

This lab folder defines a package-first intake flow for Codex work in this repository.

The target operator experience is:

1. drop one `zip` package with a full task/TZ document set into the intake inbox;
2. tell Codex to take the latest package;
3. let Codex unpack the package, choose the primary document, and continue the normal governance flow without silent assumptions.
4. for any real run, use the single governed launcher instead of relying on a plain prompt.

## Why this exists

The earlier single-spec entry assumption is too narrow for the real operator workflow.
In practice, the operator often prepares a document package outside the repository and drops the full bundle here.

This lab patch therefore optimizes for:

- one package instead of one markdown spec,
- one compact clarification block at most,
- no silent assumptions that survive into execution,
- explicit selection of one primary document plus supporting documents.

## Main pieces

- [Package Intake Contract](docs/codex/contracts/package-intake-lab.md)
- [Package Inbox](docs/codex/packages/README.md)
- [Package Runtime Prompt](docs/codex/prompts/entry/from_package.md)
- [Resume Current Phase Prompt](docs/codex/prompts/entry/resume_current_phase.md)
- `python scripts/codex_governed_bootstrap.py --request "<request>" --route auto`
- `python scripts/codex_governed_entry.py auto`
- [Launcher](scripts/codex_from_package.py)
- [Phase Orchestration](docs/codex/orchestration/README.md)
- [Phase Orchestrator](scripts/codex_phase_orchestrator.py)

## Current lab boundary

The lab now includes both:

- package-first intake,
- and an explicit worker -> acceptance -> remediation -> unlock orchestration loop.

The acceptance layer is now expected to enforce a hard contract:

- no silent fallbacks,
- no skipped required checks,
- no unresolved implementation assumptions,
- no deferred critical work,
- no `PASS` without real architecture, test, and docs evidence.

The route itself is now expected to be hard-entered:

- package intake or phase continuation should begin via `scripts/codex_governed_entry.py`,
- a plain prompt that never enters the launcher is a route miss, not a valid governed run.
- the runtime package prompt is launcher-internal and must not recursively call the launcher again.

The remaining open question is backend maturity:
- `simulate` is deterministic and testable now;
- `codex-cli` is the repo-local native backend when the local environment can invoke `codex exec` successfully.
