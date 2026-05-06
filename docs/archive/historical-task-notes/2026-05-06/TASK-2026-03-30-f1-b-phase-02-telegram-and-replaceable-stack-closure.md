# Task Note
Updated: 2026-03-31 11:39 UTC

## Goal
- Deliver: F1-B phase-02 closure patch for Telegram and replaceable-stack terminal consistency without opening next phase scope.

## Task Request Contract
- Objective: make the in-scope replaceable technologies terminal and non-ambiguous across route state, ADR set, active architecture docs, registry enforcement, and tests.
- In Scope: `docs/session_handoff.md`; one active F1-B task note; `docs/archive/product-plane-spec-v2/2026-05-06/01_Architecture_Overview.md`; `docs/archive/product-plane-spec-v2/2026-05-06/04_ADRs.md`; `docs/archive/product-plane-spec-v2/2026-05-06/08_Codex_AI_Shell_Integration.md`; `registry/stack_conformance.yaml`; `scripts/validate_stack_conformance.py`; `tests/process/test_validate_stack_conformance.py`; `scripts/build_publication_lifecycle_evidence.py`; `tests/process/test_build_publication_lifecycle_evidence.py`; `src/trading_advisor_3000/product_plane/runtime/publishing/telegram.py`; `src/trading_advisor_3000/product_plane/runtime/pipeline.py`; `src/trading_advisor_3000/product_plane/runtime/bootstrap.py`; `tests/product-plane/unit/test_runtime_components.py`; attempt-scoped publication evidence artifact under `artifacts/codex/orchestration/*/attempt-*/publication-message-lifecycle-evidence.json`.
- Out of Scope: phase-03 contracts-freeze expansion, broker-process closure, release-readiness verdict change, and any trading/business logic.
- Constraints: keep patch phase-scoped; no silent assumptions/fallbacks/skips/deferrals; preserve pointer-shim handoff contract; keep docs and validator fail-closed.
- Done Evidence: run phase checks for task contract/handoff, stack conformance, targeted runtime/research/observability tests, and canonical loop/pr gates.
- Priority Rule: quality and governance integrity over speed.

## Solution Intent
- Solution Class: staged
- Critical Contour: runtime-publication-closure
- Forbidden Shortcuts: synthetic publication
- Closure Evidence: runtime output plus durable store and publication contour checks, with end-to-end publication proven through real Telegram Bot API receipts in attempt-01 evidence.
- Shortcut Waiver: none
- Chosen Path: staged remediation that replaces local publication simulation with the real Bot API runtime path and keeps phase scope unchanged.
- Why It Is Not A Shortcut: publication evidence is tied to external Telegram API calls and fail-closed probes rather than synthetic publication artifacts.
- Future Shape Preserved: this is staged for the full F1 path; within F1-B scope, publication contour evidence is now live-real and non-synthetic.

## Current Delta
- Remediation moved runtime publishing from local simulation to real Telegram Bot API transport (create/edit/close/cancel) when a bot token is configured, with explicit in-memory mode kept for local/test.
- Phase-02 evidence builder now executes the lifecycle through the real Bot API path and records immutable operation receipts (`sendMessage`/`editMessageText`/`deleteMessage`) for attempt-scoped evidence.
- Runtime bootstrap now fail-closes when `TA3000_TELEGRAM_CHANNEL` is missing; silent default/fallback channel behavior is removed.
- Publication evidence builder no longer injects a fallback channel when channel binding is missing; missing binding now stays explicit and fail-closed.
- Attempt-01 evidence now proves `getMe`, `getChat`, and full real lifecycle (`create/edit/close/cancel`) through Telegram Bot API receipts on explicit chat binding `186419048`.

## First-Time-Right Report
1. Confirmed coverage: phase objective is mapped to route integrity + docs alignment + validator/test enforcement.
2. Missing or risky scenarios: removed technologies can silently reappear in active overview/ADR wording without failing existing checks.
3. Resource/time risks and chosen controls: keep changes limited to F1-B files and add one focused fail-closed guard with regression tests.
4. Highest-priority fixes or follow-ups: route pointer switch first, then ADR/overview correction, then validator/test reinforcement.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: if the same phase check fails twice after targeted fixes.
- Reset Action: stop edits, capture exact failing evidence, and re-scope to blocker-only remediation.
- New Search Space: stack registry guard logic and active architecture/ADR truth-source alignment.
- Next Probe: run phase-targeted validators and tests before broad closeout gates.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: replaceable_stack_alignment, publication_chat_contour
- Route Match: worker:phase-only
- Primary Rework Cause: prior acceptance blockers B1 B2 B3 P-EVIDENCE_GAP-1
- Incident Signature: none
- Improvement Action: keep explicit chat/channel binding governance so live-real evidence remains replayable and non-fallback.
- Improvement Artifact: artifacts/codex/orchestration/20260331T113542Z-f1-full-closure-phase-02/attempt-01/publication-message-lifecycle-evidence.chat-186419048.json

## Blockers
- none

## Next Step
- Hand off to acceptance with the current attempt evidence bundle and keep scope locked to F1-B (do not open phase-03 in this worker route).

## Validation
- `python scripts/build_publication_lifecycle_evidence.py --attempt "20260331T113542Z-f1-full-closure-phase-02/attempt-01" --output "artifacts/codex/orchestration/20260331T113542Z-f1-full-closure-phase-02/attempt-01/publication-message-lifecycle-evidence.json" --fail-if-not-live-real` (exit 1 observed: channel binding missing in env)
- `python scripts/build_publication_lifecycle_evidence.py --attempt "20260331T113542Z-f1-full-closure-phase-02/attempt-01" --output "artifacts/codex/orchestration/20260331T113542Z-f1-full-closure-phase-02/attempt-01/publication-message-lifecycle-evidence.chat-186419048.json" --publication-channel "186419048" --fail-if-not-live-real`
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/validate_stack_conformance.py`
- `python -m pytest tests/process/test_validate_stack_conformance.py -q`
- `python -m pytest tests/process/test_build_publication_lifecycle_evidence.py -q`
- `python -m pytest tests/product-plane/integration/test_historical_data_plane.py -q`
- `python -m pytest tests/product-plane/unit/test_runtime_components.py -q`
- `python -m pytest tests/product-plane/unit/test_durable_runtime_bootstrap.py -q`
- `python -m pytest tests/product-plane/integration/test_runtime_postgres_store.py -q`
- `python -m pytest tests/product-plane/integration/test_research_plane.py -q`
- `python -m pytest tests/product-plane/integration/test_review_observability.py -q`
- `python scripts/sync_skills_catalog.py --check`
- `python scripts/validate_skills.py --strict`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
