# Phase 9A Battle-Run Integration

## Purpose

Freeze the integrated `WS-E` flow that combines:

- `WS-A` data closure
- `WS-B` production strategy slice
- `WS-C` Telegram/PostgreSQL battle-run closure
- optional `WS-D` sidecar boundary visibility

This document describes the first integrated Phase 9A orchestration path.

## Integrated flow

The landed flow now works in this order:

1. consume or generate `MOEX` bootstrap evidence
2. run the frozen production strategy replay with `QUIK` live-smoke attachment
3. run battle-run lifecycle smoke on `PostgreSQL` + `Telegram`
4. attach observability artifacts
5. optionally attach sidecar preflight as a visible 9B boundary
6. render one integrated evidence package

## Landed command

```bash
python scripts/run_phase9_battle_run.py --output-dir artifacts/phase9-battle-run --bootstrap-report <bootstrap-report.json> --snapshot-path <quik-snapshot.json>
```

## Output model

The integrated command writes:

- bootstrap report
- strategy replay report
- runtime smoke report
- evidence markdown package
- optional sidecar preflight section

## Status semantics

The integrated script returns:

- `ready_for_review` when data, strategy, live-smoke, and battle-run runtime evidence are all green
- `blocked` when one of those integrated surfaces is not ready

This is intentionally not the same as automatic `Phase 9A accepted`.
Operator review and acceptance still remain explicit.

## Publication posture rule

The integrated command supports two publication postures:

- `shadow`
- `advisory`

Important boundary:

- runtime signal mode remains `shadow`
- `advisory` only changes the publication destination when an advisory channel is configured

This keeps current code truthful to the runtime enum surface while still allowing advisory-style operator routing.

## Phase 8 and 9B attachments

The integrated flow can also attach:

- existing or freshly generated Phase 8 proving evidence
- optional `WS-D` sidecar preflight for the same session

These attachments help operator review, but optional 9B boundary evidence does not block 9A readiness.

## Explicit non-claims

The integrated Phase 9A command does not claim:

- broker execution readiness
- automatic Phase 9A acceptance
- automatic Phase 9B canary readiness

It only claims that the current named Phase 9A battle-run contour is reproducible as one integrated run.
