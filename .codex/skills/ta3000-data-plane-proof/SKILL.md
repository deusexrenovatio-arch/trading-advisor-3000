---
name: ta3000-data-plane-proof
description: Use for TA3000 data-plane and research materialization proof when code inspection is insufficient and the answer must verify authoritative storage roots, Delta outputs, _delta_log directories, row counts, reports, or canonical tail alignment.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-DATA
scope: TA3000 authoritative data-plane proof and materialization evidence
routing_triggers:
  - data-plane proof
  - D:/TA3000-data
  - Delta proof
  - _delta_log
  - row counts
  - canonical bars
  - research materialization
  - real prod tables
---

# TA3000 Data-Plane Proof

## When To Use
- The user asks whether a data route really works, especially on authoritative `D:/TA3000-data` roots.
- A task touches MOEX canonical data, research/gold materialization, Delta tables, indicators, derived indicators, or data-prep assets.
- The risk is confusing route inspection, manifests, SQL plans, or test doubles with real persisted data.
- A failure may come from canonical tail mismatch across bars, session calendar, roll map, or supporting metadata.

Do not use this for pure docs-only changes unless the docs claim production data proof.

## Proof Standard
Prefer executable evidence over structural confidence:
- authoritative root and exact output path;
- actual Delta directory with `_delta_log`;
- readable row counts or sampled rows;
- report JSON or manifest that matches the physical output;
- failure reason and affected cutoff/window when proof fails.

## Procedure
1. Identify the authoritative root before reading code. For TA3000 product data, prefer the active `D:/TA3000-data` route over repo-local mock output.
2. Confirm the intended table family: canonical bars, instruments/contracts/session calendar/roll map, research indicators, derived indicators, campaign assets, or verification output.
3. Run the narrowest real materialization or verification command that writes outside test doubles when the task requires production-route proof.
4. Inspect physical outputs:
   - path exists;
   - `_delta_log` exists;
   - data files exist when expected;
   - row counts are plausible;
   - report output paths match the physical directories.
5. If support tables have different tails, compute or report the aligned cutoff instead of accepting the newest bar timestamp blindly.
6. Keep verification output isolated when the user did not ask to overwrite `research/gold/current`.

## Failure Patterns
- Code route inspection passes but physical Delta output is absent.
- A manifest exists while data files are missing or unreadable.
- The newest canonical bar is later than the session calendar or roll-map tail.
- Feature semantics are accidentally recreated after the user removed an active `features` path.
- A benchmark or fixture proof is presented as real production-route proof.

## Verification
- Output path and storage root are named.
- `_delta_log` and row counts are checked directly.
- Report JSON/manifest agrees with physical output.
- Any cutoff, skipped table, or missing supporting metadata is stated explicitly.
- The final claim distinguishes fixture, verification, and production-route evidence.
