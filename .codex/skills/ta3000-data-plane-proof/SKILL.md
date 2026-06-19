---
name: ta3000-data-plane-proof
description: Use for TA3000 data-plane and research materialization proof when code inspection is insufficient and the answer must verify authoritative storage roots, published current Delta truth, verification roots, _delta_log directories, row counts, reports, canonical tail alignment, timezone normalization, promotion integrity, or duplicate-key safety.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-DATA
scope: TA3000 authoritative data-plane proof, published-current Delta truth, and materialization evidence
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
- A task touches MOEX canonical data, economics side tables, research/gold materialization, Delta tables, indicators, derived indicators, campaign assets, or verification output.
- The risk is confusing route inspection, manifests, SQL plans, local smokes, verification roots, or old reports with published current Delta truth.
- A failure may come from canonical tail mismatch across bars, session calendar, roll map, economics side tables, or supporting metadata.

Do not use this for pure docs-only changes unless the docs claim production data proof.

## Proof Standard
Prefer executable storage evidence over structural confidence:
- authoritative root and exact output path;
- physical Delta directory with `_delta_log`;
- readable row counts, schema, or sampled rows;
- report JSON/manifest that binds to the same physical output;
- current-vs-verification status;
- failure reason and affected cutoff/window when proof fails.

## Current Versus Verification
- `current` means the published or pinned production-facing table root, for example accepted baseline paths or `research/gold/current`.
- `verification` means isolated proof output, usually disposable and not automatically authoritative.
- A verification run can prove code can rebuild a route, but it does not prove live-current data was promoted.
- A current-table inspection can prove the published state, but it does not prove the current code can rebuild it unless a fresh run binds to that code.
- Final claims must name which of these was proven.

## Procedure
1. Identify the authoritative root before reading code. For TA3000 product data, prefer the active `D:/TA3000-data` route over repo-local mock output.
2. Confirm the intended table family: raw/canonical bars, session calendar, roll map, economics side tables, research indicators, derived indicators, campaign outputs, rankings, candidates, or verification output.
3. Run the narrowest real materialization or verification command that writes outside test doubles when the task requires production-route proof.
4. Inspect physical outputs:
   - path exists;
   - `_delta_log` exists;
   - data files exist when rows are expected;
   - row counts are plausible for the table semantics;
   - schema/columns match the expected contract;
   - report output paths match the physical directories.
5. If support tables have different tails, compute or report the aligned cutoff instead of accepting the newest bar timestamp blindly.
6. Keep verification output isolated when the user did not ask to overwrite `research/gold/current`.

## Delta Proof Recipes
- Delta existence: check the table directory and `_delta_log`, not only a parent folder or manifest path.
- Row counts: count the Delta table through the approved Delta/Arrow/Spark helper; do not count checkpoint files or directory entries.
- Schema proof: inspect Delta schema or table columns when the claim depends on new fields.
- Empty-table proof: distinguish "table exists with `_delta_log`" from "table has rows"; some optional research outputs may legitimately be empty.
- Checkpoint exclusion: `_last_checkpoint` or checkpoint parquet files help Delta readers, but they are not business data proof by themselves.
- Report binding: compare report `output_paths`, `artifact_paths`, `row_counts`, `rows_by_table`, `run_id`, and timestamp fields with the physical table paths inspected.

## Tail And Time Proof
- Normalize timestamps before comparing tails; state whether values are UTC, Europe/Moscow, date-only sessions, or timezone-naive source strings.
- For canonical bars, compare against the supporting session calendar and roll map, then use the minimum aligned tail as the safe cutoff.
- For economics, check effective intervals and as-of semantics; a newer bar does not imply a newer economics row is valid unless the interval covers it.
- When a route uses bar-end joins, prove the tail and join timestamp follow that policy.

## Promotion Integrity
- Non-atomic promotion can leave staging, verification, and current roots out of sync. Inspect the target `current` root after promotion, not only the staging source.
- Confirm that the report or manifest was written for the same path now being claimed as current.
- Check for partial promotion: expected table set present, `_delta_log` present for every required table, row counts nonzero where required, and optional empty tables explicitly allowed.
- Do not treat an old successful verification root as evidence that a later published current table is fresh.

## Duplicate-Key Proof
- Use the table's declared business key, not file names or row positions.
- For canonical economics, the expected key is `contract_id`, `economics_session_date`, and `clearing_type`.
- For research tables, derive the key from the table contract and active contour: dataset/version, contour, series or instrument/contract, timeframe, timestamp, and indicator/parameter identifiers as applicable.
- Prove uniqueness with grouped counts or a contract validator when duplicate rows would change downstream joins, rankings, or backtests.
- If a table can contain multiple rows by design, state the key that makes the multiplicity valid.

## Failure Patterns
- Code route inspection passes but physical Delta output is absent.
- A manifest exists while data files are missing, stale, or unreadable.
- A verification root is described as published current.
- The newest canonical bar is later than the session calendar, roll map, or economics side-table coverage.
- Timezone-naive strings are compared directly to UTC or Europe/Moscow timestamps.
- Delta checkpoint files are counted as proof instead of reading table rows.
- Feature semantics are accidentally recreated after the user removed an active `features` path.
- A benchmark, fixture, or local smoke is presented as real production-route proof.

## Verification
- Output path and storage root are named.
- `_delta_log`, row counts, and relevant schema/columns are checked directly.
- Report JSON/manifest agrees with physical output.
- Current-vs-verification status is explicit.
- Any cutoff, skipped table, optional empty output, duplicate-key check, or missing supporting metadata is stated explicitly.
