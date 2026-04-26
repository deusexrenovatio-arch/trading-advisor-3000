---
name: ta3000-product-surface-naming-cleanup
description: Use for TA3000 active product-surface naming cleanup when phase/debug/internal labels must be replaced with capability or product names while preserving archive, history, and provenance intentionally.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-DOMAIN
scope: TA3000 active product-surface naming cleanup
routing_triggers:
  - product surface naming
  - active surface
  - phase labels
  - debug labels
  - capability names
  - naming cleanup
---

# TA3000 Product Surface Naming Cleanup

## When To Use
- The user wants `phase*`, numbered delivery labels, debug labels, or internal names removed from active product-facing surfaces.
- The task mentions capability names, product names, dual-surface rename, active/archive separation, or history readability.
- A previous cleanup left active residue across MOEX, research, docs, scripts, or tests.

Do not use this for immutable archive-only evidence, broad repo-wide string replacement, or shell-governance naming that is not product-facing.

## Procedure
1. Start with `scripts/validate_product_surface_naming.py` when available; use validator output as the routing list.
2. Classify each hit before editing:
   - active rename target: product-facing code, tests, docs, runbooks, scripts;
   - allowed provenance/history: archive notes, immutable evidence, historical docs, frozen fixtures;
   - ambiguous: read locally before changing.
3. Patch one cluster at a time, such as MOEX runtime, research runtime, tests, docs/runbooks, or scripts/CLI.
4. Rename active surfaces to capability/outcome names. Keep filenames, imports, exports, test names, headings, and CLI/help text aligned.
5. Do not perform global mechanical replacement across archive/history/provenance.
6. Re-run the validator after each cluster and reclassify any new residue.

## Windows Search
- Prefer `git grep` or PowerShell `Select-String` when `rg.exe` returns `Access is denied`.
- Search exact flagged identifiers first, not broad `phase` patterns.

## Failure Patterns
- Active identifiers and provenance strings are mixed together, causing either leftover active residue or history damage.
- File names change but callable/test/heading names stay stale.
- Diff churn becomes much larger than the validator output. Stop and reduce scope to one cluster.

## Verification
- Naming validator passes or remaining hits are intentionally archive/provenance only.
- Active product-facing names align across code, tests, docs, scripts, and CLI text.
- Touched docs links and targeted tests pass.
- Final diff reads as a capability-oriented rename slice, not a global rewrite.
