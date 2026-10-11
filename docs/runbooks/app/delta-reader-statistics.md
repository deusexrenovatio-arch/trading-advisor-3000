# Delta reads with incomplete file statistics

Change surface: product-plane. Delta/Arrow own table identity, native version
selection, scanning and predicate evaluation.

Spark may omit min/max statistics for Boolean columns. With Delta 0.25.5,
predicates against such columns can fail or incorrectly prune rows. The shared
reader rebuilds the Arrow dataset from the selected Delta table's active files
and partition expressions when predicate-column bounds are absent, then applies
the complete predicate to actual rows. Existing write behavior remains unchanged.

The fallback never scans a directory glob: removed Parquet files and files from
newer Delta commits are excluded. Projected rows/batches/counts/Arrow/DataFrame
reads share this behavior. Equality aliases `=` and `==` retain native semantics.

## Verification

Native regression covers absent bounds, Boolean/numeric predicates, projection,
DNF/null filters and pinned active files through each public read API. The same
test first reproduces the failure against unchanged main, then passes with the
reader fix. Native Delta 0.25.5: 65 tests pass on Linux/Arrow 24 and
Windows/Arrow 21. The unchanged-main characterization fails 42 of the 48 new
cases. Linux proof is bounded to 2 GiB/2 CPU with no network. Commands and final
counts are recorded in the PR validation evidence.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: latest fallback; directory glob; synthetic upstream closure
- Closure Evidence: native Delta reader regression and focused existing tests; not full target closure of canonical dataset/downstream research or runtime-ready surface
- Shortcut Waiver: none

## Self-review

Changed behavior: missing file statistics cannot silently remove matching rows.
Contract: exact selected Delta files/version plus the complete filter determine
the result. Forbidden old behavior: treating absent predicate bounds as proof of
no matching rows, or reading tombstoned/newer files during recovery. Regression:
`tests/product-plane/unit/test_delta_missing_statistics.py`. Residual limits:
this patch does not promote data, add snapshot publication or prove a full-size
refresh/OOM execution policy.
