# TA3000 Five-Year Rebuild Storage Layout

## Scope

- Change surface: `product-plane`.
- The job finalizes an already computed, isolated rebuild root.
- It never reads from or writes to published `current`.
- Full raw acquisition and the 31-table rebuild remain separate operator actions.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: staged partial closure; isolated Spark/Delta proof over a canonical dataset; downstream research and runtime-ready surface remain unproved; future shape preserved; not full target closure
- Shortcut Waiver: none

Chosen path: compute jobs write Delta tables below an isolated compute root.
`rebuild_storage_layout_job` then reads those tables through Spark, derives
`ts_close_year` only for manifest-selected heavy tables, sorts within files,
writes a distinct final root, and emits one fail-closed report for every table.

This preserves the target shape because logical business keys remain unchanged,
all writes stay Delta-native, and published roots are outside the operation.
The contour remains staged until the complete 31-table run and QC package pass.

## Native Runtime Choice

- Task domain: durable data materialization and file layout.
- Runtime owner: Spark and Delta Lake.
- Native primitive: Spark DataFrame repartition/sort/write into Delta tables.
- Python boundary: manifest validation, path binding, orchestration, and proof report.
- Proof surface: `_delta_log`, row counts, partition metadata, Parquet file counts and sizes.
- Fallback reason: none.

## Dagster Binding

Job: `rebuild_storage_layout_job`

Required op config:

- `layout_manifest_path`
- `source_root`
- `target_root`
- `published_current_root`
- `report_path`
- `run_id`

Optional: `spark_master`.

The source and target roots must be disjoint. Source, target, and report paths
must not overlap published `current`. The target must not already exist.
A heavy table normally produces one file per non-empty
`ts_close_year`. If that file exceeds the manifest limit, the writer performs
the smallest proven size-driven split and records it in
`size_split_partitions`.

Any missing source `_delta_log`, row-count mismatch, partition drift,
unexplained extra file, or oversized final file blocks the job.

## Remaining Gate

Do not promote or bind consumers to the rebuilt root until:

1. source acquisition is frozen;
2. all 31 compute tables exist;
3. this job returns `PASS`;
4. semantic QC and current-comparison reports pass;
5. a separate promotion decision proves reader pointer switching and rollback.
