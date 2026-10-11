# Raw UTC close-year writers

Change surface: mixed; product storage behavior and required delivery metadata.
Solution class: staged. This group activates only `raw_moex_history` defaults.
Canonical and research storage defaults remain unchanged until their owning
producers carry authoritative close metadata.

## Runtime ownership and contract

Stable raw updates remain Dagster -> Spark DataFrames -> Spark Delta write/merge
and `OPTIMIZE`. Python adapts the contract and reads active Delta add metadata;
it does not rewrite the stable table through delta-rs. `RAW_COLUMNS`, raw keys,
provider fingerprints, source timestamps and OHLCV/OI values remain unchanged.
`ts_close_year` is a physical column derived afresh from authoritative `ts_close`.
A supplied year never controls placement.

Arrow epoch values are cast to UTC, then their UTC tag is removed for calendar
extraction without a Windows timezone database dependency. Spark LTZ timestamps
are converted from the session wall-clock representation to UTC NTZ before
extracting the year. Physical raw timestamps must be LTZ instants: NTZ source
carriers are rejected before migration normalizes or writes them. Neither
adapter changes the original timestamp column. A borrowed Moscow Spark session
keeps its timezone setting.

| Open / close | Physical year |
| --- | --- |
| `2025-12-31T23:59Z` / `2026-01-01T00:00Z` | 2026 |
| `2025-12-31T21:00Z` / `2026-01-01T01:00+03:00` | 2025 |

Existing actual Delta `partitionColumns` must equal `[ts_close_year]` before a
stable mutation. A flat table containing a year column still fails this guard.
Legacy tables require a separate isolated migration/publication decision.

## Owning writers and candidate boundary

- `moex_raw_ingest_job`: native Spark bootstrap, append, correction and deletion;
  postwrite Spark compaction when raw changes occur; no-op preserves the version.
  Every successful existing outcome, including empty input/no-op, checks active
  file metadata without writing: retry cannot accept a prior fragmented result.
- `moex_raw_layout_migration_job`: new isolated staged target only, including the
  `.layout-staged.delta` alias. Existing nonempty/Delta targets are rejected even
  with the old `overwrite_staged` flag. Source/target ancestor intersections are
  rejected before Spark starts. The source is not rewritten.
- Shared Delta row/batch helpers: fresh raw candidates only. Existing raw targets
  are rejected before overwrite, replacement, deletion or schema append. A fresh
  batch stream records its created Delta ID and checks it before each append
  and finalization. A missing target/log or replaced ID fails before any write.
- Both raw consolidation CLI routes use the same native Arrow adapter and carry
  the stream's original Delta ID across shard/batch appends. A reused target is
  rejected. Their previous root deletion is removed.

Arrow compaction runs once after a completed fresh stream. Stable compaction uses
Spark Delta `OPTIMIZE`, with 512 MiB target/minimum-small-file settings and one
compaction job thread; those settings are restored afterward. Both paths validate
active add actions: every file nonempty and <=512 MiB, valid year, one active
file in a year whose total compressed bytes fit the cap. Larger years may contain
multiple capped files. A target size is not a hard size guarantee: a measured
oversize or under-cap fragmented result fails closed and requires the existing
isolated finalizer. No silent custom splitting, directory deletion, root move or
`VACUUM` is introduced. Delta logs and inactive files remain for version pins.

Postwrite layout failure can occur after a Delta commit. It blocks acceptance;
it does not mean that the write was rolled back or multiple tables were atomic.
Migration profiling reads active Delta files and excludes retained tombstones
and checkpoints. The previous file-count/average-size improvement metric remains
separate from measured storage acceptance.

## Deferred publication and remaining families

Migration root swap, stable pin-copy replacement and promotion are deferred to a
pin-aware publication group. This patch does not run those entrypoints, cut over
a data root, change a production launcher or claim dataset readiness.

| Heavy family | This group / required close source |
| --- | --- |
| raw_moex_history | Active defaults; actual `ts_close` |
| canonical_bars | Deferred; paired provenance `bar_end_ts` by existing key |
| canonical_bar_provenance | Deferred; `bar_end_ts` |
| research_bar_views | Deferred; actual end/session close under existing policy |
| research_indicator_frames | Deferred; producer carrier from original L0 bar |
| research_derived_source_frames | Deferred; inherited L0 end/session close |
| research_derived_indicator_frames | Deferred; original target L0 close binding |
| continuous_front_indicator_frames | Deferred; reconcile persisted close against source/session |
| continuous_front_derived_indicator_frames | Deferred; same CF reconciliation |

The final delivery needs physical proof for all nine families. Canonical business
JSON contracts must stay separate from physical store columns. Daily/weekly close
must follow source/session provenance; adding a nominal duration is insufficient.
The standalone finalizer's nonraw open-year map and CF roll-rule family identity
remain in later coherent groups. Existing staged 2018-2026 year directories prove
layout only, not UTC-close semantics. Research MTF/source projection repair is a
separate prerequisite and is not repaired by storage migration.

## Proof and self-review

Meaningful unit RED on exact main `25b84256`: raw shared writer unpartitioned,
legacy replacement did not reject before deleting, file policy absent.
Bounded native Delta/Arrow tests now cover UTC boundaries, supplied year, empty
raw, active files/cap, flat-with-year guards, stream identity, old version reads,
both CLI reuse guards and migration guards. Review RED also reproduced the
no-op retry bypass, vanished candidate append/finalize, both ancestor directions
and NTZ normalization; all six now pass in the bounded 59-test suite. Existing
Delta-reader tests preserve PR180 behavior. Native Spark tests prove borrowed
Moscow LTZ boundaries, genuine NTZ-source rejection before target write, staged
migration/source pins, and stable create/append/correction/delete/noop/empty input.
Linux fixture proof passed 10 integration cases plus 2 fixture-corrected physical
migration cases: 12 distinct integration cases +59 unit PASS. The integration
count includes guard-only Windows/HADOOP_HOME checks; native owning cases prove
the Spark/Delta behavior above. Measured JVM heap 4 GiB,
full-batch cgroup peak 5,006,192,640 bytes /6 GiB, OOM=0. Runner infrastructure and
test-adapter failures are retained separately; product runtime was unchanged.
Loop and actual PR gates passed, including 244 process/architecture tests and the
current 13-check mixed integration matrix. Windows-only skips in that gate are
separate from the executed native raw proof.

Old behavior now forbidden: raw Arrow mutation of an existing target, stable
legacy-layout update, caller-supplied year placement, root deletion in raw
consolidation and accepting physical layout from a year column alone.
Residual limits: only raw defaults activated; the eight remaining families,
actual-data migration/publication and a full Dagster update are still open.
