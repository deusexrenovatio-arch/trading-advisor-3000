# Data and research snapshot publication

Status: staged owner activation. Existing native calculations retained.
Change surface: mixed: product-plane owners and required delivery metadata.

## Existing route

```mermaid
flowchart LR
    raw[Raw refresh] --> econ[Economics: pinned inputs]
    econ --> canonical[Canonical owning writes]
    canonical --> data[Accepted Data snapshot]
    data --> cf[CF catch-up: exact Data parent]
    cf --> research[Data prep: five owning stages]
    research --> candidate[Candidate Research snapshot]
    candidate --> compact[Quiesce and native compaction]
    compact --> final[Final Data + same-parent Research CAS]
```

`run_moex_baseline_update` publishes Data after successful existing baseline,
canonical and economics gates. Pre-calculation economics inputs remain stage
provenance; canonical outputs get their final physical pins after owning writes.
Immutable external inputs must remain unchanged. Failed publication retains
pending changed windows and cannot report an accepted baseline.

`build_research_data_prep_run_config` enables `research_data_snapshot` and forwards
`data_snapshot_manifest_path` plus `data_snapshot_raw_output_dir`. The two
existing Data sensors forward these bindings and the calculated window. An
explicit parent must match accepted current Data. Resuming a checkpoint and
terminal publication recheck its parent ID and digest. Summary success depends
on the terminal publisher.

The materialization owner declares exact writable output paths. All other reads
pin native table identity, version, schema, layout and active files. Strict S1
consumer scopes retain their existing latest/capture rejection. Owner setup
cannot replace an active consumer scope. Subprocess manifests preserve these
pins across host/container mount mapping; unsupported envelopes fail closed.
Stage FAIL/ERROR, unknown status, invalid success or missing mapping result cannot
capture old outputs or stamp a PASS checkpoint. CF acceptance remains mandatory.

## Current control run bindings

- Data/research end: `2026-10-09T21:00:00Z`; raw remains through 10 October.
- Prepared securities: `d550b9e4-e3d6-40dc-93f0-fc0e420e3b43@1`.
- FX: `29b6ad08-7eeb-4dff-97b7-7521de4ce4cd@3`.
- Daily OI: `026176bf-9d3b-4192-adcf-559cd33f7c93@2`.
- Explicit historical margin calibration: `2026-08-14` with its paired switch.
- Parameter evidence: `bounded-tail-20261010-v1`; 13 reviewed regimes,
  exact metadata SHA checks, prepared identity/version admission.

The old bundle and prepared `@0` remain selectable. The second inference interval
extends through 10 October; actual new SETTLE/TC/OI inputs cover 6–9 October.
Current 74-row MC crosscheck is separate from the preserved 674 dated TC checks.
No invented 10 October rows or daily official STEPPRICE coverage.

This cap is one control-run boundary. The existing nightly cutoff remains
calendar-driven. Automatic history-to-raw/prepared/OI append and admitted
parameter renewal are still required before claiming continuing nightly readiness
beyond the bounded bundle. Default economics validation remains strict.

## Promotion and rollback boundary

The new root remains a candidate. Its job terminal is intermediate publication.
After fresh L1/derived MTF, complete native compaction and quiesce writers. Use
existing S1 capture/create/save/publish APIs to capture post-compaction versions,
schema/layout/files for final Data and Research. Preserve calculation QC and
provenance; final Research has the exact final Data ID as parent. CAS each
existing registry against its expected current ID. The two registries are not
one atomic transaction: if the second CAS fails, keep consumers quiesced and
complete its saved Research manifest before cutover.

No production pointers, launcher roots, schedules, deletion, VACUUM or service
state are changed by this code patch. Existing cutover/rollback operations apply
only after accepted candidate data and delivered code. Physical fresh-job and
post-compaction proof are separate from these focused unit guard checks.
