# Isolated L1/L2 tail calculation proof

Change surface: product-plane. Solution intent: staged, isolated implementation.
Base: `3f9d62a4913913463923b4dfd5a825bb784bc9bb`.

No production checkout/data writes, publication, service restart or scheduled job
was performed. Full reference calculations used only bounded isolated snapshots.
Protected source Delta versions and every log-file hash match before/after.
All 216 package source hashes in [report.json](report.json) match the tested code.

| Real input | Rows | Appended | L1 max rows/operation | L2 max rows/group |
| --- | ---: | ---: | ---: | ---: |
| FUT_BR 15m | 2100 | 20 | 532 | 84 |
| FUT_BR 1h | 2100 | 20 | 532 | 84 |
| FUT_GOLD 4h | 2100 | 20 | 532 | 84 |
| FUT_BR 1d | 1900 | 6 | 518 | 70 |

These are formula-work bounds, not comparable end-to-end timing measurements.
No full-partition L1 operation occurred on these appends. D1 bootstrapped one
complete six-row dependency group. Maximum L1 absolute difference from the
original full native formula path: `4.75e-12`; L2: `6.22e-15`.
Numeric admission: `rtol=1e-8, atol=1e-8`; keys, NULLs and event codes exact.
Native Delta write/readback passed, including serialized checkpoint restart,
unique keys and exactly one final-row L1 state carrier.

The retired input snapshots contain nonmonotonic epoch segments. The proof
explicitly selects the longest monotonic historical window and reports excluded
row counts/window bounds. Nonmonotonic input is separately tested to use full
fallback. Minute-level volume-profile production is a separate existing producer;
this proof retains its NO_SOURCE codes and masks, without minute input data.

236 focused tests passed. Two failures were independently reproduced at the
unchanged base and excluded from that matrix:

- `test_derived_native_contract_tracks_ladder_and_usage_policy`: malformed ladder
  fixture lacks `instrument_id`.
- `test_numerical_compute_contract_keeps_accepted_cache_version_one`: stale
  legacy version-one hash expectation.

Mandatory quick checks: 268 process/architecture tests passed, formatting,
lint and compilation passed. See [focused-tests.txt](focused-tests.txt) and
[boring-checks.txt](boring-checks.txt). [Loop gate](loop-gate.txt) and
[PR gate](pr-gate.txt) passed, including the native data-proof matrix. Its 24
Spark/Docker-profile skips are explicit environment limits. Independent design
and final implementation review: PASS, AS-01 through AS-10, no open findings.

Reproduction script: [prove_tail.py.txt](prove_tail.py.txt), exact executed
source. From this isolated checkout, copy it to `.tmp/prove_tail.py`, then run
`python .tmp/prove_tail.py`. It requires the preexisting isolated source snapshots
named in that script, installed native dependencies and `.tmp/tzdata` for Arrow.
Every generated table and copied package stays under this checkout's `.tmp`.

Residual scope: full reads, input hashing, masks and projection can remain O(N).
L2 exact suffix comparison can still write a large suffix after floating-point
differences in an old prefix; this does not cause full formula evaluation.
Legacy/invalid state, changed L1 history, roll/ladder/profile/backend changes
explicitly fall back. Production speed and Spark/Dagster deployment were not
measured; Docker was unavailable and was not started.
