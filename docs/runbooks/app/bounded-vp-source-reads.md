# Bounded volume-profile source reads

Change surface: product-plane. This is a staged correction to source selection
inside the existing L1 volume-profile loader. Standard indicator calculation,
minute-price interpretation and source-hash fields remain unchanged.

## Native Runtime Choice

- Domain: projected minute inputs for indicator materialization.
- Owner: Delta Lake and Arrow, through the existing approved
  `iter_delta_table_row_batches` predicate/scanner path.
- Primitive: exact instrument/contract envelope DNF, projected columns and
  Arrow record batches. The existing scanner defaults to 65,536 rows per batch.
- Python boundary: merge target intervals, test the exact half-open union by
  binary search, normalize existing source aliases and sort selected inputs.
- Proof: genuine Delta fixture tables and independent expected source rows/hash;
  code mounted read-only for the separate native Arrow execution.

Main previously read one global minute envelope with independent instrument and
contract IN predicates. Sparse target bars therefore retained gap minutes, and
unrelated instrument/contract combinations could enter that broad native read.
One DNF branch per target interval would also make native expression depth grow
with history. The selected correction uses one envelope per exact source pair
and at most 32 envelope branches in each native read.

Target intervals are merged per instrument/contract. Overlapping and touching
intervals form one union; each native source row is retained once when its open
timestamp belongs to `[target open, target close)`. Binary search applies this
exact membership after the envelope predicate. Gap minutes and close-edge
minutes are excluded. Daily session-close and clipped bar-end behavior continue
to use the existing target-close helper.

```mermaid
flowchart LR
    Bars[Target bars] --> Merge[Exact pair: merge half-open intervals]
    Merge --> Envelopes[At most 32 pair envelopes per read]
    Envelopes --> Arrow[Projected native Arrow batches]
    Arrow --> Union[Binary-search exact union]
    Union --> Inputs[Sorted existing source fields]
    Inputs --> VP[Existing volume-profile calculation and hash]
```

Canonical `instrument_id`/`contract_id`/`ts` and raw
`internal_id`/`finam_symbol`/`ts_open` aliases retain their priority. Raw
`moex_secid` remains the contract fallback. Existing source fields, NULL numeric
values, normalized instrument/contract fields, deterministic order and hash
serialization stay unchanged. Empty target input does not open Delta; an empty
source retains the requested keys with empty lists. Missing required columns
still fail closed. There is no pandas or full-table minute read fallback.

## Validation and staged execution

The native Delta tests cover sparse gaps, exact pairs, overlap/touch, right-edge
exclusion, UTC year crossing, source aliases, source NULLs and empty input. A
2,600-target-bar fixture with 65 contracts proves DNF branch counts `32/32/1`,
seven projected source columns and stable exact results across native batches
bounded to 17 rows for that test. Hash fields and selected row multiplicity are
checked against independent expected rows.

1. Deliver this isolated loader change over main's existing predicate reader.
2. Run the focused native Delta/Arrow tests and repository delivery gates.
3. Recompose it with the separate L1 cache/writer and downstream groups.
4. Run the fresh full-size owning route and independent data readback before
   claiming OOM closure or primary promotion.

The bound applies to native predicate expression depth and transient batches.
One logical series can still retain an unbounded number of matching minutes;
contract envelopes may read gap minutes that are immediately discarded. This
change alone does not close full-size memory or publication acceptance.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: full minute row read; pandas fallback; per-bar Arrow DNF; whole frozen runtime copy; synthetic upstream closure
- Closure Evidence: native Delta sparse-union red/green and read-only-code Arrow proof; full-size route and primary promotion remain pending
- Shortcut Waiver: none

## Self-review

Changed read contract: selected minutes must belong to the exact pair's target
interval union; the native expression must stay bounded independently of bar
count. Forbidden old behavior: gap-minute retention, broad pair combinations,
per-bar expression growth or a whole row-list read. Focused genuine Delta tests
catch these paths. The source hash function and numerical calculation are not
modified. Residual retained-series memory requires separate owning-route proof.
