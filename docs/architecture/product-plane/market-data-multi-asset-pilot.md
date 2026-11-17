# Multi-asset market data pilot

Change surface: product-plane. Architecture impact: contract-change.
Decision status: scoped pilot authorized; review receipts belong to the task/PR.
Implementation status requires its linked proof; this decision alone proves no
runtime result. Deployment scope: verification only; no production promotion.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: production mutation; fake futures IDs for indices; fabricated missing values; fixture-only native proof; reading unaccepted latest tables.
- Closure Evidence: real source acquisition, isolated Dagster/Spark/Delta execution, pinned bounded consumer readback, repeat/correction/foreign-preservation and rejection regressions. This closes the bounded ingestion pilot only.
- Shortcut Waiver: none

## Context and choice

The existing MOEX universe and research path specialize in futures. Preserve
their IDs, contracts, published roots and schedules. Market Data Foundation
owns an additive, explicitly invoked pilot for SBER, its source-discovered
dated futures contracts, IMOEX and IMOEX2. Initial scope is daily native candles
from 2026-09-01 through 2026-10-26; neither historical universe completeness nor
equivalence to TradingView feeds is claimed. TradingView IRUS/IRUS2 are display
aliases only, never assumed to be identical source data.

Use the existing MoexISSClient transport with a new lossless source-candle
iterator. Pagination follows raw response counts; malformed rows fail the run
rather than silently terminating a page. Store exact source rows and current
metadata snapshots with fetch timestamps. Current metadata is not historical
contract economics. Discover the SBER futures family from official metadata,
retain ASSETCODE evidence, and sample official history snapshots to identify
observed contracts, including expired contracts; full chain completeness is
not claimed. Do not infer a chain solely from ticker prefixes or today's list.

Versioned quote-series and quote-bar JSON schemas own the new boundary. The
series descriptor identifies source engine/market/board/SECID, neutral security
identity, asset class, source timezone, price basis, volume unit, and underlying
relation. Existing contract_id is not repurposed. Bar uniqueness is
(series_id, timeframe, ts_open); original begin/end and UTC bounds remain
traceable. Daily source bounds are inclusive as reported, not fabricated session
intervals or an assertion of calendar completeness. Reject partial current-day
requests. Preserve native prices and reported volume/turnover; indices expose
null analytical volume/turnover/OI even if their raw payload contains aggregates.
Other source volumes retain source-native units until independently normalized.
IMOEX and IMOEX2 remain distinct series even when source daily OHLC values match.

An isolated Dagster job coordinates acquisition and Spark DataFrame transforms;
native Delta merge writes raw candles, quote-series and quote-bars under an
explicit verification root. No default published/current root, scheduler,
production registry mutation, or production data mount. One Market Data writer
owns these pilot tables. Repeated keys replace corrections without duplicates;
unrequested series remain. Validate the full acquired batch before writes.
Explicit replay rematerializes the selected acquisition's keys, even when an
older acquisition is selected; these tables have no published latest/current
semantics. Consumers restrict bars to the accepted source_run_id and scope.
Independent Delta transactions are not a multi-table atomic publish: failures
leave the verification run unaccepted, and retry is idempotent. A successful
report is emitted only after physical Delta/schema/count/uniqueness checks.
The successful report is the sole acceptance marker and records Delta table
versions, immutable source/metadata evidence hashes and acquired scope. Later
corrections create a new accepted run without changing prior report evidence.
A bounded public read requires this report and reads its pinned versions with
series/timeframe/date predicates and a row limit. Missing/failed reports, scope
mismatch and reads from mixed latest state are rejected. The consumer validates
producer output against the same schemas. Source evidence and accepted reports
are immutable; no pilot vacuum removes their pinned Delta versions.

## Alternatives and consequences

Adding tickers to the futures YAML cannot admit equities/indices. Widening the
existing CanonicalBar schema would change contract_id meaning and existing
consumers. A new database/service or universal research rewrite costs more than
this ingestion pilot. Choose additive typed tables inside the existing owner;
revisit physical consolidation when admitting a multi-asset research consumer.
Source-native volume and current-only metadata deliberately block execution
economics claims. Corporate-action adjustment, calendar admission, continuous
front construction, indicators and strategy execution are subsequent slices.

## Migration, rollback and proof

There is no migration of existing tables. Rollback stops invoking the pilot;
its isolated output can be retained as evidence or removed explicitly later.
Real proof uses a standalone Docker process with the existing proof image,
only the candidate worktree/output mounts, and an ephemeral Dagster instance.
No existing service is restarted. Spark owns canonical transforms, Delta owns
merge/storage, Dagster owns execution; Python adapts bounded provider responses,
validates boundary payloads and coordinates. No analytical row-list fallback.

Required evidence: source responses for all asset types; expired-contract
discovery in the requested window; schema-valid producer output accepted by a
bounded consumer; UTC/source-bound agreement; physical _delta_log and counts;
duplicate rejection; repeat/correction/unrequested-series preservation; rejected
published or symlink-escaped output root; no tradeability/OI fabrication for
indices. Unit fixtures establish rejection semantics; a real Dagster/Spark/Delta
run establishes only the bounded verification ingestion route.

Sources: existing architecture standard, native-runtime ownership, contract
change policy, MoexISSClient, canonical_bar.v2, and MOEX runtime registry.
Implementation and proof procedure: [pilot runbook](docs/runbooks/app/moex-quote-pilot.md).
Exact local evidence and independent receipts belong to the task/PR; existing
production readiness is unchanged.
