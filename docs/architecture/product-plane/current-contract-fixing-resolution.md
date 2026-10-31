# Current MOEX contract parameters and their fixing session

Change surface: product-plane. Architecture impact: within-contract.
Owner: Market Data Foundation / MOEX native history renewal.
Decision status: proposed for independent design admission before implementation.
Implementation, PR acceptance and production qualification remain separate.

## Source and decision

ISS `securities.IMTIME` is labelled "Данные по ГО на": it is not the effective
time of `STEPPRICE`. The 30 October NASD observation retained after the failed
run had STEPPRICE 0.80676 and IMTIME 21:21:03. That price corresponds, after
five-place rounding, to 29 October MC / 30 October TC rate 80.6761. Selecting
30 October MC 80.5573 from IMTIME caused a false comparison across fixings.
The original failed response, including its session envelope, was not retained;
that observation does not by itself prove a session-selection policy.

Acquire unmodified current securities and `dataversion` in the same ISS page.
The snapshot owns its canonical source trading date, not the host date, ingest
cutoff, marketdata calendar date, IMTIME or whichever historical rate fits.
Require one canonical `dataversion.trade_date`; a supplied trade_session_date
must agree. All pages, including the terminal page, must have the same trading
date and data_version. Reject missing/conflicting source context before writes.
Preserve each page's dataversion and canonical securities payload hash.

The selected adapter interpretation is that current `STEPPRICE` belongs to the
parameters for that trading day. This is a bounded mapping supported by actual
ISS output and SPECTRA session semantics, not an ISS atomicity guarantee. Compare
USD parameters to the last actual dated MC strictly before the source trading
date. A same-date 19:00 fixing is the clearing value for the later MTM transition,
not grounds to redate parameters already serving that trading day. Do not use
IMTIME, an arbitrary midnight/19:00 offset, or a best-fit search. This handles
evening, weekend and holiday sessions using actual observations, not weekday
arithmetic. Fail if no preceding actual MC exists; keep the 0.1 percent threshold
and reviewed positive units. RUB remains identity conversion.

The baseline owner supplies full dated FX acquisition through the day preceding
the source session, including any gap after a delayed historical ingest cutoff.
These extra observations serve only current crosschecking; historical FX writes
retain their existing ingest cutoff. Keep raw history and OI, pinned prepared
description reads, historical dated-TC inference, money formulas, schemas and
publication/pending ownership unchanged. The native crosscheck report retains
the source session/page provenance, observed/expected prices and selected MC.
Its validation identity excludes volatile sequence numbers and fetch times;
these stay in audit context and cannot alone rewrite unchanged support rows.

## Alternatives, migration and proof

IMTIME or fetch-time selection mixes independent updates; accepting any fitting
MC hides genuine parameter errors; raising tolerance weakens economics checks;
skipping current money validation removes a protected guarantee. Existing ISS
session metadata reuses the owner and avoids a new licensed feed or state store.
Acquisition adds a dataversion block per existing page and only missing FX dates
for a delayed run. There is no new writer/service or historical table rewrite.

The legacy row-only client remains for callers that do not renew native history.
Native renewal requires the source-bound snapshot; no IMTIME compatibility path.
Rollback reverts this adapter/caller together; pending windows remain recoverable
through the existing owner. Revisit the decision if ISS changes session metadata
or a source-bound snapshot contradicts the parameter/fixing relationship.

Same-body metadata prevents inventing a session from an unrelated timestamp;
it does not guarantee that upstream fields update atomically. A price inconsistent
with the selected session must still reject, with the exact session/MC/price
diagnostic. Never fall back to a neighbouring rate to accommodate stale fields.

Required proof: NASD night/morning and session-rollover regressions; rejection of
missing/conflicting dates, mixed pages, bad timestamps, missing MC and genuine
money/unit errors; unchanged NGV6 support behavior; actual ISS producer snapshot
through the native checker with exact retained inputs. Unit/native proof is not
baseline -> CF -> Research publication acceptance or a production rollout.

Sources: [ISS source](https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/NAZ6.json),
[ISS field metadata](https://iss.moex.com/iss/engines/futures/markets/forts/columns.json),
[SPECTRA trading day and MTM](https://ftp.moex.com/pub/ClientsAPI/Spectra/CGate/prod/docs/p2gate_ru.html),
[architecture standard](docs/architecture/product-plane/architecture-standard.md).

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: best-fit FX, IMTIME fallback, tolerance relaxation
- Closure Evidence: focused regression, actual ISS producer/checker replay and
  ordinary gates. Production promotion and a new Dagster cycle are separate.
- Shortcut Waiver: none
