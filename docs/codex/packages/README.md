# Package Inbox

Drop incoming task/TZ packages into `docs/codex/packages/inbox/`.

## Expected operator flow

1. Put a `zip` archive into the inbox.
2. Tell Codex to take the latest package or give the package path explicitly.
3. Codex unpacks the archive, generates a manifest, picks the suggested primary document, and continues the governance flow.

## What the launcher produces

For each run, `scripts/codex_from_package.py` writes artifacts under `artifacts/codex/package-intake/`:

- unpacked package contents,
- a markdown manifest,
- a json manifest,
- the last Codex message when not running in dry-run mode.

## Inbox rules

- Use one archive per incoming task package.
- Keep filenames descriptive when possible.
- The launcher chooses the newest `zip` file when no explicit path is provided.

## Clarification policy

Codex should ask at most one compact clarification block when:

- two candidate primary documents conflict,
- the package has no trustworthy primary document,
- or the package requires an unavailable external dependency.

Codex must not silently turn package ambiguity into implementation assumptions.
