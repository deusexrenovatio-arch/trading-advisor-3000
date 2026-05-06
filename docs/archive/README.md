# Documentation Archive

## Purpose

`docs/archive/` is the single repository archive for documentation that is no
longer part of the active project reading path.

Archived files are preserved for forensics, provenance, and recovery only. They
must not answer current TA3000 product-state, architecture, implementation, or
readiness questions.

## Agent Rule

Agents must not read this archive by default.

Open archived material only when one of these is true:

- the user explicitly asks for historical/forensic context;
- a validator or broken reference requires a specific archived file;
- a cleanup task is explicitly auditing an archive batch;
- a current truth document names a specific archived artifact as evidence.

Otherwise, start from:

- `docs/project-map/current-truth-map-2026-05-05.md`
- `docs/project-map/documentation-currentness-map-2026-05-06.md`

## Archive Batch Rule

When active documentation is classified as obsolete:

1. Move it under `docs/archive/<source-area>/<YYYY-MM-DD>/`.
2. Add or update a README in that archive batch.
3. Record why the batch was moved and what current document replaced it.
4. Remove the old document from active indexes.
5. Run markdown link validation and the relevant focused checks.

Do not leave obsolete documents in the active tree with only a warning banner.
Use a banner only while a document is still being reviewed for extraction or
replacement.
