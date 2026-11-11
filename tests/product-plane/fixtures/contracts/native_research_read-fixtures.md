# Native Research read fixtures

`native_research_read_inputs.v1.json` contains independent synthetic wire inputs.
Tests compose complete examples with the existing versioned L0/feature context
fixtures; no runtime reader, producer, native table or formula is their oracle.
They prove closed shape, exact selectors/parent bindings, bounded requests,
false admission flags and distinct SOURCE_KEY/TEMPORAL modes only.
Arrow schema/IPC integrity, values/types/masks, native readback, cache behavior
and actual retrospective alignment require the separate real consumer proof.
