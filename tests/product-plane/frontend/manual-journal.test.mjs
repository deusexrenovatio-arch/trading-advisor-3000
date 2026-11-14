import assert from "node:assert/strict";
import { test } from "node:test";
import { readFileSync } from "node:fs";
import { newDraft, prepareWrite, readDraft, draftKey } from "../../../src/trading_advisor_3000/product_plane/interfaces/frontend/src/draft.ts";
import { checkReceipt, retainPending, readPending, clearPending } from "../../../src/trading_advisor_3000/product_plane/interfaces/frontend/src/api.ts";
import { price } from "../../../src/trading_advisor_3000/product_plane/interfaces/frontend/src/format.ts";
const storage = () => { const cache = new Map(); return { getItem: k => cache.get(k) ?? null, setItem: (k,v) => cache.set(k,v), removeItem: k => cache.delete(k) }; };
globalThis.sessionStorage = storage(); globalThis.localStorage = storage();
let lock = Promise.resolve(); Object.defineProperty(globalThis, 'navigator', { value: { locks: { request: (_, fn) => (lock = lock.then(fn, fn)) } }, configurable: true });
const payload = JSON.parse(readFileSync(new URL("../fixtures/contracts/journal_create.v1.json", import.meta.url)));
const snapshot = { trade_id: "manual-card", revision: 3, card: payload.card, source: { source_type: "import", original_text: "original", reference: "source-ref" } };
test("title edit preserves original unknowns, hidden fields, source and original revision", () => {
  const before = structuredClone(snapshot);
  before.card.targets[0].price = { value: null, reason: "author_unknown" };
  before.card.analysis.timeframe = { value: null, reason: "not_applicable" };
  const d = newDraft("card", before); d.fields.title = "Changed title";
  const write = prepareWrite(d), card = write.payload.operations[0].card;
  assert.equal(write.payload.expected_revision, 3);
  assert.equal(card.title, "Changed title");
  for (const key of ["targets", "technical_items", "shared_thesis", "related_trade_id", "relation"]) assert.deepEqual(card[key], before.card[key]);
  assert.deepEqual(card.analysis.timeframe, before.card.analysis.timeframe);
  assert.deepEqual(write.payload.source, before.source);
  assert.notEqual(before.card.title, card.title);
});
test("known zero is retained and invalid manual prices cannot create a command", () => {
  const d = newDraft("card", snapshot); d.fields.entry = "0";
  assert.deepEqual(prepareWrite(d).payload.operations[0].card.entry.price, { value: "0", reason: null });
  for (const [reason, label] of [["not_applicable", "Не применимо"], ["not_provided", "Не указано"], ["author_unknown", "Неизвестно"]]) assert.equal(price({ value: null, reason }), label);
  assert.equal(price({ value: "0", reason: null }), "0");
  d.fields.entry = "12oops"; assert.throws(() => prepareWrite(d), /Цена/);
});
test("reload keeps the draft revision even when the server revision advances; foreign draft is rejected", () => {
  const d = newDraft("card", snapshot), key = draftKey("card", snapshot.trade_id);
  sessionStorage.setItem(key, JSON.stringify(d));
  assert.equal(readDraft(key, newDraft("card", { ...snapshot, revision: 8 })).baseRevision, 3);
  sessionStorage.setItem(key, JSON.stringify({ ...d, target: "different-card" }));
  const raw = sessionStorage.getItem(key); assert.throws(() => readDraft(key, d)); assert.equal(sessionStorage.getItem(key), raw);
});
test("only exact receipt acknowledges a write; another tab cannot replace or clear frozen pending", async () => {
  const write = prepareWrite(newDraft("note", snapshot)); write.payload.reason = "original reason";
  const receipt = { command_id: write.payload.command_id, trade_id: "manual-card", revision: 4 };
  checkReceipt(write, receipt);
  for (const change of [{ command_id: "other" }, { trade_id: "other" }, { revision: 5 }]) assert.throws(() => checkReceipt(write, { ...receipt, ...change }));
  const other = prepareWrite(newDraft("note", snapshot));
  await Promise.all([retainPending(write), assert.rejects(retainPending(other), /Другая команда/)]); const frozen = structuredClone(write); write.payload.reason = "later change";
  assert.equal(readPending().payload.reason, "original reason");
  assert.equal(await clearPending(other), false); assert.deepEqual(readPending(), frozen);
  sessionStorage.removeItem('ta3000.journal.pending.v1');
  assert.equal(await clearPending(frozen), false); assert.equal(readPending(), null);
  await retainPending(frozen); assert.equal(await clearPending(frozen), true);
});

test("plan status emits only an explicit status change and author reason at the original revision", () => {
  const before = { ...structuredClone(snapshot), status: "draft" };
  const d = newDraft("status", before); d.status = "planned"; d.fields.reason = "Автор подтвердил план.";
  const write = prepareWrite(d);
  assert.equal(write.path, "/trades/manual-card/changes"); assert.equal(write.payload.expected_revision, 3);
  assert.deepEqual(write.payload.operations, [{ kind: "set_status", status: "planned" }]);
  assert.deepEqual(write.payload.source, { source_type: "manual", original_text: "Автор подтвердил план.", reference: null });
  assert.equal(before.status, "draft"); assert.deepEqual(before.card, snapshot.card);
});

test("plan status cannot invent execution, reopen a terminal episode, submit a no-op or omit its reason", () => {
  const d = newDraft("status", { ...snapshot, status: "draft" }); d.fields.reason = "Причина";
  for (const status of ["", "draft", "active", "closed"]) { d.status = status; assert.throws(() => prepareWrite(d)); }
  d.status = "planned"; d.fields.reason = "   "; assert.throws(() => prepareWrite(d)); d.fields.reason = "Причина";
  for (const baseStatus of ["active", "closed", "cancelled", "invalidated"]) { d.baseStatus = baseStatus; assert.throws(() => prepareWrite(d)); }
});

test("status drafts pin their own revision while older card/note drafts remain compatible", () => {
  const d = newDraft("status", { ...snapshot, status: "draft" }), key = draftKey("status", snapshot.trade_id);
  d.status = "planned"; sessionStorage.setItem(key, JSON.stringify(d));
  assert.equal(readDraft(key, newDraft("status", { ...snapshot, status: "planned", revision: 8 })).baseRevision, 3);
  assert.equal(readDraft(key, newDraft("status", { ...snapshot, status: "planned", revision: 8 })).baseStatus, "draft");
  for (const kind of ["card", "note"]) {
    const old = newDraft(kind, snapshot), oldKey = draftKey(kind, snapshot.trade_id); delete old.status; delete old.baseStatus;
    sessionStorage.setItem(oldKey, JSON.stringify(old)); assert.deepEqual(readDraft(oldKey, newDraft(kind, snapshot)), old);
  }
});
