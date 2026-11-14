import {
  blankCard,
  decimal,
  eventTime,
  localTime,
  source,
  STATUS,
  unknown,
} from "./format.ts";
import type { Card, Snapshot, Source, Status, Write } from "./types";
export type FormKind = "card" | "note" | "status";
export const PLAN_STATUSES = [
  "draft",
  "planned",
  "cancelled",
  "invalidated",
] as const;
export type TextFields = {
  title: string;
  introduction: string;
  symbol: string;
  asset: string;
  unit: string;
  timeframe: string;
  provider: string;
  entry: string;
  entryReason: string;
  invalidation: string;
  invalidationReason: string;
  original: string;
  reason: string;
  eventAt: string;
  note: string;
};
export type Draft = {
  version: 1;
  kind: FormKind;
  target: string;
  tradeId: string;
  baseRevision: number;
  baseStatus?: Status;
  status?: (typeof PLAN_STATUSES)[number] | "";
  card: Card;
  originalSource: Source;
  fields: TextFields;
  direction: Card["direction"] | "";
  market: Card["analysis"]["market_type"] | "";
  isTest: boolean;
};
export const draftKey = (kind: FormKind, target: string) =>
  `journal.manual.${kind}.${target}`;
export function newDraft(kind: FormKind, snapshot: Snapshot | null): Draft {
  const card = structuredClone(snapshot?.card || blankCard());
  return {
    version: 1,
    kind,
    target: snapshot?.trade_id || "new",
    tradeId: snapshot?.trade_id || crypto.randomUUID(),
    baseRevision: snapshot?.revision || 0,
    ...(kind === "status"
      ? { baseStatus: snapshot?.status || "draft", status: "" as const }
      : {}),
    card,
    originalSource: structuredClone(snapshot?.source || source("")),
    direction: snapshot ? card.direction : "",
    market: snapshot ? card.analysis.market_type : "",
    isTest: card.is_test,
    fields: {
      title: card.title,
      introduction: card.introduction,
      symbol: card.analysis.symbol,
      asset: card.analysis.asset,
      unit: card.analysis.price_unit,
      timeframe: card.analysis.timeframe.value || "",
      provider: card.analysis.data_provider.value || "",
      entry: card.entry.price.value || "",
      entryReason: card.entry.rationale,
      invalidation: card.invalidation.price.value || "",
      invalidationReason: card.invalidation.rationale,
      original: snapshot?.source.original_text || "",
      reason:
        kind === "status"
          ? ""
          : kind === "note"
            ? "Ручная заметка"
            : snapshot
              ? "Ручная правка карточки"
              : "Ручное создание карточки",
      eventAt: localTime(),
      note: "",
    },
  };
}
export function readDraft(key: string, initial: Draft): Draft {
  const raw = sessionStorage.getItem(key);
  if (!raw) return initial;
  const d = JSON.parse(raw) as Draft;
  if (
    d.version !== 1 ||
    d.kind !== initial.kind ||
    d.target !== initial.target ||
    !Number.isInteger(d.baseRevision) ||
    d.baseRevision < 0 ||
    (d.kind === "status" &&
      (!d.baseStatus ||
        !Object.hasOwn(STATUS, d.baseStatus) ||
        (d.status !== "" && !PLAN_STATUSES.some((s) => s === d.status)))) ||
    !d.card?.analysis ||
    !d.card.entry?.price ||
    !d.card.invalidation?.price ||
    !Array.isArray(d.card.targets) ||
    !Array.isArray(d.card.technical_items) ||
    typeof d.originalSource?.original_text !== "string" ||
    typeof d.isTest !== "boolean" ||
    !d.fields ||
    !Object.keys(initial.fields).every(
      (k) => typeof d.fields[k as keyof TextFields] === "string",
    ) ||
    typeof d.tradeId !== "string"
  )
    throw new Error(
      "Сохранённый черновик несовместим. Он оставлен в браузере; сбросьте его явно.",
    );
  return d;
}
export function prepareWrite(d: Draft): Write {
  const f = d.fields;
  const common = {
    schema_version: 1,
    command_id: crypto.randomUUID(),
    event_at: eventTime(f.eventAt),
    reason: f.reason,
  };
  if (d.kind === "status") {
    if (
      !d.baseRevision ||
      !["draft", "planned"].includes(d.baseStatus || "") ||
      !PLAN_STATUSES.some((s) => s === d.status) ||
      d.status === d.baseStatus
    )
      throw new Error(
        "Выберите новое состояние плана. Состояния исполнения и завершённые эпизоды здесь не меняются.",
      );
    if (!f.reason.trim()) throw new Error("Укажите причину изменения статуса.");
    return {
      path: `/trades/${d.tradeId}/changes`,
      payload: {
        ...common,
        expected_revision: d.baseRevision,
        source: source(f.reason),
        operations: [{ kind: "set_status", status: d.status }],
      },
    };
  }
  if (d.kind === "note") {
    if (!d.baseRevision)
      throw new Error("Сначала выберите сохранённую карточку.");
    return {
      path: `/trades/${d.tradeId}/changes`,
      payload: {
        ...common,
        expected_revision: d.baseRevision,
        source: source(f.note),
        operations: [
          {
            kind: "add_entry",
            entry: {
              kind: "note",
              text: f.note,
              event_at: common.event_at,
              source: source(f.note),
              execution: null,
              supersedes_entry_id: null,
            },
          },
        ],
      },
    };
  }
  if (!d.direction || !d.market)
    throw new Error("Выберите направление и тип рынка.");
  const card = structuredClone(d.card);
  Object.assign(card, {
    title: f.title,
    introduction: f.introduction,
    direction: d.direction,
    is_test: d.isTest,
  });
  Object.assign(card.analysis, {
    symbol: f.symbol,
    asset: f.asset,
    price_unit: f.unit,
    market_type: d.market,
  });
  for (const [name, text] of [
    ["timeframe", f.timeframe],
    ["data_provider", f.provider],
  ] as const)
    if (text !== (card.analysis[name].value || ""))
      card.analysis[name] = unknown(text);
  for (const [name, text, reason] of [
    ["entry", f.entry, f.entryReason],
    ["invalidation", f.invalidation, f.invalidationReason],
  ] as const) {
    if (text !== (card[name].price.value || ""))
      card[name].price = decimal(text);
    card[name].rationale = reason.trim() || "Основание не указано автором.";
  }
  const evidence =
    f.original === d.originalSource.original_text
      ? structuredClone(d.originalSource)
      : source(f.original);
  return d.baseRevision
    ? {
        path: `/trades/${d.tradeId}/changes`,
        payload: {
          ...common,
          expected_revision: d.baseRevision,
          source: evidence,
          operations: [{ kind: "replace_card", card }],
        },
      }
    : {
        path: "/trades",
        payload: {
          ...common,
          trade_id: d.tradeId,
          card,
          status: "draft",
          source: evidence,
        },
      };
}
