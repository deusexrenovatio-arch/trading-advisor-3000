import type { Card, Source, Status, Unknown } from "./types";

export const STATUS: Record<Status, string> = {
  draft: "Черновик",
  planned: "План",
  active: "Открыта",
  closed: "Закрыта",
  cancelled: "Отменена",
  invalidated: "Опровергнута",
};
export const unknown = (value = ""): Unknown<string> => ({
  value: value.trim() || null,
  reason: value.trim() ? null : "not_provided",
});
export const source = (text: string): Source => ({
  source_type: "manual",
  original_text: text,
  reference: null,
});
export const unknownLabel = (reason: string | null) =>
  reason === "not_applicable"
    ? "Не применимо"
    : reason === "not_provided"
      ? "Не указано"
      : "Неизвестно";
export function price(atom: Unknown<string>): string {
  if (atom.value === null) return unknownLabel(atom.reason);
  const [integer, fraction] = atom.value.split(".");
  return (
    integer.replace(/\B(?=(\d{3})+(?!\d))/g, "\u202f") +
    (fraction ? "," + fraction : "")
  );
}
export function decimal(value: string): Unknown<string> {
  const normalized = value.replace(/\s/g, "").replace(",", ".");
  if (normalized && !/^\d{1,20}(\.\d{1,10})?$/.test(normalized))
    throw new Error("Цена должна быть неотрицательным числом.");
  return unknown(normalized);
}
export const when = (value: string) =>
  new Intl.DateTimeFormat("ru-RU", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Europe/Moscow",
  }).format(new Date(value));
export const localTime = (value = new Date().toISOString()) =>
  new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Europe/Moscow",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  })
    .format(new Date(value))
    .replace(" ", "T");
export const eventTime = (value: string) =>
  new Date(value + ":00+03:00").toISOString();
export function blankCard(): Card {
  const level = () => ({ price: unknown(), approximate: false, rationale: "" });
  return {
    title: "",
    introduction: "",
    analysis: {
      symbol: "",
      asset: "",
      market_type: "spot",
      price_unit: "",
      timeframe: unknown(),
      data_provider: unknown(),
    },
    direction: "short",
    entry: level(),
    invalidation: level(),
    targets: [],
    technical_items: [],
    shared_thesis: null,
    is_test: false,
    related_trade_id: null,
    relation: null,
  };
}
