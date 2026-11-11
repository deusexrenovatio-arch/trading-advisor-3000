export type Status =
  "draft" | "planned" | "active" | "closed" | "cancelled" | "invalidated";
export type Unknown<T> = { value: T | null; reason: string | null };
export type Source = {
  source_type: "manual" | "import";
  original_text: string;
  reference: string | null;
};
export type Level = {
  price: Unknown<string>;
  approximate: boolean;
  rationale: string;
};
export type Technical = {
  kind:
    "pattern" | "indicator" | "zone" | "fibonacci" | "correlation" | "other";
  title: string;
  description: string;
  timeframe: Unknown<string>;
  lower: Unknown<string>;
  upper: Unknown<string>;
};
export type Card = {
  title: string;
  introduction: string;
  analysis: {
    symbol: string;
    asset: string;
    market_type: "spot" | "index" | "futures" | "other";
    price_unit: string;
    timeframe: Unknown<string>;
    data_provider: Unknown<string>;
  };
  direction: "long" | "short";
  entry: Level;
  invalidation: Level;
  targets: Level[];
  technical_items: Technical[];
  shared_thesis: {
    thesis_id: string;
    title: string;
    description: string;
    source: Source;
  } | null;
  is_test: boolean;
  related_trade_id: string | null;
  relation: "roll" | "reentry" | null;
};
export type Execution = {
  venue: "MOEX";
  contract_code: string;
  account: Unknown<string>;
  action: "increase" | "reduce";
  contracts: number;
  price: Unknown<string>;
  fee: Unknown<string>;
  currency: Unknown<string>;
};
export type EntryInput = {
  kind: "note" | "execution";
  text: string;
  event_at: string;
  source: Source;
  execution: Execution | null;
  supersedes_entry_id: string | null;
};
export type Entry = EntryInput & {
  entry_id: string;
  trade_id: string;
  revision: number;
  sequence: number;
  recorded_at: string;
};
export type Snapshot = {
  schema_version: 1;
  trade_id: string;
  revision: number;
  card: Card;
  status: Status;
  positions: { contract_code: string; contracts: number }[];
  execution_summary: {
    status: "recorded" | "not_recorded";
    total_contracts: number | null;
    reason: string | null;
  };
  entry_ids: string[];
  created_at: string;
  updated_at: string;
  event_at: string;
  source: Source;
  reason: string;
};
export type TradeRow = {
  trade_id: string;
  current_revision: number;
  title: string;
  symbol: string;
  status: Status;
  direction: "long" | "short";
  contract_code: string | null;
  open_contracts: number | null;
  created_at: string;
  updated_at: string;
};
export type TimelineItem =
  | Entry
  | {
      kind: "revision";
      revision: number;
      status: Status;
      reason: string;
      event_at: string;
      recorded_at: string;
    };
export type Page<T> = {
  schema_version: 1;
  items: T[];
  next_cursor: string | null;
  revision?: number;
};
export type Receipt = {
  schema_version: 1;
  command_id: string;
  trade_id: string;
  revision: number;
  entry_ids: string[];
  recorded_at: string;
};
export type Write = { path: string; payload: Record<string, unknown> };
