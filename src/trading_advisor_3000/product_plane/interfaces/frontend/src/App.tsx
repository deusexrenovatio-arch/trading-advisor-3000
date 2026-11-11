import { useEffect, useRef, useState } from "react";
import {
  ApiError,
  checkReceipt,
  clearPending,
  errorMessage,
  readPending,
  request,
  retainPending,
  sendWrite,
} from "./api";
import { Editor } from "./forms";
import { draftKey } from "./draft";
import type { FormKind } from "./draft";
import { price, STATUS, unknownLabel, when } from "./format";
import type {
  Page,
  Receipt,
  Snapshot,
  TimelineItem,
  TradeRow,
  Write,
} from "./types";

export function App() {
  const [query, setQuery] = useState(""),
    [cursor, setCursor] = useState<string | null>(null);
  const [rows, setRows] = useState<TradeRow[]>([]),
    [next, setNext] = useState<string | null>(null),
    [listBusy, setListBusy] = useState(false);
  const [selected, setSelected] = useState<string | null>(
    new URLSearchParams(location.search).get("trade"),
  );
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null),
    [history, setHistory] = useState<Page<TimelineItem> | null>(null);
  const [detailBusy, setDetailBusy] = useState(false),
    [refresh, setRefresh] = useState(0),
    [error, setError] = useState(""),
    [saved, setSaved] = useState("");
  const [modal, setModal] = useState<{
    kind: FormKind;
    base: Snapshot | null;
  } | null>(null);
  const [pending, setPending] = useState<Write | null>(readPending),
    [writing, setWriting] = useState(false);
  const busy = useRef(false),
    generation = useRef(0),
    revisionRequest = useRef(0),
    selectedRef = useRef(selected);
  selectedRef.current = selected;
  useEffect(() => {
    const sync = () => setPending(readPending());
    window.addEventListener("storage", sync);
    return () => window.removeEventListener("storage", sync);
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    setListBusy(true);
    const params = new URLSearchParams({
      scope: "current",
      query,
      limit: "25",
    });
    if (cursor) params.set("cursor", cursor);
    void request<Page<TradeRow>>("/trades?" + params, {
      signal: controller.signal,
    })
      .then((page) => {
        if (controller.signal.aborted) return;
        setRows(page.items);
        setNext(page.next_cursor);
        if (!selectedRef.current) setSelected(page.items[0]?.trade_id || null);
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(errorMessage(e));
      })
      .finally(() => {
        if (!controller.signal.aborted) setListBusy(false);
      });
    return () => controller.abort();
  }, [query, cursor, refresh]);
  useEffect(() => {
    const controller = new AbortController();
    ++generation.current;
    setSnapshot(null);
    setHistory(null);
    if (!selected) return () => controller.abort();
    setDetailBusy(true);
    void (async () => {
      const page = await request<Page<TimelineItem>>(
        `/trades/${encodeURIComponent(selected)}/timeline?order=desc&limit=25`,
        { signal: controller.signal },
      );
      const card = await request<Snapshot>(
        `/trades/${encodeURIComponent(selected)}?revision=${page.revision}`,
        { signal: controller.signal },
      );
      if (!controller.signal.aborted) {
        setHistory(page);
        setSnapshot(card);
      }
    })()
      .catch((e) => {
        if (!controller.signal.aborted) setError(errorMessage(e));
      })
      .finally(() => {
        if (!controller.signal.aborted) setDetailBusy(false);
      });
    return () => controller.abort();
  }, [selected, refresh]);
  useEffect(() => {
    const url = new URL(location.href);
    if (selected) url.searchParams.set("trade", selected);
    else url.searchParams.delete("trade");
    window.history.replaceState(null, "", url);
  }, [selected]);
  const accepted = async (write: Write, receipt: Receipt) => {
    checkReceipt(write, receipt);
    const operations = write.payload.operations as
      { kind: string }[] | undefined;
    if (await clearPending(write))
      sessionStorage.removeItem(
        draftKey(
          operations?.[0]?.kind === "add_entry" ? "note" : "card",
          write.path === "/trades" ? "new" : receipt.trade_id,
        ),
      );
    setPending(null);
    setModal(null);
    setError("");
    setSaved(`Сохранено · версия ${receipt.revision}`);
    setSelected(receipt.trade_id);
    setRefresh((v) => v + 1);
  };
  const commit = async (write: Write) => {
    if (
      busy.current ||
      (pending && pending.payload.command_id !== write.payload.command_id)
    )
      return;
    busy.current = true;
    setWriting(true);
    setError("");
    setSaved("");
    try {
      const frozen = structuredClone(write);
      await retainPending(frozen);
      setPending(frozen);
      await accepted(frozen, await sendWrite(frozen));
    } catch (e) {
      if (e instanceof ApiError && [404, 409, 422].includes(e.status)) {
        await clearPending(write);
        setPending(null);
      }
      if (e instanceof ApiError && e.code === "revision_conflict")
        setRefresh((v) => v + 1);
      setError(errorMessage(e));
      throw e;
    } finally {
      setPending(readPending());
      busy.current = false;
      setWriting(false);
    }
  };
  const recover = async () => {
    if (!pending || busy.current) return;
    busy.current = true;
    setWriting(true);
    try {
      await accepted(
        pending,
        await request<Receipt>(
          `/commands/${encodeURIComponent(String(pending.payload.command_id))}`,
        ),
      );
    } catch (e) {
      setError(
        e instanceof ApiError && e.status === 404
          ? "Подтверждения пока нет. Можно повторить ту же команду."
          : errorMessage(e),
      );
    } finally {
      busy.current = false;
      setWriting(false);
    }
  };
  const more = async () => {
    if (!selected || !history?.next_cursor || detailBusy) return;
    const version = generation.current;
    setDetailBusy(true);
    try {
      const page = await request<Page<TimelineItem>>(
        `/trades/${encodeURIComponent(selected)}/timeline?order=desc&limit=25&cursor=${encodeURIComponent(history.next_cursor)}`,
      );
      if (version === generation.current)
        setHistory((old) =>
          old ? { ...page, items: [...old.items, ...page.items] } : old,
        );
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      if (version === generation.current) setDetailBusy(false);
    }
  };
  const revision = async (value: number) => {
    if (!selected) return;
    const version = generation.current,
      requestId = ++revisionRequest.current;
    try {
      const card = await request<Snapshot>(
        `/trades/${encodeURIComponent(selected)}?revision=${value}`,
      );
      if (
        version === generation.current &&
        requestId === revisionRequest.current
      )
        setSnapshot(card);
    } catch (e) {
      if (
        version === generation.current &&
        requestId === revisionRequest.current
      )
        setError(errorMessage(e));
    }
  };
  const historical = !!snapshot && snapshot.revision !== history?.revision;
  return (
    <div className="app">
      <header className="topbar">
        <strong>Журнал сделок · ручные записи</strong>
        <button
          disabled={!!pending || writing}
          onClick={() => {
            setSaved("");
            setModal({ kind: "card", base: null });
          }}
        >
          + Новая карточка
        </button>
      </header>
      {pending && (
        <section className="notice pending" role="status">
          <strong>Сохранение не подтверждено</strong>
          <p>
            Команда сохранена в браузере. Проверьте запись или повторите ту же
            команду.
          </p>
          <div className="actions">
            <button disabled={writing} onClick={() => void recover()}>
              Проверить сохранение
            </button>
            <button
              disabled={writing}
              onClick={() => void commit(pending).catch(() => {})}
            >
              Повторить ту же команду
            </button>
          </div>
        </section>
      )}
      {error && (
        <p className="notice error" role="alert">
          {error}
        </p>
      )}
      {saved && (
        <p className="notice success" role="status">
          {saved}
        </p>
      )}
      <div className="layout">
        <aside aria-label="Список карточек">
          <label className="field">
            <span>Поиск</span>
            <input
              value={query}
              maxLength={100}
              onChange={(e) => {
                setQuery(e.target.value);
                setCursor(null);
              }}
              placeholder="Название или инструмент"
            />
          </label>
          <div className="list" aria-busy={listBusy}>
            {rows.map((row) => (
              <button
                className={
                  selected === row.trade_id ? "trade selected" : "trade"
                }
                key={row.trade_id}
                onClick={() => {
                  setSelected(row.trade_id);
                  setError("");
                }}
              >
                <span className="row">
                  <strong>{row.title}</strong>
                  <small>v{row.current_revision}</small>
                </span>
                <span className="row muted">
                  {row.symbol}
                  <span>{STATUS[row.status]}</span>
                </span>
              </button>
            ))}
            {!rows.length && !listBusy && (
              <p className="muted">Карточек пока нет.</p>
            )}
          </div>
          <div className="actions">
            <button
              disabled={!cursor || listBusy}
              onClick={() => setCursor(null)}
            >
              Сначала
            </button>
            <button
              disabled={!next || listBusy}
              onClick={() => setCursor(next)}
            >
              Дальше
            </button>
          </div>
        </aside>
        <main aria-busy={detailBusy}>
          {!snapshot ? (
            <section className="empty">
              <h1>{detailBusy ? "Загрузка…" : "Начните с записи"}</h1>
              <p className="muted">
                Создайте карточку или выберите её в списке.
              </p>
            </section>
          ) : (
            <>
              <header className="card-header">
                <div>
                  <p className="eyebrow">
                    {snapshot.card.analysis.symbol} · {STATUS[snapshot.status]}{" "}
                    {snapshot.card.is_test && "· Тестовая"}
                  </p>
                  <h1>{snapshot.card.title}</h1>
                  <p className="muted">
                    Версия {snapshot.revision} · {when(snapshot.updated_at)}
                  </p>
                </div>
                <div className="actions">
                  <button
                    disabled={!!pending || writing || historical}
                    onClick={() => setModal({ kind: "card", base: snapshot })}
                  >
                    Редактировать
                  </button>
                  <button
                    disabled={!!pending || writing || historical}
                    onClick={() => setModal({ kind: "note", base: snapshot })}
                  >
                    + Добавить заметку
                  </button>
                </div>
              </header>
              {historical && (
                <p className="notice">
                  Историческая версия · только чтение.{" "}
                  <button onClick={() => setRefresh((v) => v + 1)}>
                    К текущей версии
                  </button>
                </p>
              )}
              <section className="panel">
                <h2>Обоснование</h2>
                <p className="prose">{snapshot.card.introduction}</p>
                <p className="muted">
                  {snapshot.card.analysis.asset} ·{" "}
                  {snapshot.card.direction === "long" ? "Лонг" : "Шорт"} ·{" "}
                  {snapshot.card.analysis.timeframe.value ||
                    unknownLabel(snapshot.card.analysis.timeframe.reason)}{" "}
                  · {snapshot.card.analysis.price_unit}
                </p>
              </section>
              <section className="panel">
                <h2>Авторские уровни</h2>
                <p>
                  Вход: {price(snapshot.card.entry.price)} · Отмена:{" "}
                  {price(snapshot.card.invalidation.price)}
                </p>
                <p className="muted">
                  {snapshot.execution_summary.status === "not_recorded"
                    ? "Исполнение не записано. Авторские уровни не создают фактов исполнения."
                    : `Исполнение записано. Открыто контрактов: ${snapshot.execution_summary.total_contracts}.`}
                </p>
              </section>
              <details className="panel">
                <summary>Все сохранённые поля и источники</summary>
                <pre>{JSON.stringify({ snapshot, history }, null, 2)}</pre>
              </details>
              <section className="panel">
                <div className="row">
                  <h2>История · до версии {history?.revision}</h2>
                  <button
                    onClick={() => {
                      setError("");
                      setRefresh((v) => v + 1);
                    }}
                  >
                    Обновить
                  </button>
                </div>
                {history?.items.map((item, i) => (
                  <article className="timeline" key={i}>
                    {item.kind === "revision" ? (
                      <button onClick={() => void revision(item.revision)}>
                        Версия {item.revision}
                      </button>
                    ) : (
                      <h3>
                        {item.kind === "note"
                          ? "Заметка"
                          : "Записанный факт исполнения"}
                      </h3>
                    )}
                    <p className="prose">
                      {item.kind === "revision" ? item.reason : item.text}
                    </p>
                    <small className="muted">{when(item.event_at)}</small>
                  </article>
                ))}
                {history?.next_cursor && (
                  <button disabled={detailBusy} onClick={() => void more()}>
                    Ещё записи
                  </button>
                )}
              </section>
            </>
          )}
        </main>
      </div>

      {modal && (
        <Editor
          key={`${modal.kind}:${modal.base?.trade_id || "new"}`}
          kind={modal.kind}
          snapshot={modal.base}
          disabled={writing || !!pending}
          save={commit}
          close={() => setModal(null)}
        />
      )}
    </div>
  );
}
