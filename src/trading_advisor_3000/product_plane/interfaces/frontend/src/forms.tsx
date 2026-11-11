import { useEffect, useRef, useState } from "react";
import { errorMessage, request } from "./api";
import { draftKey, newDraft, prepareWrite, readDraft } from "./draft";
import type { Draft, FormKind, TextFields } from "./draft";
import type { Snapshot, Write } from "./types";

export function Field({
  label,
  value,
  onChange,
  required = false,
  multiline = false,
  type = "text",
  maxLength = multiline ? 10000 : 200,
}: {
  label: string;
  value: string;
  onChange: (s: string) => void;
  required?: boolean;
  multiline?: boolean;
  type?: string;
  maxLength?: number;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      {multiline ? (
        <textarea
          value={value}
          onChange={(e) => onChange(e.target.value)}
          required={required}
          maxLength={maxLength}
          rows={3}
        />
      ) : (
        <input
          type={type}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          required={required}
          maxLength={maxLength}
        />
      )}
    </label>
  );
}
const metadata: [keyof TextFields, string, boolean?][] = [
  ["title", "Название", true],
  ["symbol", "Инструмент анализа", true],
  ["asset", "Базовый актив", true],
  ["unit", "Единица цены", true],
  ["timeframe", "Таймфрейм"],
  ["provider", "Источник данных"],
];
const levels: [keyof TextFields, string][] = [
  ["entry", "Авторский уровень входа"],
  ["entryReason", "Комментарий ко входу"],
  ["invalidation", "Авторский уровень отмены"],
  ["invalidationReason", "Условие отмены"],
];
export function Editor({
  kind,
  snapshot,
  disabled,
  save,
  close,
}: {
  kind: FormKind;
  snapshot: Snapshot | null;
  disabled: boolean;
  save: (w: Write) => Promise<void>;
  close: () => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null),
    key = draftKey(kind, snapshot?.trade_id || "new");
  const [loaded] = useState(() => {
    const initial = newDraft(kind, snapshot);
    try {
      return { value: readDraft(key, initial), error: "" };
    } catch (e) {
      return { value: initial, error: errorMessage(e) };
    }
  });
  const [form, setForm] = useState(loaded.value),
    [broken, setBroken] = useState(!!loaded.error);
  const [error, setError] = useState(loaded.error),
    [storageError, setStorageError] = useState("");
  useEffect(() => {
    if (!dialog.current?.open) dialog.current?.showModal();
  }, []);
  useEffect(() => {
    if (broken) return;
    try {
      sessionStorage.setItem(key, JSON.stringify(form));
    } catch {
      setStorageError(
        "Черновик только в открытой форме. Не закрывайте страницу до сохранения.",
      );
    }
  }, [key, form, broken]);
  useEffect(() => {
    if (!storageError) return;
    const warn = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [storageError]);
  const change = (name: keyof TextFields, value: string) =>
    setForm((d) => ({ ...d, fields: { ...d.fields, [name]: value } }));
  const inputs = (fields: typeof metadata) =>
    fields.map(([name, label, required]) => (
      <Field
        key={name}
        label={label}
        value={form.fields[name]}
        required={required}
        onChange={(v) => change(name, v)}
      />
    ));
  const reset = async () => {
    if (
      !window.confirm(
        "Сбросить ваши несохранённые правки и загрузить актуальную карточку?",
      )
    )
      return;
    try {
      const latest = snapshot
        ? await request<Snapshot>(`/trades/${snapshot.trade_id}`)
        : null;
      setForm(newDraft(kind, latest));
      setBroken(false);
      setError("");
      setStorageError("");
    } catch (e) {
      setError(errorMessage(e));
    }
  };
  return (
    <dialog
      ref={dialog}
      onCancel={(e) => {
        e.preventDefault();
        close();
      }}
      aria-label={kind === "note" ? "Новая заметка" : "Карточка"}
    >
      <header className="dialog-header">
        <h2>
          {kind === "note"
            ? "Новая заметка"
            : form.baseRevision
              ? "Редактировать карточку"
              : "Новая карточка"}
        </h2>
        <button type="button" onClick={close} aria-label="Закрыть форму">
          ×
        </button>
      </header>
      <p className="muted">
        {form.baseRevision
          ? `Черновик привязан к версии ${form.baseRevision}.`
          : "Карточка будет сохранена как черновик."}{" "}
        Изменения сохраняются только по кнопке.
      </p>
      {(error || storageError) && (
        <p className="notice error" role="alert">
          {error || storageError}
        </p>
      )}
      <form
        onSubmit={async (e) => {
          e.preventDefault();
          if (disabled || broken) return;
          setError("");
          try {
            await save(prepareWrite(form));
          } catch (e) {
            setError(errorMessage(e));
          }
        }}
      >
        <fieldset disabled={disabled || broken}>
          {kind === "note" ? (
            <Field
              label="Текст заметки"
              value={form.fields.note}
              multiline
              required
              onChange={(v) => change("note", v)}
            />
          ) : (
            <>
              <div className="fields">
                {inputs(metadata)}
                <label className="field">
                  <span>Тип рынка</span>
                  <select
                    aria-label="Тип рынка"
                    required
                    value={form.market}
                    onChange={(e) =>
                      setForm((d) => ({
                        ...d,
                        market: e.target.value as Draft["market"],
                      }))
                    }
                  >
                    <option value="">Выберите</option>
                    <option value="spot">Спот</option>
                    <option value="index">Индекс</option>
                    <option value="futures">Фьючерс</option>
                    <option value="other">Другой</option>
                  </select>
                </label>
                <label className="field">
                  <span>Направление идеи</span>
                  <select
                    aria-label="Направление идеи"
                    required
                    value={form.direction}
                    onChange={(e) =>
                      setForm((d) => ({
                        ...d,
                        direction: e.target.value as Draft["direction"],
                      }))
                    }
                  >
                    <option value="">Выберите</option>
                    <option value="long">Лонг</option>
                    <option value="short">Шорт</option>
                  </select>
                </label>
              </div>
              <Field
                label="Обоснование"
                value={form.fields.introduction}
                multiline
                required
                onChange={(v) => change("introduction", v)}
              />
              <div className="fields">{inputs(levels)}</div>
              <p className="muted">
                Пустая цена остаётся неизвестной. Прочие сохранённые поля этим
                редактором не меняются.
              </p>
              <Field
                label="Исходная запись"
                value={form.fields.original}
                multiline
                required
                maxLength={65536}
                onChange={(v) => change("original", v)}
              />
              <label className="check">
                <input
                  type="checkbox"
                  checked={form.isTest}
                  onChange={(e) =>
                    setForm((d) => ({ ...d, isTest: e.target.checked }))
                  }
                />
                Тестовая карточка
              </label>
            </>
          )}
          <div className="fields">
            <Field
              label="Комментарий к изменению"
              value={form.fields.reason}
              required
              onChange={(v) => change("reason", v)}
            />
            <Field
              label="Время события (МСК, UTC+03)"
              type="datetime-local"
              value={form.fields.eventAt}
              required
              onChange={(v) => change("eventAt", v)}
            />
          </div>
          <button className="primary" type="submit">
            {kind === "note" ? "Сохранить заметку" : "Сохранить карточку"}
          </button>
        </fieldset>
      </form>
      <button type="button" disabled={disabled} onClick={() => void reset()}>
        Сбросить черновик и загрузить актуальное
      </button>
    </dialog>
  );
}
