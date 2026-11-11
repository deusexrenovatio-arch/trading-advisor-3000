import type { Receipt, Write } from "./types";

const BASE = "/api/trade-workspace/v1";
const PENDING = "ta3000.journal.pending.v1";
export class ApiError extends Error {
  code: string;
  status: number;
  constructor(code: string, status: number) {
    super(code);
    this.code = code;
    this.status = status;
  }
}
export async function request<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await fetch(BASE + path, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new ApiError(body.code || "http_error", response.status);
  }
  return response.json() as Promise<T>;
}
export function readPending(): Write | null {
  try {
    const saved = JSON.parse(
      localStorage.getItem(PENDING) || "null",
    ) as Write | null;
    if (!saved) return null;
    if (
      !/^\/trades(?:\/[A-Za-z0-9_.:-]+\/changes)?$/.test(saved.path) ||
      saved.payload?.schema_version !== 1 ||
      typeof saved.payload.command_id !== "string"
    )
      return null;
    return saved;
  } catch {
    return null;
  }
}
export async function retainPending(write: Write) {
  await navigator.locks.request(PENDING, () => {
    const raw = localStorage.getItem(PENDING),
      next = JSON.stringify(write);
    if (raw && raw !== next)
      throw new Error(
        "Другая команда ещё не подтверждена. Проверьте сохранение.",
      );
    try {
      if (!raw)
        sessionStorage.setItem(PENDING, String(write.payload.command_id));
      localStorage.setItem(PENDING, next);
    } catch {
      throw new Error(
        "Браузер не смог сохранить команду. Запрос не отправлен.",
      );
    }
  });
}
export async function clearPending(write: Write) {
  return navigator.locks.request(PENDING, () => {
    if (localStorage.getItem(PENDING) === JSON.stringify(write))
      localStorage.removeItem(PENDING);
    return sessionStorage.getItem(PENDING) === write.payload.command_id;
  });
}
export function sendWrite(write: Write) {
  return request<Receipt>(write.path, {
    method: "POST",
    body: JSON.stringify(write.payload),
  });
}
export const ERROR_MESSAGES: Record<string, string> = {
  revision_conflict:
    "Карточка уже изменилась. Ваш черновик и его исходная версия сохранены. Сверьте правки с актуальной карточкой.",
  command_conflict: "Эта команда уже использована для другой записи.",
  immutable_identity:
    "После исполнения нельзя изменить актив, направление или единицу цены.",
  invalid_input: "Проверьте заполненные поля, цены и время события.",
  storage_unavailable: "Хранилище журнала недоступно. Повторите запрос позже.",
  storage_unconfigured: "Хранилище журнала не настроено.",
};
export function errorMessage(error: unknown) {
  return error instanceof ApiError
    ? ERROR_MESSAGES[error.code] ||
        "Запрос не выполнен. Обновите карточку и повторите."
    : error instanceof Error && !(error instanceof TypeError)
      ? error.message
      : "Нет связи с журналом. Введённые данные сохранены в этом браузере.";
}

// shortcut: one Journal destination per browser origin; bind recovery to a destination if this changes.
export function checkReceipt(write: Write, receipt: Receipt) {
  const target =
    write.path === "/trades"
      ? write.payload.trade_id
      : write.path.split("/")[2];
  const revision =
    write.path === "/trades" ? 1 : Number(write.payload.expected_revision) + 1;
  if (
    receipt.command_id !== write.payload.command_id ||
    receipt.trade_id !== target ||
    receipt.revision !== revision
  )
    throw new Error(
      "Ответ не соответствует сохранённой команде. Проверьте сохранение.",
    );
}
