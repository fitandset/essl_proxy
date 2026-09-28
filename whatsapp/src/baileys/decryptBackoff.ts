export const DECRYPT_FAIL_WINDOW_MS = 20_000;
export const DECRYPT_FAIL_THRESHOLD = 5;
export const DECRYPT_SHORT_PAUSE_MS = 5 * 60 * 1000;
export const DECRYPT_LONG_PAUSE_MS = 3 * 60 * 60 * 1000;

const PEER_KEY_CATEGORIES = ["session", "sender-key", "sender-key-memory"] as const;

export function pauseAfterDecryptStorm(previousStrikes: number): number {
  return previousStrikes > 0 ? DECRYPT_LONG_PAUSE_MS : DECRYPT_SHORT_PAUSE_MS;
}

export function peerKeyMatches(id: string, jid: string): boolean {
  if (id === jid || id.includes(jid)) {
    return true;
  }
  const at = jid.indexOf("@");
  if (at <= 0) {
    return false;
  }
  const user = jid.slice(0, at);
  const server = jid.slice(at + 1);
  if (user.length < 8 || !server) {
    return false;
  }
  return id.startsWith(`${user}:`) && id.endsWith(`@${server}`);
}

export function stripPeerKeyBuckets(
  keys: { [category: string]: { [id: string]: unknown } | undefined },
  jid: string,
): number {
  let removed = 0;
  for (const category of PEER_KEY_CATEGORIES) {
    const bucket = keys[category];
    if (!bucket) {
      continue;
    }
    for (const id of Object.keys(bucket)) {
      if (peerKeyMatches(id, jid)) {
        delete bucket[id];
        removed += 1;
      }
    }
  }
  return removed;
}

export interface DecryptLogSignal {
  jid: string | null;
  immediate: boolean;
}

function textOf(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Error) {
    return `${value.name} ${value.message}`;
  }
  if (!value || typeof value !== "object") {
    return "";
  }
  const record = value as Record<string, unknown>;
  const err = record.err;
  const errText =
    err instanceof Error
      ? `${err.name} ${err.message}`
      : err && typeof err === "object"
        ? `${String((err as { message?: unknown }).message ?? "")}`
        : "";
  return `${String(record.msg ?? "")} ${errText} ${String(record.message ?? "")}`;
}

function jidFrom(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const record = value as Record<string, unknown>;
  const key = record.key;
  const remote =
    key && typeof key === "object"
      ? (key as { remoteJid?: unknown }).remoteJid
      : record.remoteJid;
  return typeof remote === "string" && remote.includes("@") ? remote : null;
}

export function decryptLogSignal(args: unknown[]): DecryptLogSignal | null {
  const text = args.map((arg) => textOf(arg)).join(" ");
  const immediate = text.includes("Over 2000 messages into the future");
  const failed =
    text.includes("failed to decrypt message") ||
    text.includes("No matching sessions found for message");
  if (!immediate && !failed) {
    return null;
  }
  let jid: string | null = null;
  for (const arg of args) {
    jid = jidFrom(arg) ?? jid;
  }
  return { jid, immediate };
}
