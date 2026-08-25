import type { AuthenticationCreds, SignalDataTypeMap } from "@whiskeysockets/baileys";
import { BufferJSON } from "@whiskeysockets/baileys";
import { config } from "../config.js";
import { logger } from "../logger.js";
import { getSupabase } from "../supabase.js";
import type { SessionStatus } from "../types.js";

export const SESSIONS_TABLE = "whatsapp_baileys_sessions";

export type BaileysKeyStore = {
  [T in keyof SignalDataTypeMap]?: Record<string, unknown>;
};

export interface SessionRow {
  session_id: string;
  gym_id: string | null;
  status: SessionStatus;
  creds: unknown | null;
  keys: unknown | null;
  antiban_state: unknown | null;
  phone_number: string | null;
  qr_data_url: string | null;
  connected_at: string | null;
  paused: boolean;
  daily_cap: number | null;
}

const writeLocks = new Map<string, Promise<void>>();

function withLock(sessionId: string, task: () => Promise<void>): Promise<void> {
  const previous = writeLocks.get(sessionId) ?? Promise.resolve();
  const next = previous.then(task, task);
  writeLocks.set(sessionId, next.catch(() => undefined));
  return next;
}

export function toDbJson(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value, BufferJSON.replacer));
}

export function fromDbJson<T>(value: unknown): T | null {
  if (value === null || value === undefined) {
    return null;
  }
  return JSON.parse(JSON.stringify(value), BufferJSON.reviver) as T;
}

async function assertOk(error: { message: string } | null, action: string): Promise<void> {
  if (error) {
    throw new Error(`${action}: ${error.message}`);
  }
}

export async function ensureSessionRow(
  sessionId = config.defaultSessionId,
): Promise<SessionRow> {
  const supabase = getSupabase();
  const { data, error } = await supabase
    .from(SESSIONS_TABLE)
    .select("*")
    .eq("session_id", sessionId)
    .maybeSingle();

  await assertOk(error, "Load WhatsApp session row");

  if (data) {
    return data as SessionRow;
  }

  const { data: inserted, error: insertError } = await supabase
    .from(SESSIONS_TABLE)
    .insert({
      session_id: sessionId,
      status: "disconnected",
      paused: false,
      daily_cap: config.antiban.maxPerDay,
    })
    .select("*")
    .single();

  await assertOk(insertError, "Create WhatsApp session row");
  return inserted as SessionRow;
}

export async function getSessionRow(
  sessionId = config.defaultSessionId,
): Promise<SessionRow | null> {
  const supabase = getSupabase();
  const { data, error } = await supabase
    .from(SESSIONS_TABLE)
    .select("*")
    .eq("session_id", sessionId)
    .maybeSingle();

  await assertOk(error, "Load WhatsApp session row");
  return (data as SessionRow | null) ?? null;
}

export async function saveCredsToDb(
  sessionId: string,
  creds: AuthenticationCreds,
): Promise<void> {
  await withLock(sessionId, async () => {
    await ensureSessionRow(sessionId);
    const { error } = await getSupabase()
      .from(SESSIONS_TABLE)
      .update({ creds: toDbJson(creds) })
      .eq("session_id", sessionId);
    await assertOk(error, "Save WhatsApp creds");
  });
}

export async function saveKeysToDb(
  sessionId: string,
  keys: BaileysKeyStore,
): Promise<void> {
  await withLock(sessionId, async () => {
    await ensureSessionRow(sessionId);
    const { error } = await getSupabase()
      .from(SESSIONS_TABLE)
      .update({ keys: toDbJson(keys) })
      .eq("session_id", sessionId);
    await assertOk(error, "Save WhatsApp keys");
  });
}

export async function saveAntibanState(
  sessionId: string,
  state: unknown,
): Promise<void> {
  await withLock(sessionId, async () => {
    await ensureSessionRow(sessionId);
    const { error } = await getSupabase()
      .from(SESSIONS_TABLE)
      .update({ antiban_state: state })
      .eq("session_id", sessionId);
    await assertOk(error, "Save antiban state");
  });
}

export async function updateSessionMeta(
  sessionId: string,
  patch: Partial<{
    status: SessionStatus;
    phone_number: string | null;
    qr_data_url: string | null;
    connected_at: string | null;
  }>,
): Promise<void> {
  await withLock(sessionId, async () => {
    await ensureSessionRow(sessionId);
    const { error } = await getSupabase()
      .from(SESSIONS_TABLE)
      .update(patch)
      .eq("session_id", sessionId);
    if (error) {
      logger.warn({ sessionId, error: error.message }, "Failed to update session metadata");
    }
  });
}

export async function clearSessionAuth(sessionId: string): Promise<void> {
  await withLock(sessionId, async () => {
    await ensureSessionRow(sessionId);
    const { error } = await getSupabase()
      .from(SESSIONS_TABLE)
      .update({
        creds: null,
        keys: null,
        antiban_state: null,
        phone_number: null,
        qr_data_url: null,
        connected_at: null,
        status: "disconnected",
      })
      .eq("session_id", sessionId);
    await assertOk(error, "Clear WhatsApp session");
  });
}

export function credsAreRegistered(creds: unknown): boolean {
  const parsed = fromDbJson<AuthenticationCreds>(creds);
  return Boolean(parsed?.registered);
}
