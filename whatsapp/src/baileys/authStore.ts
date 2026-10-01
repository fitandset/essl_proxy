import {
  initAuthCreds,
  proto,
  type AuthenticationCreds,
  type SignalDataSet,
  type SignalDataTypeMap,
  type SignalKeyStore,
} from "@whiskeysockets/baileys";
import fs from "node:fs/promises";
import { logger } from "../logger.js";
import { antibanPersistPath } from "./antiban.js";
import {
  clearSessionAuth,
  credsAreRegistered,
  ensureSessionRow,
  fromDbJson,
  saveCredsToDb,
  saveKeysToDb,
  type BaileysKeyStore,
} from "./sessionRepository.js";

const KEYS_SAVE_DELAY_MS = 400;
const SAVE_RETRY_DELAY_MS = 5_000;

export interface DatabaseAuthState {
  state: {
    creds: AuthenticationCreds;
    keys: SignalKeyStore;
  };
  saveCreds: () => Promise<void>;
}

interface StoredAuthState extends DatabaseAuthState {
  flush: () => Promise<void>;
  dispose: () => Promise<void>;
}

// Exactly one copy per session for the life of the process: a second copy loaded
// from the database would hold stale Signal ratchet state and overwrite newer writes.
const authStates = new Map<string, Promise<StoredAuthState>>();

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function loadAuthState(sessionId: string): Promise<StoredAuthState> {
  const row = await ensureSessionRow(sessionId);
  const creds: AuthenticationCreds =
    fromDbJson<AuthenticationCreds>(row.creds) ?? initAuthCreds();
  const keys: BaileysKeyStore = fromDbJson<BaileysKeyStore>(row.keys) ?? {};

  let disposed = false;
  let credsDirty = false;
  let keysDirty = false;
  let saveTimer: ReturnType<typeof setTimeout> | null = null;
  let writes: Promise<void> = Promise.resolve();

  const persist = async () => {
    if (!disposed && credsDirty) {
      credsDirty = false;
      try {
        await saveCredsToDb(sessionId, creds);
      } catch (error) {
        credsDirty = true;
        throw error;
      }
    }
    if (!disposed && keysDirty) {
      keysDirty = false;
      try {
        await saveKeysToDb(sessionId, keys);
      } catch (error) {
        keysDirty = true;
        throw error;
      }
    }
  };

  const flush = (): Promise<void> => {
    if (saveTimer) {
      clearTimeout(saveTimer);
      saveTimer = null;
    }
    writes = writes.then(persist).catch((error) => {
      logger.error(
        { sessionId, error: errorMessage(error) },
        "Failed to save WhatsApp login state to the database — retrying",
      );
      scheduleSave(SAVE_RETRY_DELAY_MS);
    });
    return writes;
  };

  // Not reset on every write: under steady traffic a resetting timer never fires.
  const scheduleSave = (delayMs: number) => {
    if (disposed || saveTimer) {
      return;
    }
    saveTimer = setTimeout(() => {
      saveTimer = null;
      void flush();
    }, delayMs);
  };

  return {
    state: {
      creds,
      keys: {
        get: async <T extends keyof SignalDataTypeMap>(type: T, ids: string[]) => {
          const data: { [id: string]: SignalDataTypeMap[T] } = {};
          const bucket = (keys[type] ?? {}) as Record<string, SignalDataTypeMap[T]>;
          for (const id of ids) {
            let value = bucket[id];
            if (type === "app-state-sync-key" && value) {
              value = proto.Message.AppStateSyncKeyData.fromObject(
                value,
              ) as unknown as SignalDataTypeMap[T];
            }
            if (value) {
              data[id] = value;
            }
          }
          return data;
        },
        set: async (data: SignalDataSet) => {
          for (const category of Object.keys(data) as (keyof SignalDataTypeMap)[]) {
            const bucket = { ...(keys[category] ?? {}) };
            const values = data[category];
            if (!values) {
              continue;
            }
            for (const id of Object.keys(values)) {
              const value = values[id];
              if (value) {
                bucket[id] = value;
              } else {
                delete bucket[id];
              }
            }
            keys[category] = bucket;
          }
          keysDirty = true;
          scheduleSave(KEYS_SAVE_DELAY_MS);
        },
      },
    },
    saveCreds: () => {
      credsDirty = true;
      return flush();
    },
    flush,
    dispose: async () => {
      disposed = true;
      if (saveTimer) {
        clearTimeout(saveTimer);
        saveTimer = null;
      }
      await writes;
    },
  };
}

export function useDatabaseAuthState(sessionId: string): Promise<DatabaseAuthState> {
  let stored = authStates.get(sessionId);
  if (!stored) {
    const loading = loadAuthState(sessionId);
    loading.catch(() => {
      if (authStates.get(sessionId) === loading) {
        authStates.delete(sessionId);
      }
    });
    authStates.set(sessionId, loading);
    stored = loading;
  }
  return stored;
}

export async function flushAuthState(sessionId: string): Promise<void> {
  const stored = authStates.get(sessionId);
  if (!stored) {
    return;
  }
  await (await stored).flush();
}

/** Drop the in-memory copy without writing it, so the next socket reloads from the database. */
export async function discardAuthState(sessionId: string): Promise<void> {
  const stored = authStates.get(sessionId);
  authStates.delete(sessionId);
  if (!stored) {
    return;
  }
  try {
    await (await stored).dispose();
  } catch {
    // A copy that never finished loading has nothing to dispose.
  }
}

/** Write pending changes, then stop saving, so late writes cannot land after another instance takes over. */
export async function closeAuthState(sessionId: string): Promise<void> {
  const stored = authStates.get(sessionId);
  authStates.delete(sessionId);
  if (!stored) {
    return;
  }
  let auth: StoredAuthState;
  try {
    auth = await stored;
  } catch {
    return;
  }
  await auth.flush();
  await auth.dispose();
}

export async function hasSavedAuth(sessionId: string): Promise<boolean> {
  const row = await ensureSessionRow(sessionId);
  return credsAreRegistered(row.creds);
}

export async function clearAuth(sessionId: string): Promise<void> {
  await discardAuthState(sessionId);
  await clearSessionAuth(sessionId);
  await fs.unlink(antibanPersistPath(sessionId)).catch(() => undefined);
}
