import {
  initAuthCreds,
  proto,
  type AuthenticationCreds,
  type SignalDataSet,
  type SignalDataTypeMap,
} from "@whiskeysockets/baileys";
import fs from "node:fs/promises";
import { antibanPersistPath } from "./antiban.js";
import { stripPeerKeyBuckets } from "./decryptBackoff.js";
import {
  clearSessionAuth,
  credsAreRegistered,
  ensureSessionRow,
  fromDbJson,
  saveCredsToDb,
  saveKeysToDb,
  type BaileysKeyStore,
} from "./sessionRepository.js";

function emptyKeys(): BaileysKeyStore {
  return {};
}

interface ActiveKeyStore {
  keys: BaileysKeyStore;
  flushKeys: () => Promise<void>;
}

const activeKeyStores = new Map<string, ActiveKeyStore>();

export async function useDatabaseAuthState(sessionId: string) {
  const row = await ensureSessionRow(sessionId);
  const creds: AuthenticationCreds =
    fromDbJson<AuthenticationCreds>(row.creds) ?? initAuthCreds();
  const keys: BaileysKeyStore = fromDbJson<BaileysKeyStore>(row.keys) ?? emptyKeys();

  let keysSaveTimer: ReturnType<typeof setTimeout> | null = null;
  let keysSaveChain: Promise<void> = Promise.resolve();

  const flushKeys = () => {
    keysSaveChain = keysSaveChain.then(() => saveKeysToDb(sessionId, keys));
    return keysSaveChain;
  };

  const scheduleKeysSave = () => {
    if (keysSaveTimer) {
      clearTimeout(keysSaveTimer);
    }
    keysSaveTimer = setTimeout(() => {
      keysSaveTimer = null;
      void flushKeys().catch(() => undefined);
    }, 400);
  };

  const active: ActiveKeyStore = { keys, flushKeys };
  activeKeyStores.set(sessionId, active);

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
          scheduleKeysSave();
        },
      },
    },
    saveCreds: async () => {
      await saveCredsToDb(sessionId, creds);
    },
    flushKeys,
  };
}

export async function hasSavedAuth(sessionId: string): Promise<boolean> {
  const row = await ensureSessionRow(sessionId);
  return credsAreRegistered(row.creds);
}

export async function clearAuth(sessionId: string): Promise<void> {
  activeKeyStores.delete(sessionId);
  await clearSessionAuth(sessionId);
  await fs.unlink(antibanPersistPath(sessionId)).catch(() => undefined);
}

export async function stripPeerSessions(sessionId: string, jid: string): Promise<number> {
  const active = activeKeyStores.get(sessionId);
  if (!active) {
    const row = await ensureSessionRow(sessionId);
    const keys = fromDbJson<BaileysKeyStore>(row.keys) ?? emptyKeys();
    const removed = stripPeerKeyBuckets(keys, jid);
    if (removed > 0) {
      await saveKeysToDb(sessionId, keys);
    }
    return removed;
  }

  const removed = stripPeerKeyBuckets(active.keys, jid);
  await active.flushKeys();
  return removed;
}
