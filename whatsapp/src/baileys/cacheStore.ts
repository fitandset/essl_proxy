import type { CacheStore } from "@whiskeysockets/baileys";

interface Entry {
  value: unknown;
  expiresAt: number;
}

export function createCacheStore(ttlMs: number, maxEntries: number): CacheStore {
  const entries = new Map<string, Entry>();

  return {
    get<T>(key: string): T | undefined {
      const entry = entries.get(key);
      if (!entry) {
        return undefined;
      }
      if (entry.expiresAt <= Date.now()) {
        entries.delete(key);
        return undefined;
      }
      return entry.value as T;
    },
    set<T>(key: string, value: T): void {
      entries.delete(key);
      entries.set(key, { value, expiresAt: Date.now() + ttlMs });
      if (entries.size > maxEntries) {
        const oldest = entries.keys().next();
        if (!oldest.done) {
          entries.delete(oldest.value);
        }
      }
    },
    del(key: string): void {
      entries.delete(key);
    },
    flushAll(): void {
      entries.clear();
    },
  };
}
