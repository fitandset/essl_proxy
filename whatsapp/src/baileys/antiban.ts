import {
  wrapSocket,
  type AntiBanConfig,
  type AntiBanStats,
  type WrapSocketOptions,
} from "baileys-antiban";
import type { WASocket } from "@whiskeysockets/baileys";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { config } from "../config.js";
import { logger } from "../logger.js";
import type { AntibanStatusPayload } from "../types.js";
import { ensureSessionRow, saveAntibanState } from "./sessionRepository.js";

export const antibanWrapOptions: WrapSocketOptions = {
  autoRespondToIncoming: false,
  groupOpGuard: false,
  legitimacySignals: false,
};

const antibanFlushTimers = new Map<string, ReturnType<typeof setInterval>>();

export function antibanPersistPath(sessionId: string): string {
  if (process.env.ANTIBAN_STATE_PATH) {
    return process.env.ANTIBAN_STATE_PATH;
  }
  return path.join(os.tmpdir(), `bailey-antiban-${sessionId}.json`);
}

export function getAntibanConfig(sessionId = config.defaultSessionId): AntiBanConfig {
  const { antiban } = config;
  return {
    preset: "conservative",
    maxPerDay: antiban.maxPerDay,
    maxPerHour: antiban.maxPerHour,
    maxPerMinute: antiban.maxPerMinute,
    minDelayMs: antiban.minDelayMs,
    maxDelayMs: antiban.maxDelayMs,
    newChatDelayMs: antiban.newChatDelayMs,
    maxIdenticalMessages: antiban.maxIdenticalMessages,
    burstAllowance: 1,
    warmupDays: antiban.warmupDays,
    day1Limit: antiban.maxPerDay,
    growthFactor: 1,
    inactivityThresholdHours: 168,
    autoPauseAt: "high",
    persist: antibanPersistPath(sessionId),
    logging: false,
  };
}

export async function hydrateAntibanPersist(sessionId: string): Promise<void> {
  const persistPath = antibanPersistPath(sessionId);
  await fs.mkdir(path.dirname(persistPath), { recursive: true });

  const row = await ensureSessionRow(sessionId);
  if (row.antiban_state) {
    await fs.writeFile(persistPath, JSON.stringify(row.antiban_state), "utf8");
  }
}

export async function flushAntibanPersist(sessionId: string): Promise<void> {
  const persistPath = antibanPersistPath(sessionId);
  try {
    const raw = await fs.readFile(persistPath, "utf8");
    const state = JSON.parse(raw) as unknown;
    await saveAntibanState(sessionId, state);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      logger.warn(
        { sessionId, error: error instanceof Error ? error.message : error },
        "Failed to flush antiban state to database",
      );
    }
  }
}

export function startAntibanPersistSync(sessionId: string): void {
  stopAntibanPersistSync(sessionId);
  const timer = setInterval(() => {
    void flushAntibanPersist(sessionId);
  }, 15_000);
  timer.unref?.();
  antibanFlushTimers.set(sessionId, timer);
}

export function stopAntibanPersistSync(sessionId: string): void {
  const timer = antibanFlushTimers.get(sessionId);
  if (timer) {
    clearInterval(timer);
    antibanFlushTimers.delete(sessionId);
  }
}

export async function wrapWithAntiban(
  socket: WASocket,
  sessionId = config.defaultSessionId,
): Promise<WASocket> {
  await hydrateAntibanPersist(sessionId);
  const wrapped = wrapSocket(
    socket as unknown as Parameters<typeof wrapSocket>[0],
    getAntibanConfig(sessionId),
    undefined,
    antibanWrapOptions,
  ) as unknown as WASocket;
  startAntibanPersistSync(sessionId);
  return wrapped;
}

export function isAntibanBlockError(error: unknown): error is Error {
  return error instanceof Error && error.message.includes("[baileys-antiban]");
}

export function antibanBlockMessage(error: Error): string {
  return error.message.replace(/^\[baileys-antiban\]\s*/i, "").trim();
}

function getWrappedAntiban(socket: WASocket): { getStats: () => AntiBanStats } | null {
  if (!("antiban" in socket)) {
    return null;
  }
  const antiban = (socket as { antiban?: { getStats: () => AntiBanStats } }).antiban;
  return antiban && typeof antiban.getStats === "function" ? antiban : null;
}

export function getAntibanStatus(
  socket: WASocket | null,
): AntibanStatusPayload | null {
  if (!socket) {
    return null;
  }

  const antiban = getWrappedAntiban(socket);
  if (!antiban) {
    return null;
  }

  const stats = antiban.getStats();
  const warmupLimit = stats.warmUp.todayLimit;
  const todayLimit =
    warmupLimit > 0 ? Math.min(warmupLimit, config.antiban.maxPerDay) : config.antiban.maxPerDay;
  const todaySent = Math.max(stats.warmUp.todaySent, stats.rateLimiter.lastDay);

  return {
    health: {
      risk: stats.health.risk,
      score: stats.health.score,
      recommendation: stats.health.recommendation,
      reasons: stats.health.reasons,
    },
    todaySent,
    todayLimit,
    messagesAllowed: stats.messagesAllowed,
    messagesBlocked: stats.messagesBlocked,
    lastMinute: stats.rateLimiter.lastMinute,
    lastHour: stats.rateLimiter.lastHour,
    lastDay: stats.rateLimiter.lastDay,
  };
}
