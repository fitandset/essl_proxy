import "dotenv/config";
import os from "node:os";
import path from "node:path";

export const DEFAULT_SESSION_ID = "default";

function envNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

function envString(name: string, fallback = ""): string {
  return process.env[name] ?? fallback;
}

const defaultSessionId = process.env.SESSION_ID ?? DEFAULT_SESSION_ID;

function envBasePath(): string {
  const raw = envString("BASE_PATH").trim();
  if (!raw || raw === "/") {
    return "";
  }
  const withSlash = raw.startsWith("/") ? raw : `/${raw}`;
  return withSlash.replace(/\/+$/, "");
}

export const config = {
  port: envNumber("PORT", 3000),
  basePath: envBasePath(),
  defaultSessionId,
  adminApiKey: envString("ADMIN_API_KEY"),
  bloodBookingWebhookSecret: envString("BLOOD_BOOKING_WEBHOOK_SECRET"),
  bloodBookingGroupName: envString("BLOOD_BOOKING_GROUP_NAME", "Test Group").trim() || "Test Group",
  supabaseUrl: envString("SUPABASE_URL"),
  supabaseServiceRoleKey: envString("SUPABASE_SERVICE_ROLE_KEY"),
  antiban: {
    persistPath:
      process.env.ANTIBAN_STATE_PATH ??
      path.join(os.tmpdir(), `bailey-antiban-${defaultSessionId}.json`),
    maxPerDay: envNumber("ANTIBAN_MAX_PER_DAY", 30),
    maxPerHour: envNumber("ANTIBAN_MAX_PER_HOUR", 8),
    maxPerMinute: envNumber("ANTIBAN_MAX_PER_MINUTE", 3),
    minDelayMs: envNumber("ANTIBAN_MIN_DELAY_MS", 2500),
    maxDelayMs: envNumber("ANTIBAN_MAX_DELAY_MS", 7000),
    newChatDelayMs: envNumber("ANTIBAN_NEW_CHAT_DELAY_MS", 4000),
    maxIdenticalMessages: envNumber("ANTIBAN_MAX_IDENTICAL", 3),
    warmupDays: envNumber("ANTIBAN_WARMUP_DAYS", 30),
  },
};
