import os from "node:os";
import { logger } from "../logger.js";
import { getSupabase } from "../supabase.js";
import { ensureSessionRow, SESSIONS_TABLE } from "./sessionRepository.js";

const LEASE_TTL_MS = 60_000;
const LEASE_RENEW_MS = 20_000;
const LEASE_POLL_MS = 5_000;

const owner = os.hostname();

interface LeaseState {
  held: boolean;
  renewTimer: ReturnType<typeof setInterval> | null;
}

const leases = new Map<string, LeaseState>();
let leaseColumnsMissing = false;
let leaseLostHandler: ((sessionId: string) => void) | null = null;

function getLease(sessionId: string): LeaseState {
  let lease = leases.get(sessionId);
  if (!lease) {
    lease = { held: false, renewTimer: null };
    leases.set(sessionId, lease);
  }
  return lease;
}

function expiresAt(): string {
  return new Date(Date.now() + LEASE_TTL_MS).toISOString();
}

function isMissingLeaseColumn(error: { code?: string; message: string }): boolean {
  return (
    (error.code === "42703" || error.code === "PGRST204") && error.message.includes("lease_")
  );
}

function noteLeaseColumnsMissing(): void {
  if (!leaseColumnsMissing) {
    leaseColumnsMissing = true;
    logger.warn(
      "whatsapp_baileys_sessions has no lease_owner/lease_expires_at columns, so two server instances can still connect at once during a deploy. Run whatsapp/sql/whatsapp_baileys_sessions.sql.",
    );
  }
}

function stopRenewing(lease: LeaseState): void {
  if (lease.renewTimer) {
    clearInterval(lease.renewTimer);
    lease.renewTimer = null;
  }
}

async function renewLease(sessionId: string): Promise<void> {
  const lease = getLease(sessionId);
  if (!lease.held || leaseColumnsMissing) {
    return;
  }

  const { data, error } = await getSupabase()
    .from(SESSIONS_TABLE)
    .update({ lease_expires_at: expiresAt() })
    .eq("session_id", sessionId)
    .eq("lease_owner", owner)
    .select("session_id");

  if (error) {
    logger.warn({ sessionId, error: error.message }, "Could not renew the WhatsApp lease — retrying");
    return;
  }
  if (data && data.length > 0) {
    return;
  }

  lease.held = false;
  stopRenewing(lease);
  logger.error({ sessionId, owner }, "Lost the WhatsApp lease to another server instance");
  leaseLostHandler?.(sessionId);
}

export function onLeaseLost(handler: (sessionId: string) => void): void {
  leaseLostHandler = handler;
}

export function holdsLease(sessionId: string): boolean {
  return leaseColumnsMissing || getLease(sessionId).held;
}

/** Returns false while another server instance holds the lease. */
export async function acquireLease(sessionId: string): Promise<boolean> {
  const lease = getLease(sessionId);
  if (lease.held || leaseColumnsMissing) {
    return true;
  }

  await ensureSessionRow(sessionId);
  const now = new Date().toISOString();
  const { data, error } = await getSupabase()
    .from(SESSIONS_TABLE)
    .update({ lease_owner: owner, lease_expires_at: expiresAt() })
    .eq("session_id", sessionId)
    .or(`lease_owner.is.null,lease_owner.eq."${owner}",lease_expires_at.lt."${now}"`)
    .select("session_id");

  if (error) {
    if (isMissingLeaseColumn(error)) {
      noteLeaseColumnsMissing();
      return true;
    }
    throw new Error(`Acquire WhatsApp lease: ${error.message}`);
  }
  if (!data || data.length === 0) {
    return false;
  }

  lease.held = true;
  stopRenewing(lease);
  lease.renewTimer = setInterval(() => {
    void renewLease(sessionId).catch((renewError) => {
      logger.warn(
        { sessionId, error: renewError instanceof Error ? renewError.message : renewError },
        "Could not renew the WhatsApp lease — retrying",
      );
    });
  }, LEASE_RENEW_MS);
  lease.renewTimer.unref?.();
  return true;
}

export async function waitForLease(
  sessionId: string,
  shouldStop: () => boolean,
): Promise<boolean> {
  let warned = false;
  while (!shouldStop()) {
    if (await acquireLease(sessionId)) {
      if (warned) {
        logger.warn({ sessionId, owner }, "Took over the WhatsApp lease");
      }
      return true;
    }
    if (!warned) {
      warned = true;
      logger.warn(
        { sessionId, owner },
        "Another server instance holds the WhatsApp login — waiting for it to save and let go",
      );
    }
    await new Promise((resolve) => setTimeout(resolve, LEASE_POLL_MS));
  }
  return false;
}

export async function releaseAllLeases(): Promise<void> {
  for (const [sessionId, lease] of leases) {
    stopRenewing(lease);
    if (!lease.held) {
      continue;
    }
    lease.held = false;
    const { error } = await getSupabase()
      .from(SESSIONS_TABLE)
      .update({ lease_owner: null, lease_expires_at: null })
      .eq("session_id", sessionId)
      .eq("lease_owner", owner);
    if (error) {
      logger.warn({ sessionId, error: error.message }, "Could not release the WhatsApp lease");
    }
  }
}
