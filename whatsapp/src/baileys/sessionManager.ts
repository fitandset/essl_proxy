import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  type CacheStore,
  type proto,
  type WASocket,
} from "@whiskeysockets/baileys";
import QRCode from "qrcode";
import { config } from "../config.js";
import { logger } from "../logger.js";
import {
  SessionError,
  type SessionStatus,
  type SessionStatusPayload,
} from "../types.js";
import {
  antibanBlockMessage,
  flushAntibanPersist,
  getAntibanStatus,
  isAntibanBlockError,
  stopAntibanPersistSync,
  wrapWithAntiban,
} from "./antiban.js";
import {
  clearAuth,
  closeAuthState,
  discardAuthState,
  flushAuthState,
  hasSavedAuth,
  useDatabaseAuthState,
} from "./authStore.js";
import { createCacheStore } from "./cacheStore.js";
import {
  acquireLease,
  holdsLease,
  onLeaseLost,
  releaseAllLeases,
  waitForLease,
} from "./sessionLease.js";
import { updateSessionMeta } from "./sessionRepository.js";

const PAIR_CODE_TTL_MS = 90_000;
const QR_TIMEOUT_MS = 180_000;
const RECONNECT_BASE_MS = 5_000;
const RECONNECT_MAX_MS = 5 * 60 * 1000;
const REPLACED_RECONNECT_MS = 2 * 60 * 1000;
const MSG_RETRY_TTL_MS = 60 * 60 * 1000;
const SENT_MESSAGE_TTL_MS = 24 * 60 * 60 * 1000;

interface SocketState {
  socket: WASocket | null;
  qrDataUrl: string | null;
  pairingCode: string | null;
  pairingPhone: string | null;
  pairingIssuedAt: number | null;
  status: SessionStatus;
  phoneNumber: string | null;
  reconnectAttempts: number;
  stoppedByUser: boolean;
  pairingLock: Promise<string> | null;
  pairingSucceeded: boolean;
  reconnectTimer: ReturnType<typeof setTimeout> | null;
  creating: Promise<WASocket> | null;
  // Outlive each socket so Signal retries keep counting across reconnects
  // and retry receipts for our own messages can be answered.
  msgRetryCounterCache: CacheStore;
  sentMessages: CacheStore;
}

const sockets = new Map<string, SocketState>();
let shuttingDown = false;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clearTimer(timer: ReturnType<typeof setTimeout> | null): void {
  if (timer) {
    clearTimeout(timer);
  }
}

function cancelReconnect(runtime: SocketState): void {
  clearTimer(runtime.reconnectTimer);
  runtime.reconnectTimer = null;
}

function scheduleReconnect(sessionId: string, delayMs: number): void {
  const runtime = getState(sessionId);
  if (runtime.stoppedByUser || shuttingDown) {
    return;
  }
  cancelReconnect(runtime);
  runtime.reconnectTimer = setTimeout(() => {
    runtime.reconnectTimer = null;
    if (runtime.stoppedByUser || shuttingDown) {
      return;
    }
    void startSession(sessionId, { isReconnect: true }).catch((error) => {
      logger.error({ sessionId, err: error }, "Failed to reconnect WhatsApp session");
      scheduleReconnect(sessionId, nextReconnectDelay(runtime));
    });
  }, delayMs);
}

function nextReconnectDelay(runtime: SocketState): number {
  const delayMs = Math.min(RECONNECT_BASE_MS * 2 ** runtime.reconnectAttempts, RECONNECT_MAX_MS);
  runtime.reconnectAttempts += 1;
  return delayMs;
}

export function normalizePairingPhone(input: string): string {
  const digits = input.replace(/\D/g, "");
  if (digits.length < 10 || digits.length > 15) {
    throw new SessionError(
      "Phone must include country code and be 10–15 digits, e.g. 9198xxxxxxxx",
      400,
    );
  }
  return digits;
}

function formatPairingCode(code: string): string {
  return code.replace(/[^A-Za-z0-9]/g, "").toUpperCase();
}

function getState(sessionId: string): SocketState {
  let state = sockets.get(sessionId);
  if (!state) {
    state = {
      socket: null,
      qrDataUrl: null,
      pairingCode: null,
      pairingPhone: null,
      pairingIssuedAt: null,
      status: "disconnected",
      phoneNumber: null,
      reconnectAttempts: 0,
      stoppedByUser: false,
      pairingLock: null,
      pairingSucceeded: false,
      reconnectTimer: null,
      creating: null,
      msgRetryCounterCache: createCacheStore(MSG_RETRY_TTL_MS, 2_000),
      sentMessages: createCacheStore(SENT_MESSAGE_TTL_MS, 500),
    };
    sockets.set(sessionId, state);
  }
  return state;
}

function canReusePairingCode(runtime: SocketState, phone: string): boolean {
  if (!runtime.socket || runtime.status !== "pairing") {
    return false;
  }
  if (!runtime.pairingCode || runtime.pairingPhone !== phone || !runtime.pairingIssuedAt) {
    return false;
  }
  return Date.now() - runtime.pairingIssuedAt < PAIR_CODE_TTL_MS;
}

export function getSessionStatus(
  sessionId = config.defaultSessionId,
): SessionStatusPayload {
  const state = getState(sessionId);
  return {
    sessionId,
    status: state.status,
    qrDataUrl: state.qrDataUrl,
    pairingCode: state.pairingCode,
    pairingPhone: state.pairingPhone,
    phoneNumber: state.phoneNumber,
    connected: state.status === "connected",
    antiban: getAntibanStatus(state.socket),
  };
}

export async function sendTextMessage(
  phoneInput: string,
  text: string,
  sessionId = config.defaultSessionId,
): Promise<{ phone: string; jid: string }> {
  const phone = normalizePairingPhone(phoneInput);
  const trimmed = text.trim();
  if (!trimmed) {
    throw new SessionError("Message text is required", 400);
  }

  const runtime = getState(sessionId);
  if (!runtime.socket || runtime.status !== "connected") {
    throw new SessionError(
      "WhatsApp is not connected. Link the number first, then send.",
      409,
    );
  }

  const jid = `${phone}@s.whatsapp.net`;

  try {
    await runtime.socket.sendPresenceUpdate("composing", jid).catch(() => undefined);
    await runtime.socket.sendMessage(jid, { text: trimmed }, {});
    await runtime.socket.sendPresenceUpdate("paused", jid).catch(() => undefined);
  } catch (error) {
    if (isAntibanBlockError(error)) {
      throw new SessionError(antibanBlockMessage(error), 429);
    }
    throw error;
  }

  logger.info({ sessionId, phone }, "Sent WhatsApp text message");
  return { phone, jid };
}

export async function sendGroupTextMessage(
  groupName: string,
  text: string,
  sessionId = config.defaultSessionId,
): Promise<{ groupName: string; jid: string }> {
  const name = groupName.trim();
  const trimmed = text.trim();
  if (!name) {
    throw new SessionError("Group name is required", 400);
  }
  if (!trimmed) {
    throw new SessionError("Message text is required", 400);
  }

  const runtime = getState(sessionId);
  if (!runtime.socket || runtime.status !== "connected") {
    throw new SessionError(
      "WhatsApp is not connected. Link the number first, then send.",
      409,
    );
  }

  const groups = await runtime.socket.groupFetchAllParticipating();
  const matches = Object.values(groups).filter((group) => group.subject?.trim() === name);
  if (matches.length === 0) {
    throw new SessionError(
      `WhatsApp group "${name}" was not found on the connected number.`,
      404,
    );
  }
  if (matches.length > 1) {
    throw new SessionError(`More than one WhatsApp group is named "${name}".`, 409);
  }

  const jid = matches[0].id;

  try {
    await runtime.socket.sendPresenceUpdate("composing", jid).catch(() => undefined);
    await runtime.socket.sendMessage(jid, { text: trimmed }, {});
    await runtime.socket.sendPresenceUpdate("paused", jid).catch(() => undefined);
  } catch (error) {
    if (isAntibanBlockError(error)) {
      throw new SessionError(antibanBlockMessage(error), 429);
    }
    throw error;
  }

  logger.info({ sessionId, groupName: name }, "Sent WhatsApp group message");
  return { groupName: name, jid };
}

async function waitForPairingReady(
  sessionId: string,
  socket: WASocket,
): Promise<void> {
  const deadline = Date.now() + 15000;

  while (Date.now() < deadline) {
    const runtime = getState(sessionId);
    if (runtime.socket !== socket) {
      throw new SessionError("WhatsApp socket was replaced", 500);
    }
    if (runtime.status === "connected") {
      throw new SessionError(
        "Already connected. Logout first to pair with a code.",
        409,
      );
    }
    if (
      runtime.status === "qr" ||
      runtime.status === "pairing" ||
      runtime.qrDataUrl
    ) {
      await sleep(800);
      return;
    }
    await sleep(200);
  }
}

// Concurrent callers share one attempt: two sockets on the same login kick each other off.
function createSocket(sessionId: string): Promise<WASocket> {
  const runtime = getState(sessionId);
  if (!runtime.creating) {
    runtime.creating = openSocket(sessionId).finally(() => {
      runtime.creating = null;
    });
  }
  return runtime.creating;
}

async function openSocket(sessionId: string): Promise<WASocket> {
  if (!(await acquireLease(sessionId))) {
    throw new SessionError(
      "WhatsApp is connected from another server instance (usually during a deploy). Try again in a minute.",
      503,
    );
  }
  const { state, saveCreds } = await useDatabaseAuthState(sessionId);
  const { version } = await fetchLatestBaileysVersion();
  if (shuttingDown) {
    throw new SessionError("The WhatsApp service is shutting down", 503);
  }
  const runtime = getState(sessionId);

  runtime.status = "connecting";
  runtime.qrDataUrl = null;
  runtime.pairingCode = null;
  runtime.pairingIssuedAt = null;
  void updateSessionMeta(sessionId, {
    status: "connecting",
    qr_data_url: null,
  });

  const rawSocket = makeWASocket({
    version,
    auth: state,
    browser: Browsers.ubuntu("Chrome"),
    logger,
    qrTimeout: QR_TIMEOUT_MS,
    syncFullHistory: false,
    shouldSyncHistoryMessage: () => false,
    markOnlineOnConnect: false,
    generateHighQualityLinkPreview: true,
    msgRetryCounterCache: runtime.msgRetryCounterCache,
    getMessage: async (key) =>
      key.id ? runtime.sentMessages.get<proto.IMessage>(key.id) : undefined,
  });

  const socket = await wrapWithAntiban(rawSocket, sessionId);
  runtime.socket = socket;

  socket.ev.on("creds.update", saveCreds);

  socket.ev.on("messages.upsert", ({ messages }) => {
    for (const message of messages) {
      if (message.key.fromMe && message.key.id && message.message) {
        runtime.sentMessages.set(message.key.id, message.message);
      }
    }
  });

  socket.ev.on("connection.update", async (update) => {
    if (getState(sessionId).socket !== socket) {
      return;
    }

    const { connection, lastDisconnect, qr, isNewLogin } = update;

    if (isNewLogin) {
      runtime.pairingSucceeded = true;
      logger.info({ sessionId }, "Pairing succeeded — WhatsApp will restart the connection");
    }

    if (qr) {
      const qrDataUrl = await QRCode.toDataURL(qr);
      runtime.qrDataUrl = qrDataUrl;
      if (runtime.status !== "pairing") {
        runtime.status = "qr";
        void updateSessionMeta(sessionId, { status: "qr", qr_data_url: qrDataUrl });
        logger.info(
          { sessionId },
          "QR code generated — scan in WhatsApp → Linked Devices",
        );
      }
    }

    if (connection === "open") {
      runtime.status = "connected";
      runtime.qrDataUrl = null;
      runtime.pairingCode = null;
      runtime.pairingPhone = null;
      runtime.pairingIssuedAt = null;
      runtime.pairingSucceeded = false;
      runtime.reconnectAttempts = 0;
      runtime.phoneNumber =
        socket.user?.id?.split(":")[0]?.split("@")[0] ?? runtime.phoneNumber;
      void updateSessionMeta(sessionId, {
        status: "connected",
        phone_number: runtime.phoneNumber,
        qr_data_url: null,
        connected_at: new Date().toISOString(),
      });
      void flushAntibanPersist(sessionId);

      logger.info(
        { sessionId, phoneNumber: runtime.phoneNumber },
        "WhatsApp session connected",
      );
      const antiban = getAntibanStatus(socket);
      if (antiban) {
        logger.info(
          {
            sessionId,
            todaySent: antiban.todaySent,
            todayLimit: antiban.todayLimit,
            health: antiban.health.risk,
          },
          "Antiban pacing is active",
        );
      }
    }

    if (connection === "close") {
      const statusCode = (
        lastDisconnect?.error as { output?: { statusCode?: number } } | undefined
      )?.output?.statusCode;
      const loggedOut = statusCode === DisconnectReason.loggedOut;
      const timedOut = statusCode === DisconnectReason.timedOut;
      const restartRequired = statusCode === DisconnectReason.restartRequired;
      const replaced = statusCode === DisconnectReason.connectionReplaced;
      const pairingJustSucceeded = runtime.pairingSucceeded;
      const wasUnregistered = !pairingJustSucceeded && !state.creds.registered;

      runtime.status = "disconnected";
      runtime.socket = null;
      runtime.qrDataUrl = null;
      runtime.pairingCode = null;
      runtime.pairingIssuedAt = null;
      stopAntibanPersistSync(sessionId);
      void flushAntibanPersist(sessionId);
      void updateSessionMeta(sessionId, {
        status: "disconnected",
        qr_data_url: null,
      });

      logger.warn(
        {
          sessionId,
          statusCode,
          loggedOut,
          timedOut,
          restartRequired,
          replaced,
          pairingJustSucceeded,
          wasUnregistered,
        },
        "WhatsApp session closed",
      );

      await flushAuthState(sessionId);

      if (runtime.stoppedByUser || shuttingDown) {
        return;
      }

      if (loggedOut) {
        runtime.phoneNumber = null;
        runtime.pairingPhone = null;
        runtime.pairingSucceeded = false;
        await clearAuth(sessionId);
        logger.info(
          { sessionId },
          "Logged out — cleared saved credentials. Start again to get a QR or pairing code.",
        );
        return;
      }

      // After a successful pair, WhatsApp sends 515 (restart required). Keep creds and reconnect.
      if (restartRequired || pairingJustSucceeded) {
        runtime.reconnectAttempts = 0;
        logger.info({ sessionId }, "Restart required — reconnecting with saved credentials");
        scheduleReconnect(sessionId, 500);
        return;
      }

      // Incomplete QR/pairing creds must not be reused — reconnecting them causes 401 logout.
      if (wasUnregistered) {
        await clearAuth(sessionId);
        logger.info(
          { sessionId },
          "QR or pairing code expired. Request a new code (do not reuse the old one).",
        );
        return;
      }

      // Another client logged in with these credentials. Reconnecting straight away
      // makes the two fight and each kick the other off, so give it room first.
      if (replaced) {
        const delayMs = Math.max(nextReconnectDelay(runtime), REPLACED_RECONNECT_MS);
        logger.warn(
          { sessionId, delayMs },
          "WhatsApp connection was replaced by another client — waiting before reconnecting",
        );
        scheduleReconnect(sessionId, delayMs);
        return;
      }

      scheduleReconnect(sessionId, nextReconnectDelay(runtime));
    }
  });

  return socket;
}

export async function startSession(
  sessionId = config.defaultSessionId,
  options?: { isReconnect?: boolean },
): Promise<void> {
  const runtime = getState(sessionId);

  if (!options?.isReconnect) {
    runtime.stoppedByUser = false;
    runtime.reconnectAttempts = 0;
    cancelReconnect(runtime);
  }

  if (runtime.socket && runtime.status === "connected") {
    return;
  }

  if (runtime.socket) {
    runtime.socket.end(undefined);
    runtime.socket = null;
  }

  await createSocket(sessionId);
}

async function requestPairingCode(
  phoneInput: string,
  sessionId: string,
  force: boolean,
): Promise<string> {
  const phone = normalizePairingPhone(phoneInput);
  const runtime = getState(sessionId);

  if (runtime.status === "connected") {
    throw new SessionError(
      "Already connected. Logout first to pair a number with a code.",
      409,
    );
  }

  if (!force && canReusePairingCode(runtime, phone)) {
    logger.info({ sessionId, phone }, "Reusing existing pairing code");
    return runtime.pairingCode as string;
  }

  runtime.stoppedByUser = false;
  runtime.reconnectAttempts = 0;
  cancelReconnect(runtime);

  if (!runtime.socket) {
    await createSocket(sessionId);
  }

  const socket = runtime.socket;
  if (!socket) {
    throw new SessionError("Failed to start WhatsApp socket", 500);
  }

  if (socket.authState.creds.registered) {
    throw new SessionError(
      "This session is already registered. Logout first, then request a pairing code.",
      409,
    );
  }

  await waitForPairingReady(sessionId, socket);

  const code = await socket.requestPairingCode(phone);
  runtime.pairingCode = formatPairingCode(code);
  runtime.pairingPhone = phone;
  runtime.pairingIssuedAt = Date.now();
  runtime.phoneNumber = phone;
  runtime.status = "pairing";
  void updateSessionMeta(sessionId, {
    status: "pairing",
    phone_number: phone,
  });

  logger.info(
    { sessionId, phone },
    "Pairing code generated — enter the 8 characters in WhatsApp (no dash or space)",
  );

  return runtime.pairingCode;
}

export async function startPairing(
  phoneInput: string,
  sessionId = config.defaultSessionId,
  options?: { force?: boolean },
): Promise<string> {
  const runtime = getState(sessionId);
  if (runtime.pairingLock) {
    return runtime.pairingLock;
  }

  runtime.pairingLock = requestPairingCode(
    phoneInput,
    sessionId,
    Boolean(options?.force),
  ).finally(() => {
    runtime.pairingLock = null;
  });

  return runtime.pairingLock;
}

export async function stopSession(
  sessionId = config.defaultSessionId,
): Promise<void> {
  const runtime = getState(sessionId);
  runtime.stoppedByUser = true;
  cancelReconnect(runtime);
  stopAntibanPersistSync(sessionId);
  await flushAntibanPersist(sessionId);

  if (runtime.socket) {
    runtime.socket.end(undefined);
    runtime.socket = null;
  }
  await flushAuthState(sessionId);

  runtime.status = "disconnected";
  runtime.qrDataUrl = null;
  runtime.pairingCode = null;
  runtime.pairingIssuedAt = null;
  await updateSessionMeta(sessionId, {
    status: "disconnected",
    qr_data_url: null,
  });
}

export async function logoutSession(
  sessionId = config.defaultSessionId,
): Promise<void> {
  const runtime = getState(sessionId);
  runtime.stoppedByUser = true;
  cancelReconnect(runtime);
  stopAntibanPersistSync(sessionId);

  if (runtime.socket) {
    try {
      await runtime.socket.logout();
    } catch {
      runtime.socket.end(undefined);
    }
    runtime.socket = null;
  }

  await clearAuth(sessionId);

  runtime.status = "disconnected";
  runtime.qrDataUrl = null;
  runtime.pairingCode = null;
  runtime.pairingPhone = null;
  runtime.pairingIssuedAt = null;
  runtime.phoneNumber = null;
  runtime.pairingSucceeded = false;

  logger.info({ sessionId }, "Logged out and cleared saved credentials");
}

export async function restoreSession(
  sessionId = config.defaultSessionId,
): Promise<void> {
  if (!(await waitForLease(sessionId, () => shuttingDown))) {
    return;
  }
  const saved = await hasSavedAuth(sessionId);
  logger.info(
    { sessionId, saved },
    saved
      ? "Restoring saved WhatsApp session from database"
      : "No saved session in database — starting so a QR can be generated",
  );
  await startSession(sessionId);
}

// Another instance now owns the login and has loaded it from the database, so drop
// ours without saving and wait to take over again (e.g. if that instance goes away).
onLeaseLost((sessionId) => {
  const runtime = getState(sessionId);
  cancelReconnect(runtime);
  const socket = runtime.socket;
  runtime.socket = null;
  runtime.status = "disconnected";
  runtime.qrDataUrl = null;
  runtime.pairingCode = null;
  runtime.pairingIssuedAt = null;
  stopAntibanPersistSync(sessionId);
  socket?.end(undefined);
  void discardAuthState(sessionId);

  if (runtime.stoppedByUser || shuttingDown) {
    return;
  }
  void restoreSession(sessionId).catch((error) => {
    logger.error({ sessionId, err: error }, "Failed to take the WhatsApp session back");
  });
});

/** Save everything and hand the login over, so the next instance starts from current keys. */
export async function shutdownSessions(): Promise<void> {
  shuttingDown = true;
  for (const [sessionId, runtime] of sockets) {
    cancelReconnect(runtime);
    stopAntibanPersistSync(sessionId);
    const socket = runtime.socket;
    runtime.socket = null;
    socket?.end(undefined);

    if (!holdsLease(sessionId)) {
      continue;
    }
    try {
      await closeAuthState(sessionId);
      await flushAntibanPersist(sessionId);
      if (runtime.status !== "disconnected") {
        await updateSessionMeta(sessionId, { status: "disconnected", qr_data_url: null });
      }
    } catch (error) {
      logger.error({ sessionId, err: error }, "Failed to save WhatsApp session during shutdown");
    }
    runtime.status = "disconnected";
  }
  await releaseAllLeases();
}
