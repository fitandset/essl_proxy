import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
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
import { clearAuth, hasSavedAuth, useDatabaseAuthState } from "./authStore.js";
import { updateSessionMeta } from "./sessionRepository.js";

const PAIR_CODE_TTL_MS = 90_000;
const QR_TIMEOUT_MS = 180_000;

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
}

const sockets = new Map<string, SocketState>();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

async function createSocket(sessionId: string): Promise<WASocket> {
  const { state, saveCreds } = await useDatabaseAuthState(sessionId);
  const { version } = await fetchLatestBaileysVersion();
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
    markOnlineOnConnect: false,
    generateHighQualityLinkPreview: true,
  });

  const socket = await wrapWithAntiban(rawSocket, sessionId);
  runtime.socket = socket;

  socket.ev.on("creds.update", saveCreds);

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
      const pairingJustSucceeded = runtime.pairingSucceeded;
      const wasUnregistered =
        !pairingJustSucceeded &&
        (runtime.status === "qr" ||
          runtime.status === "pairing" ||
          runtime.status === "connecting");

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
          pairingJustSucceeded,
          wasUnregistered,
        },
        "WhatsApp session closed",
      );

      if (runtime.stoppedByUser) {
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
        setTimeout(() => {
          void startSession(sessionId, { isReconnect: true }).catch((error) => {
            logger.error({ sessionId, error }, "Failed to restart WhatsApp session");
          });
        }, 500);
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

      if (runtime.reconnectAttempts < 5) {
        runtime.reconnectAttempts += 1;
        const delayMs = runtime.reconnectAttempts * 5000;
        setTimeout(() => {
          void startSession(sessionId, { isReconnect: true }).catch((error) => {
            logger.error({ sessionId, error }, "Failed to reconnect WhatsApp session");
          });
        }, delayMs);
      }
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
  stopAntibanPersistSync(sessionId);
  await flushAntibanPersist(sessionId);

  if (runtime.socket) {
    runtime.socket.end(undefined);
    runtime.socket = null;
  }

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
  const saved = await hasSavedAuth(sessionId);
  logger.info(
    { sessionId, saved },
    saved
      ? "Restoring saved WhatsApp session from database"
      : "No saved session in database — starting so a QR can be generated",
  );
  await startSession(sessionId);
}
