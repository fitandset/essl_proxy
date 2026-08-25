export type SessionStatus =
  | "disconnected"
  | "qr"
  | "pairing"
  | "connecting"
  | "connected";

export interface AntibanStatusPayload {
  health: {
    risk: string;
    score: number;
    recommendation: string;
    reasons: string[];
  };
  todaySent: number;
  todayLimit: number;
  messagesAllowed: number;
  messagesBlocked: number;
  lastMinute: number;
  lastHour: number;
  lastDay: number;
}

export interface SessionStatusPayload {
  sessionId: string;
  status: SessionStatus;
  qrDataUrl: string | null;
  pairingCode: string | null;
  pairingPhone: string | null;
  phoneNumber: string | null;
  connected: boolean;
  antiban: AntibanStatusPayload | null;
}

export class SessionError extends Error {
  constructor(
    message: string,
    readonly statusCode: number,
  ) {
    super(message);
    this.name = "SessionError";
  }
}
