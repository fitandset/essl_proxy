import { Router, type NextFunction, type Request, type Response } from "express";
import { sendGroupTextMessage } from "../baileys/sessionManager.js";
import { formatBloodBookingAlert, type BloodLabBookingRecord } from "../bloodBookingAlert.js";
import { config } from "../config.js";
import { logger } from "../logger.js";
import { SessionError } from "../types.js";

const sentBookingIds: string[] = [];
const MAX_SENT_IDS = 500;

function requireWebhookSecret(req: Request, res: Response, next: NextFunction): void {
  if (!config.bloodBookingWebhookSecret) {
    res.status(503).json({ error: "BLOOD_BOOKING_WEBHOOK_SECRET is not set" });
    return;
  }

  if (req.header("x-webhook-secret") !== config.bloodBookingWebhookSecret) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  next();
}

function rememberBooking(bookingId: string): void {
  if (sentBookingIds.includes(bookingId)) {
    return;
  }
  sentBookingIds.push(bookingId);
  if (sentBookingIds.length > MAX_SENT_IDS) {
    sentBookingIds.splice(0, sentBookingIds.length - MAX_SENT_IDS);
  }
}

export const bloodLabBookingsRouter = Router();

bloodLabBookingsRouter.use(requireWebhookSecret);

bloodLabBookingsRouter.post("/", async (req, res, next) => {
  try {
    const type = req.body?.type;
    const table = req.body?.table;
    if (type !== "INSERT" || table !== "blood_lab_bookings") {
      res.json({ ok: true, ignored: true });
      return;
    }

    const record = req.body?.record as BloodLabBookingRecord | undefined;
    const bookingId = typeof record?.id === "string" ? record.id.trim() : "";
    if (!record || !bookingId) {
      res.status(400).json({ error: "INSERT payload must include record.id" });
      return;
    }

    if (sentBookingIds.includes(bookingId)) {
      res.json({ ok: true, duplicate: true, bookingId });
      return;
    }

    const text = formatBloodBookingAlert(record);
    const result = await sendGroupTextMessage(config.bloodBookingGroupName, text);
    rememberBooking(bookingId);
    logger.info({ bookingId, groupName: result.groupName }, "Sent blood lab booking alert");
    res.json({ ok: true, bookingId, groupName: result.groupName });
  } catch (error) {
    if (error instanceof SessionError) {
      res.status(error.statusCode).json({ error: error.message });
      return;
    }
    next(error);
  }
});
