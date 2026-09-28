import express from "express";
import { config } from "./config.js";
import { logger } from "./logger.js";
import { bloodLabBookingsRouter } from "./routes/bloodLabBookings.js";
import { healthRouter } from "./routes/health.js";
import { sessionsRouter } from "./routes/sessions.js";
import { restoreSession } from "./baileys/sessionManager.js";

const app = express();

app.use(express.json());
app.use(`${config.basePath}/health`, healthRouter);
app.use(`${config.basePath}/sessions`, sessionsRouter);
app.use(`${config.basePath}/webhooks/blood-lab-bookings`, bloodLabBookingsRouter);

app.use(
  (
    error: Error,
    _req: express.Request,
    res: express.Response,
    _next: express.NextFunction,
  ) => {
    logger.error({ error: error.message, stack: error.stack }, "Unhandled error");
    res.status(500).json({ error: error.message });
  },
);

async function bootstrap(): Promise<void> {
  if (!config.supabaseUrl || !config.supabaseServiceRoleKey) {
    throw new Error(
      "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY so WhatsApp login can be saved in whatsapp_baileys_sessions.",
    );
  }

  await restoreSession();

  app.listen(config.port, () => {
    logger.info(
      {
        port: config.port,
        qrPage: `http://localhost:${config.port}${config.basePath}/sessions/qr`,
        pairPage: `http://localhost:${config.port}${config.basePath}/sessions/pair`,
        sendPage: `http://localhost:${config.port}${config.basePath}/sessions/send`,
      },
      "Bailey WhatsApp service started",
    );
  });
}

void bootstrap();
