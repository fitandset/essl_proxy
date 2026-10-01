import express from "express";
import type { Server } from "node:http";
import { config } from "./config.js";
import { logger } from "./logger.js";
import { bloodLabBookingsRouter } from "./routes/bloodLabBookings.js";
import { healthRouter } from "./routes/health.js";
import { sessionsRouter } from "./routes/sessions.js";
import { restoreSession, shutdownSessions } from "./baileys/sessionManager.js";

const RESTORE_RETRY_MS = 15_000;
const SHUTDOWN_TIMEOUT_MS = 10_000;

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

let server: Server | null = null;
let shuttingDown = false;

async function shutdown(reason: string, exitCode: number): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  logger.warn({ reason }, "Shutting down — saving the WhatsApp session first");

  setTimeout(() => process.exit(exitCode), SHUTDOWN_TIMEOUT_MS).unref();
  server?.close();
  try {
    await shutdownSessions();
  } catch (err) {
    logger.error({ err }, "Failed to save the WhatsApp session during shutdown");
  }
  process.exit(exitCode);
}

// Baileys rejects promises from background work (message retries, receipts). On Node 22
// an unhandled rejection kills the process and loses unsaved encryption keys.
process.on("unhandledRejection", (reason) => {
  logger.error({ err: reason }, "Unhandled promise rejection");
});

process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "Uncaught exception");
  void shutdown("uncaughtException", 1);
});

process.on("SIGTERM", () => void shutdown("SIGTERM", 0));
process.on("SIGINT", () => void shutdown("SIGINT", 0));

async function restoreInBackground(): Promise<void> {
  while (!shuttingDown) {
    try {
      await restoreSession();
      return;
    } catch (err) {
      logger.error({ err }, "Could not restore the WhatsApp session — retrying");
      await new Promise((resolve) => setTimeout(resolve, RESTORE_RETRY_MS));
    }
  }
}

function bootstrap(): void {
  if (!config.supabaseUrl || !config.supabaseServiceRoleKey) {
    logger.fatal(
      "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY so WhatsApp login can be saved in whatsapp_baileys_sessions.",
    );
    process.exit(1);
  }

  // Listen before restoring: during a deploy the restore waits for the old instance
  // to hand over the login, and that only happens once this one passes health checks.
  server = app.listen(config.port, () => {
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

  void restoreInBackground();
}

bootstrap();
