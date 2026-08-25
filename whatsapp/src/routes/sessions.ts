import { Router, type Request, type Response, type NextFunction } from "express";
import { config } from "../config.js";
import {
  getSessionStatus,
  logoutSession,
  sendTextMessage,
  startPairing,
  startSession,
  stopSession,
} from "../baileys/sessionManager.js";
import { SessionError } from "../types.js";

function requireAdmin(req: Request, res: Response, next: NextFunction): void {
  if (!config.adminApiKey) {
    next();
    return;
  }

  const apiKey = req.header("x-api-key");
  if (apiKey !== config.adminApiKey) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  next();
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function wantsHtml(req: Request): boolean {
  return req.accepts("html", "json") === "html" && req.query.json === undefined;
}

function base(): string {
  return config.basePath;
}

function qrPageHtml(): string {
  const status = getSessionStatus();
  const qrImg = status.qrDataUrl
    ? `<img alt="WhatsApp QR code" src="${status.qrDataUrl}" width="320" height="320" />`
    : `<p>No QR yet. Wait a few seconds, or POST ${base()}/sessions/start, then refresh.</p>`;

  const headline =
    status.status === "connected"
      ? `Connected as ${escapeHtml(status.phoneNumber ?? "unknown")}`
      : "Scan this QR in WhatsApp → Linked Devices";

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="3" />
    <title>WhatsApp QR</title>
    <style>
      body { font-family: sans-serif; max-width: 40rem; margin: 2rem auto; }
      img { display: block; }
    </style>
  </head>
  <body>
    <h1>${headline}</h1>
    <p>Status: <strong>${escapeHtml(status.status)}</strong></p>
    ${status.status === "connected" ? "" : qrImg}
    <p><a href="${base()}/sessions/pair">Use a pairing code instead</a> · <a href="${base()}/sessions/send">Send a message</a></p>
    <p>This page refreshes every 3 seconds. If the QR expires, start the session again.</p>
  </body>
</html>`;
}

function pairPageHtml(): string {
  const status = getSessionStatus();
  const connected = status.status === "connected";
  const hasCode = Boolean(status.pairingCode);
  const code = status.pairingCode
    ? `<p style="font-size:2.6rem;letter-spacing:0.35rem;font-weight:700">${escapeHtml(status.pairingCode)}</p>
       <p>Type these <strong>8 characters</strong> in WhatsApp. No dash, no space.</p>
       <p>Number: <strong>${escapeHtml(status.pairingPhone ?? "")}</strong></p>`
    : "<p>Enter the WhatsApp number (with country code) and generate a code.</p>";

  const headline = connected
    ? `Connected as ${escapeHtml(status.phoneNumber ?? "unknown")}`
    : "Link with a pairing code";

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    ${connected || hasCode ? '<meta http-equiv="refresh" content="3" />' : ""}
    <title>WhatsApp pairing code</title>
    <style>
      body { font-family: sans-serif; max-width: 40rem; margin: 2rem auto; line-height: 1.5; }
      label, input, button { font: inherit; }
      input { padding: 0.4rem 0.6rem; width: 16rem; }
      button { padding: 0.4rem 0.8rem; }
      button:disabled { opacity: 0.6; }
      .error { color: #b00020; }
    </style>
  </head>
  <body>
    <h1>${headline}</h1>
    <p>Status: <strong>${escapeHtml(status.status)}</strong></p>
    ${connected ? "" : code}
    ${
      connected
        ? `<p>To try pairing again, logout first (this unlinks the device).</p>
           <button type="button" id="logout">Logout</button>`
        : `<ol>
             <li>${hasCode ? "Keep this page open." : "Generate a code below."}</li>
             <li>Open WhatsApp → <strong>Linked Devices</strong> → <strong>Link a device</strong> → <strong>Link with phone number instead</strong>.</li>
             <li>Type the 8 characters exactly as shown. Enter them within about a minute.</li>
           </ol>
           <p>
             <label>Phone <input id="phone" inputmode="numeric" placeholder="9198xxxxxxxx" value="${escapeHtml(status.pairingPhone ?? "")}" ${hasCode ? "readonly" : ""} /></label>
             <button type="button" id="pair">${hasCode ? "Get a new code" : "Get code"}</button>
           </p>
           <p class="error" id="error"></p>`
    }
    <p><a href="${base()}/sessions/qr">Use a QR code instead</a> · <a href="${base()}/sessions/send">Send a message</a></p>
    <script>
      const errorEl = document.getElementById("error");
      const pairBtn = document.getElementById("pair");
      const logoutBtn = document.getElementById("logout");
      const hasCode = ${hasCode ? "true" : "false"};
      const apiBase = ${JSON.stringify(base())};
      if (pairBtn) {
        pairBtn.addEventListener("click", async () => {
          if (pairBtn.disabled) return;
          pairBtn.disabled = true;
          pairBtn.textContent = "Working…";
          errorEl.textContent = "";
          const phone = document.getElementById("phone").value;
          const res = await fetch(apiBase + "/sessions/pair", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ phone, force: hasCode }),
          });
          const data = await res.json();
          if (!res.ok) {
            errorEl.textContent = data.error || "Failed to create pairing code";
            pairBtn.disabled = false;
            pairBtn.textContent = hasCode ? "Get a new code" : "Get code";
            return;
          }
          location.reload();
        });
      }
      if (logoutBtn) {
        logoutBtn.addEventListener("click", async () => {
          logoutBtn.disabled = true;
          await fetch(apiBase + "/sessions/logout", { method: "POST" });
          location.reload();
        });
      }
    </script>
  </body>
</html>`;
}

function sendPageHtml(): string {
  const status = getSessionStatus();
  const connected = status.status === "connected";
  const antiban = status.antiban;
  const quota =
    connected && antiban
      ? `<p>Today: <strong>${antiban.todaySent} / ${antiban.todayLimit}</strong> · Health: <strong>${escapeHtml(antiban.health.risk)}</strong></p>
         <p>Sends wait a few seconds (jitter + typing). If the daily cap is hit, you get HTTP 429.</p>`
      : "";

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Send WhatsApp message</title>
    <style>
      body { font-family: sans-serif; max-width: 40rem; margin: 2rem auto; line-height: 1.5; }
      label, input, textarea, button { font: inherit; display: block; margin: 0.4rem 0; }
      input, textarea { width: 100%; max-width: 24rem; padding: 0.4rem 0.6rem; }
      textarea { min-height: 6rem; }
      button { padding: 0.4rem 0.8rem; }
      button:disabled { opacity: 0.6; }
      .error { color: #b00020; }
      .ok { color: #0a7a28; }
    </style>
  </head>
  <body>
    <h1>Send a message</h1>
    <p>Status: <strong>${escapeHtml(status.status)}</strong>
      ${connected ? `as ${escapeHtml(status.phoneNumber ?? "unknown")}` : ""}</p>
    ${quota}
    ${
      connected
        ? `<p>Sends from the linked WhatsApp to another number. Use country code, e.g. 9198xxxxxxxx.</p>
           <p>
             <label>To <input id="phone" inputmode="numeric" placeholder="9198xxxxxxxx" /></label>
             <label>Message <textarea id="text" placeholder="Hello"></textarea></label>
             <button type="button" id="send">Send</button>
           </p>
           <p class="ok" id="ok"></p>
           <p class="error" id="error"></p>`
        : `<p>WhatsApp is not connected. <a href="${base()}/sessions/qr">Scan QR</a> or <a href="${base()}/sessions/pair">use a pairing code</a> first.</p>`
    }
    <p><a href="${base()}/sessions/status">Status</a></p>
    <script>
      const sendBtn = document.getElementById("send");
      const errorEl = document.getElementById("error");
      const okEl = document.getElementById("ok");
      const apiBase = ${JSON.stringify(base())};
      if (sendBtn) {
        sendBtn.addEventListener("click", async () => {
          sendBtn.disabled = true;
          errorEl.textContent = "";
          okEl.textContent = "";
          const res = await fetch(apiBase + "/sessions/send", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              phone: document.getElementById("phone").value,
              text: document.getElementById("text").value,
            }),
          });
          const data = await res.json();
          if (!res.ok) {
            errorEl.textContent = data.error || "Failed to send";
            sendBtn.disabled = false;
            return;
          }
          okEl.textContent = "Sent to " + data.phone;
          sendBtn.disabled = false;
        });
      }
    </script>
  </body>
</html>`;
}

export const sessionsRouter = Router();

sessionsRouter.use(requireAdmin);

sessionsRouter.get("/status", (_req, res) => {
  res.json(getSessionStatus());
});

sessionsRouter.get("/qr", (req, res) => {
  const status = getSessionStatus();

  if (wantsHtml(req)) {
    res.type("html").send(qrPageHtml());
    return;
  }

  res.json({
    sessionId: status.sessionId,
    status: status.status,
    qrDataUrl: status.qrDataUrl,
  });
});

sessionsRouter.get("/pair", (req, res) => {
  if (wantsHtml(req)) {
    res.type("html").send(pairPageHtml());
    return;
  }

  const status = getSessionStatus();
  res.json({
    sessionId: status.sessionId,
    status: status.status,
    pairingCode: status.pairingCode,
    pairingPhone: status.pairingPhone,
    connected: status.connected,
  });
});

sessionsRouter.post("/start", async (_req, res, next) => {
  try {
    await startSession();
    res.json(getSessionStatus());
  } catch (error) {
    next(error);
  }
});

sessionsRouter.post("/pair", async (req, res, next) => {
  try {
    const phone = req.body?.phone;
    if (typeof phone !== "string" || !phone.trim()) {
      res.status(400).json({
        error: 'Send JSON { "phone": "9198xxxxxxxx" } with country code.',
      });
      return;
    }

    const pairingCode = await startPairing(phone, undefined, {
      force: Boolean(req.body?.force),
    });
    res.json({ ...getSessionStatus(), pairingCode });
  } catch (error) {
    if (error instanceof SessionError) {
      res.status(error.statusCode).json({ error: error.message });
      return;
    }
    next(error);
  }
});

sessionsRouter.get("/send", (req, res) => {
  if (wantsHtml(req)) {
    res.type("html").send(sendPageHtml());
    return;
  }

  res.json({
    hint: 'POST { "phone": "9198xxxxxxxx", "text": "Hello" }',
    connected: getSessionStatus().connected,
  });
});

sessionsRouter.post("/send", async (req, res, next) => {
  try {
    const phone = req.body?.phone;
    const text = req.body?.text;
    if (typeof phone !== "string" || !phone.trim()) {
      res.status(400).json({
        error: 'Send JSON { "phone": "9198xxxxxxxx", "text": "Hello" }.',
      });
      return;
    }
    if (typeof text !== "string") {
      res.status(400).json({ error: "text must be a string" });
      return;
    }

    const result = await sendTextMessage(phone, text);
    res.json({ ok: true, ...result });
  } catch (error) {
    if (error instanceof SessionError) {
      res.status(error.statusCode).json({ error: error.message });
      return;
    }
    next(error);
  }
});

sessionsRouter.post("/stop", async (_req, res, next) => {
  try {
    await stopSession();
    res.json({ sessionId: config.defaultSessionId, status: "disconnected" });
  } catch (error) {
    next(error);
  }
});

sessionsRouter.post("/logout", async (_req, res, next) => {
  try {
    await logoutSession();
    res.json({ sessionId: config.defaultSessionId, status: "disconnected" });
  } catch (error) {
    next(error);
  }
});
