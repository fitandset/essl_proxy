# Bailey WhatsApp

Minimal WhatsApp connection service using [WhiskeySockets/Baileys](https://github.com/WhiskeySockets/Baileys).

Links **one WhatsApp number** (session id `default`) with a QR code **or** an 8-digit pairing code, then can send a text message from that number.

Login is stored in the Supabase table `whatsapp_baileys_sessions` (row `session_id = default` unless you set `SESSION_ID`). Run `sql/whatsapp_baileys_sessions.sql` on your database before starting the server.

## Local development

```bash
cp .env.example .env
# set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY, then run sql/whatsapp_baileys_sessions.sql
npm install
npm run dev
```

The server starts a session immediately. Use the port in your `.env` (`PORT`, default 3000).

### QR

1. Open `/sessions/qr` in a browser.
2. On the phone: WhatsApp → **Linked Devices** → **Link a device** → scan the QR.
3. The page should change to **Connected**.

### Pairing code

1. Open `/sessions/pair` in a browser.
2. Enter the number **with country code** (digits only), e.g. `9198xxxxxxxx`.
3. Click **Get code**.
4. On the phone: WhatsApp → **Linked Devices** → **Link a device** → **Link with phone number instead** → type the **8 characters** (no dash, no space).

Click **Get code** once. Click **Get a new code** only if WhatsApp rejects it or it expired. You still have to enter the code **in WhatsApp**. This page only shows it.

If you are already connected, click **Logout** on that page first (or `POST /sessions/logout`). Pairing only works when the session is not logged in.

Use a dedicated WhatsApp number if you can, not your personal daily chat number.

### Send a message

Session must be **connected**. Open `/sessions/send` in a browser, or:

```bash
curl -X POST http://localhost:3000/sessions/send -H "Content-Type: application/json" -d "{\"phone\":\"9198xxxxxxxx\",\"text\":\"Hello\"}"
```

`phone` is the **recipient** (country code + number). The message is sent from the linked WhatsApp.

Sends go through [baileys-antiban](https://github.com/kobie3717/baileys-antiban): jittered delays, typing indicator, identical-message guard, health auto-pause, and a **daily cap of 30** (saved in `whatsapp_baileys_sessions.antiban_state`). Contact-graph handshake, reply-ratio blocking, and typo injection are off so cold invites can go out.

This pacing makes traffic look less bot-like. It does **not** make unofficial Baileys use allowed by WhatsApp.

If the cap or health pause blocks a send, `POST /sessions/send` returns **HTTP 429** with the reason. `GET /sessions/status` includes an `antiban` object (today's count, health) while connected.

## API

All session routes require `x-api-key: <ADMIN_API_KEY>` when `ADMIN_API_KEY` is set.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/sessions/start` | Start or restart the session (shows QR if not logged in) |
| GET | `/sessions/qr` | QR page in a browser, or JSON (`?json=1`) |
| POST | `/sessions/pair` | Body `{ "phone": "9198xxxxxxxx", "force": false }` — 8-character pairing code. Repeat calls reuse the same code for ~90s unless `force` is true. |
| GET | `/sessions/pair` | Pairing page in a browser, or JSON (`?json=1`) |
| GET | `/sessions/status` | Connection status plus `antiban` (today's count, health) while connected |
| GET | `/sessions/send` | Send-message page in a browser |
| POST | `/sessions/send` | Body `{ "phone": "9198xxxxxxxx", "text": "Hello" }` — send from the linked number |
| POST | `/sessions/stop` | Disconnect (keeps saved login) |
| POST | `/sessions/logout` | Unlink and delete saved login |

## Environment variables

See [`.env.example`](.env.example).
