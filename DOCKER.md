# Local Docker (ESSL + WhatsApp)

Test **on your PC only**. Do not deploy this to the live `essl_proxy` Render service until it works here.

## What you get

| URL | App |
|-----|-----|
| http://localhost:8080/iclock/... | ESSL (unchanged paths) |
| http://localhost:8080/wa/health | WhatsApp health |
| http://localhost:8080/wa/sessions/qr | WhatsApp QR |

## Run

1. Docker Desktop is running.
2. Stop `npm run dev` in bailey_whatsapp (one WhatsApp session at a time).
3. From this folder (`essl_proxy`):

```bash
docker compose up --build
```

4. Open the QR page above and link the number.

`whatsapp/.env` must exist (Supabase service role + session settings). Compose sets `PORT=8080` so nginx listens on 8080; gunicorn uses `ESSL_PORT=10001` and WhatsApp uses `WA_PORT=3001`.

## Render (live) — do not set PORT yourself

Leave Render’s `PORT` as-is (often `10000`). Nginx binds that. Set these on the service:

| Variable | Value |
|----------|--------|
| `PORT` | leave Render’s value |
| `ESSL_PORT` | `10001` |
| `WA_PORT` | `3001` |
| `BASE_PATH` | `/wa` |

`start.sh` also refuses to let gunicorn/Node bind the same port as nginx if someone forgets `ESSL_PORT`.

If WhatsApp Node exits, `start.sh` restarts **only Node** (5s delay). Gunicorn/ESSL keeps running. A full Render restart still recycles the whole container.

## Enquiry notify later

Call `https://YOUR-HOST/wa/sessions/send` (not `/sessions/send` on the ESSL root).
