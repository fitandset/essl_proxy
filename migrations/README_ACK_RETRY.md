# eSSL proxy — command ACK + safe retry (2026-08-01)

## What changed
- `/iclock/devicecmd` parses `ID=…&Return=…` and sets `completed` (Return=0) or retries/`error`.
- Unacked `sent` rows are re-queued to `pending` after 5 minutes (max 3 retries).
- **Only rows with `sent_at` set are reclaimed** — old historical `sent` rows are never auto-resent.

## Deploy order (important)
1. Run SQL migration **first** (Supabase SQL editor):
   `migrations/20260801_device_commands_sent_at_retry.sql`
2. Deploy this `proxy.py` to Render.
3. Watch logs for `ACK OK → completed` and `reclaimed … → pending`.

If migration is missing, proxy still works (falls back to old `sent` marking; reclaim disabled).

## Env overrides (optional)
- `ESSL_SENT_ACK_TIMEOUT_SEC` (default `300`)
- `ESSL_MAX_COMMAND_RETRIES` (default `3`)

## Verify after deploy
```sql
-- New commands should get sent_at, then completed after ACK
SELECT id, device_sn, status, sent_at, retry_count, created_at
FROM device_commands
ORDER BY id DESC
LIMIT 20;
```
