-- Safe additive columns for eSSL proxy ACK + stale-sent retry.
-- Existing rows keep sent_at NULL so they are NEVER auto-reclaimed.
ALTER TABLE public.device_commands
  ADD COLUMN IF NOT EXISTS sent_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS retry_count integer NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS device_commands_sn_status_sent_at_idx
  ON public.device_commands (device_sn, status, sent_at)
  WHERE status = 'sent' AND sent_at IS NOT NULL;
