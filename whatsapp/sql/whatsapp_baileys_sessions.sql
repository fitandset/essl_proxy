-- Run this on DEV first, then PROD.
-- Stores Baileys WhatsApp login (creds + keys) and antiban state.
-- Gym rows can still use gym_id; the Hale enquiry number uses session_id = 'default'.

CREATE TABLE IF NOT EXISTS public.whatsapp_baileys_sessions (
  session_id text PRIMARY KEY,
  gym_id uuid,
  status text NOT NULL DEFAULT 'disconnected',
  creds jsonb,
  keys jsonb,
  antiban_state jsonb,
  phone_number text,
  qr_data_url text,
  connected_at timestamptz,
  paused boolean NOT NULL DEFAULT false,
  daily_cap integer,
  lease_owner text,
  lease_expires_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT whatsapp_baileys_sessions_status_check
    CHECK (status = ANY (ARRAY[
      'disconnected'::text,
      'qr'::text,
      'pairing'::text,
      'connecting'::text,
      'connected'::text
    ]))
);

ALTER TABLE public.whatsapp_baileys_sessions
  ADD COLUMN IF NOT EXISTS session_id text;

ALTER TABLE public.whatsapp_baileys_sessions
  ADD COLUMN IF NOT EXISTS antiban_state jsonb;

-- Only one server instance may hold a login at a time (deploys briefly run two).
-- Two instances on the same keys corrupt each other's encryption sessions.
ALTER TABLE public.whatsapp_baileys_sessions
  ADD COLUMN IF NOT EXISTS lease_owner text;

ALTER TABLE public.whatsapp_baileys_sessions
  ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;

UPDATE public.whatsapp_baileys_sessions
SET session_id = gym_id::text
WHERE session_id IS NULL AND gym_id IS NOT NULL;

-- gym_id cannot be nullable while it is still the primary key
ALTER TABLE public.whatsapp_baileys_sessions
  DROP CONSTRAINT IF EXISTS whatsapp_baileys_sessions_pkey;

ALTER TABLE public.whatsapp_baileys_sessions
  ALTER COLUMN gym_id DROP NOT NULL;

DELETE FROM public.whatsapp_baileys_sessions
WHERE session_id IS NULL;

ALTER TABLE public.whatsapp_baileys_sessions
  ALTER COLUMN session_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.whatsapp_baileys_sessions'::regclass
      AND conname = 'whatsapp_baileys_sessions_pkey'
  ) THEN
    ALTER TABLE public.whatsapp_baileys_sessions
      ADD CONSTRAINT whatsapp_baileys_sessions_pkey PRIMARY KEY (session_id);
  END IF;
END $$;

ALTER TABLE public.whatsapp_baileys_sessions
  DROP CONSTRAINT IF EXISTS whatsapp_baileys_sessions_status_check;

ALTER TABLE public.whatsapp_baileys_sessions
  ADD CONSTRAINT whatsapp_baileys_sessions_status_check
  CHECK (status = ANY (ARRAY[
    'disconnected'::text,
    'qr'::text,
    'pairing'::text,
    'connecting'::text,
    'connected'::text
  ]));

CREATE UNIQUE INDEX IF NOT EXISTS whatsapp_baileys_sessions_gym_id_key
  ON public.whatsapp_baileys_sessions (gym_id)
  WHERE gym_id IS NOT NULL;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'gyms'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.whatsapp_baileys_sessions'::regclass
      AND conname = 'whatsapp_baileys_sessions_gym_id_fkey'
  ) THEN
    ALTER TABLE public.whatsapp_baileys_sessions
      ADD CONSTRAINT whatsapp_baileys_sessions_gym_id_fkey
      FOREIGN KEY (gym_id) REFERENCES public.gyms(id) ON DELETE CASCADE;
  END IF;
END $$;

CREATE OR REPLACE FUNCTION public.whatsapp_baileys_sessions_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_whatsapp_baileys_sessions_updated_at
  ON public.whatsapp_baileys_sessions;

CREATE TRIGGER trg_whatsapp_baileys_sessions_updated_at
  BEFORE UPDATE ON public.whatsapp_baileys_sessions
  FOR EACH ROW
  EXECUTE FUNCTION public.whatsapp_baileys_sessions_set_updated_at();

ALTER TABLE public.whatsapp_baileys_sessions ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.whatsapp_baileys_sessions FROM anon, authenticated;
GRANT ALL ON TABLE public.whatsapp_baileys_sessions TO service_role;

INSERT INTO public.whatsapp_baileys_sessions (session_id, status, paused, daily_cap)
VALUES ('default', 'disconnected', false, 30)
ON CONFLICT (session_id) DO UPDATE
SET daily_cap = COALESCE(public.whatsapp_baileys_sessions.daily_cap, 30);
