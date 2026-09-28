-- Run manually after BLOOD_BOOKING_WEBHOOK_SECRET is set on the WhatsApp service.
-- This does not change enquiry or any other trigger.
-- blood_lab_bookings_set_updated_at stays as it is. This trigger is AFTER INSERT only.
--
-- 1. Create the vault secret once. Use the same value as BLOOD_BOOKING_WEBHOOK_SECRET.
--    Skip this if blood_booking_webhook_secret already exists.
--
-- SELECT vault.create_secret(
--   'REPLACE_WITH_WEBHOOK_SECRET',
--   'blood_booking_webhook_secret',
--   'Header secret for blood lab booking WhatsApp alerts'
-- );

CREATE OR REPLACE FUNCTION public.notify_blood_lab_booking_insert()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions'
AS $$
DECLARE
  webhook_url text := 'https://essl.fitandset.com/wa/webhooks/blood-lab-bookings';
  webhook_secret text;
  request_id bigint;
BEGIN
  SELECT decrypted_secret
  INTO webhook_secret
  FROM vault.decrypted_secrets
  WHERE name = 'blood_booking_webhook_secret'
  LIMIT 1;

  IF webhook_secret IS NULL THEN
    RAISE WARNING 'Vault secret blood_booking_webhook_secret not found; blood lab WhatsApp alert skipped';
    RETURN NEW;
  END IF;

  BEGIN
    SELECT net.http_post(
      url := webhook_url,
      headers := jsonb_build_object(
        'Content-Type', 'application/json',
        'x-webhook-secret', webhook_secret
      ),
      body := jsonb_build_object(
        'type', 'INSERT',
        'table', 'blood_lab_bookings',
        'schema', 'public',
        'record', to_jsonb(NEW)
      )
    ) INTO request_id;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE WARNING 'blood lab WhatsApp webhook queue failed: %', SQLERRM;
  END;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_blood_lab_booking_whatsapp ON public.blood_lab_bookings;

CREATE TRIGGER trg_blood_lab_booking_whatsapp
AFTER INSERT ON public.blood_lab_bookings
FOR EACH ROW
EXECUTE FUNCTION public.notify_blood_lab_booking_insert();
