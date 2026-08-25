import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { config } from "./config.js";

let client: SupabaseClient | null = null;

export function getSupabase(): SupabaseClient {
  if (client) {
    return client;
  }

  if (!config.supabaseUrl || !config.supabaseServiceRoleKey) {
    throw new Error(
      "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required to store WhatsApp login in the database.",
    );
  }

  client = createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  return client;
}
