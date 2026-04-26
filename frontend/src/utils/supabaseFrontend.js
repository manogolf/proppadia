// src/utils/supabaseFrontend.js
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL;

const SUPABASE_KEY =
  import.meta.env.VITE_SUPABASE_ANON_KEY ||
  import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY;

function parseBool(raw, fallback) {
  if (raw == null || raw === "") return fallback;
  const v = String(raw).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(v)) return true;
  if (["0", "false", "no", "off"].includes(v)) return false;
  return fallback;
}

function parseCsv(raw, fallback = []) {
  const src = String(raw || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  return src.length ? src : fallback;
}

const configuredRedirect = String(import.meta.env.VITE_AUTH_REDIRECT_TO || "").trim();
const redirectTo =
  configuredRedirect ||
  (typeof window !== "undefined" ? `${window.location.origin}/` : "");

export const AUTH_CONFIG = Object.freeze({
  redirectTo,
  enablePasswordLogin: parseBool(import.meta.env.VITE_AUTH_ENABLE_PASSWORD_LOGIN, true),
  enableMagicLink: parseBool(import.meta.env.VITE_AUTH_ENABLE_MAGIC_LINK, true),
  // Safer default for controlled membership: do not auto-create users via magic link.
  magicLinkCreateUser: parseBool(import.meta.env.VITE_AUTH_MAGIC_CREATE_USER, false),
  oauthProviders: parseCsv(import.meta.env.VITE_AUTH_OAUTH_PROVIDERS, ["google"]),
});

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.warn(
    "[supabase] missing env: VITE_SUPABASE_URL and VITE_SUPABASE_{ANON_KEY|PUBLISHABLE_KEY|PERISHABLE_KEY}"
  );
}

if (String(import.meta.env.VITE_SUPABASE_SERVICE_ROLE_KEY || "").trim()) {
  console.warn(
    "[supabase] VITE_SUPABASE_SERVICE_ROLE_KEY is set in frontend env. Remove it from frontend env and rotate if exposed."
  );
}

export const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
  },
});
