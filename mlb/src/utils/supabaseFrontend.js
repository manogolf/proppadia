// src/utils/supabaseFrontend.js
import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.REACT_APP_SUPABASE_URL;
const supabaseAnonKey = process.env.REACT_APP_SUPABASE_ANON_KEY;

export const supabase = createClient(supabaseUrl, supabaseAnonKey, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
  },
});

// handy for quick console tests in development:
if (process.env.NODE_ENV === "development") {
  window.__supabase = supabase;
}

// dev-only globals so you can poke from DevTools
if (typeof window !== "undefined") {
  window.supabase = supabase;
  try {
    // optional: handy ET helper
    const { todayET } = await import("../shared/timeUtils.js");
    window.todayET = todayET;
  } catch {}
}
