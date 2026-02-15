export function normalizeHttpErrorMessage(error, fallback = "Request failed.") {
  const raw = String(error?.message || "").trim();
  if (!raw) return fallback;
  const lowered = raw.toLowerCase();
  if (
    lowered === "failed to fetch" ||
    lowered.includes("networkerror") ||
    lowered.includes("load failed")
  ) {
    return "Network error contacting backend. Check deploy status/base URL and retry.";
  }
  return raw;
}

