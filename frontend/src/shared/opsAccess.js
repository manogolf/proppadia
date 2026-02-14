function parseCsvEnv(name) {
  const raw = String(import.meta.env?.[name] || "")
    .split(",")
    .map((v) => v.trim().toLowerCase())
    .filter(Boolean);
  return new Set(raw);
}

const ALLOWED_EMAILS = parseCsvEnv("VITE_OPS_ALLOWED_EMAILS");
const ALLOWED_USER_IDS = parseCsvEnv("VITE_OPS_ALLOWED_USER_IDS");

export function isOpsUser(user) {
  if (!user) return false;
  const email = String(user.email || "").trim().toLowerCase();
  const userId = String(user.id || "").trim().toLowerCase();
  if (email && ALLOWED_EMAILS.has(email)) return true;
  if (userId && ALLOWED_USER_IDS.has(userId)) return true;
  return false;
}
