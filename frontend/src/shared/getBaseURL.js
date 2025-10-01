// src/shared/getBaseURL.js
export function getBaseURL() {
  const env = (process.env.REACT_APP_API_BASE || "").trim();
  if (env) return env.replace(/\/+$/, "");
  if (
    typeof window !== "undefined" &&
    window.location.hostname === "localhost"
  ) {
    return "http://localhost:8001";
  }
  return "https://baseball-streaks-sq44.onrender.com";
}
