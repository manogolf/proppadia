// /shared/getBaseURL.js

export function getBaseURL() {
  const isLocal =
    typeof window !== "undefined" && window.location.hostname === "localhost";

  return isLocal
    ? "http://localhost:8001" // 🛠 Local FastAPI server
    : "https://baseball-streaks-sq44.onrender.com"; // ✅ Correct Express API
}
