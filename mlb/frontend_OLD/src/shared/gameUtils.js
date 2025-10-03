// src/shared/gameUtils.js
export function isGameLive(status) {
  const liveStatuses = ["in progress", "live", "warmup"];
  return (
    typeof status === "string" && liveStatuses.includes(status.toLowerCase())
  );
}
