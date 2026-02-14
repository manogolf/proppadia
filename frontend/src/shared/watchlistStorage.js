export const WATCHLIST_SCOPE_MLB = "/api/props/history";
export const WATCHLIST_SCOPE_NHL = "/api/nhl/props/history";

export function watchlistStorageKey(userId, scopePath) {
  return `proppadia_watchlist_v1:${String(userId || "anon")}:${String(scopePath || "default")}`;
}

export function toWatchlistId(row) {
  const pid = row?.player_id;
  if (pid !== undefined && pid !== null && String(pid).trim() !== "") return String(pid);
  const name = String(row?.player_name || "").trim().toLowerCase();
  const team = String(row?.team || "").trim().toLowerCase();
  return `${name}:${team}`;
}
