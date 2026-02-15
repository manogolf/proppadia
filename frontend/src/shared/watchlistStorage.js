export const WATCHLIST_SCOPE_MLB = "/api/props/history";
export const WATCHLIST_SCOPE_NHL = "/api/nhl/props/history";
export const WATCHLIST_SCOPES = [WATCHLIST_SCOPE_MLB, WATCHLIST_SCOPE_NHL];
export const WATCHLIST_UPDATED_EVENT = "proppadia:watchlist-updated";

export function watchlistStorageKey(userId, scopePath) {
  return `proppadia_watchlist_v1:${String(userId || "anon")}:${String(scopePath || "default")}`;
}

export function readWatchlistScope(userId, scopePath) {
  try {
    const raw = window.localStorage.getItem(watchlistStorageKey(userId, scopePath));
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function writeWatchlistScope(userId, scopePath, rows) {
  try {
    window.localStorage.setItem(
      watchlistStorageKey(userId, scopePath),
      JSON.stringify(Array.isArray(rows) ? rows.slice(0, 100) : [])
    );
    window.dispatchEvent(new Event(WATCHLIST_UPDATED_EVENT));
  } catch {
    // ignore local storage write errors
  }
}

export function getWatchlistTotal(userId) {
  if (!userId) return 0;
  let total = 0;
  for (const scope of WATCHLIST_SCOPES) {
    total += readWatchlistScope(userId, scope).length;
  }
  return total;
}

export function toWatchlistId(row) {
  const pid = row?.player_id;
  if (pid !== undefined && pid !== null && String(pid).trim() !== "") return String(pid);
  const name = String(row?.player_name || "").trim().toLowerCase();
  const team = String(row?.team || "").trim().toLowerCase();
  if (!name && !team) return "";
  if (!name) return `team:${team}`;
  if (!team) return `name:${name}`;
  return `${name}:${team}`;
}
