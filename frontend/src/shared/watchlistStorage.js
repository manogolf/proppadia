export const WATCHLIST_SCOPE_MLB = "/api/props/history";
export const WATCHLIST_SCOPE_NHL = "/api/nhl/props/history";
export const WATCHLIST_SCOPES = [WATCHLIST_SCOPE_MLB, WATCHLIST_SCOPE_NHL];
export const WATCHLIST_UPDATED_EVENT = "proppadia:watchlist-updated";
export const WATCHLIST_MAX_ROWS = 100;

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
      JSON.stringify(Array.isArray(rows) ? rows.slice(0, WATCHLIST_MAX_ROWS) : [])
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

export function normalizeWatchlistRows(rows) {
  if (!Array.isArray(rows)) return [];
  const dedup = new Map();
  for (const row of rows) {
    const id = toWatchlistId(row);
    if (!id) continue;
    const candidate = {
      id: String(id),
      player_id:
        row?.player_id !== undefined && row?.player_id !== null && String(row.player_id).trim() !== ""
          ? row.player_id
          : null,
      player_name: row?.player_name ? String(row.player_name) : null,
      team: row?.team ? String(row.team) : row?.team_abbr ? String(row.team_abbr) : null,
      added_at:
        row?.added_at && !Number.isNaN(new Date(row.added_at).getTime())
          ? String(row.added_at)
          : new Date().toISOString(),
    };
    const existing = dedup.get(candidate.id);
    if (!existing) {
      dedup.set(candidate.id, candidate);
      continue;
    }
    const existingTs = new Date(existing.added_at || 0).getTime();
    const candidateTs = new Date(candidate.added_at || 0).getTime();
    if (candidateTs > existingTs) dedup.set(candidate.id, candidate);
  }
  return Array.from(dedup.values())
    .sort((a, b) => new Date(b.added_at || 0).getTime() - new Date(a.added_at || 0).getTime())
    .slice(0, WATCHLIST_MAX_ROWS);
}

export function encodeWatchlistPlayerQuery(row) {
  return encodeURIComponent(String(row?.player_name || row?.player_id || "").trim());
}
