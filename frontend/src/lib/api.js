// src/lib/api.js
import { getBaseURL } from "../shared/getBaseURL.js";

function toUrl(path) {
  if (/^https?:\/\//i.test(path)) return path;
  const base = getBaseURL();
  if (path.startsWith("/api/")) return `${base}${path}`;
  if (path.startsWith("/")) return `${base}/api${path}`;
  return `${base}/api/${path}`;
}

export async function api(path, opts) {
  const url = toUrl(path);
  const res = await fetch(url, { credentials: "include", ...(opts || {}) });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${url}`);
  return res.json();
}

export const playersAPI = {
  resolve: (p) => {
    const u = new URLSearchParams();
    if (p.player_id) u.set("player_id", p.player_id);
    if (p.player_name) u.set("player_name", p.player_name);
    if (p.team_abbr) u.set("team_abbr", p.team_abbr);
    return api(`/api/players/resolve?${u.toString()}`);
  },
  lookup: (player_id) =>
    api(`/api/players/lookup?player_id=${encodeURIComponent(player_id)}`),
  search: (q, limit = 10) =>
    api(`/api/players/search?q=${encodeURIComponent(q)}&limit=${limit}`),
};

export const gamesAPI = {
  context: (team_id, for_date) => {
    const u = new URLSearchParams({ team_id: String(team_id) });
    if (for_date) u.set("for_date", for_date);
    return api(`/api/games/context?${u.toString()}`);
  },
};

