//  src/lib/api.js

export async function api(path, init) {
  const res = await fetch(`/api${path}`, { credentials: "include", ...init });
  if (!res.ok)
    throw new Error(`${res.status} ${res.statusText}: ${await res.text()}`);
  return res.json(); // { ok, data }
}

export async function api(path, opts) {
  const res = await fetch(path, { credentials: "include", ...(opts || {}) });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${path}`);
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
