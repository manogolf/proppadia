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

const NHL_PLAYER_PROP_MARKETS = Object.freeze([
  {
    id: "sog",
    label: "Shots on Goal",
    status: "active",
    description: "Live now in the NHL player form.",
  },
  {
    id: "saves",
    label: "Goalie Saves",
    status: "staged",
    description: "Staged for rollout; not yet enabled in player form.",
  },
  {
    id: "points",
    label: "Points",
    status: "staged",
    description: "Staged for rollout; not yet enabled in player form.",
  },
]);

function asRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.rows)) return payload.rows;
  return [];
}

export const nhlPlayerPropsAPI = {
  getMarketAvailability: () => NHL_PLAYER_PROP_MARKETS,
  async listSogRows({ date, limit = 200, offset = 0 } = {}) {
    const params = new URLSearchParams();
    if (date) params.set("date", String(date));
    params.set("limit", String(limit));
    params.set("offset", String(offset));
    const query = params.toString();
    const payload = await api(`/api/nhl/sog${query ? `?${query}` : ""}`);
    return asRows(payload);
  },
  addProp(payload) {
    return api("/api/nhl/props/add", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  },
};
