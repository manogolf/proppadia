// frontend/src/Pages/nhl/NHLPredictions.jsx

import { useEffect, useMemo, useState } from "react";
import { todayET } from "../../shared/timeUtils.js";
import SogEvalCard from "../../Components/SogEvalCard.jsx";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

function num(x) {
  const v = Number(x);
  return Number.isFinite(v) ? v : null;
}

function fmtProb(x) {
  const v = num(x);
  if (v == null) return "";
  return `${Math.round(v * 1000) / 10}%`; // 1 decimal percent
}

function bestLineSog(r) {
  const candidates = [
    { line: 0.5, p: num(r.p_over_0_5) },
    { line: 1.5, p: num(r.p_over_1_5) },
    { line: 2.5, p: num(r.p_over_2_5) },
    { line: 3.5, p: num(r.p_over_3_5) },
  ].filter((x) => x.p != null);

  if (candidates.length === 0) return null;
  candidates.sort((a, b) => b.p - a.p);
  return candidates[0];
}

function bestLineSaves(r) {
  const candidates = [
    { line: 24.5, p: num(r.p_over_24_5) },
    { line: 28.5, p: num(r.p_over_28_5) },
  ].filter((x) => x.p != null);

  if (candidates.length === 0) return null;
  candidates.sort((a, b) => b.p - a.p);
  return candidates[0];
}

export default function NHLPredictions() {
  const slateDate = useMemo(() => todayET(), []);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [sogRows, setSogRows] = useState([]);
  const [savesRows, setSavesRows] = useState([]);

  // UI controls
  const [q, setQ] = useState("");
  const [sogSort, setSogSort] = useState("best"); // best | 0.5 | 1.5 | 2.5 | 3.5
  const [savesSort, setSavesSort] = useState("best"); // best | 24.5 | 28.5

  useEffect(() => {
    let cancelled = false;

    async function run() {
      try {
        setError("");
        setLoading(true);

        const [sogRes, savesRes] = await Promise.all([
          fetch(
            `${API_BASE}/api/nhl/sog?date=${encodeURIComponent(
              slateDate
            )}&limit=200&offset=0`
          ),
          fetch(
            `${API_BASE}/api/nhl/saves?date=${encodeURIComponent(
              slateDate
            )}&limit=200&offset=0`
          ),
        ]);

        const sogJson = await sogRes.json();
        const savesJson = await savesRes.json();

        if (!sogRes.ok || sogJson?.ok === false) {
          throw new Error(
            sogJson?.error || `SOG endpoint failed (${sogRes.status})`
          );
        }
        if (!savesRes.ok || savesJson?.ok === false) {
          throw new Error(
            savesJson?.error || `Saves endpoint failed (${savesRes.status})`
          );
        }

        if (cancelled) return;

        setSogRows(Array.isArray(sogJson) ? sogJson : sogJson?.rows || []);
        setSavesRows(
          Array.isArray(savesJson) ? savesJson : savesJson?.rows || []
        );
        setLoading(false);
      } catch (e) {
        if (cancelled) return;
        setError(e?.message || "Failed to load NHL predictions.");
        setLoading(false);
      }
    }

    run();
    return () => {
      cancelled = true;
    };
  }, [slateDate]);

  const query = useMemo(() => q.trim().toLowerCase(), [q]);

  const filteredSog = useMemo(() => {
    if (!query) return sogRows;
    return (sogRows || []).filter((r) => {
      const pid = String(r.player_id ?? "").toLowerCase();
      const gid = String(r.game_id ?? "").toLowerCase();
      return pid.includes(query) || gid.includes(query);
    });
  }, [sogRows, query]);

  const filteredSaves = useMemo(() => {
    if (!query) return savesRows;
    return (savesRows || []).filter((r) => {
      const pid = String(r.player_id ?? "").toLowerCase();
      const gid = String(r.game_id ?? "").toLowerCase();
      return pid.includes(query) || gid.includes(query);
    });
  }, [savesRows, query]);

  const sortedSog = useMemo(() => {
    const arr = [...(filteredSog || [])];

    const getKey = (r) => {
      if (sogSort === "0.5") return num(r.p_over_0_5) ?? -1;
      if (sogSort === "1.5") return num(r.p_over_1_5) ?? -1;
      if (sogSort === "2.5") return num(r.p_over_2_5) ?? -1;
      if (sogSort === "3.5") return num(r.p_over_3_5) ?? -1;

      // best
      const best = bestLineSog(r);
      return best?.p ?? -1;
    };

    arr.sort((a, b) => getKey(b) - getKey(a));
    return arr;
  }, [filteredSog, sogSort]);

  const sortedSaves = useMemo(() => {
    const arr = [...(filteredSaves || [])];

    const getKey = (r) => {
      if (savesSort === "24.5") return num(r.p_over_24_5) ?? -1;
      if (savesSort === "28.5") return num(r.p_over_28_5) ?? -1;

      // best
      const best = bestLineSaves(r);
      return best?.p ?? -1;
    };

    arr.sort((a, b) => getKey(b) - getKey(a));
    return arr;
  }, [filteredSaves, savesSort]);

  return (
    <div className="min-h-screen bg-slate-50">
      <div className="max-w-6xl mx-auto px-4 py-4">
        <div className="flex items-baseline justify-between mb-3">
          <h2 className="text-2xl font-bold text-indigo-900">
            NHL Predictions
          </h2>
          <div className="text-sm text-gray-500">Slate (ET): {slateDate}</div>
        </div>

        {/* Controls */}
        <div className="bg-white shadow rounded-xl p-4 border border-gray-200 mb-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <div>
              <div className="text-xs text-gray-500 mb-1">Search</div>
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Filter by player_id or game_id…"
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm"
              />
            </div>

            <div>
              <div className="text-xs text-gray-500 mb-1">Sort SOG by</div>
              <select
                value={sogSort}
                onChange={(e) => setSogSort(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm"
              >
                <option value="best">Best line</option>
                <option value="0.5">P(over 0.5)</option>
                <option value="1.5">P(over 1.5)</option>
                <option value="2.5">P(over 2.5)</option>
                <option value="3.5">P(over 3.5)</option>
              </select>
            </div>

            <div>
              <div className="text-xs text-gray-500 mb-1">Sort Saves by</div>
              <select
                value={savesSort}
                onChange={(e) => setSavesSort(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm"
              >
                <option value="best">Best line</option>
                <option value="24.5">P(over 24.5)</option>
                <option value="28.5">P(over 28.5)</option>
              </select>
            </div>
          </div>
        </div>

        {loading ? (
          <div className="w-full bg-gray-200 shadow rounded-xl p-6 text-center text-gray-500">
            Loading predictions…
          </div>
        ) : error ? (
          <div className="w-full bg-gray-200 shadow rounded-xl p-6 text-center text-red-600">
            {error}
          </div>
        ) : (
          <div className="space-y-6">
            <SogEvalCard />

            {/* ---------------- SOG TABLE ---------------- */}
            <div className="bg-white shadow rounded-xl p-4 border border-gray-200">
              <div className="flex items-baseline justify-between mb-3">
                <h3 className="text-lg font-semibold text-indigo-900">
                  Shots on Goal (SOG)
                </h3>
                <div className="text-sm text-gray-500">
                  Rows: {sortedSog.length}
                </div>
              </div>

              {sortedSog.length === 0 ? (
                <div className="text-gray-500 text-sm">
                  No SOG predictions found.
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead>
                      <tr className="text-left text-gray-600 border-b">
                        <th className="py-2 pr-3">game_id</th>
                        <th className="py-2 pr-3">team</th>
                        <th className="py-2 pr-3">player</th>
                        <th className="py-2 pr-3">player_id</th>
                        <th className="py-2 pr-3">P(over 1.5)</th>
                        <th className="py-2 pr-3">P(over 2.5)</th>
                        <th className="py-2 pr-3">P(over 3.5)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedSog.map((r) => (
                        <tr
                          key={`${r.game_id}-${r.player_id}`}
                          className="border-b"
                        >
                          <td className="py-2 pr-3 font-mono">{r.game_id}</td>

                          <td className="py-2 pr-3">
                            <span className="font-semibold">
                              {r.team_abbr || ""}
                            </span>
                          </td>

                          <td className="py-2 pr-3">{r.player_name || ""}</td>

                          <td className="py-2 pr-3 font-mono">{r.player_id}</td>

                          <td className="py-2 pr-3">{fmtProb(r.p_over_1_5)}</td>
                          <td className="py-2 pr-3">{fmtProb(r.p_over_2_5)}</td>
                          <td className="py-2 pr-3">{fmtProb(r.p_over_3_5)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
            {/* ---------------- SAVES TABLE ---------------- */}
            <div className="bg-white shadow rounded-xl p-4 border border-gray-200">
              <div className="flex items-baseline justify-between mb-3">
                <h3 className="text-lg font-semibold text-indigo-900">
                  Goalie Saves
                </h3>
                <div className="text-sm text-gray-500">
                  Rows: {sortedSaves.length}
                </div>
              </div>

              {sortedSaves.length === 0 ? (
                <div className="text-gray-500 text-sm">
                  No saves predictions found.
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead>
                      <tr className="text-left text-gray-600 border-b">
                        <th className="py-2 pr-3">game_id</th>
                        <th className="py-2 pr-3">team</th>
                        <th className="py-2 pr-3">player</th>
                        <th className="py-2 pr-3">player_id</th>
                        <th className="py-2 pr-3">P(over 24.5)</th>
                        <th className="py-2 pr-3">P(over 28.5)</th>
                      </tr>{" "}
                    </thead>
                    <tbody>
                      {sortedSaves.map((r) => {
                        const best = bestLineSaves(r);
                        return (
                          <tr
                            key={`${r.game_id}-${r.player_id}`}
                            className="border-b"
                          >
                            <td className="py-2 pr-3 font-mono">{r.game_id}</td>

                            <td className="py-2 pr-3">
                              <span className="font-semibold">
                                {r.team_abbr || ""}
                              </span>
                            </td>

                            <td className="py-2 pr-3">{r.player_name || ""}</td>

                            <td className="py-2 pr-3 font-mono">
                              {r.player_id}
                            </td>

                            <td className="py-2 pr-3">
                              {fmtProb(r.p_over_24_5)}
                            </td>
                            <td className="py-2 pr-3">
                              {fmtProb(r.p_over_28_5)}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
