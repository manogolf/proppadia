// src/components/PlayerPropFormv2.js
import React, { useState, useEffect, useRef } from "react";

const DEFAULT_API =
  typeof window !== "undefined" &&
  /proppadia\.com$/.test(window.location.hostname)
    ? "https://baseball-streaks-sq44.onrender.com"
    : "http://127.0.0.1:8001";

const BASE_API =
  process.env.REACT_APP_API_BASE ||
  (typeof window !== "undefined" && window.__API_BASE__) ||
  DEFAULT_API;

// ----- simple fetch helpers -----
async function getApi(path, params = {}) {
  const url = new URL(BASE_API + path);
  Object.entries(params).forEach(([k, v]) => {
    if (v != null && v !== "") url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString(), {
    mode: "cors",
    credentials: "omit",
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

async function postApi(path, body) {
  const res = await fetch(BASE_API + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    mode: "cors",
    credentials: "omit",
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

// Optional; not used in the current flow (prepareThenPredict is used instead)
async function requestPrediction({ prop_type, player_id, game_id }) {
  return postApi("/api/predict", {
    prop_type: String(prop_type).toLowerCase().trim(),
    player_id: Number(player_id),
    game_id: Number(game_id),
    features: {}, // backend merges precomputed here
  });
}

// ----- prepare → predict (snake_case + team_abbr uppercased) -----
async function prepareThenPredict({
  player_id, // number|string (required)
  player_name, // string|undefined (optional; passes through)
  team_id, // number|undefined (preferred)
  team_abbr, // string|undefined (fallback; will be uppercased)
  game_date, // "YYYY-MM-DD"
  prop_type, // e.g. "hits"
  prop_value, // number or numeric string
  over_under, // "over" | "under"
}) {
  const prepareBody = {
    player_id: Number(player_id),
    ...(player_name ? { player_name: String(player_name) } : {}),
    game_date,
    prop_type: String(prop_type).toLowerCase().trim(),
    prop_value: Number(prop_value),
    over_under,
  };
  if (team_id != null && team_id !== "") {
    prepareBody.team_id = Number(team_id);
  } else if (team_abbr) {
    prepareBody.team_abbr = String(team_abbr).toUpperCase();
  }

  // 1) prepare
  const prep = await postApi("/api/prepareProp", prepareBody);
  const features = prep.features;

  // 2) predict
  const pred = await postApi("/api/predict", {
    prop_type: prepareBody.prop_type,
    features,
  });

  return {
    features,
    probability: pred.probability, // probability of OVER
    commit_token: pred.commit_token,
    model: pred.model,
  };
}

const PROP_TYPES = [
  "doubles",
  "earned_runs",
  "hits",
  "hits_allowed",
  "hits_runs_rbis",
  "home_runs",
  "outs_recorded",
  "rbis",
  "runs_rbis",
  "runs_scored",
  "singles",
  "stolen_bases",
  "strikeouts_batting",
  "strikeouts_pitching",
  "total_bases",
  "triples",
  "walks",
  "walks_allowed",
];

const prettyProp = (key) => {
  let label = key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  label = label.replace(/\bRbis\b/i, "RBIs").replace(/\bRbi\b/i, "RBI");
  return label;
};

const todayInET = () =>
  new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" });

export default function PlayerPropFormV2() {
  // user inputs
  const [playerName, setPlayerName] = useState("");
  const [teamAbbr, setTeamAbbr] = useState("");
  const [gameDate, setGameDate] = useState(() => todayInET());
  const [propType, setPropType] = useState("hits");
  const [overUnder, setOverUnder] = useState("under");
  const [propValue, setPropValue] = useState("0.5");

  // resolved/flow
  const [playerId, setPlayerId] = useState("");
  const [commitToken, setCommitToken] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [prepPreview, setPrepPreview] = useState(null);

  // ui state
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [resolving, setResolving] = useState(false);

  // resolver stale-guard + team handling
  const lastReqId = useRef(0);
  const [teamTouched, setTeamTouched] = useState(false);
  const [lastResolvedPlayerId, setLastResolvedPlayerId] = useState("");

  // Invalidate stale prediction/token whenever inputs that affect the model change
  useEffect(() => {
    setPrediction(null);
    setCommitToken(null);
    setPrepPreview(null);
    setError("");
  }, [playerId, teamAbbr, gameDate, propType, propValue, overUnder]);

  useEffect(() => {
    console.info("[Props V2] mounted");
  }, []);

  const PROP_OPTIONS = React.useMemo(
    () =>
      PROP_TYPES.map((value) => ({ value, label: prettyProp(value) })).sort(
        (a, b) => a.label.localeCompare(b.label)
      ),
    []
  );

  // ----- name → (player_id) resolver -----
  async function resolvePlayerByNameNow() {
    setError("");
    const name = (playerName || "").trim();
    if (name.length < 2) return;

    setResolving(true);
    const reqId = ++lastReqId.current;
    try {
      const r = await getApi("/api/players/resolve", {
        name,
        date: gameDate,
      });

      if (reqId !== lastReqId.current) return; // stale

      if (r?.player_id) {
        const newId = String(r.player_id);
        if (newId !== lastResolvedPlayerId && !teamTouched) {
          setTeamAbbr(""); // drop stale team if user hasn’t touched it
        }
        setPlayerId(newId);
        setLastResolvedPlayerId(newId);
      } else {
        setPlayerId("");
      }
    } catch {
      setError("Couldn’t resolve player. Check spelling (or add team).");
    } finally {
      if (reqId === lastReqId.current) setResolving(false);
    }
  }

  // Debounce resolver as the user types
  useEffect(() => {
    const name = (playerName || "").trim();
    if (name.length < 3 || playerId) return;
    const t = setTimeout(resolvePlayerByNameNow, 600);
    return () => clearTimeout(t);
  }, [playerName, gameDate, playerId, teamAbbr]);

  // ----- predict flow (fast path with on-demand fallback) -----
  async function handlePredict() {
    setError("");
    setPrediction(null);
    setCommitToken(null);
    setPrepPreview(null);

    // validation (player id OR name+team)
    if (!playerId && (!playerName.trim() || !teamAbbr.trim())) {
      setError("Enter player name + team, or resolve to get an ID.");
      return;
    }
    if (!gameDate) return setError("Pick a game date (YYYY-MM-DD).");
    if (!propType) return setError("Pick a prop type.");
    if (propValue === "") return setError("Enter a value.");

    // require resolved player_id
    if (!playerId) return setError("Resolve a player first to get player_id.");

    setLoading(true);
    try {
      const { features, probability, commit_token } = await prepareThenPredict({
        player_id: Number(playerId),
        player_name: playerName || undefined,
        team_abbr: (teamAbbr || "").toUpperCase(),
        game_date: gameDate,
        prop_type: propType,
        prop_value: Number(propValue),
        over_under: overUnder,
      });

      // reflect canonicalizations from backend (optional niceties)
      if (features?.player_id) setPlayerId(String(features.player_id));
      if (features?.team) {
        setTeamAbbr(String(features.team).toUpperCase());
        setTeamTouched(false);
      }

      setPrepPreview({
        sample: Object.fromEntries(Object.entries(features).slice(0, 12)),
      });

      setPrediction({ probability });
      setCommitToken(commit_token || null);
    } catch (err) {
      console.error("[Props V2] predict error:", err);
      setError(err.message || "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  // Keep onSubmit working (v1 wiring)
  async function handleSubmit(e) {
    e?.preventDefault?.();
    await handlePredict();
  }

  // ----- save prop (after predict) -----
  async function handleSaveProp() {
    setError("");
    if (!commitToken) return;
    setSaving(true);
    try {
      const res = await postApi("/api/props/add", {
        prop_source: "user_added",
        commit_token: commitToken,
      });
      if (res?.duplicate) {
        setPrediction((p) => (p ? { ...p, duplicate: true } : p));
      } else if (res?.saved) {
        setPrediction((p) => (p ? { ...p, saved: true } : p));
      }
      setCommitToken(null); // avoid repeat submits
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setSaving(false);
    }
  }

  // ---- derived UI helpers (just before return) ----
  const pctClamped = (p) =>
    `${(Math.max(0, Math.min(1, Number(p) || 0)) * 100).toFixed(1)}%`;

  const addDisabled =
    !commitToken ||
    loading ||
    saving ||
    prediction?.saved ||
    prediction?.duplicate;

  const addLabel = saving
    ? "Saving…"
    : prediction?.duplicate
    ? "Already saved"
    : prediction?.saved
    ? "Saved ✓"
    : !commitToken
    ? "Predict first"
    : "➕ Add Prop";

  const addTitle = !commitToken
    ? "Run Predict to generate a commit token"
    : undefined;

  return (
    <form
      onSubmit={handleSubmit}
      className="space-y-4 p-4 bg-blue-100 rounded-xl shadow-md overflow-x-auto w-full max-w-5xl mx-auto"
    >
      <h2 className="text-2xl font-bold text-center">📋 Add Player Prop</h2>
      <p className="text-gray-500 text-center text-sm">
        You must make a prediction before adding a prop.
      </p>

      {error && (
        <div className="bg-red-100 text-red-700 p-2 rounded-md text-center">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Player Name + Resolve */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Player Name</span>
          <div className="flex gap-2">
            <input
              value={playerName}
              onChange={(e) => setPlayerName(e.target.value)}
              onBlur={resolvePlayerByNameNow}
              placeholder="e.g., Aaron Judge"
              className="w-full p-2 bg-gray-50 border border-gray-300 rounded-md"
            />
            <button
              type="button"
              onClick={resolvePlayerByNameNow}
              disabled={!playerName.trim()}
              className="px-3 py-2 bg-white border border-blue-500 text-black rounded-md hover:bg-blue-100 disabled:opacity-50"
            >
              Resolve
            </button>
          </div>
          <div className="min-h-[1.25rem] mt-1 text-xs">
            {resolving ? (
              <span className="text-gray-500">Resolving…</span>
            ) : playerId ? (
              <span className="text-green-700">
                Resolved: #{playerId}
                {teamAbbr ? ` • ${teamAbbr}` : ""}
              </span>
            ) : null}
          </div>
        </div>

        {/* Team (abbr) */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Team</span>
          <select
            value={teamAbbr}
            onChange={(e) => {
              setTeamTouched(true);
              setTeamAbbr(e.target.value.toUpperCase());
            }}
            className="w-full p-2 bg-gray-50 border border-gray-300 rounded-md"
          >
            <option value="">Select Team</option>
            {[
              "ATH",
              "ATL",
              "AZ",
              "BAL",
              "BOS",
              "CHC",
              "CWS",
              "CIN",
              "CLE",
              "COL",
              "DET",
              "HOU",
              "KC",
              "LAA",
              "LAD",
              "MIA",
              "MIL",
              "MIN",
              "NYM",
              "NYY",
              "PHI",
              "PIT",
              "SD",
              "SEA",
              "SF",
              "STL",
              "TB",
              "TEX",
              "TOR",
              "WSH",
            ].map((abbr) => (
              <option key={abbr} value={abbr}>
                {abbr}
              </option>
            ))}
          </select>
        </div>

        {/* Prop Type */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Prop Type</span>
          <select
            value={propType}
            onChange={(e) => setPropType(e.target.value)}
            className="border rounded p-2"
          >
            {PROP_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>

        {/* Prop Value */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Prop Value</span>
          <input
            type="number"
            value={propValue}
            onChange={(e) => setPropValue(e.target.value)}
            placeholder="e.g., 0.5"
            className="w-full p-2 bg-gray-50 border border-gray-300 rounded-md"
            inputMode="decimal"
            step="any"
          />
        </div>

        {/* Over/Under */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Over / Under</span>
          <select
            value={overUnder}
            onChange={(e) => setOverUnder(e.target.value)}
            className="w-full p-2 bg-gray-50 border border-gray-300 rounded-md"
          >
            <option value="">Select Over/Under</option>
            <option value="over">Over</option>
            <option value="under">Under</option>
          </select>
        </div>

        {/* Game Date */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Game Date</span>
          <input
            type="date"
            value={gameDate}
            onChange={(e) => setGameDate(e.target.value)}
            className="w-full p-2 bg-gray-50 border border-gray-300 rounded-md"
          />
        </div>
      </div>

      {/* Buttons */}
      <div className="flex space-x-2 justify-center mt-4">
        <button
          type="button"
          onClick={handlePredict}
          disabled={loading}
          className="flex-1 md:flex-none px-4 py-2 bg-white border border-blue-500 text-black rounded-md hover:bg-blue-100 disabled:opacity-50"
        >
          {loading ? "Working…" : "🧠 Predict Outcome"}
        </button>

        <button
          type="button"
          onClick={handleSaveProp}
          disabled={addDisabled}
          title={addTitle}
          className="flex-1 md:flex-none px-4 py-2 bg-white border border-green-500 text-black rounded-md hover:bg-green-100 disabled:opacity-50"
        >
          {addLabel}
        </button>
      </div>

      {/* Prediction summary (no second Add button) */}
      {prediction && (
        <div className="p-3 rounded border space-y-2">
          <div className="font-medium">
            🎯 Model (Probability of Over): {pctClamped(prediction.probability)}
          </div>

          {prediction.duplicate ? (
            <div className="text-xs text-amber-700">Already saved.</div>
          ) : prediction.saved ? (
            <div className="text-xs text-green-700">Saved ✓</div>
          ) : (
            <div className="text-xs text-gray-600">
              Not saved yet. Click “Add Prop”.
            </div>
          )}
        </div>
      )}
    </form>
  );
}
