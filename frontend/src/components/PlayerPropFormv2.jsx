// src/components/PlayerPropFormv2.js
import React, { useState, useEffect, useRef } from "react";
import { useAuth } from "../context/AuthContext.jsx";
import { getBaseURL } from "../shared/getBaseURL.js";

const BASE_API = getBaseURL();

// ----- simple fetch helpers -----
function formatApiError(status, payload) {
  if (payload && typeof payload === "object") {
    const detail = payload.detail ?? payload.error ?? payload.message;
    if (typeof detail === "string" && detail.trim()) return `${status}: ${detail}`;
    try {
      return `${status}: ${JSON.stringify(payload)}`;
    } catch {
      return `${status}: request failed`;
    }
  }
  if (typeof payload === "string" && payload.trim()) return `${status}: ${payload}`;
  return `${status}: request failed`;
}

async function getApi(path, params = {}) {
  const url = new URL(BASE_API + path);
  Object.entries(params).forEach(([k, v]) => {
    if (v != null && v !== "") url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString(), {
    mode: "cors",
    credentials: "omit",
  });
  if (!res.ok) {
    let payload;
    const ct = res.headers.get("content-type") || "";
    try {
      payload = ct.includes("application/json") ? await res.json() : await res.text();
    } catch {
      payload = null;
    }
    throw new Error(formatApiError(res.status, payload));
  }
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
  if (!res.ok) {
    let payload;
    const ct = res.headers.get("content-type") || "";
    try {
      payload = ct.includes("application/json") ? await res.json() : await res.text();
    } catch {
      payload = null;
    }
    throw new Error(formatApiError(res.status, payload));
  }
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
  market_odds_american, // optional sportsbook price, e.g. -115
  market_implied_probability, // optional implied probability, e.g. 0.535
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
  if (market_odds_american != null && market_odds_american !== "") {
    prepareBody.market_odds_american = Number(market_odds_american);
  }
  if (market_implied_probability != null && market_implied_probability !== "") {
    prepareBody.market_implied_probability = Number(market_implied_probability);
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
    warnings: prep.warnings || [],
    probability: pred.probability, // probability of OVER
    recommendation: pred.recommendation,
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

function americanOddsToImplied(odds) {
  const o = Number(odds);
  if (!Number.isFinite(o) || o === 0) return null;
  if (o > 0) return 100 / (o + 100);
  return Math.abs(o) / (Math.abs(o) + 100);
}

export default function PlayerPropFormV2({ onSaved, onPredicted }) {
  const { user } = useAuth();
  // user inputs
  const [playerName, setPlayerName] = useState("");
  const [teamAbbr, setTeamAbbr] = useState("");
  const [gameDate, setGameDate] = useState(() => todayInET());
  const [propType, setPropType] = useState("hits");
  const [overUnder, setOverUnder] = useState("under");
  const [propValue, setPropValue] = useState("0.5");
  const [marketOddsAmerican, setMarketOddsAmerican] = useState("");
  const [marketImpliedProbability, setMarketImpliedProbability] = useState("");

  // resolved/flow
  const [playerId, setPlayerId] = useState("");
  const [commitToken, setCommitToken] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [prepPreview, setPrepPreview] = useState(null);
  const [prepWarnings, setPrepWarnings] = useState([]);
  const [notice, setNotice] = useState("");

  // ui state
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [resolving, setResolving] = useState(false);
  const [loadingMarket, setLoadingMarket] = useState(false);
  const [marketSourceLabel, setMarketSourceLabel] = useState("");
  const [supportedMarketMap, setSupportedMarketMap] = useState({});
  const [loadingMarketSupport, setLoadingMarketSupport] = useState(true);

  // resolver stale-guard + team handling
  const lastReqId = useRef(0);
  const [teamTouched, setTeamTouched] = useState(false);
  const [lastResolvedPlayerId, setLastResolvedPlayerId] = useState("");

  // Invalidate stale prediction/token whenever inputs that affect the model change
  useEffect(() => {
    setPrediction(null);
    setCommitToken(null);
    setPrepPreview(null);
    setPrepWarnings([]);
    setNotice("");
    setError("");
    setMarketSourceLabel("");
  }, [playerId, teamAbbr, gameDate, propType, propValue, overUnder, marketOddsAmerican, marketImpliedProbability]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setLoadingMarketSupport(true);
        const data = await getApi("/api/mlb/market-supported-props");
        if (cancelled) return;
        const rows = Array.isArray(data?.rows) ? data.rows : [];
        const map = {};
        for (const row of rows) {
          if (!row?.prop_type || !row?.market_key) continue;
          map[String(row.prop_type)] = String(row.market_key);
        }
        setSupportedMarketMap(map);
      } catch {
        if (cancelled) return;
        setSupportedMarketMap({});
      } finally {
        if (!cancelled) setLoadingMarketSupport(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const PROP_OPTIONS = React.useMemo(
    () =>
      PROP_TYPES.map((value) => ({ value, label: prettyProp(value) })).sort(
        (a, b) => a.label.localeCompare(b.label)
      ),
    []
  );

  const isMarketSupported = React.useMemo(() => {
    if (loadingMarketSupport) return true; // avoid blocking while metadata loads
    return Boolean(supportedMarketMap[propType]);
  }, [loadingMarketSupport, propType, supportedMarketMap]);

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
    setNotice("");
    setPrediction(null);
    setCommitToken(null);
    setPrepPreview(null);
    setPrepWarnings([]);

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
      const oddsBasedImplied = americanOddsToImplied(marketOddsAmerican);
      const explicitImplied =
        marketImpliedProbability !== "" ? Number(marketImpliedProbability) : null;
      const finalMarketImplied =
        explicitImplied != null && Number.isFinite(explicitImplied)
          ? explicitImplied
          : oddsBasedImplied;

      const { features, warnings, probability, recommendation, commit_token, model } =
        await prepareThenPredict({
        player_id: Number(playerId),
        player_name: playerName || undefined,
        team_abbr: (teamAbbr || "").toUpperCase(),
        game_date: gameDate,
        prop_type: propType,
        prop_value: Number(propValue),
        over_under: overUnder,
        market_odds_american: marketOddsAmerican,
        market_implied_probability: finalMarketImplied,
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
      setPrepWarnings(Array.isArray(warnings) ? warnings : []);

      setPrediction({ probability, recommendation, model });
      setCommitToken(commit_token || null);
      setNotice("Prediction ready. Review and click Add Prop to save.");
      onPredicted?.({
        probability,
        marketProbability:
          features?.market_implied_probability != null
            ? Number(features.market_implied_probability)
            : finalMarketImplied,
        marketOddsAmerican:
          features?.market_odds_american != null
            ? Number(features.market_odds_american)
            : (marketOddsAmerican !== "" ? Number(marketOddsAmerican) : null),
        recommendation,
        model,
        features,
        updatedAt: new Date().toISOString(),
        marketSource: marketSourceLabel || null,
      });
    } catch (err) {
      console.error("[Props V2] predict error:", err);
      setError(err.message || "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  async function handleFetchMarketOdds() {
    setError("");
    setNotice("");
    const name = (playerName || "").trim();
    if (!name) {
      setError("Enter player name before fetching market odds.");
      return;
    }
    if (!propType) {
      setError("Pick a prop type before fetching market odds.");
      return;
    }
    if (!isMarketSupported) {
      setNotice("Market odds not available for this prop type in current OddsAPI mapping.");
      return;
    }
    setLoadingMarket(true);
    try {
      const data = await getApi("/api/mlb/market-odds", {
        player_name: name,
        prop_type: propType,
        game_date: gameDate,
        over_under: overUnder || "over",
        line: propValue,
      });

      if (!data?.ok) {
        const reason = data?.reason || "lookup failed";
        setError(`Market odds lookup failed: ${reason}`);
        return;
      }
      if (!data?.found) {
        const reason = data?.reason || "no match found";
        setNotice(`No market odds match found: ${reason}`);
        return;
      }

      if (data.price_american != null) {
        setMarketOddsAmerican(String(data.price_american));
      }
      if (data.implied_probability != null) {
        setMarketImpliedProbability(String(Number(data.implied_probability).toFixed(4)));
      }
      const source = data.bookmaker
        ? `${data.bookmaker} (${data.market_key || "market"})`
        : (data.market_key || "OddsAPI");
      setMarketSourceLabel(source);
      setNotice(`Market odds loaded from ${source}.`);
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setLoadingMarket(false);
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
    setNotice("");
    if (!commitToken) return;
    setSaving(true);
    try {
      const res = await postApi("/api/props/add", {
        prop_source: "user_added",
        commit_token: commitToken,
        user_id: user?.id || undefined,
      });
      if (res?.duplicate) {
        setPrediction((p) => (p ? { ...p, duplicate: true, savedId: res.id ?? null } : p));
        setNotice(
          res?.id
            ? `This prop was already saved (id: ${res.id}).`
            : "This prop was already saved."
        );
        onSaved?.({ duplicate: true, id: res?.id ?? null, gameDate });
      } else if (res?.saved) {
        setPrediction((p) => (p ? { ...p, saved: true, savedId: res.id ?? null } : p));
        setNotice(res?.id ? `Prop saved (id: ${res.id}).` : "Prop saved.");
        onSaved?.({ duplicate: false, id: res?.id ?? null, gameDate });
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
      className="pp-card space-y-4 p-4 overflow-x-auto w-full max-w-5xl mx-auto"
    >
      <h2 className="text-2xl font-bold text-center">📋 Add Player Prop</h2>
      <p className="text-slate-500 text-center text-sm">
        You must make a prediction before adding a prop.
      </p>

      {error && (
        <div className="pp-chip bg-rose-50 text-rose-700 p-2 rounded-md text-center">
          {error}
        </div>
      )}
      {!error && notice && (
        <div className="pp-chip bg-emerald-50 text-emerald-800 p-2 rounded-md text-center">
          {notice}
        </div>
      )}
      {prepWarnings.length > 0 && (
        <div className="pp-chip bg-amber-100 text-amber-800 p-2 rounded-md text-sm">
          {prepWarnings.join(" ")}
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
              className="w-full p-2 pp-chip rounded-md"
            />
            <button
              type="button"
              onClick={resolvePlayerByNameNow}
              disabled={!playerName.trim()}
              className="pp-btn pp-btn-secondary pp-btn-md"
            >
              Resolve
            </button>
          </div>
          <div className="min-h-[1.25rem] mt-1 text-xs">
            {resolving ? (
              <span className="text-slate-500">Resolving…</span>
            ) : playerId ? (
              <span className="text-emerald-700">
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
            className="w-full p-2 pp-chip rounded-md"
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
            className="pp-chip rounded p-2"
          >
            {PROP_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
          <div className="min-h-[1.1rem] mt-1 text-xs text-slate-600">
            {loadingMarketSupport
              ? "Checking market coverage..."
              : isMarketSupported
                ? `Market key: ${supportedMarketMap[propType]}`
                : "No market odds mapping for this prop type"}
          </div>
        </div>

        {/* Prop Value */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Prop Value</span>
          <input
            type="number"
            value={propValue}
            onChange={(e) => setPropValue(e.target.value)}
            placeholder="e.g., 0.5"
            className="w-full p-2 pp-chip rounded-md"
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
            className="w-full p-2 pp-chip rounded-md"
          >
            <option value="">Select Over/Under</option>
            <option value="over">Over</option>
            <option value="under">Under</option>
          </select>
        </div>

        {/* Market Odds (American) */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Market Odds (American)</span>
          <input
            type="number"
            value={marketOddsAmerican}
            onChange={(e) => setMarketOddsAmerican(e.target.value)}
            placeholder="e.g., -115 or +135"
            className="w-full p-2 pp-chip rounded-md"
            inputMode="numeric"
            step="1"
          />
        </div>

        {/* Market Implied Probability */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Market Implied Prob (0-1)</span>
          <input
            type="number"
            value={marketImpliedProbability}
            onChange={(e) => setMarketImpliedProbability(e.target.value)}
            placeholder="optional; overrides odds conversion"
            className="w-full p-2 pp-chip rounded-md"
            inputMode="decimal"
            step="0.001"
            min="0"
            max="1"
          />
        </div>

        {/* Game Date */}
        <div className="flex flex-col">
          <span className="text-sm font-medium mb-1">Game Date</span>
          <input
            type="date"
            value={gameDate}
            onChange={(e) => setGameDate(e.target.value)}
            className="w-full p-2 pp-chip rounded-md"
          />
        </div>
      </div>

      {/* Buttons */}
      <div className="flex space-x-2 justify-center mt-4">
        <button
          type="button"
          onClick={handleFetchMarketOdds}
          disabled={loadingMarket || !playerName.trim() || !propType || !isMarketSupported}
          className="pp-btn pp-btn-secondary pp-btn-md flex-1 md:flex-none"
        >
          {loadingMarket ? "Loading Market…" : "📈 Fetch Market Odds"}
        </button>

        <button
          type="button"
          onClick={handlePredict}
          disabled={loading}
          className="pp-btn pp-btn-secondary pp-btn-md flex-1 md:flex-none"
        >
          {loading ? "Working…" : "🧠 Predict Outcome"}
        </button>

        <button
          type="button"
          onClick={handleSaveProp}
          disabled={addDisabled}
          title={addTitle}
          className="pp-btn pp-btn-primary pp-btn-md flex-1 md:flex-none"
        >
          {addLabel}
        </button>
      </div>

      {/* Prediction summary (no second Add button) */}
      {prediction && (
        <div className="p-3 rounded pp-chip space-y-2">
          <div className="font-medium">
            🎯 Model (Probability of Over): {pctClamped(prediction.probability)}
          </div>
          <div className="text-xs text-slate-700">
            Recommendation: {(prediction.recommendation || "over").toUpperCase()}
            {prediction.model ? ` • Model: ${prediction.model}` : ""}
            {marketSourceLabel ? ` • Market: ${marketSourceLabel}` : ""}
          </div>

          {prediction.duplicate ? (
            <div className="text-xs text-amber-700">
              Already saved{prediction.savedId ? ` (id: ${prediction.savedId})` : ""}.
            </div>
          ) : prediction.saved ? (
            <div className="text-xs text-emerald-700">
              Saved ✓{prediction.savedId ? ` (id: ${prediction.savedId})` : ""}
            </div>
          ) : (
            <div className="text-xs text-slate-600">
              Not saved yet. Click “Add Prop”.
            </div>
          )}
        </div>
      )}
    </form>
  );
}
