// File: backend/scripts/shared/propUtilsBackend.js

export const VALID_PROP_TYPES = [
  "hits",
  "strikeouts_batting",
  "home_runs",
  "rbis",
  "runs",
  "total_bases",
  "walks",
  "stolen_bases",
  "strikeouts_pitching",
  "outs_recorded",
  "earned_runs",
  "hits_allowed",
  "walks_allowed",
  "pitching_outs",
  "pitch_count",
  "runs_allowed",
  "singles",
  "doubles",
  "triples",
];

// backend/scripts/shared/propUtilsBackend.js

// Convert innings pitched like "5.2" -> 5*3 + 2 = 17 outs
function ipToOuts(ip) {
  if (ip == null) return null;
  const s = String(ip);
  const [w, f = "0"] = s.split(".");
  const whole = Number(w);
  if (Number.isNaN(whole)) return null;
  const frac = f === "1" ? 1 : f === "2" ? 2 : 0;
  return whole * 3 + frac;
}

function num(x) {
  if (x == null) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

// Maps propType to appropriate stat in the nested stats object
export function extractStatForPropType(stats, propType) {
  if (!stats || typeof stats !== "object") return null;

  // StatsAPI typically nests as stats.batting / stats.pitching
  // Some pre-processing may also flatten keys; we handle both.
  const b = stats.batting || stats.hitting || stats; // batting-ish
  const p = stats.pitching || stats; // pitching-ish

  switch (propType) {
    // ── Batting props ───────────────────────────────────────────────
    case "hits":
      return num(b.hits);
    case "strikeouts_batting":
      // seen as strikeOuts (camelCase); fall back to common variants
      return num(b.strikeOuts ?? b.strikeouts ?? b.strikeouts_batting);
    case "home_runs":
      return num(b.homeRuns ?? b.home_runs);
    case "rbis":
      return num(b.rbi ?? b.rbis);
    case "runs":
    case "runs_scored":
      return num(b.runs);
    case "walks":
      return num(b.baseOnBalls ?? b.walks);
    case "stolen_bases":
      return num(b.stolenBases ?? b.stolen_bases);
    case "doubles":
      return num(b.doubles);
    case "triples":
      return num(b.triples);
    case "total_bases":
      // StatsAPI has totalBases for batting
      return num(b.totalBases ?? b.total_bases);
    case "singles": {
      // derive if not provided
      const H = num(b.hits) ?? 0;
      const _2 = num(b.doubles) ?? 0;
      const _3 = num(b.triples) ?? 0;
      const HR = num(b.homeRuns ?? b.home_runs) ?? 0;
      const sgl = H - _2 - _3 - HR;
      return Number.isFinite(sgl) ? sgl : null;
    }
    case "hits_runs_rbis": {
      const H = num(b.hits) ?? 0;
      const R = num(b.runs) ?? 0;
      const I = num(b.rbi ?? b.rbis) ?? 0;
      return H + R + I;
    }
    case "runs_rbis": {
      const R = num(b.runs) ?? 0;
      const I = num(b.rbi ?? b.rbis) ?? 0;
      return R + I;
    }

    // ── Pitching props ───────────────────────────────────────────────
    case "strikeouts_pitching":
      return num(p.strikeOuts ?? p.strikeouts ?? p.strikeouts_pitching);
    case "outs_recorded":
    case "pitching_outs":
      // Some feeds expose p.outs; others require IP -> outs
      return num(p.outs) ?? ipToOuts(p.inningsPitched);
    case "earned_runs":
      return num(p.earnedRuns ?? p.earned_runs);
    case "hits_allowed":
      return num(p.hits ?? p.hits_allowed);
    case "walks_allowed":
      return num(p.baseOnBalls ?? p.walks_allowed ?? p.walks);
    case "pitch_count":
      // Seen as numberOfPitches / pitchesThrown depending on endpoint
      return num(p.numberOfPitches ?? p.pitchesThrown ?? p.pitches);
    case "runs_allowed":
      return num(p.runs);

    default:
      return null;
  }
}

// Compute rolling average over recent games
export function getRollingAverage(history, propType, windowSize) {
  if (!Array.isArray(history) || !propType) return null;

  const recent = history.slice(0, windowSize);
  const values = recent
    .map((game) => extractStatForPropType(game?.stats, propType))
    .filter((v) => typeof v === "number");

  if (!values.length) return null;

  const sum = values.reduce((a, b) => a + b, 0);
  return sum / values.length;
}

export function normalizePropType(label) {
  return label.toLowerCase().replace(/[()]/g, "").replace(/\s+/g, "_");
}

export function determineStatus(actual, line, overUnder) {
  const direction = overUnder?.toLowerCase?.();

  if (typeof actual !== "number" || typeof line !== "number" || !direction) {
    return "invalid";
  }

  if (actual === line) return "push";

  const isWin =
    (direction === "over" && actual > line) ||
    (direction === "under" && actual < line);

  return isWin ? "win" : "loss";
}

export function expireOldPendingProps(props = []) {
  const todayISO = toISODate(todayET());
  return props.map((prop) => {
    const propDate = toISODate(prop.game_date);
    if (prop.status === "pending" && propDate < todayISO) {
      return { ...prop, status: "expired" };
    }
    return prop;
  });
}
