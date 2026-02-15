// backend/scripts/insertStatDerivedProps.js
import {
  getGameStartTimeET,
  getDayOfWeekET,
  getTimeOfDayBucketET,
  toISODate,
  toEasternDateTime,
} from "./shared/timeUtilsBackend.js";
import { supabase } from "./shared/supabaseBackend.js";
import {
  BATTER_PROP_TYPES,
  PITCHER_PROP_TYPES,
  isBatterProp,
  determineOutcome,
} from "../../mlb/shared/propUtils.js";
import {
  getStreaksForPlayer,
  getPlayerPositionMap,
  isPitcher,
  isStarterPitcher,
} from "./shared/playerUtilsBackend.js";
import { fetchBoxscoreStatsForGame } from "./shared/fetchBoxscoreStats.js";
import {
  getGameContextFields,
  getLiveFeedFromGameID,
} from "./shared/mlbApiUtils.js";
import { getTeamIdFromAbbr } from "../../mlb/shared/teamNameMap.js";
import { extractStatForPropType } from "./shared/propUtilsBackend.js";
import crypto from "node:crypto";

// ---- logging controls ----
const ARGS = process.argv.slice(2);
const QUIET = ARGS.includes("--quiet") || process.env.QUIET === "1"; // cron-friendly
const VERBOSE =
  !QUIET && (ARGS.includes("--verbose") || process.env.VERBOSE === "1");
const DEBUG = !QUIET && (ARGS.includes("--debug") || process.env.DEBUG === "1");

function argValue(flag) {
  const i = ARGS.indexOf(flag);
  if (i < 0 || i + 1 >= ARGS.length) return null;
  return ARGS[i + 1];
}

// chatty (only with --verbose and not --quiet)
const log = (...a) => {
  if (VERBOSE) console.log(...a);
};
// deep dumps (only with --debug and not --quiet)
const dbg = (...a) => {
  if (DEBUG) console.log(...a);
};
// warnings visible unless --quiet
const warn = (...a) => {
  if (!QUIET) console.warn(...a);
};
// always show (final summaries etc.)
const forceLog = (...a) => console.log(...a);
// always errors
const error = (...a) => console.error(...a);

dbg(
  "BATTER_PROP_TYPES:",
  BATTER_PROP_TYPES.map((p) => typeof p)
);
dbg(
  "PITCHER_PROP_TYPES:",
  PITCHER_PROP_TYPES.map((p) => typeof p)
);

function parseDateOnly(value, label) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
  if (!m) throw new Error(`${label} must be YYYY-MM-DD`);
  const d = new Date(`${raw}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) throw new Error(`${label} invalid date: ${raw}`);
  return d;
}

function buildDatesToProcess() {
  const fromArg = argValue("--from-date") || process.env.MLB_STAT_FROM_DATE;
  const toArg = argValue("--to-date") || process.env.MLB_STAT_TO_DATE;
  const daysAgoRaw =
    argValue("--days-ago") || process.env.MLB_STAT_DAYS_AGO || "2";
  const daysAgo = Math.max(1, Number.parseInt(String(daysAgoRaw), 10) || 2);

  let startDate;
  let endDate;
  if (fromArg || toArg) {
    if (!fromArg || !toArg) {
      throw new Error("both --from-date and --to-date are required when date override is used");
    }
    startDate = parseDateOnly(fromArg, "--from-date");
    endDate = parseDateOnly(toArg, "--to-date");
  } else {
    const today = new Date();
    endDate = new Date(today);
    endDate.setDate(endDate.getDate() - 1);
    startDate = new Date(today);
    startDate.setDate(startDate.getDate() - daysAgo);
  }
  if (startDate > endDate) {
    throw new Error(`from-date must be <= to-date (${toISODate(startDate)} > ${toISODate(endDate)})`);
  }

  const out = [];
  for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
    out.push(toISODate(new Date(d)));
  }
  return out;
}

const datesToProcess = buildDatesToProcess();

if (!supabase) {
  throw new Error(
    "Supabase client is not configured (missing SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY)"
  );
}

const LOG_EVERY = 150;
const SLEEP_MS = 10;

const propTypeWins = {};
const propTypeLosses = {};
let overCount = 0;
let underCount = 0;
let winCount = 0;
let lossCount = 0;

const propTypeInsertCounts = {};

const quietMode = true;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function hashString(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function shouldInclude(playerId, gameId, propType, ratio = 0.2) {
  const str = `${playerId}-${gameId}-${propType}`;
  const normalized = (hashString(str) % 1000) / 1000;
  return normalized < ratio;
}

async function processDate(gameDate) {
  log(`\n📅 ${gameDate}`);
  const schedURL = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${gameDate}`;
  const schedRes = await fetch(schedURL)
    .then((r) => r.json())
    .catch(() => null);
  const gameIds = (schedRes?.dates?.[0]?.games || [])
    .filter((g) => g.status?.detailedState === "Final")
    .map((g) => g.gamePk);

  const teamContextCache = new Map();
  const positionMap = await getPlayerPositionMap(gameDate);

  for (const gameId of gameIds) {
    try {
      log(`📍  Game ${gameId}`);
      const liveData = await getLiveFeedFromGameID(gameId);
      const allPlays = liveData?.liveData?.plays?.allPlays || [];
      const allPlayers = await fetchBoxscoreStatsForGame(gameId);

      if (!liveData?.gameData?.teams) {
        console.warn(`⚠️ liveData missing teams for game ${gameId}`);
        continue;
      }

      // define per-game team cache
      const homeTeam = liveData.gameData.teams.home ?? {};
      const awayTeam = liveData.gameData.teams.away ?? {};
      const TEAM = {
        home: {
          id: homeTeam.id ?? homeTeam.teamId ?? null,
          abbr: homeTeam.abbreviation ?? homeTeam.teamCode ?? null,
        },
        away: {
          id: awayTeam.id ?? awayTeam.teamId ?? null,
          abbr: awayTeam.abbreviation ?? awayTeam.teamCode ?? null,
        },
      };
      if (!Array.isArray(allPlayers)) {
        console.error(
          `❌ allPlayers for game ${gameId} is not an array:`,
          allPlayers
        );
        continue;
      }

      console.log(
        `🧾 Boxscore fetch: ${gameId} → ${allPlayers.length} players`
      );

      if (allPlayers.some((p) => !p || typeof p !== "object" || !("id" in p))) {
        console.error(
          `❌ Invalid player object found in allPlayers for game ${gameId}`
        );
        for (const bad of allPlayers.filter(
          (p) => !p || typeof p !== "object" || !("id" in p)
        )) {
          console.error("👉 Bad player:", bad);
        }
        continue;
      }

      if (!allPlayers) continue;

      // 🔍 Diagnostic print of all players' stat keys
      // Log a summary of player stat availability before filtering
      dbg(`📦 Fetched ${allPlayers.length} players for game ${gameId}`);
      for (const p of allPlayers) {
        const batKeys = Object.keys(p.stats?.batting || []).join(", ");
        const pitchKeys = Object.keys(p.stats?.pitching || []).join(", ");

        if (!batKeys && !pitchKeys) {
          if (DEBUG) warn(`⚠️ No stats found for ${p.fullName} (${p.id})`);
        } else {
          dbg(
            `📊 ${p.fullName} (${p.id}) → Batting: [${batKeys}] | Pitching: [${pitchKeys}]`
          );
        }
      }
      // Build the player list we actually want to process
      const players = allPlayers.filter((p) => {
        const { batting = {}, pitching = {} } = p.stats || {};
        const hasBat = Object.keys(batting).length > 0;
        const hasPitch = Object.keys(pitching).length > 0;
        const position = positionMap.get(Number(p.id));
        const isPitch = isPitcher(position) || hasPitch;
        // Include batters and pitchers in pool; starter gating is applied per prop below.
        return hasBat || isPitch;
      });

      log(`✅ Final player pool after filtering: ${players.length}`);
      log(`🔍 ${players.length} players to process`);

      let inserted = 0;

      for (const pl of players) {
        const {
          id: player_id,
          fullName,
          teamAbbr: teamAbbrRaw,
          isHome,
          stats,
        } = pl;

        // position lookup for pitcher/batter flags
        const position = positionMap.get(Number(player_id)) || null;

        // derive team ids/abbrs from per-game TEAM cache
        const teamId = isHome ? TEAM.home.id : TEAM.away.id;
        const opponentId = isHome ? TEAM.away.id : TEAM.home.id;
        const teamAbbr =
          (isHome ? TEAM.home.abbr : TEAM.away.abbr) ?? teamAbbrRaw ?? null;
        const opponentAbbr = isHome ? TEAM.away.abbr : TEAM.home.abbr;

        // keep opponent_encoded for back-compat (prefer IDs when present)
        const opponent_encoded =
          opponentId != null
            ? String(opponentId)
            : opponentAbbr
            ? getTeamIdFromAbbr(opponentAbbr)
            : null;

        const hasBat = !!(
          stats?.batting && Object.keys(stats.batting).length > 0
        );
        const hasPitch = !!(
          stats?.pitching && Object.keys(stats.pitching).length > 0
        );
        const isPitch = isPitcher(position) || hasPitch;
        const isStarter = isStarterPitcher(position, stats);
        const isBatterOnly = hasBat && !isPitch;
        const isPitcherOnly = isPitch && !hasBat;
        const isTwoWayPlayer = hasBat && isPitch;

        // cache game context by (game, teamAbbr)
        const contextKey = `${gameId}_${teamAbbr}`;
        let contextFields = teamContextCache.get(contextKey);
        if (!contextFields) {
          contextFields = await getGameContextFields(gameId, teamAbbr);
          teamContextCache.set(contextKey, contextFields);
        }

        dbg(
          `🔍 ${fullName} (${player_id}) | ${teamAbbr} vs ${opponentAbbr} (${
            isHome ? "home" : "away"
          })`
        );
        dbg(
          `📌 Position: ${position} | Pitcher: ${isPitch} | Batter: ${hasBat} | Role: ${[
            isBatterOnly && "batter",
            isPitcherOnly && "pitcher",
            isTwoWayPlayer && "two-way",
          ]
            .filter(Boolean)
            .join(", ")}`
        );

        dbg(
          "📦 Full stats object for",
          fullName,
          JSON.stringify(stats, null, 2)
        );

        // Build eligible prop types from actual stat presence
        let eligiblePropTypes = [];
        if (hasBat) eligiblePropTypes.push(...BATTER_PROP_TYPES);
        // ✅ Starter-only: pitcher props only if starter
        if (isPitch && isStarter) {
          eligiblePropTypes.push(...PITCHER_PROP_TYPES);
        }
        // If they’re a pitcher but not the starter, you can either:
        //   a) keep their batter props (two-way guys) and skip pitcher props (current behavior), or
        //   b) drop them entirely. To drop entirely, uncomment:
        // if (isPitch && !isStarter && !hasBat) continue;        eligiblePropTypes = [...new Set(eligiblePropTypes)]; // de-dupe

        dbg(`🔁 eligiblePropTypes: ${JSON.stringify(eligiblePropTypes)}`);

        // Optional diagnostics for pitchers with numeric stats
        if ((isPitcherOnly || isTwoWayPlayer) && DEBUG) {
          const seen = [];
          for (const pType of PITCHER_PROP_TYPES) {
            const v = extractStatForPropType(stats, pType);
            if (Number.isFinite(v)) seen.push(pType);
          }
          if (seen.length === 0) {
            warn(
              `⚠️ No pitcher stats extracted for ${fullName} (${player_id}). ` +
                `pitching keys: ${Object.keys(stats?.pitching || {}).join(",")}`
            );
          } else {
            log(`✅ Pitcher props with numeric values: ${seen.join(", ")}`);
          }
        }

        for (const propType of eligiblePropTypes) {
          // Belt-and-suspenders: never emit pitcher props unless starter
          if (!isStarter && !isBatterProp(propType)) continue; // ratio sampling applies to ALL batter props (two-way included)
          if (
            isBatterProp(propType) &&
            !shouldInclude(player_id, gameId, propType)
          ) {
            continue;
          }

          if (typeof propType !== "string") {
            console.error(`❌ Invalid propType (not a string):`, propType);
            continue;
          }

          dbg(
            `🧪 Calling extractStatForPropType with: propType=${JSON.stringify(
              propType
            )} (${typeof propType})`
          );

          const result = extractStatForPropType(stats, propType);
          dbg(
            `🔍 Raw stat extraction for ${fullName} | ${propType} → ${result}`
          );

          if (!Number.isFinite(result)) {
            dbg(
              `🟡 No valid result for ${fullName} (${player_id}) | ${propType}`
            );
            continue;
          }

          // create a half-step line (avoid pushes)
          let line =
            result === 0
              ? 0.5
              : Math.random() < 0.5
              ? result - 0.5
              : result + 0.5;
          line = Math.round(line * 2) / 2;

          const over_under = Math.random() < 0.5 ? "over" : "under";
          const outcome = determineOutcome(result, line, over_under);
          if (!["win", "loss"].includes(outcome)) continue;

          const was_correct = outcome === "win";
          if (over_under === "over") overCount++;
          else underCount++;
          if (outcome === "win") winCount++;
          else lossCount++;
          propTypeWins[propType] =
            (propTypeWins[propType] || 0) + (outcome === "win" ? 1 : 0);
          propTypeLosses[propType] =
            (propTypeLosses[propType] || 0) + (outcome === "loss" ? 1 : 0);

          // streak + game time
          let streak, game_time;
          try {
            streak = await getStreaksForPlayer(
              supabase,
              player_id,
              propType,
              "mlb_api"
            );
            game_time = await getGameStartTimeET(gameId);
          } catch (e) {
            warn(`⚠️ Error fetching streak/time for ${fullName}, ${propType}`);
            continue;
          }
          if (!game_time) continue;
          // Prefer full ISO-with-offset if provided by MLB; otherwise fall back
          let gameDateTimeET;
          if (typeof game_time === "string" && game_time.includes("T")) {
            const dt = new Date(game_time); // ISO with offset → valid Date
            if (!Number.isNaN(dt.getTime())) {
              gameDateTimeET = dt;
            }
          }
          if (!gameDateTimeET) {
            // Legacy path: when game_time is just "HH:mm:ss" or similar
            gameDateTimeET = toEasternDateTime(gameDate, game_time);
          }

          // check existing
          const { data: existingRows, error: fetchErr } = await supabase
            .from("model_training_props")
            .select("id, prop_value, outcome")
            .eq("player_id", String(player_id))
            .eq("game_id", gameId)
            .eq("prop_type", propType)
            .eq("prop_source", "mlb_api")
            .limit(1);

          const existing = existingRows?.[0];
          if (fetchErr || !Array.isArray(existingRows)) {
            const errMsg =
              fetchErr?.message ||
              (typeof existingRows === "string"
                ? existingRows.slice(0, 100)
                : "Unknown Supabase error");
            warn(`⚠️ Fetch error for ${fullName}, ${propType}: ${errMsg}`);
            continue;
          }
          if (
            existing &&
            existing.prop_value != null &&
            existing.outcome != null
          ) {
            dbg(
              `📭 Existing row found: prop_value=${existing.prop_value}, outcome=${existing.outcome}`
            );
            continue;
          }
          if (!contextFields) {
            dbg(`⚠️ contextFields missing for ${teamAbbr} in game ${gameId}`);
          }

          const now = new Date().toISOString();

          const row = {
            id: crypto.randomUUID(),
            game_id: gameId,
            player_id: String(player_id),
            player_name: fullName,

            // display/back-compat
            team: teamAbbr,
            opponent: opponentAbbr,

            // canonical ids used by MT
            team_id: teamId != null ? String(teamId) : null,
            opponent_team_id: opponentId != null ? String(opponentId) : null,

            // legacy helper (safe to keep)
            opponent_encoded,

            is_home: isHome ? 1 : 0,
            prop_type: propType,
            prop_value: result,
            line,
            over_under,
            outcome,
            status: "resolved",
            created_at: now,
            updated_at: now,
            prop_source: "mlb_api",
            was_correct,

            game_date: gameDate,
            ...contextFields,
            game_day_of_week: getDayOfWeekET(gameDateTimeET),
            time_of_day_bucket: getTimeOfDayBucketET(gameDateTimeET),
            streak_type: streak?.streak_type ?? null,
            streak_count: streak?.streak_count ?? null,
          };

          forceLog(`📥 Attempting insert: ${JSON.stringify(row, null, 2)}`);

          try {
            const { error } = await supabase
              .from("model_training_props")
              .upsert(row, {
                onConflict: "player_id,game_id,prop_type,prop_source",
              }); // v2 format
            if (!error) {
              inserted++;
              propTypeInsertCounts[propType] =
                (propTypeInsertCounts[propType] || 0) + 1;
              log(
                `📊 Count updated for ${propType} (${propTypeInsertCounts[propType]} total)`
              );
              if (inserted % LOG_EVERY === 0)
                log(`   ↳ ${inserted} rows so far for ${gameDate}…`);
            } else {
              console.error(
                `❌ Upsert failed (${fullName}, ${propType}):`,
                error.message
              );
            }
          } catch (err) {
            console.error(
              `❌ Exception during upsert for ${fullName}, ${propType}:`,
              err
            );
          }
        } // end for propType
      } // end for players

      log(`✅ Game ${gameId} finished — ${inserted} rows inserted`);
      await sleep(SLEEP_MS);
    } catch (err) {
      console.error(`❌ Crash during game ${gameId}:`, err);
    }
  }
}

(async () => {
  let dateFailures = 0;
  for (const d of datesToProcess) {
    try {
      await processDate(d);
    } catch (err) {
      dateFailures += 1;
      console.error(`❌ Crash during processDate(${d}):`, err);
    }
  }

  try {
    forceLog("\n🎯 Over/Under Pick Distribution:");
    forceLog(`   ➕ Over:  ${overCount}`);
    forceLog(`   ➖ Under: ${underCount}`);

    forceLog("\n🏁 Final Outcome Totals:");
    forceLog(`   ✅ Wins:   ${winCount}`);
    forceLog(`   ❌ Losses: ${lossCount}`);

    forceLog("\n📊 Outcome by prop type:");

    log("\n📊 Prop type summary:");

    const allTypes = Array.from(
      new Set([
        ...Object.keys(propTypeInsertCounts),
        ...Object.keys(propTypeWins),
        ...Object.keys(propTypeLosses),
      ])
    );

    for (const type of allTypes.sort()) {
      const inserted = propTypeInsertCounts[type] || 0;
      const w = propTypeWins[type] || 0;
      const l = propTypeLosses[type] || 0;

      forceLog(
        `${type.padEnd(20)} ${String(inserted).padStart(4)} inserted | ${String(
          w
        ).padStart(3)}W / ${String(l).padStart(3)}L`
      );
    }

    if (dateFailures > 0) {
      forceLog(`\n⚠️ Script finished with ${dateFailures} date failure(s).`);
      process.exitCode = 1;
      return;
    }

    forceLog("\n🏁 Script finished successfully.");
  } catch (err) {
    console.error("❌ Error in final summary block:", err.message);
    console.error("⚠️ Falling back to basic totals:");
    console.log({ overCount, underCount, winCount, lossCount });
  }
})().catch((err) => {
  console.error("❌ Top-level script crash:", err);
});
