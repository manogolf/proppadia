#!/usr/bin/env bash
set -euo pipefail

# =========================
# NHL end-to-end daily runner (LOCAL, ALL-IN-ONE)
# - Imports schedule/roster
# - Seeds features, exports, scores, loads predictions
# - Fetches odds (SOG + Saves) inline (uses ODDS_API_KEY if present)
# - Builds sog_with_market (+ vig-less when odds present)
# - Finalizes yesterday’s logs (goalies + skaters stage→raw)
# =========================

# ---- portable date helpers (BSD/macOS & GNU/Linux) ----
date_et_today()      { TZ=America/New_York date +%F; }
date_et_yesterday()  {
  if TZ=America/New_York date -v-1d +%F >/dev/null 2>&1; then
    TZ=America/New_York date -v-1d +%F     # macOS/BSD
  else
    TZ=America/New_York date -d 'yesterday' +%F   # GNU/Linux
  fi
}
date_et_tomorrow()   {
  if TZ=America/New_York date -v+1d +%F >/dev/null 2>&1; then
    TZ=America/New_York date -v+1d +%F
  else
    TZ=America/New_York date -d 'tomorrow' +%F
  fi
}

mkdir -p nhl/site/data exports

# ---- repo root & python ----
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"
PYTHON="${PYTHON:-python3}"

# ---- dates (allow override via env) ----
SLATE_DATE="${SLATE_DATE:-$(date_et_today)}"
YDAY="${YDAY:-$(date_et_yesterday)}"
export SLATE_DATE
export YDAY
echo "SLATE_DATE (ET): ${SLATE_DATE}    YDAY (ET): ${YDAY}"

# ---- small retry helper ----
retry() {
  local n=1; local max=3; local delay=5
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        echo "Attempt $n for '$*' failed; retrying in ${delay}s..." >&2
        sleep $delay
        n=$((n+1))
        delay=$((delay*2))
      else
        echo "All attempts for '$*' failed." >&2
        return 1
      fi
    }
  done
}

# ---- sanity: DB connectivity ----
echo "Checking DB connectivity..."
psql "${SUPABASE_DB_URL:?SUPABASE_DB_URL missing}" -v ON_ERROR_STOP=1 -c "select now();" >/dev/null
echo "DB OK."

echo
echo "== TODAY: import schedule & roster =="
SLATE_DATE="$SLATE_DATE" retry $PYTHON backend/nhl/scripts/import_schedule_today.py
SLATE_DATE="$SLATE_DATE" SKIP_ROSTER_STATUS=1 SKIP_PLAYERS=1 retry $PYTHON backend/nhl/scripts/import_roster_today.py
SLATE_DATE="$SLATE_DATE" retry $PYTHON backend/nhl/scripts/refresh_players_and_roster_today.py

echo
echo "== TODAY: seed features (SOG + goalies) =="
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/seed_sog_features_for_slate.sql
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/seed_goalie_features_for_slate.sql

echo
echo "== TODAY: export training joins (SOG + goalies) =="
export PGOPTIONS='-c statement_timeout=0'
mkdir -p exports
psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_sog.sql > exports/train_nhl_sog_v2.csv
psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_saves.sql > exports/train_goalie_saves_v2.csv

echo
echo "== TODAY: score & attach names =="

# 1) Do the usual wrapper (SOG + SAVES) so DB gets refreshed
$PYTHON backend/nhl/scripts/run_daily_slate.py \
  --project nhl \
  --sog-csv exports/train_nhl_sog_v2.csv \
  --saves-csv exports/train_goalie_saves_v2.csv \
  --db-url "$SUPABASE_DB_URL" \
  --scorer "$ROOT/backend/nhl/scripts/score_nhl_props.py"

# 2) Force goalie-saves re-score with our exact line set (includes 17.5)
SAVES_LINES="17.5,18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,30.5"
echo "▶ Re-scoring SAVES with lines: $SAVES_LINES"
$PYTHON backend/nhl/scripts/score_nhl_props.py \
  --model-dir backend/nhl/models/latest/goalie_saves \
  --csv exports/train_goalie_saves_v2.csv \
  --feature-json backend/nhl/features/feature_metadata_nhl.json \
  --feature-key goalie_saves \
  --line "$SAVES_LINES" \
  --out backend/nhl/data/processed/saves_predictions.csv

# =========================
# INTEGRATED ODDS FETCH + BUILD
# =========================
echo
echo "== TODAY: fetch player props odds (SOG + Saves) =="
OUTDIR="nhl/site/data"
MARKETS="player_shots_on_goal,player_total_saves"
REGIONS="us"
FORMAT="american"
mkdir -p "$OUTDIR"

# If jq missing, skip live fetch but keep last-known odds if present
if ! command -v jq >/dev/null 2>&1; then
  echo "⚠️  jq not found — skipping live odds fetch. Using previous odds if available."
  [[ -f "${OUTDIR}/odds_latest.json" ]] || echo "[]" > "${OUTDIR}/odds_latest.json"
else
  if [[ -n "${ODDS_API_KEY:-}" ]]; then
    echo "→ Fetching events (daysFrom=1)…"
    if curl -fsS "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events?dateFormat=iso&daysFrom=1&apiKey=${ODDS_API_KEY}" \
      -o "${OUTDIR}/events_today.json"; then

      ECOUNT=$(jq 'length' "${OUTDIR}/events_today.json" 2>/dev/null || echo 0)
      echo "   events_today.json → ${ECOUNT} events"

      if [[ "${ECOUNT}" -gt 0 ]]; then
        TMP_NDJSON="$(mktemp)"
        : > "${TMP_NDJSON}"

        echo "→ Fetching player props (markets=${MARKETS}, regions=${REGIONS})…"
        # Safe loop (no xargs-too-long), mild retries per event
        while IFS= read -r EID; do
          ok=""
          for n in 1 2 3; do
            if curl -fsS \
              "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events/${EID}/odds?regions=${REGIONS}&markets=${MARKETS}&oddsFormat=${FORMAT}&apiKey=${ODDS_API_KEY}" \
              >> "${TMP_NDJSON}"; then
              ok="yes"
              echo "" >> "${TMP_NDJSON}"   # newline separator
              break
            fi
            sleep $((n*2))
          done
          [[ -z "$ok" ]] && echo "{}" >> "${TMP_NDJSON}"
        done < <(jq -r '.[].id' "${OUTDIR}/events_today.json")

        jq -s 'map(select(type=="object"))' "${TMP_NDJSON}" > "${OUTDIR}/odds_nhl_playerprops_today.json" || echo "[]" > "${OUTDIR}/odds_nhl_playerprops_today.json"
        cp -f "${OUTDIR}/odds_nhl_playerprops_today.json" "${OUTDIR}/odds_latest.json"
        rm -f "${TMP_NDJSON}"

        SIZE=$(wc -c < "${OUTDIR}/odds_nhl_playerprops_today.json" 2>/dev/null || echo 0)
        echo "✅ Wrote ${OUTDIR}/odds_nhl_playerprops_today.json"
        echo "   size: ${SIZE} bytes | events: ${ECOUNT}"
      else
        echo "⚠️  No events — writing empty odds and preserving odds_latest.json if any."
        echo "[]" > "${OUTDIR}/odds_nhl_playerprops_today.json"
        [[ -f "${OUTDIR}/odds_latest.json" ]] || cp -f "${OUTDIR}/odds_nhl_playerprops_today.json" "${OUTDIR}/odds_latest.json"
      fi
    else
      echo "⚠️  Event fetch failed — keeping odds_latest.json if present."
      [[ -f "${OUTDIR}/odds_latest.json" ]] || echo "[]" > "${OUTDIR}/odds_latest.json"
    fi
  else
    echo "⚠️  ODDS_API_KEY not set — skipping live odds fetch. Using previous odds if available."
    [[ -f "${OUTDIR}/odds_latest.json" ]] || echo "[]" > "${OUTDIR}/odds_latest.json"
  fi
fi

echo
echo "== TODAY: build sog_with_market.csv (and vig-less if odds present) =="
SLATE_DATE="$SLATE_DATE" $PYTHON backend/nhl/scripts/build_sog_with_market.py \
  --pred backend/nhl/data/processed/sog_predictions.csv \
  --names exports/train_nhl_sog_v2.csv \
  --out nhl/site/data/sog_with_market.csv \
  --unmatched nhl/site/data/unmatched_sog.csv

# Build vig-less version (non-fatal if odds missing/empty)
$PYTHON - <<'PY' || true
import json, unicodedata as ud, math
from pathlib import Path
import pandas as pd

def norm(s):
    s=(s or "")
    s=ud.normalize("NFKD",s).encode("ascii","ignore").decode("ascii")
    return " ".join(s.replace("-", " ").replace(".", " ").replace("’","").replace("'","").lower().split())

def a2p(a):
    try: A=float(a)
    except: return float("nan")
    if A==0 or not math.isfinite(A): return float("nan")
    return 100/(A+100) if A>0 else (-A)/((-A)+100)

def p2a(p):
    if not (0<p<1): return ""
    return f"-{round((p/(1-p))*100)}" if p>=0.5 else f"+{round(((1-p)/p)*100)}"

csv=Path("nhl/site/data/sog_with_market.csv")
if not csv.exists(): raise SystemExit
df=pd.read_csv(csv)
if "full_name" not in df.columns and "player" in df.columns:
    df=df.rename(columns={"player":"full_name"})
df["name_norm"]=df.get("full_name","").map(norm)
df["line_str"]=df["line"].astype(str)

raw=None
for p in (Path("nhl/site/data/odds_nhl_playerprops_today.json"), Path("nhl/site/data/odds_latest.json")):
    if p.exists():
        try:
            raw=json.loads(p.read_text())
            break
        except Exception:
            pass
if raw is None:
    print("No odds JSON; skipping vig-less.")
    raise SystemExit

recs=[]
def walk(x):
    if isinstance(x, dict):
        if x.get("key")=="player_shots_on_goal":
            for o in (x.get("outcomes") or []):
                side=o.get("name"); pt=o.get("point")
                desc=o.get("description") or o.get("player") or ""
                price=o.get("price")
                if side in ("Over","Under") and price is not None and pt is not None:
                    recs.append({"name_norm":norm(desc),"line_str":str(pt),"side":side,"price":float(price)})
        for v in x.values():
            walk(v)
    elif isinstance(x, list):
        for it in x:
            walk(it)

walk(raw)
od=pd.DataFrame(recs)
if od.empty:
    print("Odds JSON had no SOG; skipping vig-less.")
    raise SystemExit

med=(od.groupby(["name_norm","line_str","side"],as_index=False)
        .agg(price_median=("price","median"))
        .pivot(index=["name_norm","line_str"],columns="side",values="price_median")
        .reset_index().rename_axis(None,axis=1))
med["p_over_raw"]=med["Over"].map(a2p)
med["p_under_raw"]=med["Under"].map(a2p)
med["sum"]=med["p_over_raw"]+med["p_under_raw"]
med["p_over_vigless"]=med.apply(lambda r: r["p_over_raw"]/r["sum"] if isinstance(r["sum"],float) and r["sum"]>0 else float("nan"),axis=1)
med["fair_over_vigless"]=med["p_over_vigless"].map(p2a)
med=med.rename(columns={"Over":"price_over_median","Under":"price_under_median"})

m=df.merge(med,on=["name_norm","line_str"],how="left")
m["diff_pp"]=(m["p_over"]-m["p_over_vigless"])*100.0

keep=[c for c in [
  "full_name","player_id","game_id","team_id",
  "line","p_over","price_over",
  "price_over_median","price_under_median",
  "p_over_mkt","edge_over",
  "p_over_vigless","fair_over_vigless","diff_pp"
] if c in m.columns]
m[keep].to_csv("nhl/site/data/sog_with_market_vigless.csv",index=False)
print("Wrote nhl/site/data/sog_with_market_vigless.csv")
PY

echo
echo "== TODAY: build saves_with_market.csv =="
mkdir -p nhl/site/data
SLATE_DATE="$SLATE_DATE" $PYTHON backend/nhl/scripts/build_saves_with_market.py \
  --pred backend/nhl/data/processed/saves_predictions.csv \
  --names exports/train_goalie_saves_v2.csv \
  --odds-json nhl/site/data/odds_latest.json \
  --out nhl/site/data/saves_with_market.csv \
  --unmatched nhl/site/data/unmatched_saves.csv

echo
echo "== TODAY: sanity counts =="
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c "
  WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '${SLATE_DATE}')
  SELECT 'games_today'            AS which, COUNT(*) FROM nhl.games                 WHERE game_date = DATE '${SLATE_DATE}'
  UNION ALL SELECT 'roster_rows_today', COUNT(*) FROM nhl.roster_status r           WHERE r.game_id IN (SELECT game_id FROM g)
  UNION ALL SELECT 'sog_stage',         COUNT(*) FROM nhl.predictions_sog_stage s   WHERE s.game_id IN (SELECT game_id FROM g)
  UNION ALL SELECT 'saves_stage',       COUNT(*) FROM nhl.predictions_saves_stage s WHERE s.game_id IN (SELECT game_id FROM g)
  UNION ALL SELECT 'predictions',       COUNT(*) FROM nhl.predictions p             WHERE p.game_id IN (SELECT game_id FROM g);
"

echo
echo "== YDAY: goalie logs =="
SLATE_DATE="$YDAY" retry $PYTHON backend/nhl/scripts/seed_goalie_logs_for_date.py

echo
echo "== YDAY: skater logs (ID-mapped) & promote =="
# Make sure YDAY roster is refreshed before mapping
SLATE_DATE="$YDAY" retry $PYTHON backend/nhl/scripts/refresh_players_and_roster_today.py

# Seed stage → import_skater_logs_stage
SLATE_DATE="$YDAY" retry $PYTHON backend/nhl/scripts/seed_skater_logs_for_date.py

# Promote stage → raw
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c "
WITH src AS (
  SELECT DISTINCT
    s.player_id, s.game_id, s.game_date,
    s.shots_on_goal, s.shot_attempts, s.toi_minutes, s.pp_toi_minutes
  FROM nhl.import_skater_logs_stage s
  WHERE s.game_date = DATE '${YDAY}'
),
rs AS (
  SELECT DISTINCT game_id, team_id, player_id
  FROM nhl.roster_status
  WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date = DATE '${YDAY}')
),
g AS (
  SELECT game_id, home_team_id, away_team_id
  FROM nhl.games
  WHERE game_date = DATE '${YDAY}'
),
joined AS (
  SELECT
    src.player_id,
    src.game_id,
    rs.team_id,
    CASE
      WHEN rs.team_id = g.home_team_id THEN g.away_team_id
      WHEN rs.team_id = g.away_team_id THEN g.home_team_id
      ELSE NULL
    END AS opponent_id,
    (rs.team_id = g.home_team_id) AS is_home,
    src.game_date,
    src.shots_on_goal,
    src.shot_attempts,
    src.toi_minutes,
    src.pp_toi_minutes
  FROM src
  JOIN rs ON rs.game_id = src.game_id AND rs.player_id = src.player_id
  JOIN g  ON g.game_id  = src.game_id
)
INSERT INTO nhl.skater_game_logs_raw
  (player_id, game_id, team_id, opponent_id, is_home, game_date,
   shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes)
SELECT
  player_id, game_id, team_id, opponent_id, is_home, game_date,
  shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes
FROM joined
WHERE opponent_id IS NOT NULL
ON CONFLICT (player_id, game_id) DO UPDATE SET
  team_id        = EXCLUDED.team_id,
  opponent_id    = EXCLUDED.opponent_id,
  is_home        = EXCLUDED.is_home,
  game_date      = EXCLUDED.game_date,
  shots_on_goal  = EXCLUDED.shots_on_goal,
  shot_attempts  = COALESCE(EXCLUDED.shot_attempts, nhl.skater_game_logs_raw.shot_attempts),
  toi_minutes    = EXCLUDED.toi_minutes,
  pp_toi_minutes = EXCLUDED.pp_toi_minutes;
"

# Refresh views/materializations and show counts for YDAY
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/scripts/refresh.sql
psql "$SUPABASE_DB_URL" -F $'\t' -Atqc "
  SELECT 'stage_yday', COUNT(*) FROM nhl.import_skater_logs_stage WHERE game_date = DATE '${YDAY}'
  UNION ALL
  SELECT 'raw_yday',   COUNT(*) FROM nhl.skater_game_logs_raw   WHERE game_date = DATE '${YDAY}';
"

echo
echo "✅ Done. Site files under nhl/site/data/. Enjoy the coffee ☕"
