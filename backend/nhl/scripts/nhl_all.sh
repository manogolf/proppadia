#!/usr/bin/env bash
set -euo pipefail

# =========================
# NHL all-in-one daily run
# =========================
# What it does:
# - Imports TODAY schedule & roster, seeds features, exports CSVs
# - Scores & builds site CSV (includes Odds API fetch + vig-less build)
# - Finalizes YDAY goalie+skater logs and promotes to raw
#
# Required env:
#   SUPABASE_DB_URL   -> your Postgres URL (no quotes)
#   ODDS_API_KEY     5d2de2712453ef000413ca071a812191
#
# Optional env:
#   PYTHON            -> python interpreter (default python3)

# -------- Config / Env guards --------
: "${SUPABASE_DB_URL:?Set SUPABASE_DB_URL to your Postgres URL}"
export PSYCOPG_DISABLE_PREPARES=1
PYTHON="${PYTHON:-python3}"

# Harden DB URL flags (avoid DNS/SSL funk)
case "$SUPABASE_DB_URL" in
  *sslmode=* ) : ;;
  *\?* ) SUPABASE_DB_URL="${SUPABASE_DB_URL}&sslmode=require" ;;
  *   ) SUPABASE_DB_URL="${SUPABASE_DB_URL}?sslmode=require" ;;
esac
case "$SUPABASE_DB_URL" in
  *gssencmode=* ) : ;;
  *\?* ) SUPABASE_DB_URL="${SUPABASE_DB_URL}&gssencmode=disable" ;;
  *   ) SUPABASE_DB_URL="${SUPABASE_DB_URL}?gssencmode=disable" ;;
esac
export SUPABASE_DB_URL

# -------- Dates (ET) --------
if TZ=America/New_York date -v-1d +%F >/dev/null 2>&1; then
  # macOS/BSD date(1)
  SLATE_DATE="${SLATE_DATE:-$(TZ=America/New_York date +%F)}"
  YDAY="$(TZ=America/New_York date -v-1d +%F)"
else
  # GNU date(1)
  SLATE_DATE="${SLATE_DATE:-$(TZ=America/New_York date +%F)}"
  YDAY="$(TZ=America/New_York date -d 'yesterday' +%F)"
fi
echo "SLATE_DATE (ET): ${SLATE_DATE}    YDAY (ET): ${YDAY}"

# -------- Helpers --------
retry() {
  # retry CMD up to 3 times with backoff
  local n=1 max=3
  until "$@"; do
    if (( n == max )); then return 1; fi
    echo "Retry $n/$max failed: $* — sleeping $((n*10))s..."
    sleep $((n*10)); ((n++))
  done
}

guard_psql() {
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -q -c "select now();" >/dev/null
}

mkdir -p exports nhl/site/data backend/nhl/data/processed

echo "Checking DB connectivity..."
retry guard_psql
echo "DB OK."

# =========================
# 1) TODAY: ingest + seed
# =========================
echo ""
echo "== TODAY: import schedule & roster =="
retry $PYTHON backend/nhl/scripts/import_schedule_today.py

# Ensure players and roster_status actually update from API
SKIP_ROSTER_STATUS=0 SKIP_PLAYERS=0 retry $PYTHON backend/nhl/scripts/import_roster_today.py
retry $PYTHON backend/nhl/scripts/refresh_players_and_roster_today.py

echo "== TODAY: seed features (SOG + goalies) =="
retry psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/seed_sog_features_for_slate.sql
retry psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/seed_goalie_features_for_slate.sql

# Keep views/materializations fresh
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/scripts/refresh.sql >/dev/null

echo "== TODAY: export training joins (SOG + goalies) =="
psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_sog.sql > exports/train_nhl_sog_v2.csv

psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_saves.sql > exports/train_goalie_saves_v2.csv

# Guard: ensure SOG export is for SLATE_DATE
awk -F, -v want="$SLATE_DATE" '
  NR==1 { for(i=1;i<=NF;i++) if($i=="game_date") c=i; next }
  NR>1 { d[$c]=1 }
  END {
    if (!c) { print "FATAL: no game_date column" >"/dev/stderr"; exit 2 }
    if (length(d)==0) { print "FATAL: export has no rows" >"/dev/stderr"; exit 2 }
    for (k in d) if (k!=want) { print "FATAL: export contains " k " (want " want ")" >"/dev/stderr"; exit 2 }
  }
' exports/train_nhl_sog_v2.csv

echo "== TODAY: score & attach names =="
retry $PYTHON backend/nhl/scripts/run_daily_slate.py \
  --project nhl \
  --sog-csv exports/train_nhl_sog_v2.csv \
  --saves-csv exports/train_goalie_saves_v2.csv \
  --db-url "$SUPABASE_DB_URL" \
  --scorer "$PWD/backend/nhl/scripts/score_nhl_props.py"

$PYTHON backend/nhl/scripts/attach_names.py || true

# =========================
# 2) TODAY: Odds fetch + build site CSVs
# =========================
echo "== TODAY: fetch player props odds (SOG + Saves) =="
OUTDIR="nhl/site/data"
MARKETS="player_shots_on_goal,player_total_saves"
REGIONS="us"
FORMAT="american"
mkdir -p "$OUTDIR"

# Ensure jq
if ! command -v jq >/dev/null 2>&1; then
  echo "Installing jq (sudo may prompt) ..."
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -y >/dev/null && sudo apt-get install -y jq >/dev/null
  else
    echo "jq not found and automatic install unavailable. Please install jq." >&2
    exit 1
  fi
fi

if [[ -n "${ODDS_API_KEY:-}" ]]; then
  echo "Fetching NHL events…"
  curl -fsS "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events?dateFormat=iso&daysFrom=1&apiKey=${ODDS_API_KEY}" \
    -o "${OUTDIR}/events_today.json"

  EIDS=$(jq -r '.[].id' "${OUTDIR}/events_today.json" || true)
  if [[ -z "${EIDS}" ]]; then
    echo "⚠️  No events in events_today.json — writing empty odds file."
    echo "[]" > "${OUTDIR}/odds_nhl_playerprops_today.json"
    cp -f "${OUTDIR}/odds_nhl_playerprops_today.json" "${OUTDIR}/odds_latest.json"
  else
    echo "${EIDS}" | \
      xargs -I{} -n1 -P4 bash -c '
        EID="$1"; OUTDIR="$2"; MARKETS="$3"; REGIONS="$4"; FORMAT="$5"; KEY="$6"
        for n in 1 2 3; do
          if curl -fsS "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events/${EID}/odds?regions=${REGIONS}&markets=${MARKETS}&oddsFormat=${FORMAT}&apiKey=${KEY}"; then
            exit 0
          fi
          sleep $((n*2))
        done
        curl -sS "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events/${EID}/odds?regions=${REGIONS}&markets=${MARKETS}&oddsFormat=${FORMAT}&apiKey=${KEY}" || echo "{}"
      ' _ {} "${OUTDIR}" "${MARKETS}" "${REGIONS}" "${FORMAT}" "${ODDS_API_KEY}" \
      | jq -s 'map(select(type=="object"))' > "${OUTDIR}/odds_nhl_playerprops_today.json"

    cp -f "${OUTDIR}/odds_nhl_playerprops_today.json" "${OUTDIR}/odds_latest.json"
  fi
  echo "Wrote ${OUTDIR}/odds_nhl_playerprops_today.json"
else
  echo "⚠️  ODDS_API_KEY not set — skipping odds fetch. (build will still run)"
  # ensure a fallback exists if previous run didn’t
  [[ -f "${OUTDIR}/odds_latest.json" ]] || echo "[]" > "${OUTDIR}/odds_latest.json"
fi

echo "== TODAY: build sog_with_market.csv (and vig-less if odds present) =="
$PYTHON backend/nhl/scripts/build_sog_with_market.py \
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
    s=(s or ""); s=ud.normalize("NFKD",s).encode("ascii","ignore").decode("ascii")
    return " ".join(s.replace("-", " ").replace(".", " ").replace("’","").replace("'","").lower().split())
def a2p(a):
    try: A=float(a)
    except: return float("nan")
    return 100/(A+100) if A>0 else (-A)/((-A)+100) if A<0 else float("nan")
def p2a(p): 
    return f"-{round((p/(1-p))*100)}" if 0<p<1 and p>=0.5 else (f"+{round(((1-p)/p)*100)}" if 0<p<1 else "")
csv=Path("nhl/site/data/sog_with_market.csv")
if not csv.exists(): raise SystemExit
df=pd.read_csv(csv)
if "full_name" not in df and "player" in df: df=df.rename(columns={"player":"full_name"})
df["name_norm"]=df.get("full_name","").map(norm); df["line_str"]=df["line"].astype(str)
raw=None
for p in (Path("nhl/site/data/odds_nhl_playerprops_today.json"), Path("nhl/site/data/odds_latest.json")):
    if p.exists():
        try: raw=json.loads(p.read_text()); break
        except: pass
if raw is None: print("No odds JSON; skipping vig-less."); raise SystemExit
recs=[]
def walk(x):
    if isinstance(x, dict):
        if x.get("key")=="player_shots_on_goal":
            for o in x.get("outcomes") or []:
                side=o.get("name"); pt=o.get("point"); desc=o.get("description") or o.get("player") or ""; price=o.get("price")
                if side in ("Over","Under") and price is not None and pt is not None:
                    recs.append({"name_norm":norm(desc),"line_str":str(pt),"side":side,"price":float(price)})
        for v in x.values(): walk(v)
    elif isinstance(x, list):
        for it in x: walk(it)
walk(raw)
import pandas as pd
od=pd.DataFrame(recs)
if od.empty: print("Odds JSON had no SOG; skipping vig-less."); raise SystemExit
med=(od.groupby(["name_norm","line_str","side"],as_index=False).agg(price_median=("price","median"))
        .pivot(index=["name_norm","line_str"],columns="side",values="price_median").reset_index().rename_axis(None,axis=1))
med["p_over_raw"]=med["Over"].map(a2p); med["p_under_raw"]=med["Under"].map(a2p)
med["sum"]=med["p_over_raw"]+med["p_under_raw"]
med["p_over_vigless"]=med.apply(lambda r: r["p_over_raw"]/r["sum"] if isinstance(r["sum"],float) and r["sum"]>0 else float("nan"),axis=1)
med["fair_over_vigless"]=med["p_over_vigless"].map(p2a)
med=med.rename(columns={"Over":"price_over_median","Under":"price_under_median"})
m=df.merge(med,on=["name_norm","line_str"],how="left")
m["diff_pp"]=(m["p_over"]-m["p_over_vigless"])*100.0
keep=[c for c in ["full_name","player_id","game_id","team_id","line","p_over","price_over",
                  "price_over_median","price_under_median","p_over_mkt","edge_over",
                  "p_over_vigless","fair_over_vigless","diff_pp"] if c in m.columns]
m[keep].to_csv("nhl/site/data/sog_with_market_vigless.csv",index=False)
print("Wrote nhl/site/data/sog_with_market_vigless.csv")
PY

echo "== TODAY: sanity counts =="
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c "
  WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '${SLATE_DATE}')
  SELECT 'games_today'            AS which, COUNT(*) FROM nhl.games                 WHERE game_date = DATE '${SLATE_DATE}'
  UNION ALL SELECT 'roster_rows_today', COUNT(*) FROM nhl.roster_status r           WHERE r.game_id IN (SELECT game_id FROM g)
  UNION ALL SELECT 'sog_stage',         COUNT(*) FROM nhl.predictions_sog_stage s   WHERE s.game_id IN (SELECT game_id FROM g)
  UNION ALL SELECT 'saves_stage',       COUNT(*) FROM nhl.predictions_saves_stage s WHERE s.game_id IN (SELECT game_id FROM g)
  UNION ALL SELECT 'predictions',       COUNT(*) FROM nhl.predictions p             WHERE p.game_id IN (SELECT game_id FROM g);
"

# =========================
# 3) YDAY: logs & promote
# =========================
echo ""
echo "== YDAY: goalie logs =="
SLATE_DATE="$YDAY" retry $PYTHON backend/nhl/scripts/seed_goalie_logs_for_date.py || true

echo "== YDAY: skater logs (ID-mapped) & promote =="
SLATE_DATE="$YDAY" retry $PYTHON backend/nhl/scripts/seed_skater_logs_for_date.py || true

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

psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/scripts/refresh.sql >/dev/null

echo "== YDAY: stage vs raw counts =="
psql "$SUPABASE_DB_URL" -F $'\t' -Atqc "
  SELECT 'stage_yday', COUNT(*) FROM nhl.import_skater_logs_stage WHERE game_date = DATE '${YDAY}'
  UNION ALL
  SELECT 'raw_yday',   COUNT(*) FROM nhl.skater_game_logs_raw   WHERE game_date = DATE '${YDAY}';
"

echo ""
echo "✅ Done. Site files under nhl/site/data/. Enjoy the coffee ☕"
