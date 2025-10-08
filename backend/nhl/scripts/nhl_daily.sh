
# What it does:
# 1) Scores SOG and Saves → writes `sog_predictions.csv` and `saves_predictions.csv`  
# 2) Loads to Supabase via **Session Pooler**, inserting into stage and calling:
#   - `nhl.load_sog_predictions_from_stage(...)`
#   - `nhl.load_saves_predictions_from_stage(...)`


# NHL daily runner — scores SOG/Saves, then loads via Supabase *Session Pooler* URL.
# Put your Session Pooler URI (from Supabase UI) in: <project>/db_url.txt
#   e.g. postgresql://postgres.<project_ref>:PASSWORD@aws-0-<region>.pooler.supabase.com:5432/postgres?sslmode=require

# ✅ Wrote predictions to: .../sog_predictions.csv
# ✅ Wrote predictions to: .../saves_predictions.csv
# 🔌 Loading to Supabase via Session Pooler…
# 🧹 Truncating nhl.predictions_sog_stage …
# 📥 Inserting N SOG stage rows …
# 🚀 Upserting SOG to nhl.predictions …
# ✅ (N_lines * rows)
# ...
# 🎉 Done. Upserts complete via pooler.

set -euo pipefail

# --- paths ---
PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$PROJ/.venv/bin/activate"

FEATURE_JSON="$PROJ/features/feature_metadata_nhl.json"

SOG_MODEL_DIR="$PROJ/models/latest/shots_on_goal"
SAVES_MODEL_DIR="$PROJ/models/latest/goalie_saves"

SOG_INPUT="$PROJ/data/processed/nhl_sog_training.csv"      # features input
SAVES_INPUT="$PROJ/data/processed/nhl_saves_training.csv"  # features input

SOG_OUT="$PROJ/data/processed/sog_predictions.csv"
SAVES_OUT="$PROJ/data/processed/saves_predictions.csv"

DB_URL_FILE="$PROJ/db_url.txt"  # put your Session Pooler URI in here

echo "🏒 NHL daily run"
echo "• Project: $PROJ"
echo "• SOG in : $SOG_INPUT"
echo "• Saves in: $SAVES_INPUT"

# --- env / venv ---
if [[ ! -f "$VENV" ]]; then
  echo "❌ Missing venv at $VENV"; exit 1
fi
# shellcheck disable=SC1090
source "$VENV"

# --- score SOG ---
python "$PROJ/scripts/score_nhl_props.py" \
  --model-dir "$SOG_MODEL_DIR" \
  --csv "$SOG_INPUT" \
  --feature-json "$FEATURE_JSON" \
  --feature-key shots_on_goal \
  --line 0.5,1.5,2.5,3.5 \
  --out "$SOG_OUT"

# --- score Saves ---
python "$PROJ/scripts/score_nhl_props.py" \
  --model-dir "$SAVES_MODEL_DIR" \
  --csv "$SAVES_INPUT" \
  --feature-json "$FEATURE_JSON" \
  --feature-key goalie_saves \
  --line 24.5,28.5 \
  --out "$SAVES_OUT"

# --- DB load via Session Pooler URL ---
DB_URL="${SUPABASE_DB_URL:-}"
if [[ -z "$DB_URL" && -f "$DB_URL_FILE" ]]; then
  # strip trailing newline(s)
  DB_URL="$(tr -d '\r\n' < "$DB_URL_FILE")"
fi

if [[ -n "$DB_URL" ]]; then
  echo "🔌 Loading to Supabase via Session Pooler…"
  python "$PROJ/scripts/load_predictions_pooler.py" \
    --project "$PROJ" \
    --db-url "$DB_URL" \
    --sog-csv "$SOG_OUT" \
    --saves-csv "$SAVES_OUT"
  echo "✅ Done."
else
  echo "ℹ️ No DB URL provided."
  echo "   Put your Session Pooler URI in: $DB_URL_FILE"
  echo "   or export SUPABASE_DB_URL to enable auto-load."
fi
