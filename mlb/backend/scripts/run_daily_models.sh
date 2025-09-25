#!/usr/bin/env bash
set -euo pipefail

# ABSOLUTE path to your repo root
REPO="$HOME/Projects/baseball-streaks"
VENV_PY="$REPO/.venv/bin/python"

# Where to write/read models locally
export MODELS_DIR="$REPO/models_out"

# Choose which .env to load: backend/.env.train > .env
if [[ -f "$REPO/backend/.env.train" ]]; then
  DOTENV_FILE="$REPO/backend/.env.train"
else
  DOTENV_FILE="$REPO/.env"
fi

cd "$REPO"

# 1) Train all props quietly (loads env from the chosen file)
"$VENV_PY" -m dotenv -f "$DOTENV_FILE" run -- \
  "$VENV_PY" -m backend.scripts.retrain_all_models --quiet

# 2) Package + upload tarball to Supabase Storage (also loads env)
"$VENV_PY" -m dotenv -f "$DOTENV_FILE" run -- \
  "$VENV_PY" -m backend.scripts.models.package_and_upload

echo "✅ Local train+upload complete."
