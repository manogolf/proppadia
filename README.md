# Proppadia — NHL Props (SOG & Goalie Saves)

Operational notes and runbook for the NHL props pipeline using **Poisson/NegBin** models and **Supabase Session Pooler** for database loads.

---

## What this does

- Trains and scores two pilot NHL props:
  - **Shots on Goal (SOG)** — lines: 0.5, 1.5, 2.5, 3.5
  - **Goalie Saves** — lines: 24.5, 28.5
- Writes predictions to CSVs and **upserts** them into `nhl.predictions` via stage tables.

---

## Repo layout (key paths)

features/feature_metadata_nhl.json # feature map for SOG/Saves
models/latest/shots_on_goal/ # SOG model + MODEL_INDEX.json
models/latest/goalie_saves/ # Saves model + MODEL_INDEX.json
data/processed/nhl_sog_training.csv # input features for SOG scoring
data/processed/nhl_saves_training.csv # input features for Saves scoring
data/processed/sog_predictions.csv # output predictions (wide)
data/processed/saves_predictions.csv # output predictions (wide)
scripts/score_nhl_props.py # scorer (both props)
scripts/train_nhl_sog.py # trainer for SOG
scripts/train_nhl_saves.py # trainer for Saves
scripts/load_predictions_pooler.py # loader (pooler-safe inserts + upsert RPC)
scripts/nhl_daily.sh # one-command daily runner
db_url.txt # Session Pooler URL (not committed)
