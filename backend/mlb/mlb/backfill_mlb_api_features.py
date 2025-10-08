# backend/scripts/backfill_mlb_api_features.py

"""
Back-fill missing feature columns for mlb_api rows in model_training_props.

Fills:
  • line_diff
  • hit_streak, win_streak       (from player_streak_profiles)
  • is_home, opponent, opponent_encoded
  • opponent_avg_win_rate        (via live standings)
Run hourly (or daily) until ready_for_training rows are above threshold.
"""

import os
import pandas as pd
from supabase import create_client
from shared.mlbUtils import getTeamWinRates   # ← you just created this

# ── Supabase ───────────────────────────────────────────────────────
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)

# ── 1 · Pull unresolved mlb_api rows needing line_diff ─────────────
resp = (
    supabase.table("model_training_props")
    .select("*")
    .match({"prop_source": "mlb_api"})
    .in_("status", ["win", "loss"])
    .is_("line_diff", "null")
    .limit(2000)
    .execute()
)

df = pd.DataFrame(resp.data or [])
if df.empty:
    print("Nothing to back-fill."); exit()

# ── 2 · Compute line_diff (result – prop_value) ────────────────────
df["line_diff"] = df["result"] - df["prop_value"]

# ── 3 · Merge hit/win streaks from player_streak_profiles ──────────
ids = df[["player_id", "prop_type"]].drop_duplicates()
streak_df = (
    supabase.table("player_streak_profiles")
    .select("player_id, prop_type, hit_streak, win_streak")
    .in_("player_id", ids["player_id"].tolist())
    .in_("prop_type", ids["prop_type"].tolist())
    .eq("prop_source", "mlb_api")
    .execute()
)
streak_df = pd.DataFrame(streak_df.data or [])

df = df.merge(streak_df, on=["player_id", "prop_type"], how="left")

# ── 4 · Derive is_home, opponent, opponent_encoded ────────────────
def derive_opponent_info(row):
    # assumes team + opponent columns already exist in row; replace with proper lookup!
    player_team = row.get("team")
    opponent    = row.get("opponent")
    is_home     = row.get("home_away") == "home" if row.get("home_away") else None
    return pd.Series([is_home, opponent])

df[["is_home", "opponent"]] = df.apply(derive_opponent_info, axis=1)
df["opponent_encoded"] = df["opponent"].astype("category").cat.codes

# ── 5 · Map opponent win-rates ─────────────────────────────────────
team_win_rates = getTeamWinRates()            # { 'ATL': 0.593, ... }
df["opponent_avg_win_rate"] = df["opponent"].map(team_win_rates).round(3)

# ── 6 · Ensure feature columns are present ─────────────────────────
feature_cols = [
    "line_diff", "hit_streak", "win_streak",
    "is_home", "opponent_encoded", "opponent_avg_win_rate",
]
for col in feature_cols:
    if col not in df.columns:
        df[col] = pd.NA

print("✅ Non-null feature counts:")
print(df[feature_cols].notna().sum())

# ── 7 · Keep only fully-populated rows; upsert back ────────────────
df_ready = df.dropna(subset=feature_cols)
if df_ready.empty:
    print("No fully populated rows to upsert."); exit()

rows = df_ready.where(pd.notnull(df_ready), None).to_dict(orient="records")

supabase.table("model_training_props").upsert(
    rows,
    on_conflict="id",
).execute()

print(f"Back-filled {len(rows)} mlb_api rows.")
