# Run from project root:
# python3 src/scripts/batch_enrich_props.py

import pandas as pd
import os

# Load your full training dataset
df = pd.read_csv("data/model_training_props.csv")
df['game_date'] = pd.to_datetime(df['game_date'])

# Get unique prop types (from your Supabase table or this data)
prop_types = df['prop_type'].dropna().unique()

# Enrichment functions
def compute_streaks(series, condition_func):
    streak = 0
    out = []
    for val in series:
        if condition_func(val):
            streak += 1
        else:
            streak = 0
        out.append(streak)
    return out

# Create output folder
os.makedirs("by_prop_type", exist_ok=True)

for prop_type in prop_types:
    prop_df = df[df['prop_type'] == prop_type].copy()
    if prop_df.empty:
        continue

    try:
        # Sort by player + date
        prop_df = prop_df.sort_values(['player_name', 'game_date'])

        # Rolling average (shifted to avoid leakage)
        prop_df['rolling_result_avg_7'] = (
            prop_df.groupby('player_name')['result']
            .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
        )

        # Hit streak: result > 0
        prop_df['hit_streak'] = (
            prop_df.groupby('player_name')['result']
            .transform(lambda x: compute_streaks(x, lambda v: v > 0))
        )

        # Win streak: outcome == 'win' (shifted)
        prop_df['win_streak'] = (
            prop_df.groupby('player_name')['outcome']
            .transform(lambda x: compute_streaks(x, lambda v: v == 'win'))
            .shift(1).fillna(0).astype(int)
        )

        # line_diff = hot/cold signal
        prop_df['line_diff'] = prop_df['rolling_result_avg_7'] - prop_df['prop_value']

        # Save to file
        out_path = f"by_prop_type/{prop_type}.csv"
        prop_df.to_csv(out_path, index=False)
        print(f"✅ Enriched: {out_path} ({len(prop_df)} rows)")

    except Exception as e:
        print(f"❌ Failed on {prop_type}: {e}")
