# Run this script from the project root:
# python3 src/scripts/rebuild_hits_with_context.py

import pandas as pd
import os

# Load full dataset with enriched context
df = pd.read_csv("data/model_training_props.csv")

# Filter to 'Hits' prop type
df = df[df['prop_type'] == 'Hits'].copy()

# Convert game_date
df['game_date'] = pd.to_datetime(df['game_date'])

# Drop rows missing core fields
required_cols = ['player_name', 'game_date', 'result', 'prop_value', 'outcome']
optional_cols = ['home_away', 'opponent']
existing_cols = [col for col in required_cols + optional_cols if col in df.columns]

df = df.dropna(subset=existing_cols)


# Sort
df = df.sort_values(['player_name', 'game_date'])

# Rolling 7-game result average (shifted)
df['rolling_result_avg_7'] = (
    df.groupby('player_name')['result']
    .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
)

# Streak logic
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

# Hit streak
df['hit_streak'] = (
    df.groupby('player_name')['result']
    .transform(lambda x: compute_streaks(x, lambda v: v > 0))
)

# Win streak (non-leaky)
df['win_streak'] = (
    df.groupby('player_name')['outcome']
    .transform(lambda x: compute_streaks(x, lambda v: v == 'win'))
    .shift(1)
    .fillna(0)
    .astype(int)
)

# Save to by_prop_type/
os.makedirs("by_prop_type", exist_ok=True)
df.to_csv("by_prop_type/Hits_with_streaks.csv", index=False)
print(f"✅ Saved: by_prop_type/Hits_with_streaks.csv ({len(df)} rows)")
