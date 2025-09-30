import pandas as pd

# Load hits prop data
df = pd.read_csv("by_prop_type/Hits.csv")

# Ensure date is datetime
df['game_date'] = pd.to_datetime(df['game_date'])

# Sort by player and date
df = df.sort_values(['player_name', 'game_date'])

# Compute rolling features
df['rolling_hits_avg_7'] = (
    df.groupby('player_name')['result']
    .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
)

# Save enriched version
df.to_csv("by_prop_type/Hits_with_rolling.csv", index=False)
print("Saved: Hits_with_rolling.csv with rolling features.")
