import pandas as pd
import os

input_dir = "by_prop_type"
output_suffix = "_with_streaks.csv"

def compute_streaks(series, condition_func):
    streak = 0
    streaks = []
    for val in series:
        if condition_func(val):
            streak += 1
        else:
            streak = 0
        streaks.append(streak)
    return streaks

for filename in os.listdir(input_dir):
    if not filename.endswith(".csv"):
        continue

    filepath = os.path.join(input_dir, filename)
    print(f"Processing: {filename}")

    df = pd.read_csv(filepath)

    if 'player_name' not in df.columns or 'result' not in df.columns or 'game_date' not in df.columns:
        print(f"Skipping {filename}: missing required columns")
        continue

    # Ensure datetime
    df['game_date'] = pd.to_datetime(df['game_date'])
    df = df.sort_values(['player_name', 'game_date'])

    # Rolling 7-game average (shifted to avoid leakage)
    df['rolling_result_avg_7'] = (
        df.groupby('player_name')['result']
        .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
    )

    # Hit streak: result > 0
    df['hit_streak'] = (
        df.groupby('player_name')['result']
        .transform(lambda x: compute_streaks(x, lambda val: val > 0))
    )

    # Win streak: outcome == 'win'
    if 'outcome' in df.columns:
        df['win_streak'] = (
            df.groupby('player_name')['outcome']
            .transform(lambda x: compute_streaks(x, lambda val: val == 'win'))
            .shift(1)
            .fillna(0)
            .astype(int)
        )
    else:
        df['win_streak'] = None  # Safeguard if 'outcome' missing

    # Save output
    output_path = os.path.join(input_dir, filename.replace(".csv", output_suffix))
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path} ({len(df)} rows)")
