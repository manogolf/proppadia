import json
import pandas as pd
import os

# Load JSON data
with open("data/historical_player_props.json", "r") as f:
    data = json.load(f)

# Convert to DataFrame
df = pd.DataFrame(data)

# Filter out rows with 'push' outcome or missing prop_type
df = df[df['outcome'].isin(['win', 'loss'])]
df = df.dropna(subset=['prop_type'])

# Save full cleaned CSV
df.to_csv("model_training_props.csv", index=False)
print(f"Saved cleaned full CSV with {len(df)} rows.")

# Create output directory
output_dir = "by_prop_type"
os.makedirs(output_dir, exist_ok=True)

# Split and save each prop_type
for prop in df['prop_type'].unique():
    prop_df = df[df['prop_type'] == prop]
    filename = f"{output_dir}/{prop.replace(' ', '_')}.csv"
    prop_df.to_csv(filename, index=False)
    print(f"Saved: {filename} ({len(prop_df)} rows)")
