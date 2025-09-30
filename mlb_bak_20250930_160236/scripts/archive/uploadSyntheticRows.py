import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import json

load_dotenv()

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)

print("📥 Loading scripts/data/synthetic_backfill_props.csv...")
df = pd.read_csv("scripts/data/synthetic_backfill_props.csv")
print(f"📊 Initial row count: {len(df)}")

# Clean all columns: convert inf/-inf and NaN to None
df = df.replace([np.inf, -np.inf], np.nan).astype(object).where(pd.notnull(df), None)

# Drop rows missing essential fields
required_fields = ["id", "player_name", "prop_type", "outcome"]
df = df.dropna(subset=required_fields)

from datetime import datetime

# Build clean records
rows = []
for _, row in df.iterrows():
    row = row.to_dict()

    # Remove invalid field if exists
    row.pop("created_et", None)

    # Fix game_time field: convert to full timestamp
    if row.get("game_time") and row.get("game_date"):
        try:
            full_ts = f"{row['game_date']}T{row['game_time']}:00"
            # Validate format
            datetime.fromisoformat(full_ts)
            row["game_time"] = full_ts
        except Exception as e:
            print(f"⚠️ Skipping row with bad game_time: {row['game_time']}")
            continue

    rows.append(row)


print(f"📉 Cleaned row count: {len(rows)}")

if rows:
    print("✅ Sample cleaned row:")
    print(json.dumps(rows[0], indent=2))

print(f"📤 Uploading {len(rows)} rows to Supabase...")
res = supabase.table("model_training_props").upsert(rows).execute()

if res.data:
    print("✅ Upload complete.")
    print(f"🔢 Inserted rows: {len(res.data)}")
else:
    print("⚠️ Upload returned no data or failed silently.")



