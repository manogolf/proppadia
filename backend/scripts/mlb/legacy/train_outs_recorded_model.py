import pandas as pd
import joblib
import os
from supabase import create_client, Client
from sklearn.ensemble import RandomForestClassifier

# --- Config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
MODEL_OUTPUT_PATH = "models/Outs Recorded_model.pkl"

# --- Supabase client ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Fetch data ---
def fetch_outs_recorded_data():
    response = supabase.table("model_training_props").select("*").eq("prop_type", "Outs Recorded").in_("outcome", ["win", "loss"]).limit(1000).execute()
    return pd.DataFrame(response.data)

# --- Train model ---
def train_model(df: pd.DataFrame):
    df['line_diff'] = df['rolling_result_avg_7'] - df['prop_value']

    if 'hit_streak' not in df.columns:
        df['hit_streak'] = 0
    if 'win_streak' not in df.columns:
        df['win_streak'] = 0
    if 'is_home' not in df.columns:
        df['is_home'] = 0

    if 'opponent_avg_win_rate' in df.columns:
        df['opponent_encoded'] = df['opponent_avg_win_rate'].fillna(0.5)
    else:
        print("⚠️ 'opponent_avg_win_rate' missing — defaulting to 0.5")
        df['opponent_encoded'] = 0.5

    features = ['line_diff', 'hit_streak', 'win_streak', 'is_home', 'opponent_encoded']
    X = df[features]
    y = df['outcome'].map({'win': 1, 'loss': 0})

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)
    return model

# --- Main ---
def main():
    print("📦 Fetching data for 'Outs Recorded'...")
    df = fetch_outs_recorded_data()
    if df.empty:
        print("⚠️ No data found for 'Outs Recorded'.")
        return

    print(f"✅ {len(df)} rows loaded.")
    model = train_model(df)

    print(f"💾 Saving model to: {MODEL_OUTPUT_PATH}")
    joblib.dump(model, MODEL_OUTPUT_PATH)
    print("✅ Model training complete.")

if __name__ == "__main__":
    main()
