
# scripts/train_all_models.py

import os
import joblib
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

load_dotenv()

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_ROLE_KEY"]
)

FEATURES = [
    "rolling_result_avg_7",
    "hit_streak",
    "win_streak",
    "is_home",
    "opponent_avg_win_rate",
    "opponent_avg_result_vs_player"
]

def train_model_for_prop(prop_type: str):
    print(f"📥 Fetching data for {prop_type}...")

    response = supabase.table("model_training_props") \
        .select(",".join(FEATURES + ["outcome"])) \
        .eq("prop_type", prop_type) \
        .not_("outcome", "is", None) \
        .execute()

    df = pd.DataFrame(response.data)

    if df.empty:
        print(f"⚠️ No data for {prop_type}, skipping.")
        return

    print(f"🧹 Cleaning {prop_type} data...")
    df = df.dropna(subset=FEATURES + ["outcome"])
    X = df[FEATURES]
    y = df["outcome"].map({"win": 1, "loss": 0})

    if y.nunique() < 2:
        print(f"⚠️ Insufficient outcome variation for {prop_type}, skipping.")
        return

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"🧠 Training model for {prop_type}...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    accuracy = accuracy_score(y_test, model.predict(X_test))
    print(f"✅ {prop_type} model accuracy: {accuracy:.4f}")

    filename = f"models/{prop_type.replace(' ', '_').replace('+', '_plus_')}_model.pkl"
    joblib.dump(model, filename)
    print(f"💾 Saved to {filename}")

PROP_TYPES = [
    "Hits", "Home Runs", "RBIs", "Strikeouts (Pitching)", "Strikeouts (Batting)",
    "Runs Scored", "Walks", "Doubles", "Triples", "Outs Recorded",
    "Earned Runs", "Stolen Bases", "Hits Allowed", "Walks Allowed",
    "Total Bases", "Runs + RBI", "Hits + Runs + RBIs", "Singles"
]

for prop in PROP_TYPES:
    train_model_for_prop(prop)

print("🎉 All models trained.")
