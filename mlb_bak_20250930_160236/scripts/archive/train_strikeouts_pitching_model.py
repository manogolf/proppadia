import pandas as pd
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from pathlib import Path

print("🚨 Running training script from:", __file__)  # ✅ Place this at the top

# Load CSV
df = pd.read_csv("scripts/data/strikeouts_pitching_training.csv")
df = df[df["outcome"].isin(["win", "loss"])]

# Compute features that match predict()
df["line_diff"] = df["rolling_result_avg_7"] - df["prop_value"]
df["opponent_encoded"] = df["opponent_avg_win_rate"].fillna(0.5)

features = [
    "line_diff",
    "hit_streak",
    "win_streak",
    "is_home",
    "opponent_encoded"
]

X = df[features]
y = df["outcome"].map({"win": 1, "loss": 0})

print("📊 Training on features:", X.columns.tolist())  # ✅ Right after X is defined

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

Path("backend/models").mkdir(parents=True, exist_ok=True)
with open("backend/models/strikeouts_pitching_model.pkl", "wb") as f:
    pickle.dump(clf, f)

print("✅ strikeouts_pitching_model.pkl saved with correct features.")
