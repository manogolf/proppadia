# backend/scripts/train_model_from_csv.py

import sys
import os
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score

# Usage: python train_model_from_csv.py data.csv prop_type
csv_file = sys.argv[1]
prop_type = sys.argv[2]

df = pd.read_csv(csv_file)

# Convert or fill in missing features
df["line_diff"] = (
    pd.to_numeric(df.get("rolling_result_avg_7", 0), errors="coerce").fillna(0)
    - pd.to_numeric(df.get("prop_value", 0), errors="coerce").fillna(0)
)

df["hit_streak"] = pd.to_numeric(df.get("hit_streak", 0), errors="coerce").fillna(0)
df["win_streak"] = pd.to_numeric(df.get("win_streak", 0), errors="coerce").fillna(0)
df["is_home"] = pd.to_numeric(df.get("is_home", 0), errors="coerce").fillna(0)

# Handle opponent_encoded safely
if "opponent_encoded" in df.columns:
    df["opponent_encoded"] = pd.to_numeric(df["opponent_encoded"], errors="coerce")
else:
    df["opponent_encoded"] = 0  # Replace with encoded 'opponent' string if needed

# Normalize outcome
df["outcome"] = df["outcome"].str.strip().str.lower()

# Drop rows missing required features
df.dropna(subset=["line_diff", "hit_streak", "win_streak", "is_home", "opponent_encoded", "outcome"], inplace=True)

# Prepare features and labels
X = df[["line_diff", "hit_streak", "win_streak", "is_home", "opponent_encoded"]]
y = df["outcome"].map({"win": 1, "loss": 0})

if y.nunique() < 2:
    raise ValueError("Outcome imbalance: model needs both win and loss samples.")

# Train/test split and model training
X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.2, random_state=42)
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Accuracy report
acc = accuracy_score(y_test, model.predict(X_test))
print(f"✅ {prop_type} model accuracy: {acc:.3f}")

# Save model
output_path = f"models/{prop_type}_model.pkl"
os.makedirs("models", exist_ok=True)
joblib.dump(model, output_path)
print(f"💾 Saved model to: {output_path}")
