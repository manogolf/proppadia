# File: backend/scripts/modeling/test_rf_predict.py
import joblib
import pandas as pd
import os

MODEL_PATH = "backend/models/hits_model.pkl"

model = joblib.load(MODEL_PATH)

# ✅ Match feature order exactly
features = pd.DataFrame([{
    "line_diff": 0.5,
    "hit_streak": 3,
    "win_streak": 1,
    "is_home": 1,
    "opponent_encoded": 12
}])

print("📊 Features:", features.columns.tolist())

try:
    prediction = model.predict(features)[0]
    prob = model.predict_proba(features)[0][1]
    print(f"✅ Prediction: {prediction} | Prob: {round(prob, 4)}")
except Exception as e:
    print(f"❌ Prediction failed: {e}")
