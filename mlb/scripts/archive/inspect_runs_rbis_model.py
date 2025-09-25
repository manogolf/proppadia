# inspect_runs_rbis_model.py

import joblib
import pandas as pd

# Load the model
model = joblib.load("backend/models/runs_rbis_model.pkl")

# Construct a sample feature vector (adjust if needed based on your real features)
sample_features = pd.DataFrame([{
    "line_diff": 0.1,
    "hit_streak": 1,
    "win_streak": 1,
    "is_home": 1,
    "opponent_encoded": 0.55
}])

# Predict probabilities
proba = model.predict_proba(sample_features)[0]
print(f"📊 Probabilities: {proba}")
print(f"🔮 Predicted class: {model.predict(sample_features)[0]}")
