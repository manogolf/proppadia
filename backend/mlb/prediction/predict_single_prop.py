# File: backend/scripts/prediction/predict_single_prop.py

import os
import sys
import json
import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from build_feature_vector import build_feature_vector, load_feature_spec

# ───── Load environment variables ─────
load_dotenv()

# ───── Load input ─────
input_data = json.loads(sys.argv[1])
prop_type = input_data.get("prop_type")
features = input_data.get("features")

if not prop_type or not features:
    print(json.dumps({"error": "Missing prop_type or features in input."}))
    sys.exit(1)

# ───── Load feature spec ─────
spec = load_feature_spec()


# ───── Build full feature vector ─────
try:
    transformed = build_feature_vector(features)
except Exception as e:
    print(json.dumps({"error": f"Feature vector transformation failed: {str(e)}"}))
    sys.exit(1)

# ───── Prepare model paths ─────
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
model_dir = os.path.join(project_root, "backend/models", prop_type)
rf_model_path = os.path.join(model_dir, f"{prop_type}_random_forest.pkl")
log_model_path = os.path.join(model_dir, f"{prop_type}_logistic_regression.pkl")

# ───── Validate model existence ─────
if not os.path.exists(rf_model_path) or not os.path.exists(log_model_path):
    print(json.dumps({"error": f"Model(s) not found for prop type: {prop_type}"}))
    sys.exit(1)

# ───── Load models ─────
rf_model = joblib.load(rf_model_path)
log_model = joblib.load(log_model_path)

# ───── Predict ─────
X = pd.DataFrame([transformed])
try:
    rf_pred = rf_model.predict_proba(X)[0][1]
    log_pred = log_model.predict_proba(X)[0][1]
    hybrid_pred = (rf_pred + log_pred) / 2
except Exception as e:
    print(json.dumps({"error": f"Prediction failed: {str(e)}"}))
    sys.exit(1)

# ───── Return result ─────
print(json.dumps({
    "prop_type": prop_type,
    "random_forest": rf_pred,
    "logistic_regression": log_pred,
    "hybrid_prediction": hybrid_pred
}))
