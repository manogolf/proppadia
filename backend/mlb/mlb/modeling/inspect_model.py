# File: backend/scripts/modeling/inspect_model.py

import joblib
import os

MODEL_PATH = "backend/models/rbis_model.pkl"  # change as needed

model = joblib.load(MODEL_PATH)

print("✅ Model type:", type(model))

# Try direct feature inspection
try:
    print("📋 Features:", model.feature_names_in_)
except AttributeError:
    print("⚠️ No direct feature_names_in_ — maybe it's a Pipeline?")

# Try pipeline inspection
try:
    print("🔍 Pipeline steps:", model.named_steps)
    for step_name, step in model.named_steps.items():
        try:
            print(f"📋 {step_name}.feature_names_in_: {step.feature_names_in_}")
        except AttributeError:
            print(f"❌ {step_name} has no feature_names_in_")
except AttributeError:
    print("❌ Not a pipeline")

# Optional: print coefficients
try:
    print("⚖️ Coefficients:", model.coef_)
except:
    pass
