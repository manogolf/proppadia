import pandas as pd
import joblib
import os
import pathlib
import sys

# Ensure the current directory is on the path for local imports
sys.path.append(os.path.dirname(__file__))

from backend.scripts.shared.prop_utils import get_canonical_model_name

# Get absolute path to project root (2 levels up from score_any_prop.py)
MODEL_DIR = pathlib.Path(__file__).resolve().parent.parent / "models"

def predict_prop(prop_type: str, input_data: dict) -> dict:
    over_under = input_data.get("over_under", "under")  # default to under

    # ✅ Get canonical model name
    canonical_type = get_canonical_model_name(prop_type)

    if not canonical_type:
        print(f"❌ Unknown or unsupported prop type: {prop_type}")
        return {"error": f"Unsupported or unknown prop type: {prop_type}"}

    model_path = MODEL_DIR / f"{canonical_type}_model.pkl"
    print(f"📂 Looking for model file: {model_path}")

    if not os.path.exists(model_path):
        print(f"❌ Model file not found: {model_path}")
        return {"error": f"Model not found for prop type: {canonical_type}"}

    try:
        model = joblib.load(model_path)
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        return {"error": "Model load failure"}

    # Prepare input features
    line_diff = input_data['rolling_result_avg_7'] - input_data['prop_value']
    is_home = input_data.get('is_home', 0)
    opponent_encoded = input_data.get('opponent_avg_win_rate', 0)

    features = pd.DataFrame([{
        'line_diff': line_diff,
        'hit_streak': input_data['hit_streak'],
        'win_streak': input_data['win_streak'],
        'is_home': is_home,
        'opponent_encoded': opponent_encoded
    }])

    # Run prediction
    try:
        prob = model.predict_proba(features)[0][1]
        raw_prediction = "win" if prob >= 0.5 else "loss"

        prediction = (
            "win" if over_under == "under" and raw_prediction == "loss"
            else "loss" if over_under == "under"
            else raw_prediction
        )

        return {
            "predicted_outcome": prediction,
            "confidence_score": float(round(prob, 4))
        }

    except Exception as e:
        print(f"❌ Prediction error: {e}")
        return {"error": "Prediction failed"}
