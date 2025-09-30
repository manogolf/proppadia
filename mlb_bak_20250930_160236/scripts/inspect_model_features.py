# File: scripts/inspect_model_features.py

import joblib
import os
import json

MODEL_DIR = "backend/models"  # or wherever your compressed models live
OUTPUT_PATH = "backend/scripts/modeling/feature_metadata.json"

PROP_TYPES = [
     "hits", "singles", "doubles", "triples", "home_runs",
    "total_bases", "rbis", "runs_scored", "strikeouts_batting",
    "walks", "stolen_bases", "strikeouts_pitching",
    "walks_allowed", "earned_runs", "hits_allowed", "outs_recorded",
    "hits_runs_rbis", "runs_rbis"

]

feature_metadata = {}

def inspect_features(prop_type):
    feature_metadata[prop_type] = {}

    for model_kind in ["random_forest", "logistic_regression"]:
        path = f"{MODEL_DIR}/{prop_type}/{prop_type}_{model_kind}_compressed.pkl"
        if not os.path.exists(path):
            print(f"❌ Missing: {path}")
            continue
        try:
            model = joblib.load(path)
            features = getattr(model, "feature_names_in_", None)
            if features is not None:
                feature_list = list(features)
                feature_metadata[prop_type][model_kind] = feature_list

                print(f"\n📦 {prop_type} - {model_kind}:")
                for i, feat in enumerate(feature_list):
                    print(f"  {i+1:2d}. {feat}")
            else:
                print(f"⚠️  {prop_type} - {model_kind} has no feature_names_in_ attribute")
        except Exception as e:
            print(f"❌ Failed to load {path}: {e}")

if __name__ == "__main__":
    for prop in PROP_TYPES:
        inspect_features(prop)

    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(feature_metadata, f, indent=2)
    print(f"\n📝 Feature metadata written to: {OUTPUT_PATH}")
