import pandas as pd
import yaml

# Load feature spec from YAML
with open("model_features.yaml", "r") as f:
    feature_spec = yaml.safe_load(f)

yaml_features = set(feature_spec.get("features", {}).keys())

# Load training data for a prop type (change this if batching)
from backend.scripts.retrain_utils import fetch_training_data
df = fetch_training_data("hits")  # Replace "hits" with any prop_type

actual_columns = set(df.columns)

print("✅ YAML Features:", len(yaml_features))
print("✅ Data Columns:", len(actual_columns))

# Comparison
missing_in_data = yaml_features - actual_columns
extra_in_data = actual_columns - yaml_features

print("\n🔍 Features in YAML but not in training data:")
for col in sorted(missing_in_data):
    print(f"  - {col}")

print("\n📦 Columns in training data but not listed in YAML:")
for col in sorted(extra_in_data):
    print(f"  - {col}")
