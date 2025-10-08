import os
import json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_ROLE_KEY"]
)

# Load normalized → label mapping
with open("backend/app/prop_types.json") as f:
    normalized_to_label = json.load(f)

# Invert: label → normalized key
label_to_normalized = {
    label.replace("_", " ").replace("+", "+").strip(): key
    for key, label in normalized_to_label.items()
}

def normalize_model_training_props():
    print("🔍 Scanning model_training_props for legacy prop types...")

    # Pull all prop_type rows (id + prop_type)
    response = supabase.table("model_training_props") \
        .select("id, prop_type") \
        .execute()

    updated = 0

    for row in response.data:
        original = row["prop_type"]
        normalized = label_to_normalized.get(original.strip())

        if normalized and normalized != original:
            supabase.table("model_training_props") \
                .update({"prop_type": normalized}) \
                .eq("id", row["id"]) \
                .execute()
            updated += 1
            print(f"🔧 {original} → {normalized}")

    print(f"✅ Normalization complete. Updated {updated} rows.")

if __name__ == "__main__":
    normalize_model_training_props()
