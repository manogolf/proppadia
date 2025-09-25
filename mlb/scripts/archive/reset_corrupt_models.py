import os
from dotenv import load_dotenv
from supabase import create_client
from backend.scripts.model_trainer import train_and_save_model


# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)

# Models known to be corrupt
corrupt_models = [
    "hits_model.pkl",
    "runs_scored_model.pkl",
    "total_bases_model.pkl",
    "strikeouts_pitching_model.pkl"
]

# Delete them from Supabase storage
def delete_old_models():
    print("🧹 Deleting corrupt model files from Supabase...")
    response = supabase.storage.from_("models").remove(corrupt_models)
    if hasattr(response, "error") and response.error:
        print(f"❌ Error during deletion: {response.error.message}")
    else:
        print("✅ Deleted corrupt model files.")

# Train models from scratch
def retrain_models():
    for filename in corrupt_models:
        prop_type = filename.replace("_model.pkl", "")
        print(f"\n🎯 Re-training model for: {prop_type}")
        try:
            train_and_save_model(prop_type)
        except Exception as e:
            print(f"❌ Failed to train {prop_type}: {e}")

if __name__ == "__main__":
    delete_old_models()
    retrain_models()
