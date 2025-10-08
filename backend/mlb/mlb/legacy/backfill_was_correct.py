from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])

# Fetch all resolved props with predictions and outcomes
response = supabase.table("model_training_props") \
    .select("id, predicted_outcome, outcome") \
    .is_("was_correct", None) \
    .in_("outcome", ["win", "loss"]) \
    .in_("predicted_outcome", ["win", "loss"]) \
    .limit(1000) \
    .execute()

props = response.data

print(f"📦 {len(props)} props to backfill...")

for prop in props:
    was_correct = prop["predicted_outcome"] == prop["outcome"]
    supabase.table("model_training_props").update({
        "was_correct": was_correct
    }).eq("id", prop["id"]).execute()

print("✅ Done updating was_correct.")
