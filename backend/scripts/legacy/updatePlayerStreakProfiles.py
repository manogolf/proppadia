import os
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from collections import defaultdict

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_recent_resolved_props(days=30):
    today = datetime.utcnow().date()
    since = today - timedelta(days=days)

    response = supabase.table("player_props") \
        .select("*") \
        .gte("game_date", since.isoformat()) \
        .neq("outcome", None) \
        .eq("status", "resolved") \
        .order("game_date", desc=True) \
        .execute()

    return response.data or []

def compute_streaks_and_avg(props):
    streak_profiles = {}

    grouped = defaultdict(list)
    for prop in props:
        key = (prop["player_id"], prop["prop_type"])
        grouped[key].append(prop)

    for (player_id, prop_type), entries in grouped.items():
        sorted_entries = sorted(entries, key=lambda p: p["game_date"], reverse=True)
        outcomes = [p["outcome"] for p in sorted_entries[:7]]
        wins = sum(1 for o in outcomes if o == "win")
        rolling_avg = round(wins / len(outcomes), 3) if outcomes else 0

        hit_streak = win_streak = 0
        for o in outcomes:
            if o == "win":
                hit_streak += 1
                win_streak += 1
            else:
                break

        streak_profiles[(player_id, prop_type)] = {
            "player_id": player_id,
            "prop_type": prop_type,
            "hit_streak": hit_streak,
            "win_streak": win_streak,
            "rolling_result_avg_7": rolling_avg,
            "streak_type": "neutral",  # Safe default
        }

    return list(streak_profiles.values())

def upsert_streak_profiles(profiles):
    if not profiles:
        print("⚠️ No streak data to upsert.")
        return

    sql_values = ",".join(
        f"('{p['player_id']}', '{p['prop_type']}', {p['hit_streak']}, {p['win_streak']}, {p['rolling_result_avg_7']}, '{p['streak_type']}')"
        for p in profiles
    )

    sql = f"""
        INSERT INTO player_streak_profiles (player_id, prop_type, hit_streak, win_streak, rolling_result_avg_7, streak_type)
        VALUES {sql_values}
        ON CONFLICT (player_id, prop_type)
        DO UPDATE SET
            hit_streak = EXCLUDED.hit_streak,
            win_streak = EXCLUDED.win_streak,
            rolling_result_avg_7 = EXCLUDED.rolling_result_avg_7,
            streak_type = EXCLUDED.streak_type;
    """

    try:
        supabase.rpc("execute_raw_sql", {"sql": sql}).execute()
        print(f"✅ Upserted {len(profiles)} streak profiles using raw SQL.")
    except Exception as e:
        print(f"❌ Raw SQL upsert failed: {e}")

def main():
    print("📦 Fetching recent resolved props...")
    props = fetch_recent_resolved_props()

    if not props:
        print("⚠️ No resolved props found — skipping.")
        return

    print(f"🧠 Processing {len(props)} props...")
    profiles = compute_streaks_and_avg(props)
    upsert_streak_profiles(profiles)

if __name__ == "__main__":
    main()
