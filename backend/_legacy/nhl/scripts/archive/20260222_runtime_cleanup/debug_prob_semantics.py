import pandas as pd
import psycopg
import os

PRED = "backend/nhl/data/processed/sog_predictions_wide_calibrated.csv"
SLATE_DATE = os.getenv("SLATE_DATE", "2026-01-17")

df = pd.read_csv(PRED)

# keep only the lines you’re exporting
keep = ["player_id","game_id","p_over_1_5","p_over_2_5","p_over_3_5"]
df = df[keep].dropna()

# pull actual shots_on_goal for those (player_id, game_id)
url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
assert url, "Set SUPABASE_DB_URL"

with psycopg.connect(url) as conn:
    q = """
    SELECT player_id::bigint, game_id::bigint, shots_on_goal::int
    FROM nhl.skater_game_logs_raw
    WHERE game_date = %s
    """
    actual = pd.read_sql(q, conn, params=(SLATE_DATE,))

m = df.merge(actual, on=["player_id","game_id"], how="inner")
print("joined rows:", len(m))
assert len(m) > 0, "No join — wrong date or table"

def summarize(line, col):
    over = m["shots_on_goal"] >= int(line + 0.5)  # over 2.5 => >=3
    a = m.loc[over, col].mean()
    b = m.loc[~over, col].mean()
    print(f"{col}: mean when OVER {line} = {a:.4f} | mean when UNDER {line} = {b:.4f} | diff(over-under) = {(a-b):.4f}")

summarize(1.5, "p_over_1_5")
summarize(2.5, "p_over_2_5")
summarize(3.5, "p_over_3_5")

# Also test if they behave like UNDER probabilities:
def summarize_under(line, col):
    under = m["shots_on_goal"] <= int(line - 0.5)  # under 2.5 => <=2
    a = m.loc[under, col].mean()
    b = m.loc[~under, col].mean()
    print(f"{col}: mean when UNDER {line} = {a:.4f} | mean when NOT-under {line} = {b:.4f} | diff(under-not) = {(a-b):.4f}")

summarize_under(1.5, "p_over_1_5")
summarize_under(2.5, "p_over_2_5")
summarize_under(3.5, "p_over_3_5")
