from __future__ import annotations

import re
from pathlib import Path


MIGRATIONS = Path(__file__).resolve().parents[1] / "sql" / "migrations"
FORWARD = MIGRATIONS / "20261017_mlb_research_source_read.sql"
ROLLBACK = MIGRATIONS / "20261017_mlb_research_source_read_rollback.sql"


def _grants(sql: str, keyword: str) -> dict[str, set[str]]:
    found = {}
    pattern = rf"{keyword}\s*\((.*?)\)\s*ON\s+([\w.]+)\s+(?:TO|FROM)\s+proppadia_mlb_research_ro"
    for columns, relation in re.findall(pattern, sql, flags=re.IGNORECASE | re.DOTALL):
        found[relation.lower()] = {column.strip().lower() for column in columns.split(",")}
    return found


def _expected_grants() -> dict[str, set[str]]:
    player_stats = {
        "player_id", "game_id", "game_date", "team", "opponent", "position", "hits",
        "hits_allowed", "outs_recorded", "is_starter", "earned_runs", "strikeouts_pitching",
        "at_bats", "plate_appearances",
    }
    derived_metrics = (
        "hits", "runs_scored", "rbis", "home_runs", "singles", "doubles", "triples",
        "walks", "strikeouts_batting", "stolen_bases", "total_bases", "hits_runs_rbis",
        "runs_rbis", "outs_recorded", "strikeouts_pitching", "walks_allowed", "earned_runs",
        "hits_allowed", "at_bats", "plate_appearances",
    )
    player_derived_stats = {"player_id", "game_id", "game_date", "team"}
    player_derived_stats.update(
        f"d{window}_{metric}" for window in (7, 15, 30) for metric in derived_metrics
    )
    return {
        "mlb.player_stats": player_stats,
        "mlb.player_derived_stats": player_derived_stats,
        "mlb.player_ids": {"player_id", "player_name", "team", "team_id"},
        "mlb.player_team_by_game": {"player_id", "game_id", "team_id"},
        "public.mlb_team_map": {"team_id", "abbr"},
        "mlb.model_training_props": {"game_date", "game_id", "player_id", "prop_type", "rolling_result_avg_7"},
    }


def test_research_read_migration_is_column_scoped_and_paired_rollback_matches():
    forward = FORWARD.read_text()
    rollback = ROLLBACK.read_text()
    expected = _expected_grants()

    assert _grants(forward, "GRANT SELECT") == expected
    assert _grants(rollback, "REVOKE SELECT") == expected
    assert "GRANT SELECT ON" not in forward.upper()
    assert "GRANT ALL" not in forward.upper()
    assert "V_GROUP.ROLBYPASSRLS" in forward.upper()
    assert "V_LOGIN.ROLBYPASSRLS" in forward.upper()
    assert "ALTER ROLE" not in forward.upper()
    assert "CREATE ROLE" not in forward.upper()
    assert "proppadia_mlb_research_select" in forward
    assert forward.count("CREATE POLICY proppadia_mlb_research_select") == 4
    assert "ON mlb.model_training_props" in forward
    assert "FOR SELECT TO proppadia_mlb_research_ro USING (true)" in forward
    assert "public_game_moneyline" not in forward + rollback
    assert "prop_features_precomputed" not in forward + rollback


def test_existing_moneyline_and_bvp_research_routes_remain_process_scoped():
    moneyline = (Path(__file__).resolve().parents[1] / "scripts" / "audit_mlb_moneyline_probability_region_premise_v1.py").read_text()
    bvp_verifier = (Path(__file__).resolve().parents[1] / "scripts" / "run_mlb_bvp_inline_daily.py").read_text()
    assert 'AUDIT_DATABASE_URL_ENV = "MLB_MONEYLINE_AUDIT_DATABASE_URL"' in moneyline
    assert 'os.environ.get("MLB_MONEYLINE_AUDIT_DATABASE_URL", "").strip()' in bvp_verifier
    assert "SELECT prop_type,player_id,game_id,game_date,features,feature_set_tag,model_tag,computed_at" in bvp_verifier
