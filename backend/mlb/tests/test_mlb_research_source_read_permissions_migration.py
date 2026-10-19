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


DAILY_READER_FORWARD = MIGRATIONS / "20261019_mlb_research_daily_reader_read.sql"
DAILY_READER_ROLLBACK = MIGRATIONS / "20261019_mlb_research_daily_reader_read_rollback.sql"


def test_daily_reader_grant_matches_reader_columns_and_rollback():
    forward = DAILY_READER_FORWARD.read_text()
    rollback = DAILY_READER_ROLLBACK.read_text()
    expected = {
        "mlb.game_info": {"game_id", "game_date", "home_team_abbr", "away_team_abbr"},
        "mlb.player_stats": {"walks_allowed"},
    }

    assert _grants(forward, "GRANT SELECT") == expected
    assert _grants(rollback, "REVOKE SELECT") == expected
    upper = forward.upper()
    for forbidden in ("GRANT SELECT ON", "GRANT ALL", "INSERT ON", "UPDATE ON", "CREATE POLICY", "ALTER ROLE", "CREATE ROLE"):
        assert forbidden not in upper
    assert "NOT C.RELROWSECURITY" in upper
    assert "NOT APPLIED" in forward

    # Every game_info column the two readers select must be granted.
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    for name in ("build_mlb_slate_output.py", "export_mlb_book_upload.py"):
        source = (scripts / name).read_text()
        fetch = source[source.index("def _fetch_games"):]
        fetch = fetch[: fetch.index("\ndef ", 1)]
        # game_type is selected only if present; production game_info has none.
        selected = set(re.findall(r"(\w+)::\w+ AS \w+", fetch)) - {"game_type", "NULL"}
        assert selected <= expected["mlb.game_info"], (name, selected)

    # Starter skill/workload reads player_stats columns: walks_allowed plus
    # columns granted by 20261017_mlb_research_source_read.sql.
    granted = _expected_grants()["mlb.player_stats"] | expected["mlb.player_stats"]
    for name in ("update_mlb_starter_skill_workload_observation.py", "build_mlb_starter_skill_workload_research.py"):
        source = (scripts / name).read_text()
        select = re.search(r"SELECT (.*?)\s+FROM mlb\.player_stats", source, flags=re.DOTALL).group(1)
        selected = {column.strip() for column in select.split(",")}
        assert selected <= granted, (name, selected - granted)


DAILY_CHECK_FORWARD = MIGRATIONS / "20261019_mlb_research_daily_check_read.sql"
DAILY_CHECK_ROLLBACK = MIGRATIONS / "20261019_mlb_research_daily_check_read_rollback.sql"

# Columns the nine remaining daily readers select beyond the earlier grants.
DAILY_CHECK_EXPECTED = {
    "mlb.model_training_props": {"id", "prop_source", "prop_value", "line"},
    "mlb.player_stats": {
        "is_home", "singles", "doubles", "triples", "home_runs", "total_bases", "runs_scored",
        "rbis", "walks", "strikeouts_batting", "stolen_bases", "pa_source", "pa_backfilled_at",
        "hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference",
    },
    "mlb.game_info": {"game_time", "home_team_id", "away_team_id"},
    "mlb.player_ids": {"is_placeholder"},
    "mlb.prop_features_precomputed": {"lineup_slot"},
    "mlb.today_odds_book_rows": {"slate_date", "snapshot_ts"},
    "mlb.today_market_snapshot": {"game_date", "last_snapshot_ts"},
}


def test_daily_check_grant_is_column_scoped_and_rollback_matches():
    forward = DAILY_CHECK_FORWARD.read_text()
    rollback = DAILY_CHECK_ROLLBACK.read_text()
    grants = _grants(forward, "GRANT SELECT")
    workspace = grants.pop("mlb.today_workspace_mlb")
    assert grants == DAILY_CHECK_EXPECTED
    assert len(workspace) == 39 and {"game_date", "player_name", "decision_reason"} <= workspace
    revokes = _grants(rollback, "REVOKE SELECT")
    assert revokes.pop("mlb.today_workspace_mlb") == workspace
    assert revokes == DAILY_CHECK_EXPECTED
    upper = forward.upper()
    for forbidden in ("GRANT SELECT ON", "GRANT ALL", "CREATE POLICY", "ALTER ROLE", "CREATE ROLE", "GRANT INSERT", "GRANT UPDATE"):
        assert forbidden not in upper
    assert "NOT APPLIED" in forward


def test_daily_check_grant_covers_reader_columns():
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    granted: dict[str, set[str]] = {}
    earlier_daily_reader = {
        "mlb.game_info": {"game_id", "game_date", "home_team_abbr", "away_team_abbr"},
        "mlb.player_stats": {"walks_allowed"},
    }
    for grant_set in (_expected_grants(), DAILY_CHECK_EXPECTED, earlier_daily_reader):
        for relation, columns in grant_set.items():
            granted.setdefault(relation, set()).update(columns)
    reconcile = (scripts / "build_mlb_reconcile_rows.py").read_text()
    for column in re.findall(r"COALESCE\((\w+), 0\)::float8 AS \1", reconcile):
        assert column in granted["mlb.player_stats"], column
    completeness = (scripts / "player_stats_game_completeness.py").read_text()
    local = re.search(r"SELECT (player_id,game_id,[\w,]+) FROM mlb\.player_stats WHERE game_id", completeness).group(1)
    assert set(local.split(",")) <= granted["mlb.player_stats"]
    pa_health = (scripts / "check_mlb_pa_foundation_health.py").read_text()
    direct = re.search(r"DIRECT_PA_FIELDS = \[(.*?)\]", pa_health, flags=re.DOTALL).group(1)
    assert {c.strip().strip('"') for c in direct.split(",") if c.strip()} <= granted["mlb.player_stats"]
    o15 = (scripts / "hydrate_expanded_o15_context.py").read_text()
    game_info = re.search(r"SELECT (game_id, game_time, [^\n]+)\nFROM mlb\.game_info", o15).group(1)
    assert {c.strip().split("::")[0] for c in game_info.split(",")} <= granted["mlb.game_info"]
