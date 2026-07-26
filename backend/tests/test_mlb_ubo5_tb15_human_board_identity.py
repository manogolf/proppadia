from pathlib import Path

from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import (
    canonical_game,
    canonical_player_name,
    price_index,
    run_tag_from_path,
)


def test_board_identity_normalizes_athletics_code_and_diacritics():
    snapshot = {
        "captured_at_utc": "2026-07-26T16:30:05Z",
        "events": [{
            "home_team": "Minnesota Twins",
            "away_team": "Athletics",
            "bookmakers": [{
                "key": "betonlineag",
                "markets": [{
                    "key": "batter_total_bases",
                    "outcomes": [
                        {"description": "Jeremy Pena", "name": "Over", "point": 1.5, "price": 120},
                        {"description": "Jeremy Pena", "name": "Under", "point": 1.5, "price": -150},
                    ],
                }],
            }],
        }],
    }
    team_names = {
        "MIN": "Minnesota Twins",
        # Exercise the stale internal code that previously leaked into the board lookup.
        "OAK": "Athletics",
    }

    prices = price_index(snapshot, team_names)

    key = (
        canonical_game("OAK @ MIN"),
        canonical_player_name("Jeremy Peña"),
        "1.5",
    )
    assert prices[key]["over"] == 120
    assert prices[key]["under"] == -150
    assert key[:2] == ("ATH @ MIN", "jeremy pena")


def test_player_name_normalization_is_exact_after_unicode_folding():
    assert canonical_player_name("Mauricio Dubón") == canonical_player_name("Mauricio Dubon")
    assert canonical_player_name("Luis García Jr.") == canonical_player_name("Luis Garcia Jr.")
    assert canonical_player_name("Luis García Jr.") != canonical_player_name("Luis Garcia")


def test_archived_route_ledger_preserves_same_run_binding():
    path = Path("route_ledger__local_daily_20260726T180003Z.csv")
    assert run_tag_from_path(path) == "local_daily_20260726T180003Z"
    assert run_tag_from_path(Path("route_ledger.csv")) == ""
