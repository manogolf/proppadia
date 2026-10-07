from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.eightrain_adapter import build_rows, UPLOAD_COLUMNS, validate_upload
from backend.nhl.scripts.select_nhl_points_saves_8rain_candidates import (
    DEFAULT_POLICIES, load_active_policy, require_current_slate, select_lane,
    select_raw_lane,
)
from backend.nhl.scripts.build_points_with_market import parse_points_odds
from backend.nhl.scripts.build_saves_with_market import parse_odds_candidates
from backend.nhl.prediction_lineage import POINTS_LINES, SAVES_LINES


class PointsSavesEightRainPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy_points = load_active_policy(DEFAULT_POLICIES["points"], lane="points")
        self.policy_saves = load_active_policy(DEFAULT_POLICIES["saves"], lane="saves")
        self.names = pd.DataFrame([{
            "player_id": 10, "game_id": 100, "team_code": "BOS",
        }])
        self.schedule = pd.DataFrame([{
            "game_id": 100, "game_date": "2026-10-01",
            "home_team": "BOS", "away_team": "NYR",
        }])
        self.common = dict(
            slate_date="2026-10-01", policy_sha256="policy-hash",
            source_artifact_sha256="source-hash", odds_observation_id="obs-1",
            odds_observation_manifest_sha256="manifest-hash",
            capture_timestamp_utc="2026-10-01T15:00:00Z",
            team_map={"BOS": "team-bos", "NYR": "team-nyr"},
            player_map={("alex example", "team-bos"): "exact-player-code"},
            ambiguous_player_keys=set(), allowed_bets={
                "points": {"over", "under"}, "saves": {"over", "under"},
            },
        )

    def source(self, *, name="Alex Example", lines=(0.5, 1.5)):
        return pd.DataFrame([{
            "full_name": name, "player_id": 10, "game_id": 100,
            "team_id": 1, "line": line, "p_over": 0.70,
            "price_over": -110, "price_under": -110,
            "p_over_mkt": 0.50, "p_under_mkt": 0.50,
            "attachment_status": "MATCHED", "game_date": "2026-10-01",
            "parent_daily_run_id": "daily-1", "prediction_artifact_sha256": "pred-hash",
        } for line in lines])

    def choose(self, lane, name="Alex Example", lines=(0.5, 1.5)):
        return select_lane(
            self.source(name=name, lines=lines), self.names, self.schedule,
            lane=lane, policy=self.policy_points if lane == "points" else self.policy_saves,
            **self.common,
        )

    def test_both_explicit_policies_load_and_do_not_involve_scoring(self):
        for lane, policy in (("points", self.policy_points), ("saves", self.policy_saves)):
            self.assertEqual(policy["lane"], lane.upper())
            self.assertEqual(policy["selection"]["minimum_ev_exclusive"], 0.0)
            self.assertEqual(policy["selection"]["minimum_model_market_gap_exclusive"], 0.0)
        self.assertEqual(self.policy_points["selection"]["allowed_lines"], None)
        self.assertEqual(self.policy_saves["selection"]["allowed_lines"], None)

    def test_points_and_saves_select_mapped_lines_with_lineage(self):
        for lane in ("points", "saves"):
            decisions, selected, summary = self.choose(lane)
            self.assertEqual(len(selected), 2)
            self.assertEqual(set(selected.market), {lane})
            self.assertEqual(set(selected.side), {"OVER"})
            self.assertEqual(set(selected.catalog_player_code), {"exact-player-code"})
            self.assertEqual(summary["policy_qualified_before_mapping"], 2)
            self.assertTrue(selected.odds_observation_id.eq("obs-1").all())
            self.assertTrue(selected.capture_timestamp_utc.eq("2026-10-01T15:00:00Z").all())
            self.assertTrue(selected.candidate_policy_version.eq("v1").all())
            self.assertEqual(int(decisions.policy_qualified.sum()), 2)

    def test_unmapped_and_ambiguous_identities_are_reported_and_not_selected(self):
        decisions, selected, summary = self.choose("points", name="Unknown Player", lines=(0.5,))
        self.assertTrue(selected.empty)
        self.assertEqual(summary["unmapped_policy_qualified"], 1)
        self.assertIn("PLAYER_CODE_UNMAPPED", decisions.decision_reason.iloc[0])

        common = dict(self.common)
        common["ambiguous_player_keys"] = {("alex example", "team-bos")}
        common.pop("slate_date")
        decisions, selected, summary = select_lane(
            self.source(lines=(0.5,)), self.names, self.schedule,
            lane="saves", slate_date="2026-10-01", policy=self.policy_saves,
            **common,
        )
        self.assertTrue(selected.empty)
        self.assertEqual(summary["ambiguous_policy_qualified"], 1)

    def test_both_prop_types_and_multiple_lines_survive_adapter_as_pairs(self):
        frames = []
        for lane in ("points", "saves"):
            _, selected, _ = self.choose(lane)
            selected = selected.drop(columns=["full_name"])
            selected["model_pick"] = selected.side.str.lower()
            selected["model_side_prob"] = selected.model_side_prob
            frames.append(selected)
        props = pd.concat(frames, ignore_index=True)

        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp)
            pd.DataFrame([{"game_id": 100, "game_date": "2026-10-01", "home_team": "BOS", "away_team": "NYR"}]).to_csv(package / "schedule_event_identity.csv", index=False)
            pd.DataFrame([{"game_id": 100, "v2_home_win_probability": .55, "v2_away_win_probability": .45}]).to_csv(package / "v2_immutable_predictions.csv", index=False)
            pd.DataFrame([{"game_id": 100, "away_by_2_plus_probability": .25, "one_goal_game_probability": .40, "home_by_2_plus_probability": .35}]).to_csv(package / "puck_line_v1_immutable_predictions.csv", index=False)
            spec = {"league": {"code": "nhl"}, "markets": {"h2h": {}, "spread": {}}, "stats": [
                {"code": "points", "bet": ["over", "under"]},
                {"code": "saves", "bet": ["over", "under"]},
            ]}
            upload, meta = build_rows(
                package_dir=package, spec=spec,
                team_map={"BOS": "team-bos", "NYR": "team-nyr"},
                player_map={("alex example", "team-bos"): "exact-player-code"},
                allowed_bets={"points": {"over", "under"}, "saves": {"over", "under"}},
                prop_candidates=props,
            )
            prop_rows = upload[upload.SECTION.eq("player_prop")]
            self.assertEqual(len(prop_rows), 8)
            self.assertEqual(set(prop_rows.MARKET), {"points", "saves"})
            self.assertEqual(set(prop_rows.POINT), {"0.5", "1.5"})
            self.assertTrue(meta["challengers_included"] is False)
            for _, pair in prop_rows.groupby(["MARKET", "POINT", "SELECTOR"]):
                self.assertEqual(set(pair.SIDE), {"over", "under"})
            self.assertEqual(list(upload.columns), UPLOAD_COLUMNS)

    def test_challenger_rows_are_rejected_and_current_slate_is_required(self):
        self.assertFalse(self.policy_points.get("model_scoring_changes", False))
        require_current_slate("2026-10-01", current_date="2026-10-01")
        with self.assertRaisesRegex(ValueError, "CURRENT_SLATE"):
            require_current_slate("2026-09-30", current_date="2026-10-01")
        candidate = pd.DataFrame([{
            "game_id": 100, "game_date": "2026-10-01", "player_name": "Alex Example",
            "team": "BOS", "market": "points", "line": 0.5,
            "model_pick": "over", "model_side_prob": .7,
            "model_identity": "NHL_POINTS_CHALLENGER_V9",
        }])
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp)
            pd.DataFrame([{"game_id": 100, "game_date": "2026-10-01", "home_team": "BOS", "away_team": "NYR"}]).to_csv(package / "schedule_event_identity.csv", index=False)
            pd.DataFrame([{"game_id": 100, "v2_home_win_probability": .55, "v2_away_win_probability": .45}]).to_csv(package / "v2_immutable_predictions.csv", index=False)
            pd.DataFrame([{"game_id": 100, "away_by_2_plus_probability": .25, "one_goal_game_probability": .40, "home_by_2_plus_probability": .35}]).to_csv(package / "puck_line_v1_immutable_predictions.csv", index=False)
            with self.assertRaisesRegex(ValueError, "CHALLENGER_PROP_ROWS_FORBIDDEN"):
                build_rows(package_dir=package, spec={"league": {"code": "nhl"}, "markets": {"h2h": {}, "spread": {}}, "stats": [{"code": "points", "bet": ["over", "under"]}]},
                           team_map={"BOS": "team-bos", "NYR": "team-nyr"}, player_map={},
                           allowed_bets={"points": {"over", "under"}}, prop_candidates=candidate)

    def test_raw_points_and_saves_ignore_ev_gap_price_and_line_count_policies(self):
        raw_source = self.source(lines=(0.5, 1.5)).copy()
        raw_source["p_over"] = [0.2, 0.8]
        raw_source["price_over"] = [-300, -200]
        raw_source["price_under"] = [None, None]
        raw_source["p_over_mkt"] = [0.7, 0.2]
        raw_source["p_under_mkt"] = [None, None]
        raw_source["attachment_status"] = ["UNMATCHED", "MATCHED"]
        raw_common = dict(
            lane="points", slate_date="2026-10-01", source_sha256="sha",
            observation_id="obs", observation_manifest_sha256="manifest",
            capture_timestamp_utc="2026-10-01T15:00:00Z",
            team_map=self.common["team_map"], player_map=self.common["player_map"],
            unique_name_map={}, ambiguous_team_keys=set(), ambiguous_names=set(),
            allowed_bets={"points": {"over", "under"}, "saves": {"over", "under"}},
            parent_run_id="daily-1",
        )
        for lane in ("points", "saves"):
            raw_common["lane"] = lane
            decisions, upload, summary = select_raw_lane(
                raw_source, self.names, self.schedule, **raw_common,
            )
            self.assertEqual(len(upload), 2)
            self.assertEqual(summary["mapped_upload_predictions"], 2)
            self.assertEqual(set(upload.line), {0.5, 1.5})
            self.assertEqual(set(upload.model_identity.str.endswith("REFERENCE")), {True})
            self.assertTrue(upload.ev_over.dropna().lt(0).any())
            self.assertTrue(upload.edge_over.dropna().lt(0).any())
            self.assertTrue(upload.price_under.isna().all())
            self.assertEqual(set(decisions.candidate_policy_name), {"RAW_PREDICTION_COLLECTION"})

    def test_raw_sog_accepts_valid_probability_without_policy_or_market_quote(self):
        source = self.source(lines=(1.5, 2.5)).drop(columns=["price_over", "price_under", "p_over_mkt", "p_under_mkt", "attachment_status"])
        common = dict(
            lane="shots_on_goal", slate_date="2026-10-01", source_sha256="sha",
            observation_id="obs", observation_manifest_sha256="manifest",
            capture_timestamp_utc="2026-10-01T15:00:00Z",
            team_map=self.common["team_map"], player_map=self.common["player_map"],
            unique_name_map={}, ambiguous_team_keys=set(), ambiguous_names=set(),
            allowed_bets={"shots_on_goal": {"over", "under"}}, parent_run_id="daily-1",
        )
        _, upload, summary = select_raw_lane(source, self.names, self.schedule, **common)
        self.assertEqual(len(upload), 2)
        self.assertEqual(summary["mapped_upload_predictions"], 2)

    def test_raw_zero_probability_is_preserved_but_classified_as_format_limit(self):
        source = self.source(lines=(1.5,)).copy()
        source["p_over"] = 0.0
        common = dict(
            lane="shots_on_goal", slate_date="2026-10-01", source_sha256="sha",
            observation_id="obs", observation_manifest_sha256="manifest",
            capture_timestamp_utc="2026-10-01T15:00:00Z",
            team_map=self.common["team_map"], player_map=self.common["player_map"],
            unique_name_map={}, ambiguous_team_keys=set(), ambiguous_names=set(),
            allowed_bets={"shots_on_goal": {"over", "under"}}, parent_run_id="daily-1",
        )
        decisions, upload, summary = select_raw_lane(source, self.names, self.schedule, **common)
        self.assertTrue(upload.empty)
        self.assertEqual(summary["valid_model_predictions"], 1)
        self.assertEqual(summary["win_percent_unrepresentable"], 1)
        self.assertEqual(decisions.raw_export_status.iloc[0], "WIN_PERCENT_UNREPRESENTABLE")
        self.assertEqual(decisions.exclusion_class.iloc[0], "SCHEMA_REQUIRED")

    def test_points_and_saves_normalizers_preserve_both_provider_sides(self):
        raw = [{
            "id": "game-1", "commence_time": "2026-10-01T23:00:00Z",
            "bookmakers": [{"key": "book-a", "markets": [
                {"key": "player_points", "outcomes": [
                    {"name": "Over", "description": "Alex Example", "point": 0.5, "price": -120},
                    {"name": "Under", "description": "Alex Example", "point": 0.5, "price": 100},
                ]},
                {"key": "player_total_saves", "outcomes": [
                    {"name": "Over", "description": "Alex Example", "point": 24.5, "price": -110},
                    {"name": "Under", "description": "Alex Example", "point": 24.5, "price": -105},
                ]},
            ]}],
        }]
        points = parse_points_odds(raw).iloc[0]
        saves = parse_odds_candidates(raw)
        saves = saves[saves.alias_type.eq("AUTHORITATIVE_FULL_NAME")].iloc[0]
        self.assertEqual(points.price_over, -120)
        self.assertEqual(points.price_under, 100)
        self.assertEqual(points.source_quote_count_under, 1)
        self.assertEqual(saves.price_over, -110)
        self.assertEqual(saves.price_under, -105)
        self.assertEqual(saves.source_quote_count_under, 1)

    def test_points_quote_evidence_retains_multiple_books_without_row_expansion(self):
        scenarios = (
            ("two sportsbooks", ["draftkings", "fanduel"]),
            ("sportsbook and exchange", ["draftkings", "novig"]),
            ("several exchanges", ["betopenly", "novig", "prophetx"]),
        )
        for label, book_keys in scenarios:
            with self.subTest(label=label):
                raw = [{"bookmakers": [
                    {"key": book, "markets": [{"key": "player_points", "outcomes": [
                        {"name": "Over", "description": "Alex Example", "point": 0.5, "price": -120 + index},
                    ]}]} for index, book in enumerate(book_keys)
                ]}]
                points = parse_points_odds(raw)
                self.assertEqual(len(points), 1)
                self.assertEqual(points.iloc[0].source_quote_count_over, len(book_keys))
                self.assertEqual(points.iloc[0].source_books_over, "|".join(sorted(book_keys)))

    def test_alternate_provider_markets_stay_distinct_from_prediction_quotes(self):
        raw = [{"bookmakers": [{"key": "kalshi", "markets": [
            {"key": "player_points", "outcomes": [
                {"name": "Over", "description": "Alex Example", "point": 0.5, "price": -110},
            ]},
            {"key": "player_points_alternate", "outcomes": [
                {"name": "Over", "description": "Alex Example", "point": 1.0, "price": -110},
            ]},
            {"key": "player_total_saves_alternate", "outcomes": [
                {"name": "Over", "description": "Goalie Example", "point": 24, "price": -110},
            ]},
        ]}]}]
        points = parse_points_odds(raw)
        saves = parse_odds_candidates(raw)
        self.assertEqual(points.line_str.tolist(), ["0.5"])
        self.assertTrue(saves.empty)
        self.assertEqual(POINTS_LINES, (0.5, 1.5, 2.5))
        self.assertEqual(SAVES_LINES, tuple(float(line) + 0.5 for line in range(18, 31)))


if __name__ == "__main__":
    unittest.main()
