"""Daily input watch: sanity flags, disagreement reasons, and a run that never blocks."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from backend.nhl import input_watch as watch

RANGES = json.loads(watch.POINTS_RANGES_PATH.read_text())


def points_row(player_id, **values):
    row = {"game_id": 1, "player_id": player_id, "player_name": f"P{player_id}", "is_home": 1,
           "d10_sog_per60": 8.0, "attempts_d10_per60": 16.0, "player_points_last10": 0.5,
           "current_season_points_prior": 5, "current_season_games_prior": 10,
           "mean_toi_last10": 18.0, "mean_pp_toi_last10": 2.0, "team_d10_sf_per_game": 28.0,
           "last10_team_sog_share": 0.085, "prob_over_0_5": 0.4, "prob_over_1_5": 0.1,
           "prob_over_2_5": 0.02, "game_start_utc": "2026-10-28T23:00:00Z"}
    row.update(values)
    return row


class SanityTests(unittest.TestCase):
    def test_impossible_and_substituted_values_are_reported(self):
        frame = pd.DataFrame([
            points_row(1),
            points_row(2, attempts_d10_per60=4.0),          # attempts below shots
            points_row(3, attempts_d10_per60=None),         # unknown, imputed
            points_row(4, team_d10_sf_per_game=11.0),       # implausible team shots
            points_row(5, player_points_last10=2.7),        # outside training range
        ])
        checks = {c["check"]: c for c in watch.points_sanity(frame, RANGES)}
        self.assertTrue(checks["ATTEMPTS_BELOW_SOG"]["flag"])
        self.assertEqual(checks["ATTEMPTS_BELOW_SOG"]["examples"], ["P2"])
        self.assertTrue(checks["TEAM_SHOTS_IMPLAUSIBLE"]["flag"])
        self.assertEqual(checks["ATTEMPTS_UNKNOWN_IMPUTED"]["count"], 1)
        # One of five players outside the range is above the drift share.
        self.assertTrue(checks["OUT_OF_TRAINING_RANGE:player_points_last10"]["flag"])

    def test_sog_inputs_are_checked_against_the_frozen_training_ranges(self):
        sog_ranges = json.loads(watch.SOG_RANGES_PATH.read_text())
        self.assertEqual(sog_ranges["training_frame_sha256"],
                         "4042e31c758c6b0083272cf82904426b8732e06a9f2837c79e7e0d2e9aa093f6")
        rows = [{"game_id": 1, "player_id": i, "player_name": f"S{i}", "feature_sog60_10": 7.0,
                 "feature_att60_10": 14.0, "feature_toi_avg_10": 17.0, "feature_pp_toi_avg_10": 1.0,
                 "feature_team_sf_10": 28.0, "feature_league_prior": 5.5} for i in range(1, 6)]
        rows[0]["feature_sog60_10"] = 30.0                      # far above training p99
        checks = {c["check"]: c for c in watch.sog_sanity(pd.DataFrame(rows), sog_ranges)}
        self.assertTrue(checks["OUT_OF_TRAINING_RANGE:feature_sog60_10"]["flag"])
        self.assertEqual(checks["OUT_OF_TRAINING_RANGE:feature_sog60_10"]["examples"], ["S1"])
        self.assertFalse(checks["OUT_OF_TRAINING_RANGE:feature_toi_avg_10"]["flag"])
        # 2026 defenseman league prior (3.947) is below both training values: reported, not flagged.
        for row in rows:
            row["feature_league_prior"] = 3.947
        checks = {c["check"]: c for c in watch.sog_sanity(pd.DataFrame(rows), sog_ranges)}
        self.assertEqual(checks["OUT_OF_TRAINING_RANGE:feature_league_prior"]["count"], 5)
        self.assertFalse(checks["OUT_OF_TRAINING_RANGE:feature_league_prior"]["flag"])

    def test_clean_inputs_raise_no_flags(self):
        frame = pd.DataFrame([points_row(i) for i in range(1, 30)])
        self.assertFalse(any(c["flag"] for c in watch.points_sanity(frame, RANGES)))


class ReasonTests(unittest.TestCase):
    def test_star_ceiling_missed_games_and_defenseman(self):
        slate = pd.DataFrame([points_row(i) for i in range(1, 10)]).assign(
            season_ppg=lambda f: f.current_season_points_prior / f.current_season_games_prior)
        star = pd.Series(points_row(99, current_season_points_prior=34, current_season_games_prior=12,
                                    player_points_last10=2.7, selected_side="UNDER")).astype(object)
        star["season_ppg"] = 34 / 12
        reasons = watch._points_reasons(star, slate, RANGES)
        self.assertTrue(any("ceiling" in r for r in reasons))
        self.assertTrue(any("outside training range" in r for r in reasons))
        returning = pd.Series(points_row(98, current_season_games_prior=3, current_season_points_prior=3,
                                         last10_team_sog_share=0.04, attempts_d10_per60=None,
                                         is_defense=1, selected_side="UNDER")).astype(object)
        returning["season_ppg"] = 1.0
        reasons = watch._points_reasons(returning, slate, RANGES)
        for text in ("attempts unknown", "only 3 games", "missed games", "defenseman"):
            self.assertTrue(any(text in r for r in reasons), text)


class RunTests(unittest.TestCase):
    def test_run_writes_report_and_explains_the_largest_gap(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            temp = Path(temp)
            capture = temp / "capture"
            capture.mkdir()
            rows = pd.DataFrame([points_row(i) for i in range(1, 12)])
            feature_cols = [c for c in rows.columns if not c.startswith("prob_")]
            rows[feature_cols].to_csv(capture / "features.csv", index=False)
            rows[["game_id", "player_id", "player_name", "prob_over_0_5", "prob_over_1_5",
                  "prob_over_2_5"]].to_csv(capture / "predictions.csv", index=False)

            def quotes(_root, candidates, price_both_sides):
                self.assertTrue(price_both_sides)
                out = []
                for c in candidates:
                    if c["market_line"] != "0.5":
                        continue
                    # Player 1: the market makes our UNDER an underdog.
                    under = 0.40 if c["player_id"] == "1" else 0.60
                    for side, p in (("UNDER", under), ("OVER", 1 - under)):
                        out.append({"candidate_key": c["candidate_key"], "sportsbook_key": "book",
                                    "quote_side": side, "selected_side": c["selected_side"],
                                    "implied_probability": p, "american_price": 100,
                                    "quote_qualification_status": "QUALIFIED_X"})
                return out, []

            with mock.patch.object(watch.candidate_export, "load_retained_quotes", side_effect=quotes), \
                    mock.patch.object(watch.candidate_export, "deduplicate_quotes", side_effect=lambda q: q):
                result = watch.run_input_watch(
                    slate_date="2026-10-28", run_id="r", prediction_run_dir=temp / "none",
                    points_capture_dir=capture, operational_root=temp, output_root=temp / "out",
                    now_utc="2026-10-28T18:00:00Z")
            top = result["disagreements"]["lanes"]["points"][0]
            self.assertEqual(top["player_id"], 1)
            self.assertAlmostEqual(top["gap"], 0.2, places=4)
            self.assertTrue((Path(result["path"]) / "input_watch.md").is_file())
            self.assertEqual(result["database_writes"], 0)

    def test_quote_failure_leaves_the_rest_of_the_watch(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            temp = Path(temp)
            capture = temp / "capture"
            capture.mkdir()
            rows = pd.DataFrame([points_row(1, attempts_d10_per60=4.0)])
            rows.to_csv(capture / "features.csv", index=False)
            rows.to_csv(capture / "predictions.csv", index=False)
            with mock.patch.object(watch.candidate_export, "load_retained_quotes", side_effect=OSError("x")):
                result = watch.run_input_watch(
                    slate_date="2026-10-28", run_id="r", prediction_run_dir=temp,
                    points_capture_dir=capture, operational_root=temp, output_root=temp / "out")
            self.assertEqual(result["disagreements"]["status"], "UNAVAILABLE")
            self.assertIn("points:ATTEMPTS_BELOW_SOG", result["flags"])


def usage_rows(rows):
    return pd.DataFrame([{"season": s, "player_id": p, "games": g, "toi_per_game": toi,
                          "pp_toi_per_game": pp, "pp_games": g} for s, p, g, toi, pp in rows])


class RoleChangeTests(unittest.TestCase):
    def test_role_changes_scale_league_pp_and_need_enough_games(self):
        points = pd.DataFrame([
            points_row(1, mean_toi_last10=18.4, mean_pp_toi_last10=3.3, current_season_points_prior=14,
                       current_season_games_prior=14),                       # role up
            points_row(2, mean_toi_last10=18.0, mean_pp_toi_last10=2.6),      # PP up only by the league shift
            points_row(3, mean_toi_last10=11.9, mean_pp_toi_last10=0.0),      # role down
            points_row(4, mean_toi_last10=22.0, current_season_games_prior=3),  # too few games now
            points_row(5, mean_toi_last10=22.0),                              # too few games last season
        ])
        usage = usage_rows([(2025, 1, 76, 13.2, 1.3), (2025, 2, 80, 17.8, 2.0), (2025, 3, 70, 15.6, 2.5),
                            (2025, 4, 70, 15.0, 1.0), (2025, 5, 10, 15.0, 1.0), (2024, 1, 63, 17.3, 2.0)])
        totals = pd.DataFrame({"player_id": [1, 1], "season": [2024, 2025], "games": [63, 76],
                               "ppg": [55 / 63, 22 / 76]})
        roles = watch.role_changes(points, usage, 2026, totals)
        self.assertEqual(sorted(roles.player_id), [1, 3])
        johnson = roles[roles.player_id.eq(1)].iloc[0]
        self.assertEqual(johnson.direction, "UP")
        self.assertAlmostEqual(johnson.ppg_before, 55 / 63)
        self.assertAlmostEqual(johnson.ppg_now, 1.0)
        self.assertAlmostEqual(johnson.toi_before, 17.3)
        self.assertEqual(roles[roles.player_id.eq(3)].iloc[0].direction, "DOWN")
        self.assertGreater(roles.attrs["pp_scale"], 1.0)

    def test_direction_comes_from_the_qualifying_moves(self):
        points = pd.DataFrame([
            points_row(1, mean_toi_last10=18.5, mean_pp_toi_last10=1.8),  # TOI up; PP down a little
            points_row(2, mean_toi_last10=18.5, mean_pp_toi_last10=0.5),  # TOI up; PP down 1.5
            points_row(3, mean_toi_last10=15.0, mean_pp_toi_last10=3.7),  # PP up only (keeps the scale at 1)
        ])
        usage = usage_rows([(2025, p, 80, 15.0, 2.0) for p in (1, 2, 3)])
        roles = watch.role_changes(points, usage, 2026).set_index("player_id")
        self.assertAlmostEqual(roles.attrs["pp_scale"], 1.0)
        self.assertEqual(roles.loc[1, "direction"], "UP")
        self.assertEqual(roles.loc[2, "direction"], "MIXED")
        self.assertEqual(roles.loc[3, "direction"], "UP")

    def test_role_read_names_who_is_behind(self):
        self.assertTrue(watch.role_read("UP", 0.38, 0.50).startswith("model likely behind"))
        self.assertTrue(watch.role_read("DOWN", 0.45, 0.30).startswith("model likely behind"))
        self.assertTrue(watch.role_read("UP", 0.77, 0.65).startswith("model further along"))
        self.assertEqual(watch.role_read("UP", 0.56, 0.58), "model and market agree")
        self.assertEqual(watch.role_read("UP", None, 0.5), "no two-sided market")

    def test_highlights_lead_the_report(self):
        player = {"player_name": "Kent Johnson", "direction": "UP", "read": "model likely behind the role change",
                  "read_from": "points", "points_model_p_over": 0.38, "points_market_p_over": 0.50,
                  "sog_model_p_over": 0.64, "sog_market_p_over": 0.53, "toi_prior": 13.2, "toi_now": 18.4,
                  "pp_prior": 1.8, "pp_now": 3.3, "toi_before": 17.3, "ppg_before": 0.87, "ppg_prior": 0.29,
                  "ppg_now": 1.0}
        result = {"slate_date": "2026-11-06", "run_id": "r", "flag_count": 0, "flags": [], "sanity": {},
                  "players": {}, "role_changes": {"status": "COMPLETE", "players": [player]},
                  "disagreements": {"lanes": {"points": [{"player": "X", "line": 0.5, "side": "UNDER",
                                                          "model_p": 0.58, "market_p": 0.45, "gap": 0.13,
                                                          "reasons": ["no input stands out; check the market"]}]}}}
        text = watch.render(result)
        self.assertLess(text.index("## Highlights"), text.index("## Role changes"))
        self.assertIn("Kent Johnson (up): Points 0.5 model 38% vs market 50%; TOI 13.2 to 18.4, PP 1.8 to 3.3", text)
        self.assertIn("X POINTS 0.5 UNDER model 58% vs market 45%", text)
        self.assertIn("Kent Johnson model 64% vs market 53% (role up)", text)
        self.assertIn("- Data flags: none", text)

    def test_highlights_name_players_beyond_the_limit(self):
        base = {"direction": "UP", "read": "model likely behind the role change", "read_from": "points",
                "points_model_p_over": 0.38, "points_market_p_over": 0.50, "sog_model_p_over": 0.70,
                "sog_market_p_over": 0.60, "toi_prior": 13.0, "toi_now": 18.0, "pp_prior": 1.0, "pp_now": 3.0}
        players = [{**base, "player_name": f"P{i}"} for i in range(8)]
        text = "\n".join(watch.render_highlights({"role_changes": {"players": players}, "flags": []}))
        self.assertIn("and 2 more: P6, P7", text)
        self.assertIn("; and 2 more: P6, P7", text)

    def test_season_of_starts_in_september(self):
        self.assertEqual(watch.season_of("2026-11-06"), 2026)
        self.assertEqual(watch.season_of("2027-03-01"), 2026)

    def test_missing_usage_file_reports_and_keeps_the_watch(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            temp = Path(temp)
            capture = temp / "capture"
            capture.mkdir()
            rows = pd.DataFrame([points_row(1)])
            rows.to_csv(capture / "features.csv", index=False)
            rows.to_csv(capture / "predictions.csv", index=False)
            with mock.patch.object(watch.candidate_export, "load_retained_quotes", return_value=([], [])):
                result = watch.run_input_watch(
                    slate_date="2026-11-06", run_id="r", prediction_run_dir=temp,
                    points_capture_dir=capture, operational_root=temp, output_root=temp / "out",
                    prior_usage_path=temp / "missing.csv")
            self.assertTrue(result["role_changes"]["status"].startswith("UNAVAILABLE:missing"))
            text = (Path(result["path"]) / "input_watch.md").read_text()
            self.assertIn("## Role changes vs last season", text)
            self.assertIn("build_nhl_prior_season_usage.py", text)


def role_row(player_id, toi_delta, pp_delta, model, market, game_id=7):
    return {"game_id": game_id, "player_id": player_id, "player_name": f"P{player_id}", "toi_delta": toi_delta,
            "pp_delta": pp_delta, "points_model_p_over": model, "points_market_p_over": market}


class RemovalGradeTests(unittest.TestCase):
    def test_candidates_are_the_model_side_when_the_model_is_behind(self):
        early = {"generated_at_utc": "2026-11-07T13:45:00Z", "role_changes": {"players": [
            role_row(1, 3.0, 0.0, 0.40, 0.50),     # up, model below: skip the Under
            role_row(2, -3.0, 0.0, 0.50, 0.40),    # down, model above: skip the Over
            role_row(3, 3.0, 0.0, 0.60, 0.50),     # up, model above: model ahead, not a removal
            role_row(4, 3.0, -0.2, 0.40, 0.42)]}}  # agreement
        late = {"generated_at_utc": "2026-11-07T19:30:00Z", "role_changes": {"players": [
            role_row(1, 3.0, 0.0, 0.38, 0.50)]}}   # the later read wins
        picks = {p["player_id"]: p for p in watch.removal_candidates([late, early])}
        self.assertEqual(sorted(picks), [1, 2])
        self.assertEqual(picks[1]["side"], "UNDER")
        self.assertAlmostEqual(picks[1]["model_p"], 0.62)
        self.assertAlmostEqual(picks[1]["market_p"], 0.50)
        self.assertEqual(picks[2]["side"], "OVER")

    def test_grades_at_the_no_vig_price_and_voids_non_participants(self):
        picks = [{"game_id": 7, "player_id": 1, "side": "UNDER", "market_p": 0.6},
                 {"game_id": 7, "player_id": 2, "side": "OVER", "market_p": 0.4},
                 {"game_id": 7, "player_id": 3, "side": "UNDER", "market_p": 0.5}]
        outcomes = pd.DataFrame([
            {"game_id": 7, "player_id": 1, "official_points": 0, "participation_state": "PARTICIPATED", "official_final": True},
            {"game_id": 7, "player_id": 2, "official_points": 0, "participation_state": "PARTICIPATED", "official_final": True},
            {"game_id": 7, "player_id": 3, "official_points": 0, "participation_state": "DID_NOT_PLAY", "official_final": True}])
        graded = {p["player_id"]: p for p in watch.grade_removals(picks, outcomes)}
        self.assertEqual(graded[1]["result"], "W")
        self.assertAlmostEqual(graded[1]["dollars"], round(10 * 0.4 / 0.6, 2))
        self.assertEqual((graded[2]["result"], graded[2]["dollars"]), ("L", -10.0))
        self.assertEqual(graded[3]["result"], "VOID")

    def test_section_grades_prior_slate_and_season(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            temp = Path(temp)
            for day, points in (("2026-11-06", 1), ("2026-11-07", 0)):
                run = temp / "out" / f"slate_date={day}" / "run_id=r"
                run.mkdir(parents=True)
                (run / "input_watch.json").write_text(json.dumps({"role_changes": {
                    "status": "COMPLETE", "players": [role_row(1, 3.0, 0.0, 0.40, 0.50)]}}))
                package = temp / "ops" / "postgame_reconciliation" / day / "reconciliation=a"
                package.mkdir(parents=True)
                (package / "RUN_COMPLETE.json").write_text("{}")
                pd.DataFrame([{"game_id": 7, "player_id": 1, "official_points": points,
                               "participation_state": "PARTICIPATED", "official_final": True}]).to_csv(
                    package / "canonical_skater_outcomes.csv", index=False)
            section = watch.removal_section(prior_date="2026-11-07", operational_root=temp / "ops",
                                            output_root=temp / "out")
            self.assertEqual(section["totals"]["won"], 1)          # 11-07: skipped Under won
            self.assertEqual(section["season"]["slates"], 2)
            self.assertEqual((section["season"]["won"], section["season"]["lost"]), (1, 1))
            self.assertIn("Removal candidates graded", "\n".join(watch.render_removals(section)))
            missing = watch.removal_section(prior_date="2026-11-08", operational_root=temp / "ops",
                                            output_root=temp / "out")
            self.assertEqual(missing["status"], "NO_WATCH_FOR_PRIOR_DATE")


class HotStreakTests(unittest.TestCase):
    def _package(self, root, day, rows):
        package = root / "postgame_reconciliation" / day / "reconciliation=a"
        package.mkdir(parents=True)
        (package / "RUN_COMPLETE.json").write_text("{}")
        pd.DataFrame(rows).to_csv(package / "canonical_skater_outcomes.csv", index=False)

    def test_tags_players_who_cleared_the_line_in_each_of_the_last_three_games(self):
        def game(day, player, sog, points, state="PARTICIPATED"):
            return {"game_id": int(day[-2:]), "player_id": player, "official_sog": sog, "official_points": points,
                    "participation_state": state, "official_final": True}
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            ops = Path(temp)
            self._package(ops, "2026-11-01", [game("2026-11-01", 1, 0, 0), game("2026-11-01", 2, 3, 1)])
            self._package(ops, "2026-11-02", [game("2026-11-02", 1, 2, 1), game("2026-11-02", 2, 2, 1)])
            self._package(ops, "2026-11-03", [game("2026-11-03", 1, 4, 1), game("2026-11-03", 2, 0, 0, "DID_NOT_PLAY")])
            self._package(ops, "2026-11-04", [game("2026-11-04", 1, 2, 2), game("2026-11-04", 2, 1, 1)])
            self._package(ops, "2026-11-05", [game("2026-11-05", 1, 5, 0)])   # tonight: not history
            history = watch.season_outcomes(ops, "2026-11-05")
            self.assertNotIn("2026-11-05", set(history.slate_date))
            sog = pd.DataFrame([{"game_id": 9, "player_id": 1, "player_name": "A", "p_over_1_5": 0.6},
                                {"game_id": 9, "player_id": 2, "player_name": "B", "p_over_1_5": 0.5}])
            points = pd.DataFrame([{"game_id": 9, "player_id": 1, "player_name": "A", "prob_over_0_5": 0.55},
                                   {"game_id": 9, "player_id": 2, "player_name": "B", "prob_over_0_5": 0.5}])
            tags = watch.hot_streaks(points=points, sog=sog, history=history, market={("sog", 1): 0.52})
            # Player 1: SOG 2,4,2 all clear 1.5; points 1,1,2 all clear 0.5. Player 2: SOG 3,2,1 -> no;
            # points 1,1,1 over played games only (the DNP is skipped) -> yes.
            self.assertEqual(sorted((t["lane"], t["player_id"]) for t in tags),
                             [("points", 1), ("points", 2), ("sog", 1)])
            sog_tag = next(t for t in tags if t["lane"] == "sog")
            self.assertEqual((sog_tag["model_p_over"], sog_tag["market_p_over"]), (0.6, 0.52))

    def test_prior_slate_tags_are_graded_as_overs(self):
        tags = [{"lane": "sog", "game_id": 7, "player_id": 1, "model_p_over": 0.6, "market_p_over": 0.5},
                {"lane": "points", "game_id": 7, "player_id": 1, "model_p_over": 0.5, "market_p_over": None},
                {"lane": "sog", "game_id": 7, "player_id": 2, "model_p_over": 0.6, "market_p_over": 0.5}]
        outcomes = pd.DataFrame([
            {"game_id": 7, "player_id": 1, "official_sog": 2, "official_points": 0,
             "participation_state": "PARTICIPATED", "official_final": True},
            {"game_id": 7, "player_id": 2, "official_sog": 0, "official_points": 0,
             "participation_state": "DID_NOT_PLAY", "official_final": True}])
        graded = watch.grade_hot(tags, outcomes)
        self.assertEqual([(g["result"], g["dollars"]) for g in graded], [("W", 10.0), ("L", 0.0), ("VOID", 0.0)])
        text = "\n".join(watch.render_hot({"status": "COMPLETE", "players": [], "season": {"2026-11-11": graded},
                                           "prior": {"date": "2026-11-11", "picks": graded}}))
        self.assertIn("SOG 1.5 Over tags graded: prior slate 1-0 (100%)", text)
        self.assertIn("Points 0.5 Over tags graded: prior slate 0-1 (0%)", text)



class GoalieTests(unittest.TestCase):
    """Projected starter vs usual starter next to the moneyline favorite (NHL Backlog P2.7)."""

    PAGE = """<html><head><script type="application/ld+json">
{{"@type":"NewsArticle","datePublished":"{published}","dateModified":"{published}"}}
</script></head><body><main>
<p>{header}</p>
<p>Lightning projected lineup</p>
<p>Brandon Hagel -- Brayden Point -- Nikita Kucherov</p>
<p>Victor Hedman -- Ryan McDonagh</p>
<p>Dennis Hildeby</p>
<p>Andrei Vasilevskiy</p>
<p>Blue Jackets projected lineup</p>
<p>Kirill Marchenko -- Adam Fantilli -- Kent Johnson</p>
<p>Zach Werenski -- Damon Severson</p>
<p>Jet Greaves</p>
<p>Elvis Merzlikins</p>
<p>Status report</p>
</main></body></html>"""

    def _ops(self, root, *, header, published, folder_day):
        package = root / "postgame_reconciliation" / "2026-11-10" / "reconciliation=a"
        package.mkdir(parents=True)
        (package / "RUN_COMPLETE.json").write_text("{}")
        starts = ([(1, 14, 100)] * 8 + [(1, 14, 101)] * 2      # TBL: Vasilevskiy 8, Hildeby 2
                  + [(1, 29, 200)] * 6 + [(1, 29, 201)] * 4)   # CBJ: Greaves 6, Merzlikins 4 (tandem)
        pd.DataFrame([{"game_id": i, "team_id": t, "goalie_id": g, "actual_start_flag": True}
                      for i, (_, t, g) in enumerate(starts)]).to_csv(package / "canonical_goalie_outcomes.csv",
                                                                     index=False)
        capture = root / "projected_lineups" / "season=2026" / f"slate_date={folder_day}" / "observation=20261112T193000Z"
        capture.mkdir(parents=True)
        (capture / "page.html").write_text(self.PAGE.format(header=header, published=published))
        cross = root / "cross_market_shadow" / "season=2026" / "slate_date=2026-11-12" / "run_type=REFRESH" / "state=a"
        cross.mkdir(parents=True)
        (cross / "RUN_COMPLETE.json").write_text("{}")
        pd.DataFrame([{"game_id": 7, "home_team": "CBJ", "away_team": "TBL", "challenger_favored_team": "TBL",
                       "challenger_home_win_probability": 0.45, "challenger_away_win_probability": 0.55,
                       "home_back_to_back": 0, "away_back_to_back": 1}]).to_csv(
            cross / "moneyline_production_predictions.csv", index=False)
        return pd.DataFrame([{"player_id": pid, "full_name": name, "last_name": name.split()[-1], "team_id": team,
                              "team_code": code, "game_id": 7}
                             for pid, name, team, code in ((100, "Andrei Vasilevskiy", 14, "TBL"),
                                                           (101, "Dennis Hildeby", 14, "TBL"),
                                                           (200, "Jet Greaves", 29, "CBJ"),
                                                           (201, "Elvis Merzlikins", 29, "CBJ"))])

    def _section(self, root, names):
        return watch.goalie_section(slate_date="2026-11-12", operational_root=root, names=names,
                                    lineup_root=root / "projected_lineups",
                                    cross_market_root=root / "cross_market_shadow")

    def test_backup_on_the_model_favorite_reads_pass_and_a_split_is_a_tandem(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            root = Path(temp)
            names = self._ops(root, header="LIGHTNING (10-3-2) at BLUE JACKETS (7-7-0)",
                              published="2026-11-12T14:30:00Z", folder_day="2026-11-12")
            game = self._section(root, names)["games"][0]
            self.assertEqual((game["away_goalie"]["status"], game["away_goalie"]["projected_starts"]), ("BACKUP", 2))
            self.assertEqual(game["home_goalie"]["status"], "USUAL")   # Greaves leads 6-4
            self.assertEqual(game["read"], "backup on the model's favorite (TBL): pass")
            self.assertEqual(game["back_to_back"], ["TBL"])
            md = "\n".join(watch.render_goalies(self._section(root, names)))
            self.assertIn("**backup**, 2 of 10 starts; usual Andrei Vasilevskiy, 8", md)
            highlights = "\n".join(watch.render_highlights({"goalies": self._section(root, names)}))
            self.assertIn("TBL at CBJ — backup on the model's favorite (TBL): pass", highlights)

    def test_a_game_posted_the_day_before_is_read_from_the_previous_capture(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            root = Path(temp)
            names = self._ops(root, header="LIGHTNING (10-3-2) at BLUE JACKETS (7-7-0)",
                              published="2026-11-11T14:30:00Z", folder_day="2026-11-11")
            page = root / "projected_lineups/season=2026/slate_date=2026-11-11/observation=20261112T193000Z/page.html"
            page.write_text(page.read_text().replace("<p>LIGHTNING", "<p>THURSDAY</p><p>LIGHTNING"))
            game = self._section(root, names)["games"][0]
            self.assertEqual(game["away_goalie"]["projected"], "Dennis Hildeby")
            # Without the weekday heading the rows are the 11-11 article's own games: not used.
            page.write_text(page.read_text().replace("<p>THURSDAY</p>", ""))
            self.assertEqual(self._section(root, names)["games"][0]["read"], "starter not posted yet")

    def test_tandem_goalie_is_not_a_backup(self):
        with tempfile.TemporaryDirectory(dir=watch.ROOT / "tmp") as temp:
            root = Path(temp)
            names = self._ops(root, header="LIGHTNING (10-3-2) at BLUE JACKETS (7-7-0)",
                              published="2026-11-12T14:30:00Z", folder_day="2026-11-12")
            page = next(root.glob("projected_lineups/**/page.html"))
            page.write_text(page.read_text().replace("<p>Jet Greaves</p>\n<p>Elvis Merzlikins</p>",
                                                     "<p>Elvis Merzlikins</p>\n<p>Jet Greaves</p>"))
            game = self._section(root, names)["games"][0]
            self.assertEqual(game["home_goalie"]["status"], "TANDEM")
            self.assertEqual(game["read"], "backup on the model's favorite (TBL): pass")

if __name__ == "__main__":
    unittest.main()
