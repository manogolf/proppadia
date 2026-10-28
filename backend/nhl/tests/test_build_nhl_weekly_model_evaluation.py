"""Weekly NHL model evaluation: record extraction, quality and segments."""
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.scripts import build_nhl_weekly_model_evaluation as ev


def summary(ml_ref, ml_chal, hgb_role="PRODUCTION"):
    return {"models": {
        "moneyline": {"reference": {"correct": 3, "incorrect": 1, "model_identity": ml_ref},
                      "challenger": {"correct": 3, "incorrect": 1, "model_identity": ml_chal}},
        "sog": {"reference": {"overall": {"wins": 8, "losses": 2}},
                "A_ARM": {"overall": {"wins": 7, "losses": 3}},
                "REFERENCE": {"overall": {"wins": 0, "losses": 0}}},
        "points": {"authority_context": {"hgb_evaluation": {"role": hgb_role, "metrics": {
            "selected_side_record": {"wins": 9, "losses": 1}},
            "phoenix_same_capture_selected_side_record": {"wins": 4, "losses": 6}}}},
        "saves": {"reference": {"overall": {"wins": 5, "losses": 5}}}}}


V3 = {"model_version": "V3"}


class RecordTests(unittest.TestCase):
    def test_same_model_in_two_roles_counts_once_and_empty_models_drop(self):
        frame = ev.model_records({"2026-10-20": summary(V3, V3)})
        ml = frame[frame.lane == "moneyline"]
        self.assertEqual(ml.model.tolist(), ["V3"])
        self.assertEqual((int(ml.wins.sum()), int(ml.losses.sum())), (3, 1))
        self.assertNotIn("shadow: REFERENCE", frame.model.tolist())
        self.assertIn("shadow: A_ARM", frame.model.tolist())

    def test_control_names_label_models_and_shadow_hgb_days_are_excluded(self):
        frame = ev.model_records({"2026-10-05": summary({"control_name": "V2"}, V3, hgb_role="SHADOW")})
        self.assertEqual(sorted(frame[frame.lane == "moneyline"].model), ["V2", "V3"])
        self.assertFalse((frame.lane == "points").any())
        self.assertFalse((frame.lane == "sog").any())  # before the SOG season start


    def test_shrunk_rate_shadow_counts_its_latest_pregame_record(self):
        s = summary(V3, V3)
        s["sog_shrunk_rate_k180_shadow"] = {"status": "COMPLETE", "latest_pregame": {"overall": {"wins": 9, "losses": 1}}}
        frame = ev.model_records({"2026-10-20": s})
        row = frame[frame.model == "shadow: SHRUNK_180 (latest pregame)"]
        self.assertEqual((int(row.wins.sum()), int(row.losses.sum())), (9, 1))

    def test_hgb_count_shadow_counts_its_latest_pregame_record(self):
        s = summary(V3, V3)
        s["sog_count_hgb_shadow"] = {"status": "COMPLETE", "latest_pregame": {"overall": {"wins": 8, "losses": 2}}}
        row = ev.model_records({"2026-10-20": s})
        row = row[row.model == "shadow: HGB count model (latest pregame)"]
        self.assertEqual((int(row.wins.sum()), int(row.losses.sum())), (8, 2))


    def test_moneyline_shadow_records_come_from_grade_packages_latest_grade_wins(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for name, ts, correct in (("grade=a", "2026-10-21T08:00:00Z", [True, True]),
                                      ("grade=b", "2026-10-21T09:00:00Z", [False, True])):
                d = root / "state=x" / name; d.mkdir(parents=True)
                pd.DataFrame({"slate_date": ["2026-10-20"] * 2, "game_id": [1, 2], "correct": correct,
                              "evaluation_status": ["REGULAR_SEASON_GRADED"] * 2,
                              "control_name": ["V2"] * 2, "grading_timestamp_utc": [ts] * 2}
                             ).to_csv(d / "graded_moneyline_shadow_results.csv", index=False)
            extra = ev.moneyline_shadow_records(root)
            self.assertEqual(extra.to_dict("records"), [
                {"slate_date": "2026-10-20", "lane": "moneyline", "model": "V2", "wins": 1, "losses": 1}])
            frame = ev.model_records({"2026-10-20": summary(V3, V3)}, extra)
            row = frame[frame.model == "V2"]
            self.assertEqual((int(row.wins.sum()), int(row.losses.sum())), (1, 1))
            # A day the summary already covers keeps the summary's record.
            covered = summary({"control_name": "V2"}, V3)
            frame = ev.model_records({"2026-10-20": covered}, extra)
            row = frame[frame.model == "V2"]
            self.assertEqual((int(row.wins.sum()), int(row.losses.sum())), (3, 1))


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.rows = ev.enrich(pd.DataFrame({
            "slate_date": ["2026-10-20"] * 4, "game_id": [1, 1, 2, 2], "player_id": [1, 2, 3, 4],
            "line": [1.5, 1.5, 1.5, 1.5], "p_over": [0.8, 0.2, 0.6, 0.3],
            "selected_side": ["OVER", "UNDER", "OVER", "UNDER"],
            "grading_state": ["WIN", "WIN", "LOSS", "LOSS"]}).assign(lane="sog"))

    def test_quality_uses_over_outcomes_and_pick_confidence(self):
        q = ev.quality(self.rows)
        self.assertEqual((q["wins"], q["losses"]), (2, 2))
        self.assertAlmostEqual(q["mean_pick_probability"], (0.8 + 0.8 + 0.6 + 0.7) / 4)
        # y_over: 1, 0, 0, 1
        self.assertAlmostEqual(q["brier"], ((0.8 - 1) ** 2 + 0.2 ** 2 + 0.6 ** 2 + (0.3 - 1) ** 2) / 4)

    def test_segments_report_gap_and_respect_minimum(self):
        seg = ev.segments(self.rows, min_n=1)
        self.assertTrue((seg.gap == seg.win_rate - seg.predicted).all())
        self.assertTrue(ev.segments(self.rows, min_n=5).empty)

    def test_render_has_every_section(self):
        records = ev.model_records({"2026-10-20": summary(V3, V3)})
        text = ev.render("2026-10-20", ["2026-10-20"], records, self.rows, "NHL PULSE x: PASS")
        for heading in ("Where every model stands", "Probability quality", "Two-sided lines",
                        "By line and side", "Areas for improvement", "Data health", "Not yet covered"):
            self.assertIn(heading, text)

    def test_two_sided_marking_and_report_level(self):
        rows = self.rows.copy()
        rows["two_sided"] = ev.mark_two_sided(rows, [[1, 1, 1.5], [2, 3, 1.5]])
        self.assertEqual(rows.two_sided.tolist(), [True, False, True, False])
        self.assertTrue(ev.mark_two_sided(rows, None).isna().all())
        two = ev.two_sided_rows(rows)
        self.assertEqual(ev.quality(two)["n"], 2)
        self.assertTrue(ev.two_sided_rows(self.rows).empty)  # no evidence column
        records = ev.model_records({"2026-10-20": summary(V3, V3)})
        text = ev.render("2026-10-20", ["2026-10-20"], records, rows, None)
        # 2 of 4 settled picks were two-sided: one win, one loss.
        self.assertIn("| sog | week | 2 | 50.0% | 1-1 | 50.0% |", text)
        self.assertIn("| sog | 1.5 | OVER | 1-1 | 1-1 | 1-1 | 50.0% | 1-1 | 50.0% |", text)

    def test_gap_bands_and_table_compare_model_with_market(self):
        rows = self.rows.copy()
        # p_over 0.8 OVER vs market 0.6 (gap +20); 0.2 UNDER vs market 0.33 over -> 0.67 under (+13);
        # 0.6 OVER vs 0.55 (+5); 0.3 UNDER vs market 0.10 over -> 0.90 under (-20).
        keys = [[1, 1, 1.5, 0.6], [1, 2, 1.5, 0.33], [2, 3, 1.5, 0.55], [2, 4, 1.5, 0.10]]
        rows["market_p_over"] = ev.market_p_over(rows, keys)
        rows = ev.enrich(rows.drop(columns=["win", "p_pick", "y_over", "band"]))
        self.assertEqual(rows.gap_band.tolist(), ["over 15 above", "10-15 above", "within 10", "model 10+ below"])
        table = ev.gap_table(rows, ["2026-10-20"])
        self.assertEqual(list(table.gap_band), ["model 10+ below", "within 10", "10-15 above", "over 15 above"])
        top = table[table.gap_band.eq("over 15 above")].iloc[0]
        self.assertEqual((top.wins, top.losses), (1, 0))
        self.assertAlmostEqual(top.market_p, 0.6)
        text = ev.render("2026-10-20", ["2026-10-20"], ev.model_records({"2026-10-20": summary(V3, V3)}), rows, None)
        self.assertIn("Model vs market by gap", text)
        self.assertIn("| sog | over 15 above | OVER | 1-0 | 1-0 |", text)

    def test_keys_without_a_market_price_still_mark_two_sided(self):
        keys = [[1, 1, 1.5], [1, 2, 1.5, None]]
        self.assertEqual(ev.mark_two_sided(self.rows, keys).tolist(), [True, True, False, False])
        self.assertTrue(ev.market_p_over(self.rows, keys).isna().all())

    def test_formula_shadow_rows_are_graded_on_the_shadow_side(self):
        import tempfile
        from unittest import mock
        shadow = pd.DataFrame({"game_id": [1, 1], "player_id": [7, 8], "line": [1.5, 2.5],
                               "shadow_p_over": [0.3, 0.7], "official_sog": [1, 2]})
        with tempfile.TemporaryDirectory(dir=ev.ROOT / "tmp") as temp, \
                mock.patch.object(ev, "CACHE", Path(temp)), \
                mock.patch.object(ev.baseline, "_package", return_value=Path(temp)), \
                mock.patch("backend.nhl.sog_shrunk_rate_shadow.latest_pregame_rows", return_value=shadow):
            rows = ev.formula_shadow_rows("2026-10-26", {"summary_identity": "x" * 20, "reconciliation_package": "p"})
        # 0.3 -> UNDER 1.5 with 1 shot: WIN; 0.7 -> OVER 2.5 with 2 shots: LOSS.
        self.assertEqual(rows.selected_side.tolist(), ["UNDER", "OVER"])
        self.assertEqual(rows.grading_state.tolist(), ["WIN", "LOSS"])


if __name__ == "__main__":
    unittest.main()
