"""Weekly NHL model evaluation: record extraction, quality and segments."""
import unittest

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
        for heading in ("Where every model stands", "Probability quality", "By line and side",
                        "Areas for improvement", "Data health", "Not yet covered"):
            self.assertIn(heading, text)


if __name__ == "__main__":
    unittest.main()
