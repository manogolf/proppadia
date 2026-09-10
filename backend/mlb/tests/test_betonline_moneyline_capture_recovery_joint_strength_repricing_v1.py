import unittest

from backend.mlb.scripts import audit_mlb_betonline_moneyline_capture_recovery_joint_strength_repricing_v1 as audit


def event(away="+120", home="-132"):
    odds = {}
    if away is not None:
        odds[audit.AWAY_ODD] = {"started": False, "byBookmaker": {"betonline": {
            "available": True, "odds": away, "lastUpdatedAt": "2026-08-07T12:29:00Z"}}}
    if home is not None:
        odds[audit.HOME_ODD] = {"started": False, "byBookmaker": {"betonline": {
            "available": True, "odds": home, "lastUpdatedAt": "2026-08-07T12:29:00Z"}}}
    return {"eventID": "source-1", "teams": {
        "away": {"names": {"long": "Away Club"}}, "home": {"names": {"long": "Home Club"}}},
        "status": {"startsAt": "2026-08-07T20:00:00Z"}, "odds": odds}


class RecoveryTests(unittest.TestCase):
    def test_two_sided_moneyline_supports_roi_and_no_vig(self):
        row = audit.extract_sgo_betonline_event(event())
        self.assertEqual(row["away_american_price"], 120)
        self.assertEqual(row["home_american_price"], -132)
        self.assertIs(row["both_sides_available"], True)
        self.assertAlmostEqual(row["no_vig_home_probability"] + row["no_vig_away_probability"], 1)

    def test_one_sided_moneyline_is_retained_for_selected_side_roi(self):
        row = audit.extract_sgo_betonline_event(event(home=None))
        self.assertEqual(row["away_american_price"], 120)
        self.assertNotIn("home_american_price", row)
        self.assertIs(row["both_sides_available"], False)
        self.assertIsNone(row["no_vig_home_probability"])
        self.assertEqual(audit.decimal_price(row["away_american_price"]), 2.2)

    def test_invalid_or_unavailable_price_is_not_recovered(self):
        self.assertIsNone(audit.extract_sgo_betonline_event(event(away="0", home=None)))

    def test_authoritative_schedule_has_five_fixed_windows(self):
        self.assertEqual([x[0] for x in audit.WINDOWS],
                         ["05:30", "08:30", "11:00", "13:00", "16:30"])


if __name__ == "__main__":
    unittest.main()
