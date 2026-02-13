import unittest

from backend.scripts.check_validators import (
    expect_list_or_error_object,
    expect_ok,
    expect_ok_count_rows,
    expect_ping_sport,
    expect_predict_probability_and_token,
)


class TestSharedCheckValidators(unittest.TestCase):
    def test_expect_ok(self):
        self.assertTrue(expect_ok({"ok": True})[0])
        self.assertFalse(expect_ok({"ok": False})[0])

    def test_expect_ping_sport(self):
        self.assertTrue(expect_ping_sport("mlb")({"ok": True, "sport": "mlb"})[0])
        self.assertFalse(expect_ping_sport("mlb")({"ok": True, "sport": "nhl"})[0])

    def test_expect_predict_probability_and_token(self):
        self.assertTrue(
            expect_predict_probability_and_token({"probability": 0.5, "commit_token": "a.b.c"})[0]
        )
        self.assertFalse(
            expect_predict_probability_and_token({"probability": "bad", "commit_token": "a.b.c"})[0]
        )

    def test_expect_ok_count_rows(self):
        self.assertTrue(expect_ok_count_rows({"ok": True, "count": 2, "rows": [{}, {}]})[0])
        self.assertFalse(expect_ok_count_rows({"ok": True, "count": "2", "rows": []})[0])

    def test_expect_list_or_error_object(self):
        self.assertTrue(expect_list_or_error_object([{"x": 1}])[0])
        self.assertTrue(expect_list_or_error_object({"ok": False, "error": "x"})[0])
        self.assertFalse(expect_list_or_error_object({"ok": True})[0])


if __name__ == "__main__":
    unittest.main()
