import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as study


def prediction(home_probability=0.65, away_probability=0.35, strong_side="HOME"):
    return pd.Series({
        "game_key": "MLB|2026-09-10|1", "game_date": "2026-09-10", "game_id": 1,
        "scheduled_start_utc": "2026-09-10T23:00:00Z", "home_team": "Home Club", "away_team": "Away Club",
        "home_model_probability": home_probability, "away_model_probability": away_probability,
        "model_strong_side": strong_side, "prediction_payload_sha256": "a"*64,
    })


def event(home=-180, away=160, include_pinnacle=True):
    books = []
    if include_pinnacle:
        books.append({"key": "pinnacle", "last_update": "2026-09-10T12:29:00Z", "markets": [{
            "key": "h2h", "last_update": "2026-09-10T12:29:00Z",
            "outcomes": [{"name": "Home Club", "price": home}, {"name": "Away Club", "price": away}],
        }]})
    return {"id": "provider-1", "commence_time": "2026-09-10T23:00:00Z",
            "home_team": "Home Club", "away_team": "Away Club", "bookmakers": books}


class ProspectiveAgreementSeparationTest(unittest.TestCase):
    def test_freeze_is_fixed_and_excludes_prior_cohort(self):
        freeze = study.freeze_payload("2026-09-09T20:00:00Z")
        self.assertEqual(freeze["prospective_start_game_date"], "2026-09-10")
        self.assertFalse(freeze["prior_56_20_rows_admitted"])
        self.assertEqual(freeze["market"]["reference_book"], "pinnacle")
        self.assertEqual(freeze["bookmakers"], list(study.BOOKS))
        self.assertEqual(len(freeze["bookmakers"]), 10)
        self.assertEqual(freeze["model"]["strong_definition"], "selected probability > 0.60 strict")

    def test_agreement_and_nonagreement_states_are_frozen(self):
        requested, returned = "2026-09-10T12:30:00Z", "2026-09-10T12:29:55Z"
        agree, prices = study.classify_risk(prediction(), event(), requested, returned, "b"*64)
        self.assertEqual(agree["risk_state"], "MARKET_STRONG_MODEL_AGREES")
        self.assertEqual(agree["agreement_indicator"], 1)
        self.assertEqual(len(prices), 10)
        not_strong, _ = study.classify_risk(prediction(.58, .42, "NONE"), event(), requested, returned, "b"*64)
        self.assertEqual(not_strong["risk_state"], "MARKET_STRONG_MODEL_NOT_STRONG")
        self.assertEqual(not_strong["agreement_indicator"], 0)
        disagrees, _ = study.classify_risk(prediction(.35, .65, "AWAY"), event(), requested, returned, "b"*64)
        self.assertEqual(disagrees["risk_state"], "MARKET_STRONG_MODEL_DISAGREES")
        self.assertEqual(disagrees["agreement_indicator"], 0)

    def test_missing_model_and_price_states_are_retained(self):
        requested, returned = "2026-09-10T12:30:00Z", "2026-09-10T12:29:55Z"
        missing_model, _ = study.classify_risk(None, event(), requested, returned, "b"*64)
        self.assertEqual(missing_model["risk_state"], "MARKET_STRONG_MISSING_MODEL")
        self.assertEqual(missing_model["risk_set_eligible"], 0)
        missing_price, prices = study.classify_risk(prediction(), event(include_pinnacle=False), requested, returned, "b"*64)
        self.assertEqual(missing_price["risk_state"], "REFERENCE_BOOKMAKER_ABSENT")
        self.assertEqual(len(prices), 10)

    def test_market_update_after_snapshot_is_not_admitted(self):
        requested, returned = "2026-09-10T12:30:00Z", "2026-09-10T12:29:55Z"
        payload = event()
        payload["bookmakers"][0]["markets"][0]["last_update"] = "2026-09-10T12:30:00Z"
        risk, prices = study.classify_risk(prediction(), payload, requested, returned, "b"*64)
        self.assertEqual(risk["risk_state"], "REFERENCE_STALE_OR_POST_START")
        self.assertEqual(risk["reference_market_last_update_utc"], "2026-09-10T12:30:00Z")
        pinnacle = next(row for row in prices if row["bookmaker_key"] == "pinnacle")
        self.assertEqual(pinnacle["price_state"], "STALE_OR_POST_START")

    def test_no_vig_and_paid_break_even_are_distinct(self):
        home, away = study.no_vig(-180, 160)
        self.assertAlmostEqual(home + away, 1.0)
        self.assertGreater(home, study.BOUNDARY)
        self.assertNotAlmostEqual(home, study.implied_probability(-180))

    def test_append_only_composite_bookmaker_identity(self):
        with sqlite3.connect(":memory:") as conn:
            study.schema(conn)
            base = {"game_key": "g", "bookmaker_last_update_utc": None, "home_american_price": None,
                    "away_american_price": None, "selected_american_price": None, "selected_decimal_price": None,
                    "selected_paid_break_even": None, "price_state": "BOOKMAKER_ABSENT",
                    "raw_response_sha256": "a"*64}
            for book in ("pinnacle", "betonlineag"):
                row = {**base, "bookmaker_key": book}; row["row_sha256"] = study.digest(row)
                conn.execute("PRAGMA foreign_keys=OFF")
                self.assertTrue(study.immutable_insert(conn, "bookmaker_prices", ("game_key", "bookmaker_key"), row))
                self.assertFalse(study.immutable_insert(conn, "bookmaker_prices", ("game_key", "bookmaker_key"), row))
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM bookmaker_prices").fetchone()[0], 2)

    def test_request_ledger_redacts_and_retains_quota(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp); credential = "prospective-dynamic-test-credential"
            study.append_request_ledger(out, {"request_id": "r", "status": "HTTP_ERROR",
                "x_requests_last": 0, "x_requests_used": 10, "x_requests_remaining": 90,
                "error": f"https://example.test/?apiKey={credential}"})
            text = (out/"append_only_request_ledger.csv").read_text()
            self.assertNotIn(credential, text)
            self.assertIn("apiKey=[REDACTED]", text)
            self.assertIn(",0,10,90,", text)

    def test_empty_initialized_study_is_interim_insufficient(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); out, ledger = base/"out", base/"ledger.sqlite3"
            study.initialize(out, ledger)
            summary = study.report(out, ledger)
            self.assertEqual(summary["classification"], "SEPARATION_EVIDENCE_INSUFFICIENT")
            self.assertFalse(summary["decision_made"])
            self.assertEqual(summary["eligible_resolved_unique_games"], 0)
            self.assertFalse(summary["prior_56_20_rows_included"])
            self.assertEqual(pd.read_csv(out/"prospective_risk_set_ledger.csv").shape[0], 0)

    def test_acquisition_requires_separate_positive_authorization(self):
        with sqlite3.connect(":memory:") as conn:
            study.schema(conn)
            with self.assertRaises(ValueError): study.establish_authorization(conn, 0)
            study.establish_authorization(conn, 20)
            with self.assertRaises(RuntimeError): study.establish_authorization(conn, 30)

    def test_clustered_economic_contrast_uses_one_row_per_game_and_book(self):
        frame = pd.DataFrame({"game_date": ["2026-09-10", "2026-09-11"] * 2,
                              "agreement_indicator": [1, 1, 0, 0],
                              "flat_risk_return": [.5, -.2, -.5, -.4]})
        result = study.clustered_group_difference(frame, "flat_risk_return")
        self.assertAlmostEqual(result["difference"], .6)

    def test_unknown_charge_state_refuses_retry_before_credential_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); out, ledger = base/"out", base/"ledger.sqlite3"
            study.initialize(out, ledger)
            values = {"game_key": "MLB|2026-09-10|1", "game_date": "2026-09-10", "game_id": 1,
                      "scheduled_start_utc": "2026-09-10T23:00:00Z",
                      "prediction_timestamp_utc": "2026-09-10T12:30:00Z",
                      "prediction_cutoff_utc": "2026-09-10T12:00:00Z", "home_team": "Home Club",
                      "away_team": "Away Club", "home_model_probability": .65,
                      "away_model_probability": .35, "model_strong_side": "HOME",
                      "prediction_payload_sha256": "a"*64}
            values["row_sha256"] = study.digest(values)
            with sqlite3.connect(ledger) as conn:
                study.immutable_insert(conn, "predictions", "game_key", values); conn.commit()
            study.append_request_ledger(out, {"request_id": "PROSPECTIVE_H2H_2026-09-10",
                "recorded_at_utc": "2026-09-10T12:31:00Z", "game_date": "2026-09-10",
                "requested_timestamp_utc": "2026-09-10T12:30:00Z", "attempt": 1,
                "status": "REQUEST_STARTED", "authorized_credit_ceiling": 10})
            with self.assertRaisesRegex(RuntimeError, "unknown charge state"):
                study.acquire_date(out, ledger, "2026-09-10", 10, 1)


if __name__ == "__main__":
    unittest.main()
