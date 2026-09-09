import csv
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.mlb.scripts import acquire_and_audit_mlb_oddsapi_historical_joint_strength_transfer_v1 as audit


class HistoricalJointTransferTest(unittest.TestCase):
    def test_frozen_book_list_and_cost_contract(self):
        selected, rationale = audit.bookmaker_frames()
        self.assertEqual(len(selected), 10)
        self.assertIn("pinnacle", set(selected.bookmaker_key))
        self.assertIn("betonlineag", set(selected.bookmaker_key))
        self.assertNotIn("fliff", set(selected.bookmaker_key))
        self.assertEqual(rationale.loc[rationale.bookmaker_key.eq("fliff"), "selected"].iloc[0], False)
        self.assertEqual(29 * audit.EXPECTED_COST_PER_REQUEST, 290)
        self.assertLessEqual(290, audit.MAX_CREDITS)

    def test_secret_is_removed(self):
        self.assertEqual(audit.safe_params({"apiKey": "secret", "markets": "h2h"}), {"markets": "h2h"})

    def test_http_error_is_status_and_code_only(self):
        credential = "dynamic-test-credential-material"
        content = (f'{{"error_code":"UPSTREAM_FAILURE","message":"https://example.test/?apiKey={credential}"}}').encode()
        rendered = audit.http_error_message("historical odds", 500, content)
        self.assertNotIn(credential, rendered)
        self.assertIn("HTTP 500", rendered)
        self.assertIn("error_code=UPSTREAM_FAILURE", rendered)
        self.assertNotIn("example.test", rendered)
        persisted = audit.redact_sensitive_bytes(content)
        self.assertNotIn(credential.encode(), persisted)
        self.assertIn(b"apiKey=[REDACTED]", persisted)

    def test_invalid_timestamp_error_preserves_only_safe_code(self):
        credential = "another-dynamic-test-credential"
        content = (f'{{"error_code":"INVALID_HISTORICAL_TIMESTAMP","message":"apiKey={credential}"}}').encode()
        rendered = audit.http_error_message("historical odds", 422, content)
        self.assertEqual(
            rendered,
            "Odds API historical odds HTTP 422; error_code=INVALID_HISTORICAL_TIMESTAMP; "
            "raw response and quota headers preserved",
        )

    def test_retry_logging_is_append_only_and_secret_free(self):
        credential = "retry-dynamic-test-credential"
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            audit.append_retry_ledger(output, {
                "request_id": "R1", "prior_status": "HTTP_ERROR", "x_requests_last": "0",
                "retry_allowed": True, "decision_reason": f"apiKey={credential}",
            })
            audit.append_retry_ledger(output, {
                "request_id": "R2", "prior_status": "TRANSPORT_ERROR", "x_requests_last": "",
                "retry_allowed": False, "decision_reason": "UNKNOWN_CHARGE",
            })
            text = (output / "append_only_retry_decision_ledger.csv").read_text()
            self.assertNotIn(credential, text)
            self.assertIn("apiKey=[REDACTED]", text)
            self.assertEqual(len(list(csv.DictReader(text.splitlines()))), 2)
            self.assertEqual(audit.retry_decision("HTTP_ERROR", "0"),
                             (True, "DEMONSTRABLY_UNCHARGED_HTTP_FAILURE"))
            self.assertFalse(audit.retry_decision("TRANSPORT_ERROR", "")[0])

    def test_request_ledger_preserves_quota_metadata_without_secret(self):
        credential = "ledger-dynamic-test-credential"
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            audit.append_ledger(output, {
                "request_id": "R1", "status": "HTTP_ERROR", "http_status": 422,
                "x_requests_last": 0, "x_requests_used": 101, "x_requests_remaining": 899,
                "error": f"https://example.test/odds?apiKey={credential}&markets=h2h",
            })
            row = next(csv.DictReader((output / "append_only_request_ledger.csv").read_text().splitlines()))
            self.assertNotIn(credential, str(row))
            self.assertIn("apiKey=[REDACTED]", row["error"])
            self.assertEqual(row["x_requests_last"], "0")
            self.assertEqual(row["x_requests_used"], "101")
            self.assertEqual(row["x_requests_remaining"], "899")

    def test_subprocess_command_and_traceback_rendering_are_redacted(self):
        credential = "subprocess-dynamic-test-credential"
        url = f"https://example.test/odds?apiKey={credential}&markets=h2h"
        command = ["curl", url, "--api-key", credential]
        safe_command = audit.sanitize_command(command)
        self.assertNotIn(credential, safe_command)
        self.assertEqual(safe_command.count("[REDACTED]"), 2)
        try:
            raise subprocess.CalledProcessError(1, command, stderr=f"request failed: {url}")
        except subprocess.CalledProcessError as exc:
            rendered_exception = audit.render_exception(exc)
            rendered_traceback = audit.render_traceback(exc)
        self.assertNotIn(credential, rendered_exception)
        self.assertNotIn(credential, rendered_traceback)
        self.assertIn("[REDACTED]", rendered_exception)
        self.assertIn("[REDACTED]", rendered_traceback)

    def test_economics(self):
        rows = pd.DataFrame({"game_id": [1, 2], "game_date": ["2026-08-01", "2026-08-02"],
                             "admission_class": ["VALID_PREGAME_PRICE", "VALID_PREGAME_PRICE"],
                             "evaluated_win": [1, 0], "selected_american_price": [-150, -110],
                             "selected_decimal_price": [1.666666666667, 1.909090909091]})
        result = audit.economics(rows)
        self.assertAlmostEqual(result["net_units"], -1/3)
        self.assertAlmostEqual(result["hypothetical_flat_risk_roi"], -1/6)

    def test_team_normalization(self):
        self.assertEqual(audit.norm_team("N.Y. Yankees"), "new york yankees")


if __name__ == "__main__": unittest.main()
