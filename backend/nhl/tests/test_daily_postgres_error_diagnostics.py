from __future__ import annotations

import unittest

from backend.nhl.daily_orchestration import postgres_error_diagnostic


class PostgresErrorDiagnosticTests(unittest.TestCase):
    def test_extracts_sqlstate_category_and_redacted_error_line(self):
        diagnostic = postgres_error_diagnostic(
            "ERROR:  42501: permission denied for function safe_fill\n"
            "DETAIL: password=hunter2 source_value=18\n"
            "CONTEXT: SQL statement \"UPDATE ...\"\n"
            "STATEMENT: SELECT secret_token='abc123';\n"
            "connection postgresql://user:pass@host/db"
        )

        self.assertEqual(diagnostic["postgres_sqlstate"], "42501")
        self.assertEqual(diagnostic["postgres_error_category"], "INSUFFICIENT_PRIVILEGE")
        self.assertIn("permission denied for function safe_fill", diagnostic["sanitized_stderr"])
        self.assertNotIn("hunter2", str(diagnostic))
        self.assertNotIn("source_value=18", str(diagnostic))
        self.assertNotIn("abc123", str(diagnostic))
        self.assertNotIn("postgresql://", str(diagnostic))

    def test_classifies_client_and_network_sqlstates(self):
        self.assertEqual(
            postgres_error_diagnostic("psql: error: connection refused")
            ["postgres_error_category"],
            "PSQL_CLIENT_ERROR",
        )
        self.assertEqual(
            postgres_error_diagnostic("ERROR:  08006: connection failure")
            ["postgres_error_category"],
            "CONNECTION_EXCEPTION",
        )


if __name__ == "__main__":
    unittest.main()
