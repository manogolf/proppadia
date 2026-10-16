from __future__ import annotations

import ast
import inspect
import os
import re
import sys
import unittest
from unittest.mock import Mock, patch

from backend.mlb.scripts import audit_mlb_moneyline_probability_region_premise_v1 as audit


class MoneylineAuditReadOnlyIsolationTests(unittest.TestCase):
    def test_only_two_postgres_query_shapes_are_selects_on_target_relations(self) -> None:
        queries = (audit.PREDICTIONS_QUERY, audit.LATEST_RESOLVED_QUERY)
        relations = set()
        for query in queries:
            normalized = " ".join(query.split()).lower()
            self.assertTrue(normalized.startswith("select "))
            self.assertIsNone(re.search(r"\b(insert|update|delete|truncate|alter|drop|create|call|do)\b", normalized))
            relations.update(re.findall(r"\b(?:from|join)\s+(mlb\.[a-z_][a-z0-9_]*)", normalized))
        self.assertEqual(
            relations,
            {
                "mlb.public_game_moneyline_predictions",
                "mlb.public_game_moneyline_outcomes",
            },
        )

    def test_postgres_execute_sites_reference_only_the_two_query_constants(self) -> None:
        tree = ast.parse(inspect.getsource(audit))
        calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "cur"
        ]
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            {ast.unparse(call.args[0]) for call in calls},
            {"sql", "LATEST_RESOLVED_QUERY"},
        )
        load_predictions = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "load_predictions"
        )
        sql_assignment = next(
            node for node in load_predictions.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "sql" for target in node.targets)
        )
        self.assertEqual(ast.unparse(sql_assignment.value), "PREDICTIONS_QUERY")

    def test_override_is_used_only_during_cli_run(self) -> None:
        connect = Mock(return_value="dedicated-connection")
        fake_psycopg = Mock()
        fake_psycopg.connect = connect
        fake_psycopg.rows.dict_row = "dict-row"
        with patch.dict(os.environ, {audit.AUDIT_DATABASE_URL_ENV: "placeholder-dsn"}), patch.dict(
            sys.modules, {"psycopg": fake_psycopg, "psycopg.rows": fake_psycopg.rows}
        ), patch.object(audit, "_shared_pg_connect", return_value="shared-connection") as shared:
            self.assertEqual(audit.pg_connect(), "shared-connection")
            shared.assert_called_once_with()

            def invoke_scoped_factory(*_args):
                self.assertEqual(audit.pg_connect(), "dedicated-connection")

            with patch.object(audit, "run", side_effect=invoke_scoped_factory), patch.object(
                sys, "argv", ["audit", "--resolved-cutoff", "2026-09-08"]
            ), patch("builtins.print"):
                audit.main()

            self.assertIsNone(audit._CLI_DATABASE_URL_OVERRIDE)
            connect.assert_called_once_with(
                "placeholder-dsn",
                row_factory="dict-row",
                prepare_threshold=None,
                options="-c search_path=mlb,public",
            )

    def test_other_shared_callers_ignore_audit_environment_override(self) -> None:
        from backend.shared.db import pg as shared_pg

        connect = Mock(return_value="shared-connection")
        with patch.dict(os.environ, {audit.AUDIT_DATABASE_URL_ENV: "placeholder-dsn"}), patch.object(
            shared_pg, "get_database_url", return_value="ordinary-database-url"
        ) as get_url, patch.object(shared_pg.psycopg, "connect", connect):
            self.assertEqual(shared_pg.pg_connect(), "shared-connection")
        get_url.assert_called_once_with()
        self.assertEqual(connect.call_args.args[0], "ordinary-database-url")


if __name__ == "__main__":
    unittest.main()
