"""PostgreSQL rollback-only exercise of active NHL PP-TOI write SQL.

Run with NHL_PP_TOI_TEST_DSN set to the configured database URL. All fixture
DDL and DML are performed in a uniquely named schema and rolled back.
"""

from __future__ import annotations

import ast
import csv
import importlib
import json
import os
import tempfile
import unittest
import uuid
import warnings
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import psycopg

from backend.nhl import pp_toi_source
from backend.nhl.pp_toi_source import capture_stage_pp_toi_conflicts
from backend.nhl.scripts import run_nhl_postgame_reconciliation as postgame
from backend.nhl.scripts import fill_pp_toi_minutes_for_date as pp_fill


ROOT = Path(__file__).resolve().parents[3]
RECOVERY = ROOT / "artifacts/operational/nhl/pp_toi_recovery_execution/run_id=pp_toi_recovery_20261016T152648Z"
DSN = os.environ.get("NHL_PP_TOI_TEST_DSN")


def _daily_promotion_sql(schema: str, slate_date: str) -> str:
    """Evaluate the actual daily promotion f-string from cli.py's AST."""
    cli = importlib.import_module("backend.nhl.cli")
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        tree = ast.parse(Path(cli.__file__).read_text())
    value = None
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(isinstance(target, ast.Name) and target.id == "promote_sql" for target in targets):
            value = node.value
            break
    if value is None:
        raise AssertionError("daily promote_sql assignment not found")
    namespace = dict(vars(cli))
    namespace.update({
        "yday": slate_date,
        "src_blocks": ", s.blocks",
        "joined_blocks": ", src.blocks AS blocks",
        "insert_blocks": ", blocks",
        "select_blocks": ", blocks",
        "update_blocks": ",\n      blocks = COALESCE(EXCLUDED.blocks, nhl.skater_game_logs_raw.blocks)",
    })
    rendered = eval(compile(ast.Expression(value), cli.__file__, "eval"), namespace)
    return rendered.replace("nhl.", f"{schema}.")


def _postgame_promotion_sql(schema: str, slate_date: str) -> tuple[str, tuple]:
    """Capture the actual skater promotion SQL from the production function."""
    captured = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            captured.append((sql, params))

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

        def commit(self):
            pass

    with patch.object(postgame.psycopg, "connect", return_value=Connection()):
        postgame._promote_stage("postgresql://capture-only", slate_date)
    if not captured:
        raise AssertionError("postgame skater promotion SQL was not captured")
    sql, params = captured[0]
    return sql.replace("nhl.", f"{schema}."), tuple(params)


def _fill_conflict_sql(schema: str) -> str:
    """Extract the actual existing-value conflict query from fill.main()."""
    tree = ast.parse(Path(pp_fill.__file__).read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or node.func.attr != "execute":
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        sql = node.args[0].value
        if isinstance(sql, str) and "SELECT l.player_id::bigint" in sql and "team_external_ids" in sql:
            return sql.replace("nhl.", f"{schema}.")
    raise AssertionError("backfill conflict query not found")


@unittest.skipUnless(DSN, "set NHL_PP_TOI_TEST_DSN for PostgreSQL integration")
class PpToiPostgresWritePathTests(unittest.TestCase):
    def setUp(self):
        self.schema = "pp_toi_test_" + uuid.uuid4().hex[:12]
        self.conn = psycopg.connect(DSN)
        self.conn.execute("BEGIN")
        self.conn.execute("SET LOCAL statement_timeout = '45s'")
        self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self._create_fixture_tables()

    def tearDown(self):
        schema = self.schema
        self.conn.rollback()
        self.conn.close()
        with psycopg.connect(DSN) as check:
            remaining = check.execute("SELECT to_regnamespace(%s)", (schema,)).fetchone()[0]
        self.assertIsNone(remaining, "rollback-only integration schema was not removed")

    def _create_fixture_tables(self):
        qschema = f'"{self.schema}"'
        self.conn.execute(f"CREATE TABLE {qschema}.games(game_id bigint PRIMARY KEY, game_date date, home_team_id bigint, away_team_id bigint)")
        self.conn.execute(f"CREATE TABLE {qschema}.team_external_ids(team_id bigint, provider text, provider_team_id text)")
        self.conn.execute(f"""CREATE TABLE {qschema}.roster_status(
            game_id bigint, player_id bigint, team_id bigint, asof_ts timestamptz)""")
        self.conn.execute(f"""CREATE TABLE {qschema}.import_skater_logs_stage(
            player_id bigint, game_id bigint, game_date date, team_id bigint,
            shots_on_goal integer, shot_attempts integer, toi_minutes numeric,
            pp_toi_minutes numeric, blocks integer, hits integer, fenwick_for numeric,
            missed_shots integer, blocked_shots_taken integer, rebounds_for integer,
            takeaways integer, giveaways integer, penalties_drawn integer,
            penalties_taken integer, ev_shot_attempts numeric, pp_shot_attempts numeric,
            sh_shot_attempts numeric, ev_sog integer, pp_sog integer, sh_sog integer,
            goals integer, assists integer, PRIMARY KEY(game_id, player_id))""")
        self.conn.execute(f"""CREATE TABLE {qschema}.skater_game_logs_raw(
            player_id bigint, game_id bigint, team_id bigint, opponent_id bigint,
            is_home boolean, game_date date, shots_on_goal integer, shot_attempts integer,
            toi_minutes numeric, pp_toi_minutes numeric, blocks integer, hits integer,
            fenwick_for numeric, missed_shots integer, blocked_shots_taken integer,
            rebounds_for integer, takeaways integer, giveaways integer,
            penalties_drawn integer, penalties_taken integer, ev_shot_attempts numeric,
            pp_shot_attempts numeric, sh_shot_attempts numeric, ev_sog integer,
            pp_sog integer, sh_sog integer, goals integer, assists integer, points integer,
            PRIMARY KEY(player_id, game_id))""")

    def test_daily_postgame_and_backfill_preserve_recovered_source_values(self):
        with (RECOVERY / "pp_toi_recovery_candidates.csv").open(newline="") as handle:
            candidates = list(csv.DictReader(handle))
        by_key = {(int(row["game_id"]), int(row["player_id"])): row for row in candidates}
        # These identities are retained official-PBP/boxscore/shift reconciliations.
        chosen = [by_key[(2023020001, pid)] for pid in (8474151, 8474568, 8475158, 8474679)]
        selected_ids = {int(row["player_id"]) for row in chosen}
        new_acquisition = next(
            row for row in candidates
            if int(row["game_id"]) == 2023020001 and int(row["player_id"]) not in selected_ids
        )
        chosen.append(new_acquisition)
        positive, zero, null_fill, unknown, new_acquisition = chosen
        game_id = int(positive["game_id"])
        home_id, away_id = 14, 18  # official numeric NHL IDs from the retained game receipt
        self.conn.execute(f'INSERT INTO "{self.schema}".games VALUES (%s,%s,%s,%s)', (game_id, "2023-10-10", home_id, away_id))
        with self.conn.cursor() as cursor:
            cursor.executemany(
                f'INSERT INTO "{self.schema}".team_external_ids VALUES (%s,%s,%s)',
                [(home_id, "nhl", str(home_id)), (away_id, "nhl", str(away_id))],
            )
        rows = []
        raw_values = []
        source_values = {}
        for row in chosen:
            pid = int(row["player_id"])
            team_id = int(row["official_team_id"])
            raw_team_id = team_id
            original = Decimal(row["reconstructed_pp_toi_minutes"])
            source_values[pid] = original
            self.conn.execute(
                f'INSERT INTO "{self.schema}".roster_status VALUES (%s,%s,%s,now())',
                (game_id, pid, team_id),
            )
            # Simulate the post-recovery state: two verified protected values,
            # one supported NULL fill candidate, and one unknown source row.
            current = (original if row is positive or row is zero else None)
            if row is not new_acquisition:
                raw_values.append((pid, game_id, raw_team_id, current))
            incoming = {
                int(positive["player_id"]): original + Decimal("0.8"),
                int(zero["player_id"]): Decimal("0.4"),
                int(null_fill["player_id"]): None,
                int(unknown["player_id"]): None,
                int(new_acquisition["player_id"]): original,
            }[pid]
            rows.append((pid, game_id, "2023-10-10", team_id, 1, 1, 15,
                         incoming, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         0, 0, 0, 1, 0, 0, 0, 0))
        with self.conn.cursor() as cursor:
            cursor.executemany(
                f'INSERT INTO "{self.schema}".skater_game_logs_raw(player_id,game_id,team_id,pp_toi_minutes) VALUES (%s,%s,%s,%s)',
                raw_values,
            )
            cursor.executemany(
                f'INSERT INTO "{self.schema}".import_skater_logs_stage VALUES ({",".join(["%s"]*26)})',
                rows,
            )

        daily_sql = _daily_promotion_sql(self.schema, "2023-10-10")
        self.conn.execute(daily_sql)
        postgame_sql, postgame_params = _postgame_promotion_sql(self.schema, "2023-10-10")
        self.conn.execute(postgame_sql, postgame_params)

        # Execute the actual backfill helper against the same isolated schema.
        pp_map = {
            int(positive["player_id"]): 10.0,
            int(zero["player_id"]): 2.0,
            int(null_fill["player_id"]): float(source_values[int(null_fill["player_id"])]),
            # Unknown source is deliberately absent from the reconstructed map.
        }
        with self.conn.cursor() as cursor:
            changed = pp_fill.update_game(
                cursor, game_id, pp_map,
                target_relation=f"{self.schema}.skater_game_logs_raw",
            )
        self.assertEqual(changed[2], 1)
        ids = [int(row["player_id"]) for row in chosen]
        with self.conn.cursor() as cursor:
            cursor.execute(
                f'SELECT player_id,pp_toi_minutes FROM "{self.schema}".skater_game_logs_raw WHERE game_id=%s',
                (game_id,),
            )
            observed = {int(pid): value for pid, value in cursor.fetchall()}
        self.assertEqual(Decimal(str(observed[ids[0]])), source_values[ids[0]])
        self.assertEqual(Decimal(str(observed[ids[1]])), Decimal("0"))
        self.assertEqual(Decimal(str(observed[ids[2]])), source_values[ids[2]])
        self.assertIsNone(observed[ids[3]])
        self.assertEqual(Decimal(str(observed[ids[4]])), source_values[ids[4]])

        # Re-run every writer: populated positive/zero values remain stable and
        # the already-filled NULL is not rewritten.
        self.conn.execute(daily_sql)
        self.conn.execute(postgame_sql, postgame_params)
        with self.conn.cursor() as cursor:
            repeated = pp_fill.update_game(
                cursor, game_id, pp_map,
                target_relation=f"{self.schema}.skater_game_logs_raw",
            )
        self.assertEqual(repeated[2], 0)
        with self.conn.cursor() as cursor:
            cursor.execute(
                f'SELECT player_id,pp_toi_minutes FROM "{self.schema}".skater_game_logs_raw WHERE game_id=%s',
                (game_id,),
            )
            after_repeat = {int(pid): value for pid, value in cursor.fetchall()}
        self.assertEqual(after_repeat, observed)

        with self.conn.cursor() as cursor:
            cursor.execute(_fill_conflict_sql(self.schema), (game_id, list(pp_map)))
            fill_conflict_rows = cursor.fetchall()
        fill_conflicts = [
            pp_toi_source.pp_toi_conflict(
                game_id=game_id, player_id=row[0], team_id=row[1],
                official_team_id=row[2], official_team_mapping_count=row[3],
                existing_value=row[4], incoming_value=pp_map[int(row[0])],
                source_kind="RECONSTRUCTED_SHIFT_OVERLAP",
                source_reference="isolated recovered-record fixture",
            )
            for row in fill_conflict_rows
        ]
        fill_conflicts = [conflict for conflict in fill_conflicts if conflict is not None]
        self.assertEqual({int(row["player_id"]) for row in fill_conflicts}, {ids[0], ids[1]})
        self.assertEqual({int(row["official_team_id"]) for row in fill_conflicts},
                         {int(positive["official_team_id"]), int(zero["official_team_id"])})

        # Run the actual stage/raw conflict SELECT against this isolated
        # schema, then persist its run-bound diagnostic with source evidence.
        class ProxyCursor:
            def __enter__(self):
                self.cursor = self.connection.cursor()
                return self

            def __exit__(self, *_args):
                self.cursor.close()
                return False

            def __init__(self, connection, schema):
                self.connection = connection
                self.schema = schema

            def execute(self, sql, params):
                self.cursor.execute(sql.replace("nhl.", f"{self.schema}."), params)

            def fetchall(self):
                return self.cursor.fetchall()

        class ProxyConnection:
            def __init__(self, connection, schema):
                self.connection = connection
                self.schema = schema

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def cursor(self):
                return ProxyCursor(self.connection, self.schema)

        with tempfile.TemporaryDirectory() as root:
            journal = Path(root) / "official_source_fixture.json"
            journal.write_text(json.dumps({
                "game_id": game_id,
                "boxscore_sha256": positive["source_boxscore_sha256"],
                "shiftcharts_sha256": positive["source_shiftcharts_sha256"],
            }))
            with patch.dict(os.environ, {
                "NHL_PP_TOI_CONFLICT_ROOT": root,
                "NHL_OFFICIAL_REQUEST_JOURNAL": str(journal),
            }), patch.object(pp_toi_source.psycopg, "connect", return_value=ProxyConnection(self.conn, self.schema)):
                diagnostic = capture_stage_pp_toi_conflicts(
                    "postgresql://fixture", slate_date="2023-10-10",
                    run_id="rollback_sql_test", stage="integration",
                )
            self.assertEqual(diagnostic["conflict_count"], 2)
            payload = json.loads(Path(diagnostic["path"]).read_text())
            self.assertEqual(payload["source_evidence_sha256"], __import__("hashlib").sha256(journal.read_bytes()).hexdigest())
            conflict_keys = {(int(row["game_id"]), int(row["player_id"])) for row in payload["conflicts"]}
            self.assertEqual(conflict_keys, {(game_id, ids[0]), (game_id, ids[1])})
            self.assertEqual({int(row["team_id"]) for row in payload["conflicts"]},
                             {int(positive["official_team_id"]), int(zero["official_team_id"])})
            self.assertEqual({int(row["official_team_id"]) for row in payload["conflicts"]},
                             {int(positive["official_team_id"]), int(zero["official_team_id"])})
            self.assertTrue(all(row["official_team_mapping_status"] == "VERIFIED_UNIQUE_CROSSWALK"
                                for row in payload["conflicts"]))


if __name__ == "__main__":
    unittest.main()
