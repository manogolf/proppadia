from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from backend.mlb.public_game_predictions.durable_store_v1 import append_outcome_grade, append_prediction_rows
from backend.mlb.public_game_predictions.phase_gating_v1 import (
    MoneylinePhaseGateError,
    classify_moneyline_row,
)
from backend.mlb.scripts import run_mlb_public_game_moneyline_daily_v1 as runner
from backend.mlb.scripts.activate_mlb_postseason_schedule_authority_v4 import (
    CLOSE_SHA,
    PKS,
    ROUNDS,
    validate_child,
)
from backend.mlb.season_transition.contract_v1 import GAME_TYPE_CONTRACT
from backend.mlb.season_transition.game_phase_authority_v1 import (
    GamePhaseAuthorityError,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT, sha256_file
from backend.mlb.season_transition.runtime_schedule_authority_v1 import (
    RuntimeScheduleAuthority,
    phase_authority_binding,
)


OCT4_SCHEDULE = REPO_ROOT / (
    "artifacts/ops/mlb_public_game_moneyline_history_schedules/2026-10-04/"
    "20261004T153006154701Z_2026-08-05_2026-10-04_"
    "64a8ec297e9522f418e4405c2691007422d425ff5a00412cbb9c3a96a47c94e9.json"
)


class RuntimeScheduleAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.holder = Path(tempfile.mkdtemp(prefix="runtime_phase_test_", dir=REPO_ROOT / "tmp"))

    def tearDown(self) -> None:
        shutil.rmtree(self.holder)

    @staticmethod
    def game(pk: int, kind: str = "D", *, round_label: str = "NL Division Series",
             start: str = "2026-10-20T20:00:00Z", status: dict | None = None) -> dict:
        return {
            "gamePk": pk, "gameType": kind, "season": "2026", "officialDate": start[:10],
            "gameDate": start, "seriesDescription": round_label,
            "status": status or {"abstractGameState": "Preview", "codedGameState": "S",
                                  "detailedState": "Scheduled", "statusCode": "S"},
        }

    def authority(self, games: list[dict]) -> RuntimeScheduleAuthority:
        path = self.holder / "retained_schedule.json"
        raw = json.dumps({"dates": [{"games": games}]}, sort_keys=True, separators=(",", ":")).encode()
        path.write_bytes(raw)
        return RuntimeScheduleAuthority(
            source_path=path,
            expected_source_sha256=hashlib.sha256(raw).hexdigest(),
            base=HashedProposalAuthority(),
        )

    def test_retained_october_schedules_bind_all_six_division_series_identities(self) -> None:
        result = validate_child(allow_candidate=True)
        self.assertEqual(result["new_game_pks"], list(PKS))
        self.assertEqual(sha256_file(OCT4_SCHEDULE), OCT4_SCHEDULE.stem.rsplit("_", 1)[-1])
        authority = RuntimeScheduleAuthority(
            source_path=OCT4_SCHEDULE,
            expected_source_sha256=sha256_file(OCT4_SCHEDULE),
        )
        binding = phase_authority_binding(authority)
        postseason = {item["game_pk"]: item for item in binding["decisions"]
                      if item["source_game_type"] in {"F", "D", "L", "W", "P", "C"}}
        self.assertEqual(set(postseason), set(PKS) | {849841, 849842, 849843, 849844,
                                                       849845, 849846, 849848, 849849, 849851})
        for pk in PKS:
            self.assertEqual(postseason[pk]["normalized_phase"], "POSTSEASON")
            self.assertEqual(postseason[pk]["source_round"], ROUNDS[pk])
            self.assertEqual(postseason[pk]["schedule_source_sha256"], sha256_file(OCT4_SCHEDULE))
            decision = classify_moneyline_row({"game_id": pk, "game_date": "2026-10-04"},
                                              authority=authority)
            self.assertEqual(decision.evaluation_partition, "POSTSEASON")
            self.assertEqual(decision.schedule_source_sha256, sha256_file(OCT4_SCHEDULE))
        self.assertEqual(sha256_file(REPO_ROOT / "artifacts/operational/mlb/season_close/2026/regular_season_close.json"), CLOSE_SHA)

    def test_all_contract_postseason_round_codes_remain_representable(self) -> None:
        for index, (game_type, spec) in enumerate(GAME_TYPE_CONTRACT.items()):
            if spec["phase"] != "POSTSEASON":
                continue
            label = f"Source label {game_type}"
            authority = self.authority([self.game(990000 + index, game_type, round_label=label)])
            record = authority.lookup_exact(990000 + index)
            self.assertEqual(record.source_game_type, game_type)
            self.assertEqual(record.postseason_round, spec["postseason_round"])
            self.assertEqual(record.source_round, label)

    def test_exact_game_authority_not_date_coverage_admits_runtime_identity_only(self) -> None:
        authority = self.authority([self.game(991001)])
        accepted = classify_moneyline_row({"game_id": 991001, "game_date": "2026-10-20"}, authority=authority)
        self.assertEqual(accepted.evaluation_partition, "POSTSEASON")
        with self.assertRaisesRegex(MoneylinePhaseGateError, "MONEYLINE_PHASE_AUTHORITY_INVALID"):
            classify_moneyline_row({"game_id": 991002, "game_date": "2026-10-20"}, authority=authority)

    def test_source_binding_distinguishes_authoritative_special_exclusion(self) -> None:
        authority = self.authority([self.game(991003, "A", round_label="All-Star Game")])
        decisions = phase_authority_binding(authority)["decisions"]
        decision = next(item for item in decisions if item["game_pk"] == 991003)
        self.assertEqual(decision["authority_decision"], "EXCLUDED_SPECIAL_AUTHORITATIVE_TYPE")
        self.assertIsNone(decision["failure_code"])

    def test_hash_duplicate_conflict_unknown_type_and_missing_identity_fail_closed(self) -> None:
        path = self.holder / "schedule.json"
        raw = json.dumps({"dates": [{"games": [self.game(991010)]}]}, separators=(",", ":")).encode()
        path.write_bytes(raw)
        with self.assertRaisesRegex(GamePhaseAuthorityError, "SOURCE_HASH_MISMATCH"):
            RuntimeScheduleAuthority(source_path=path, expected_source_sha256="0" * 64)
        duplicate = self.game(991011)
        with self.assertRaisesRegex(GamePhaseAuthorityError, "DUPLICATE_GAME_PK"):
            self.authority([duplicate, duplicate])
        conflict = self.game(991012)
        conflict["gameData"] = {"game": {"pk": 991013}}
        with self.assertRaisesRegex(GamePhaseAuthorityError, "GAME_PK_CONFLICT"):
            self.authority([conflict])
        for bad_game in (
            {**self.game(991014), "gameType": "X"},
            {key: value for key, value in self.game(991015).items() if key != "seriesDescription"},
            {**self.game(991016), "status": None},
            {**self.game(991017), "season": None},
        ):
            with self.subTest(pk=bad_game["gamePk"]), self.assertRaises(GamePhaseAuthorityError):
                self.authority([bad_game])

    def test_persistence_and_grading_gates_precede_database_access(self) -> None:
        with patch("backend.mlb.public_game_predictions.durable_store_v1.pg_connect") as connect:
            with self.assertRaises(MoneylinePhaseGateError):
                append_prediction_rows([{"admission_status": "ADMITTED_SHADOW", "game_id": 991020,
                                         "game_date": "2026-10-20"}])
            with self.assertRaises(MoneylinePhaseGateError):
                append_outcome_grade({"official_status": "Final", "game_id": 991020,
                                      "game_date": "2026-10-20"})
            connect.assert_not_called()

    def test_source_bound_postseason_identity_can_pass_prediction_and_grade_writes(self) -> None:
        pk = 991021
        authority = self.authority([self.game(pk)])
        prediction = {
            "game_date": "2026-10-20", "game_id": pk, "winner_model_version": "test",
            "prediction_snapshot_class": "test", "scheduled_start_utc": "2026-10-20T20:00:00Z",
            "prediction_timestamp_utc": "2026-10-20T18:00:00Z", "prediction_cutoff_utc": "2026-10-20T18:00:00Z",
            "home_team": "H", "away_team": "A", "home_win_probability": .6,
            "away_win_probability": .4, "predicted_winner": "H", "confidence_band": "TEST",
            "data_quality_status": "PASS", "winner_model_hash": "a" * 64,
            "scorer_hash": "b" * 64, "source_schedule_hash": "c" * 64,
            "team_state_hash": "d" * 64, "admission_status": "ADMITTED_SHADOW",
        }
        grade = {
            "official_status": "Final", "game_date": "2026-10-20", "game_id": pk,
            "winner_model_version": "test", "prediction_snapshot_class": "test",
            "official_home_runs": 4, "official_away_runs": 2, "official_winner": "H",
            "prediction_correct": True, "observed_outcome_probability": .6,
            "brier_contribution": .16, "log_loss_contribution": .51,
            "confidence_band": "TEST", "official_source_path": "retained/final.json",
            "official_source_sha256": "e" * 64, "grading_timestamp_utc": "2026-10-21T00:00:00Z",
        }
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (pk,)
        with patch("backend.mlb.public_game_predictions.durable_store_v1.pg_connect",
                   return_value=connection):
            self.assertEqual(append_prediction_rows([prediction], authority=authority), 1)
            self.assertTrue(append_outcome_grade(grade, authority=authority))
        self.assertEqual(connection.cursor.call_count, 2)

    def test_failure_receipt_writer_cannot_mask_original_stage_error(self) -> None:
        original = RuntimeError("original phase-gate error")
        runner._ATTEMPT_CONTEXT.update({"run_identity": "test-run", "mlb_date": "2026-10-04",
                                        "stage": "MONEYLINE_PREDICTION_PERSISTENCE"})
        with patch.object(runner, "_execute", side_effect=original), \
             patch.object(runner, "_write_attempt_receipt", side_effect=OSError("receipt disk error")):
            with self.assertRaises(RuntimeError) as raised:
                runner.main()
        self.assertIs(raised.exception, original)

    def test_attempt_receipt_is_create_only_and_hash_identified(self) -> None:
        runner._ATTEMPT_CONTEXT.clear()
        runner._ATTEMPT_CONTEXT.update({"run_identity": "unique-test-run", "mlb_date": "2026-10-04",
                                        "started_at_utc": "2026-10-04T20:00:00Z", "stage": "GATE"})
        with patch.object(runner, "DEFAULT_ATTEMPT_RECEIPT_DIR", self.holder / "receipts"):
            receipt = runner._write_attempt_receipt(classification="MONEYLINE_PHASE_GATE_FAILURE",
                                                     error=RuntimeError("blocked"))
            self.assertEqual(sha256_file(Path(receipt["path"])), receipt["sha256"])
            with self.assertRaises(FileExistsError):
                runner._write_attempt_receipt(classification="MONEYLINE_PHASE_GATE_FAILURE",
                                              error=RuntimeError("different"))


if __name__ == "__main__":
    unittest.main()
