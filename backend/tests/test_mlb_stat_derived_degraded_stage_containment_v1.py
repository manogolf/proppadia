from __future__ import annotations

import fcntl
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from backend.mlb.scripts.contain_mlb_stat_derived_failure import (
    CLASSIFICATION,
    KNOWN_DEFECT,
    SKIP_STATUS,
    Context,
    run_containment,
)


CONTRACT = Path("backend/mlb/contracts/mlb_stat_derived_degraded_stage_containment_v1.json")


class FakeRunner:
    def __init__(self, exits=None, error=None):
        self.exits = exits or {}
        self.error = error
        self.calls = []

    def __call__(self, command, check=False):
        assert check is False
        self.calls.append(tuple(command))
        if self.error and len(self.calls) == self.error[0]:
            raise self.error[1]
        return subprocess.CompletedProcess(command, self.exits.get(command[0], 0))


class ContainmentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def context(self, *, rc: int = 2, evidence: str = "") -> Context:
        failure_log = self.root / "failure.log"
        boundary = "MLB_STAT_DERIVED_STAGE_BOUNDARY run_identity=fixture_run"
        failure_log.write_text(boundary + "\n" + evidence, encoding="utf-8")
        return Context(
            slate_date="2026-09-24",
            completed_slate_date="2026-09-23",
            run_identity="fixture_run",
            wrapper_started_at_utc="2026-09-24T15:30:05Z",
            stat_derived_rc=rc,
            failure_boundary=boundary,
            failure_log=failure_log,
            receipt_root=self.root / "receipts",
            bvp_inline_rc=0,
            bvp_inline_result="BVP_INLINE_SUCCESS_ALREADY_EXISTS",
        )

    @staticmethod
    def commands():
        return {
            "full-game-totals-daily-hook": ["fake-full-game-totals", "2026-09-24", "fixture_run"],
            "totals-prospective-shadow-daily-hook": ["fake-totals-prospective", "2026-09-24", "2026-09-23"],
        }

    @staticmethod
    def stages(receipt):
        return {stage["name"]: stage for stage in receipt["stages"]}

    def test_success_path_refuses_containment_and_runs_nothing(self):
        runner = FakeRunner()
        with self.assertRaisesRegex(ValueError, "successful"):
            run_containment(self.context(rc=0), commands=self.commands(), runner=runner, contract_path=CONTRACT)
        self.assertEqual(runner.calls, [])
        self.assertFalse((self.root / "receipts").exists())

    def test_exact_uniqueness_failure_is_visible_and_overall_nonzero(self):
        evidence = (
            "psycopg.errors.UniqueViolation: duplicate key value violates unique constraint "
            '"player_derived_stats_player_id_game_id_key"\n'
            "DETAIL: Key (player_id, game_id)=(453286, 824785) already exists.\n"
        )
        path, receipt = run_containment(self.context(evidence=evidence), commands=self.commands(), runner=FakeRunner(), contract_path=CONTRACT)
        self.assertTrue(path.exists())
        self.assertEqual(receipt["failure"]["fingerprint"]["classification"], KNOWN_DEFECT)
        self.assertEqual(receipt["failure"]["exit_code"], 2)
        self.assertEqual(receipt["overall"]["classification"], CLASSIFICATION)
        self.assertEqual(receipt["overall"]["wrapper_exit_code"], 2)
        self.assertEqual(receipt["overall"]["receipt_state"], "COMPLETE")

    def test_other_known_nonzero_remains_visible(self):
        _, receipt = run_containment(self.context(rc=75, evidence="psycopg.OperationalError: server closed the connection\n"), commands=self.commands(), runner=FakeRunner(), contract_path=CONTRACT)
        self.assertEqual(receipt["failure"]["exit_code"], 75)
        self.assertEqual(receipt["failure"]["fingerprint"]["classification"], "UNCLASSIFIED_STAT_DERIVED_NONZERO")
        self.assertEqual(receipt["overall"]["wrapper_exit_code"], 75)

    def test_unexpected_exception_remains_visible(self):
        _, receipt = run_containment(self.context(rc=3, evidence="RuntimeError: unexpected exception\n"), commands=self.commands(), runner=FakeRunner(), contract_path=CONTRACT)
        self.assertEqual(receipt["failure"]["exit_code"], 3)
        self.assertIn("unexpected exception", receipt["failure"]["fingerprint"]["exact_error_text"])

    def test_independent_stages_run_once_and_repeat_is_idempotent(self):
        runner = FakeRunner()
        ctx = self.context(evidence="RuntimeError: failed\n")
        _, first = run_containment(ctx, commands=self.commands(), runner=runner, contract_path=CONTRACT)
        _, second = run_containment(ctx, commands=self.commands(), runner=runner, contract_path=CONTRACT)
        self.assertEqual([call[0] for call in runner.calls], ["fake-full-game-totals", "fake-totals-prospective"])
        self.assertTrue(all(self.stages(first)[name]["attempt_count"] == 1 for name in self.commands()))
        self.assertEqual(second, json.loads((ctx.receipt_root / ctx.slate_date / "fixture_run.json").read_text()))

    def test_dependent_and_unknown_stages_fail_closed_without_stale_fallback(self):
        _, receipt = run_containment(self.context(), commands=self.commands(), runner=FakeRunner(), contract_path=CONTRACT)
        by_name = self.stages(receipt)
        for stage in receipt["stages"]:
            if stage["classification"] in {"TRUE_DATA_DEPENDENCY", "UNKNOWN_REQUIRES_PROOF"}:
                self.assertEqual(stage["status"], SKIP_STATUS)
        self.assertEqual(by_name["optional-routine-market-sidecar"]["status"], SKIP_STATUS)
        self.assertFalse(receipt["governance"]["stale_stat_derived_output_admitted"])
        self.assertFalse(receipt["governance"]["completion_checkpoint_advanced"])

    def test_independent_nonzero_is_recorded_without_hiding_stat_failure(self):
        runner = FakeRunner(exits={"fake-full-game-totals": 9})
        _, receipt = run_containment(self.context(rc=2), commands=self.commands(), runner=runner, contract_path=CONTRACT)
        self.assertEqual(self.stages(receipt)["full-game-totals-daily-hook"]["status"], "FAILED_INDEPENDENT_STAGE")
        self.assertEqual(receipt["overall"]["wrapper_exit_code"], 2)
        self.assertEqual(receipt["overall"]["classification"], CLASSIFICATION)

    def test_interruption_after_durable_claim_does_not_retry(self):
        runner = FakeRunner(error=(1, InterruptedError("fixture interrupt")))
        ctx = self.context()
        with self.assertRaises(InterruptedError):
            run_containment(ctx, commands=self.commands(), runner=runner, contract_path=CONTRACT)
        receipt = json.loads((ctx.receipt_root / ctx.slate_date / "fixture_run.json").read_text())
        self.assertEqual(self.stages(receipt)["full-game-totals-daily-hook"]["status"], "INTERRUPTED_OR_EXCEPTION_AFTER_DURABLE_CLAIM")
        self.assertEqual(receipt["overall"]["receipt_state"], "INTERRUPTED")
        retry = FakeRunner()
        _, recovered = run_containment(ctx, commands=self.commands(), runner=retry, contract_path=CONTRACT)
        self.assertEqual([call[0] for call in retry.calls], ["fake-totals-prospective"])
        self.assertEqual(self.stages(recovered)["full-game-totals-daily-hook"]["attempt_count"], 1)
        self.assertEqual(recovered["overall"]["receipt_state"], "INCOMPLETE_CLAIMED_STAGE_NOT_RETRIED")

    def test_lock_is_released_and_bvp_is_not_reinvoked(self):
        ctx = self.context()
        path, receipt = run_containment(ctx, commands=self.commands(), runner=FakeRunner(), contract_path=CONTRACT)
        with path.with_suffix(".lock").open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertEqual(receipt["upstream"]["bvp_inline_result"], "BVP_INLINE_SUCCESS_ALREADY_EXISTS")
        self.assertTrue(receipt["upstream"]["bvp_was_not_reinvoked"])

    def test_command_allowlist_rejects_unknown_or_provider_command(self):
        bad = dict(self.commands())
        bad["provider-api"] = ["curl", "https://example.invalid"]
        runner = FakeRunner()
        with self.assertRaisesRegex(RuntimeError, "command set mismatch"):
            run_containment(self.context(), commands=bad, runner=runner, contract_path=CONTRACT)
        self.assertEqual(runner.calls, [])


if __name__ == "__main__":
    unittest.main()
