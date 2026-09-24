from __future__ import annotations

import fcntl
import hashlib
import json
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

from backend.mlb.scripts.contain_mlb_stat_derived_failure import (
    CLASSIFICATION,
    FINGERPRINT_CLASSIFIER_VERSION,
    KNOWN_FAILURE_CLASSIFICATION,
    MAX_EXCERPT_LINE_CHARS,
    MAX_EXCERPT_LINES,
    SKIP_STATUS,
    Context,
    failure_fingerprint,
    run_containment,
)


CONTRACT = Path("backend/mlb/contracts/mlb_stat_derived_degraded_stage_containment_v1.json")
FIXTURE_ROOT = Path("backend/tests/fixtures/mlb_stat_derived_containment_v1")
NATURAL_STDOUT = FIXTURE_ROOT / "2026-09-24_local_daily_20260924T180004Z.stdout.txt"
NATURAL_STDERR = FIXTURE_ROOT / "2026-09-24_local_daily_20260924T180004Z.stderr.txt"
KNOWN_STDOUT = (
    "Crash during processDate(2026-09-23): UniqueViolation: duplicate key value "
    "violates unique constraint \"player_derived_stats_player_id_game_id_key\"\n"
)
KNOWN_DETAIL = "DETAIL: Key (player_id, game_id)=(453286, 824785) already exists.\n"


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

    def stream_paths(self, stdout: str = "", stderr: str = "") -> tuple[Path, Path]:
        stdout_path = self.root / "stage.stdout.log"
        stderr_path = self.root / "stage.stderr.log"
        stdout_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
        return stdout_path, stderr_path

    def context(self, *, rc: int = 2, stdout: str = "", stderr: str = "") -> Context:
        stdout_path, stderr_path = self.stream_paths(stdout, stderr)
        return Context(
            slate_date="2026-09-24",
            completed_slate_date="2026-09-23",
            run_identity="fixture_run",
            wrapper_started_at_utc="2026-09-24T15:30:05Z",
            stat_derived_rc=rc,
            failure_stdout_log=stdout_path,
            failure_stderr_log=stderr_path,
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

    def fingerprint(self, stdout: str = "", stderr: str = "", rc: int = 2):
        stdout_path, stderr_path = self.stream_paths(stdout, stderr)
        return failure_fingerprint(stdout_path, stderr_path, rc)

    def assert_known(self, fingerprint):
        self.assertEqual(fingerprint["classifier_version"], FINGERPRINT_CLASSIFIER_VERSION)
        self.assertEqual(fingerprint["classification"], KNOWN_FAILURE_CLASSIFICATION)
        self.assertTrue(fingerprint["boundary_found"])
        self.assertEqual(fingerprint["exception_class"], "UniqueViolation")
        self.assertEqual(fingerprint["constraint_name"], "player_derived_stats_player_id_game_id_key")
        self.assertEqual(fingerprint["player_id"], 453286)
        self.assertEqual(fingerprint["game_id"], 824785)
        self.assertEqual(fingerprint["gamePk"], 824785)
        self.assertEqual(fingerprint["stage_exit_code"], 2)

    def test_success_path_refuses_containment_and_creates_no_fingerprint(self):
        runner = FakeRunner()
        with self.assertRaisesRegex(ValueError, "successful"):
            run_containment(self.context(rc=0), commands=self.commands(), runner=runner, contract_path=CONTRACT)
        self.assertEqual(runner.calls, [])
        self.assertFalse((self.root / "receipts").exists())

    def test_known_unique_violation_only_on_stdout(self):
        self.assert_known(self.fingerprint(KNOWN_STDOUT + KNOWN_DETAIL, "make: *** Error 2\n"))

    def test_known_unique_violation_only_on_stderr(self):
        self.assert_known(self.fingerprint("", KNOWN_STDOUT + KNOWN_DETAIL))

    def test_known_unique_violation_split_across_streams(self):
        self.assert_known(self.fingerprint(KNOWN_STDOUT, KNOWN_DETAIL))

    def test_outer_make_errors_do_not_hide_database_exception(self):
        stdout = "make: entering directory\n" + KNOWN_STDOUT + KNOWN_DETAIL
        stderr = "make[1]: *** [mlb-insert-stat-derived] Error 1\nmake: *** [mlb-stat-derived-refresh] Error 2\n"
        self.assert_known(self.fingerprint(stdout, stderr))

    def test_natural_september24_fixture_classifies_exact_identity(self):
        fingerprint = failure_fingerprint(NATURAL_STDOUT, NATURAL_STDERR, 2)
        self.assert_known(fingerprint)
        self.assertEqual(
            fingerprint["database_detail"],
            "Key (player_id, game_id)=(453286, 824785) already exists.",
        )
        self.assertEqual(fingerprint["stdout_sha256"], hashlib.sha256(NATURAL_STDOUT.read_bytes()).hexdigest())
        self.assertEqual(fingerprint["stderr_sha256"], hashlib.sha256(NATURAL_STDERR.read_bytes()).hexdigest())

    def test_unknown_nonzero_has_no_false_structured_identity(self):
        fingerprint = self.fingerprint("", "OperationalError: server closed the connection\n", rc=75)
        self.assertEqual(fingerprint["classification"], "UNCLASSIFIED_STAT_DERIVED_NONZERO")
        self.assertFalse(fingerprint["boundary_found"])
        for key in ("exception_class", "constraint_name", "player_id", "game_id", "gamePk", "database_detail"):
            self.assertIsNone(fingerprint[key])
        self.assertEqual(fingerprint["stage_exit_code"], 75)

    def test_empty_stdout_is_hashed_and_stderr_remains_visible(self):
        fingerprint = self.fingerprint("", "RuntimeError: fixture failure\n")
        self.assertEqual(fingerprint["stdout_sha256"], hashlib.sha256(b"").hexdigest())
        self.assertIn("fixture failure", fingerprint["stderr_excerpt"])

    def test_empty_stderr_is_hashed_and_stdout_remains_visible(self):
        fingerprint = self.fingerprint("RuntimeError: fixture failure\n", "")
        self.assertEqual(fingerprint["stderr_sha256"], hashlib.sha256(b"").hexdigest())
        self.assertIn("fixture failure", fingerprint["stdout_excerpt"])

    def test_excerpts_are_bounded(self):
        stdout = "\n".join(f"RuntimeError: {index} " + ("x" * 2000) for index in range(30))
        fingerprint = self.fingerprint(stdout, "")
        lines = fingerprint["stdout_excerpt"].splitlines()
        self.assertEqual(len(lines), MAX_EXCERPT_LINES)
        self.assertTrue(all(len(line) <= MAX_EXCERPT_LINE_CHARS for line in lines))
        self.assertEqual(fingerprint["stdout_byte_count"], len(stdout.encode("utf-8")))

    def test_secret_patterns_are_sanitized_but_raw_stream_hash_is_preserved(self):
        secret = (
            "RuntimeError: postgresql://person:secret@example.invalid/db "
            "api_key=abc123 token:tok456 password=pw789 "
            "Authorization: Bearer bearer-secret\n"
        )
        fingerprint = self.fingerprint(secret, "")
        excerpt = fingerprint["stdout_excerpt"]
        for value in ("person:secret", "abc123", "tok456", "pw789", "bearer-secret"):
            self.assertNotIn(value, excerpt)
        self.assertGreaterEqual(excerpt.count("[REDACTED]"), 5)
        self.assertEqual(fingerprint["stdout_sha256"], hashlib.sha256(secret.encode()).hexdigest())

    def test_identical_input_produces_identical_fingerprint_fields(self):
        first = self.fingerprint(KNOWN_STDOUT, KNOWN_DETAIL)
        second = self.fingerprint(KNOWN_STDOUT, KNOWN_DETAIL)
        keys = (
            "fingerprint_sha256",
            "stdout_sha256",
            "stderr_sha256",
            "combined_canonical_output_sha256",
            "classification",
            "boundary_found",
        )
        self.assertEqual({key: first[key] for key in keys}, {key: second[key] for key in keys})

    def test_player_game_or_constraint_change_changes_fingerprint(self):
        baseline = self.fingerprint(KNOWN_STDOUT, KNOWN_DETAIL)["fingerprint_sha256"]
        changed_identity = self.fingerprint(
            KNOWN_STDOUT,
            "DETAIL: Key (player_id, game_id)=(453287, 824786) already exists.\n",
        )["fingerprint_sha256"]
        changed_constraint = self.fingerprint(
            KNOWN_STDOUT.replace("player_derived_stats_player_id_game_id_key", "other_unique_key"),
            KNOWN_DETAIL,
        )["fingerprint_sha256"]
        self.assertEqual(len({baseline, changed_identity, changed_constraint}), 3)

    def test_zsh_multios_preserves_live_streams_files_and_exit_code(self):
        stdout_path = self.root / "multios.stdout.log"
        stderr_path = self.root / "multios.stderr.log"
        script = r'''setopt MULTIOS
exec {live_out}>&1
exec {live_err}>&2
(umask 077; : > "$1"; : > "$2")
set +e
/bin/zsh -c 'print out-one; print -u2 err-one; print out-two; print -u2 err-two; exit 7' > "$1" >&$live_out 2> "$2" 2>&$live_err
rc=$?
exec {live_out}>&-
exec {live_err}>&-
exit "$rc"
'''
        result = subprocess.run(
            ["/bin/zsh", "-c", script, "fixture", str(stdout_path), str(stderr_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 7)
        self.assertEqual(result.stdout, "out-one\nout-two\n")
        self.assertEqual(result.stderr, "err-one\nerr-two\n")
        self.assertEqual(stdout_path.read_text(), result.stdout)
        self.assertEqual(stderr_path.read_text(), result.stderr)
        self.assertEqual(stat.S_IMODE(stdout_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(stderr_path.stat().st_mode), 0o600)

    def test_exact_uniqueness_receipt_preserves_original_nonzero(self):
        path, receipt = run_containment(
            self.context(stdout=KNOWN_STDOUT + KNOWN_DETAIL),
            commands=self.commands(),
            runner=FakeRunner(),
            contract_path=CONTRACT,
        )
        self.assertTrue(path.exists())
        self.assertEqual(receipt["failure"]["fingerprint"]["classification"], KNOWN_FAILURE_CLASSIFICATION)
        self.assertEqual(receipt["failure"]["exit_code"], 2)
        self.assertEqual(receipt["overall"]["classification"], CLASSIFICATION)
        self.assertEqual(receipt["overall"]["wrapper_exit_code"], 2)
        self.assertEqual(receipt["overall"]["receipt_state"], "COMPLETE")

    def test_other_known_nonzero_remains_visible(self):
        _, receipt = run_containment(
            self.context(rc=75, stderr="OperationalError: server closed the connection\n"),
            commands=self.commands(),
            runner=FakeRunner(),
            contract_path=CONTRACT,
        )
        self.assertEqual(receipt["failure"]["exit_code"], 75)
        self.assertEqual(receipt["failure"]["fingerprint"]["classification"], "UNCLASSIFIED_STAT_DERIVED_NONZERO")
        self.assertEqual(receipt["overall"]["wrapper_exit_code"], 75)

    def test_independent_stages_run_once_and_repeat_is_idempotent(self):
        runner = FakeRunner()
        ctx = self.context(stderr="RuntimeError: failed\n")
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
