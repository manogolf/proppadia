from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HELPER = ROOT / "bin/mlb_rolling_integrity_failure_containment.sh"
LOCK_HELPER = ROOT / "backend/mlb/scripts/launchagent_lock.zsh"
INSTALLED_WRAPPER = Path.home() / "bin/proppadia_mlb_refresh_daily.sh"


class RollingIntegrityContinuationTests(unittest.TestCase):
    def _make_hook(self, directory: Path, filename: str, rc: int) -> Path:
        path = directory / filename
        path.write_text(
            "#!/bin/zsh\n"
            "print -r -- \"$0|$*\" >> \"$CALL_LOG\"\n"
            f"exit {rc}\n",
            encoding="utf-8",
        )
        path.chmod(0o700)
        return path

    def test_independent_hooks_run_once_and_preserve_original_failure_and_locks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            call_log = root / "calls.log"
            lock_root = root / "locks"
            full_hook = self._make_hook(root, "full-game-hook", 19)
            totals_hook = self._make_hook(root, "totals-shadow-hook", 23)
            dependent_marker = root / "dependent-stage-ran"

            harness = root / "wrapper-harness.zsh"
            harness.write_text(
                "#!/bin/zsh\n"
                "set -euo pipefail\n"
                f"source {LOCK_HELPER}\n"
                "trap 'rc=$?; release_launchagent_locks; exit \"$rc\"' EXIT\n"
                "acquire_launchagent_lock mlb-daily-refresh 0 21600\n"
                "acquire_launchagent_lock mlb-pipeline 0 21600\n"
                "set +e\n"
                f"CALL_LOG={call_log} MLB_FULL_GAME_TOTALS_HOOK={full_hook} "
                f"MLB_TOTALS_PROSPECTIVE_SHADOW_HOOK={totals_hook} "
                f"{HELPER} 2 2026-09-26 2026-09-25 run-test 2026-09-26T12:30:00Z\n"
                "helper_rc=$?\n"
                "set -e\n"
                f"[[ ! -e {dependent_marker} ]]\n"
                "exit \"$helper_rc\"\n",
                encoding="utf-8",
            )
            harness.chmod(0o700)
            env = {**os.environ, "LA_LOCK_ROOT": str(lock_root)}
            result = subprocess.run(
                ["/bin/zsh", str(harness)], cwd=ROOT, env=env,
                text=True, capture_output=True, check=False,
            )

            self.assertEqual(result.returncode, 2, result.stderr)
            calls = call_log.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(calls), 2, calls)
            self.assertEqual(calls[0].split("|", 1)[1], "2026-09-26 run-test")
            self.assertEqual(
                calls[1].split("|", 1)[1],
                "2026-09-26 2026-09-25 run-test 2026-09-26T12:30:00Z auto",
            )
            self.assertIn(
                "SKIPPED_UPSTREAM_ROLLING_INTEGRITY_FAILED stage=predictions-wide-and-player-prop-capture",
                result.stdout,
            )
            self.assertIn(
                "SKIPPED_UPSTREAM_ROLLING_INTEGRITY_FAILED stage=optional-routine-market-sidecar",
                result.stdout,
            )
            self.assertIn(
                "full_game_rc=19 totals_shadow_rc=23 original_rc=2",
                result.stderr,
            )
            self.assertFalse((lock_root / "mlb-daily-refresh.lock").exists())
            self.assertFalse((lock_root / "mlb-pipeline.lock").exists())

    def test_rejects_zero_or_malformed_integrity_status_without_running_hooks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            call_log = root / "calls.log"
            full_hook = self._make_hook(root, "full-game-hook", 0)
            totals_hook = self._make_hook(root, "totals-shadow-hook", 0)
            env = {
                **os.environ,
                "CALL_LOG": str(call_log),
                "MLB_FULL_GAME_TOTALS_HOOK": str(full_hook),
                "MLB_TOTALS_PROSPECTIVE_SHADOW_HOOK": str(totals_hook),
            }
            for invalid_rc in ("0", "bad"):
                result = subprocess.run(
                    ["/bin/zsh", str(HELPER), invalid_rc, "2026-09-26", "2026-09-25", "run-test", "start"],
                    cwd=ROOT, env=env, text=True, capture_output=True, check=False,
                )
                self.assertEqual(result.returncode, 2)
            self.assertFalse(call_log.exists())

    def test_installed_wrapper_routes_failure_before_dependent_chain_and_exit_trap(self) -> None:
        self.assertTrue(INSTALLED_WRAPPER.is_file())
        text = INSTALLED_WRAPPER.read_text(encoding="utf-8")
        gate = text.index("make mlb-check-rolling-integrity")
        captured_rc = text.index("rolling_integrity_rc=$?", gate)
        containment = text.index("bin/mlb_rolling_integrity_failure_containment.sh", captured_rc)
        fail_exit = text.index('exit "$rolling_integrity_rc"', containment)
        dependent_chain = text.index("bin/mlb_predictions_wide_guarded.sh", fail_exit)
        self.assertLess(gate, captured_rc)
        self.assertLess(captured_rc, containment)
        self.assertLess(containment, fail_exit)
        self.assertLess(fail_exit, dependent_chain)
        self.assertIn("trap 'wrapper_rc=$?; release_launchagent_locks;", text)
        self.assertIn('write_launchagent_summary "$wrapper_rc"; exit "$wrapper_rc"\' EXIT', text)
        self.assertIn("write_mlb_rolling_integrity_receipt", text)
        self.assertIn('rolling_integrity_rc=$?', text)
        self.assertIn('--exit-code "$rolling_integrity_rc"', text)
        self.assertIn('--output "$MLB_ROLLING_CHECK_OUTPUT"', text)


if __name__ == "__main__":
    unittest.main()
