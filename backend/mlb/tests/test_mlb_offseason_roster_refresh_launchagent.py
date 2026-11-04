"""Off-season MLB roster-only refresh: LaunchAgent template and wrapper behavior."""

import os
import plistlib
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
LABEL = "com.proppadia.mlb.offseason-roster-refresh"
PLIST = ROOT / "backend/mlb/launchagents" / f"{LABEL}.plist"
WRAPPER = ROOT / "bin/mlb_offseason_roster_refresh.sh"
INSTALLER = ROOT / "bin/mlb_install_offseason_roster_refresh.sh"
LOCK_HELPERS = ROOT / "backend/mlb/scripts/launchagent_lock.zsh"


def _write_exe(path: Path, body: str) -> None:
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class TestOffseasonRosterRefreshTemplate(unittest.TestCase):
    def test_plist_runs_repo_wrapper_daily_at_0615(self):
        data = plistlib.loads(PLIST.read_bytes())
        self.assertEqual(data["Label"], LABEL)
        self.assertEqual(data["ProgramArguments"], ["/bin/zsh", str(WRAPPER)])
        self.assertEqual(data["StartCalendarInterval"], {"Hour": 6, "Minute": 15})
        self.assertFalse(data["RunAtLoad"])
        for key in ("StandardOutPath", "StandardErrorPath"):
            self.assertTrue(data[key].startswith(str(ROOT / "artifacts/ops") + "/"))

    def test_installer_uses_this_template_and_offers_rollback(self):
        text = INSTALLER.read_text()
        self.assertIn(f'label="{LABEL}"', text)
        self.assertIn("backend/mlb/launchagents/$label.plist", text)
        self.assertIn("--rollback", text)
        self.assertNotIn("com.proppadia.mlb.refresh.daily", text)
        self.assertNotIn("com.proppadia.nhl", text)


class TestOffseasonRosterRefreshWrapper(unittest.TestCase):
    """Run a copy of the wrapper in a scratch repo with stub launchctl/make."""

    def setUp(self):
        base = ROOT / "tmp"
        base.mkdir(exist_ok=True)
        self.work = Path(tempfile.mkdtemp(dir=base, prefix="mlb_offseason_roster_"))
        self.addCleanup(shutil.rmtree, self.work, ignore_errors=True)
        (self.work / "bin").mkdir()
        (self.work / "backend/mlb/scripts").mkdir(parents=True)
        shutil.copy(WRAPPER, self.work / "bin" / WRAPPER.name)
        shutil.copy(LOCK_HELPERS, self.work / "backend/mlb/scripts" / LOCK_HELPERS.name)
        (self.work / "backend/.env").write_text("STUB_ENV_LOADED=1\n")
        self.stubs = self.work / "stubs"
        self.stubs.mkdir()
        self.make_log = self.work / "make.log"
        _write_exe(
            self.stubs / "make",
            f'echo "$* restricted=${{MLB_USE_RESTRICTED_INGEST:-}} date=${{MLB_ROSTER_DATE:-}} '
            f'env=${{STUB_ENV_LOADED:-}}" >> "{self.make_log}"\nexit "${{STUB_MAKE_RC:-0}}"\n',
        )
        _write_exe(self.stubs / "dscacheutil", "echo 'ip_address: 127.0.0.1'\n")

    def _run(self, *, full_refresh_loaded: bool, make_rc: int = 0):
        _write_exe(self.stubs / "launchctl", f"exit {0 if full_refresh_loaded else 113}\n")
        env = dict(os.environ)
        env.update(
            PATH=f"{self.stubs}:{env.get('PATH', '/usr/bin:/bin')}",
            STUB_MAKE_RC=str(make_rc),
            MLB_ROSTER_REFRESH_RETRY_SLEEP_SEC="0",
            MLB_PIPELINE_LOCK_WAIT_SEC="0",
        )
        return subprocess.run(
            ["/bin/zsh", str(self.work / "bin" / WRAPPER.name)],
            env=env, capture_output=True, text=True, timeout=60,
        )

    def test_skips_while_full_daily_refresh_is_loaded(self):
        out = self._run(full_refresh_loaded=True)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("SKIP com.proppadia.mlb.refresh.daily is loaded", out.stdout)
        self.assertFalse(self.make_log.exists())

    def test_runs_only_restricted_roster_refresh_while_paused(self):
        out = self._run(full_refresh_loaded=False)
        self.assertEqual(out.returncode, 0, out.stderr)
        calls = self.make_log.read_text().splitlines()
        self.assertEqual(len(calls), 1)
        self.assertRegex(
            calls[0], r"^mlb-roster-refresh-all restricted=1 date=\d{4}-\d{2}-\d{2} env=1$"
        )
        self.assertFalse(any((self.work / "artifacts/ops/locks").glob("*.lock")))

    def test_retries_then_fails_with_make_exit_code(self):
        out = self._run(full_refresh_loaded=False, make_rc=3)
        self.assertEqual(out.returncode, 3)
        self.assertEqual(len(self.make_log.read_text().splitlines()), 4)
        self.assertIn("failed after 4 attempts", out.stderr)


if __name__ == "__main__":
    unittest.main()
