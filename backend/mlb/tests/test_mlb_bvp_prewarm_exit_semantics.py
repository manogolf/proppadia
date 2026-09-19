import os
import hashlib
import subprocess
import sys
from pathlib import Path


WRAPPER = Path(
    os.environ.get(
        "MLB_BVP_PREWARM_WRAPPER_UNDER_TEST",
        "/Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh",
    )
)

# After inline consolidation these are historical exit-semantics fixtures, not
# permission to execute the retired installed wrapper or schedule acquisition.
if (not os.environ.get("MLB_BVP_PREWARM_WRAPPER_UNDER_TEST") and WRAPPER.exists()
        and "RETIRED_BVP_PREWARM" in WRAPPER.read_text()[:512]):
    WRAPPER = Path(__file__).resolve().parents[3] / (
        "artifacts/analysis/mlb/operational_reconciliation/2026-09-18/"
        "bvp_inline_consolidation_v1/bvp_prewarm.prechange.rollback-source.txt"
    )
    assert hashlib.sha256(WRAPPER.read_bytes()).hexdigest() == "23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb"


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(0o755)


def _run(tmp_path: Path, *, bvp_rc: int = 0, slate_mode: str = "skip",
         acquisition_mode: str = "stub") -> subprocess.CompletedProcess[str]:
    home = tmp_path / "home"
    repo = home / "Projects/proppadia"
    (repo / "backend/mlb/scripts").mkdir(parents=True)
    (repo / "backend").mkdir(exist_ok=True)
    (repo / "bin").mkdir()
    (repo / "fake-bin").mkdir()
    (repo / "backend/.env").write_text("")
    source_repo = Path(__file__).resolve().parents[3]
    (repo / "backend/mlb/scripts/launchagent_lock.zsh").write_text(
        (source_repo / "backend/mlb/scripts/launchagent_lock.zsh").read_text()
    )
    (tmp_path / "offline_bvp.py").write_text(
        "import os, socket, sys\n"
        f"sys.path.insert(0, {str(source_repo)!r})\n"
        "from unittest.mock import Mock\n"
        "import requests\n"
        "from backend.mlb.scripts import refresh_mlb_bvp_pvb as b\n"
        "clock = [0.0]\n"
        "b.time.monotonic = lambda: clock[0]\n"
        "def sleep(seconds): clock[0] += seconds\n"
        "b.time.sleep = sleep\n"
        "reply = Mock()\n"
        "reply.raise_for_status.return_value = None\n"
        "reply.json.return_value = {'dates': []}\n"
        "failure = requests.ConnectionError(socket.gaierror(8, 'OFFLINE_DNS_FIXTURE'))\n"
        "b.requests.get = Mock(side_effect=([failure, reply] if os.environ['BVP_FIXTURE_MODE'] == 'recover' else failure))\n"
        "b.pg_connect = Mock(side_effect=AssertionError('LIVE_DATABASE_FORBIDDEN'))\n"
        "b._upsert_rows = Mock(return_value=0)\n"
        "b._map_games_to_local_game_ids = lambda games, date: (games, {})\n"
        "b._augment_games_with_db_starters = lambda games, date: (games, {})\n"
        "sys.exit(b.main(['--date', '2026-09-18']))\n"
    )
    _write_executable(
        repo / "fake-bin/make",
        "#!/bin/zsh\n"
        "case \"$*\" in\n"
        "  *mlb-bvp-pvb-refresh*)\n"
        "    if [[ \"${BVP_FIXTURE_MODE:-stub}\" != stub ]]; then\n"
        "      \"$BVP_FIXTURE_PYTHON\" \"${FIXTURE_ROOT}/offline_bvp.py\"; exit $?\n"
        "    fi\n"
        "    exit ${BVP_RC:-0} ;;\n"
        "  *mlb-predictions-wide*) exit 0 ;;\n"
        "  *mlb-bvp-impact-report*) print called > \"${FIXTURE_ROOT}/impact_called\"; exit 0 ;;\n"
        "  *) exit 0 ;;\n"
        "esac\n",
    )
    _write_executable(repo / "fake-bin/psql", "#!/bin/zsh\nexit 0\n")
    _write_executable(
        repo / "bin/mlb_predictive_command_guarded.sh",
        "#!/bin/zsh\n"
        "status_file=\"\"\n"
        "while [[ $# -gt 0 ]]; do\n"
        "  case \"$1\" in\n"
        "    --status-file) status_file=\"$2\"; shift 2 ;;\n"
        "    --) break ;;\n"
        "    *) shift ;;\n"
        "  esac\n"
        "done\n"
        "case \"${SLATE_MODE:-skip}\" in\n"
        "  skip) print 'SKIPPED operation=production_slate_generation' > \"$status_file\"; exit 0 ;;\n"
        "  success) print 'SUCCESS operation=production_slate_generation' > \"$status_file\"; exit 0 ;;\n"
        "  fail) print 'FAILED operation=production_slate_generation rc=9' > \"$status_file\"; exit 9 ;;\n"
        "esac\n",
    )
    result = subprocess.run(
        ["/bin/zsh", str(WRAPPER)],
        cwd=repo,
        env={
            "HOME": str(home),
            "PATH": f"{repo / 'fake-bin'}:/usr/bin:/bin:/usr/sbin:/sbin",
            "BVP_RC": str(bvp_rc),
            "SLATE_MODE": slate_mode,
            "FIXTURE_ROOT": str(tmp_path),
            "BVP_FIXTURE_MODE": acquisition_mode,
            "BVP_FIXTURE_PYTHON": sys.executable,
        },
        text=True,
        capture_output=True,
    )
    assert "acquired lock name=mlb-bvp-prewarm" in result.stdout
    assert "acquired lock name=mlb-pipeline" in result.stdout
    assert result.stdout.count("INFO released lock path=") == 2
    assert not list((repo / "artifacts/ops/locks").iterdir())
    return result


def test_successful_bvp_plus_governed_model_skip_returns_zero_and_skips_impact(tmp_path):
    result = _run(tmp_path, bvp_rc=0, slate_mode="skip")
    assert result.returncode == 0, result.stderr
    assert "BVP_ACQUISITION_STATUS=SUCCESS" in result.stdout
    assert "BVP_DOWNSTREAM_STATUS=SKIPPED_NO_QUALIFIED_MODEL" in result.stdout
    assert "BVP_IMPACT_STATUS=SKIPPED_NO_QUALIFIED_MODEL" in result.stdout
    assert "DONE local MLB BvP prewarm" in result.stdout
    assert "wrapper_rc=0 acquisition_status=SUCCESS" in result.stderr
    assert not (tmp_path / "impact_called").exists()


def test_genuine_bvp_acquisition_failure_remains_nonzero(tmp_path):
    result = _run(tmp_path, bvp_rc=7, slate_mode="skip")
    assert result.returncode == 7
    assert "BVP_ACQUISITION_STATUS=FAILED" in result.stderr
    assert "wrapper_rc=7 acquisition_status=FAILED" in result.stderr
    assert "DONE local MLB BvP prewarm" not in result.stdout


def test_genuine_slate_failure_remains_nonzero(tmp_path):
    result = _run(tmp_path, bvp_rc=0, slate_mode="fail")
    assert result.returncode == 9
    assert "wrapper_rc=9 acquisition_status=SUCCESS" in result.stderr
    assert "BVP_DOWNSTREAM_STATUS=FAILED_TECHNICAL" in result.stderr
    assert "downstream_status=FAILED_TECHNICAL" in result.stderr


def test_retry_success_then_model_skip_retains_done_and_zero_exit(tmp_path):
    result = _run(tmp_path, acquisition_mode="recover")
    assert result.returncode == 0, result.stderr
    assert "ACQUISITION_SUCCESS_AFTER_TRANSIENT_NETWORK_RETRY" in result.stderr
    assert "BVP_DOWNSTREAM_STATUS=SKIPPED_NO_QUALIFIED_MODEL" in result.stdout
    assert "BVP_IMPACT_STATUS=SKIPPED_NO_QUALIFIED_MODEL" in result.stdout
    assert "DONE local MLB BvP prewarm" in result.stdout
    assert not (tmp_path / "impact_called").exists()


def test_retry_exhaustion_retains_nonzero_and_no_done(tmp_path):
    result = _run(tmp_path, acquisition_mode="exhaust")
    assert result.returncode != 0
    assert "ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED" in result.stderr
    assert "acquisition_status=FAILED downstream_status=NOT_STARTED impact_status=NOT_STARTED" in result.stderr
    assert "DONE local MLB BvP prewarm" not in result.stdout
