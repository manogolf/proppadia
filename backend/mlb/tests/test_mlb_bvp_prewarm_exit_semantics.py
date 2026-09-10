import os
import subprocess
from pathlib import Path


WRAPPER = Path(
    os.environ.get(
        "MLB_BVP_PREWARM_WRAPPER_UNDER_TEST",
        "/Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh",
    )
)


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(0o755)


def _run(tmp_path: Path, *, bvp_rc: int = 0, slate_mode: str = "skip") -> subprocess.CompletedProcess[str]:
    home = tmp_path / "home"
    repo = home / "Projects/proppadia"
    (repo / "backend/mlb/scripts").mkdir(parents=True)
    (repo / "backend").mkdir(exist_ok=True)
    (repo / "bin").mkdir()
    (repo / "fake-bin").mkdir()
    (repo / "backend/.env").write_text("")
    (repo / "backend/mlb/scripts/launchagent_lock.zsh").write_text(
        "acquire_launchagent_lock() { return 0; }\n"
        "release_launchagent_locks() { return 0; }\n"
    )
    _write_executable(
        repo / "fake-bin/make",
        "#!/bin/zsh\n"
        "case \"$*\" in\n"
        "  *mlb-bvp-pvb-refresh*) exit ${BVP_RC:-0} ;;\n"
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
        },
        text=True,
        capture_output=True,
    )
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


def test_genuine_slate_failure_remains_nonzero(tmp_path):
    result = _run(tmp_path, bvp_rc=0, slate_mode="fail")
    assert result.returncode == 9
    assert "wrapper_rc=9 acquisition_status=SUCCESS" in result.stderr
    assert "BVP_DOWNSTREAM_STATUS=FAILED_TECHNICAL" in result.stderr
    assert "downstream_status=FAILED_TECHNICAL" in result.stderr
