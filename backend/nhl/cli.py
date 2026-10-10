#!/usr/bin/env python3
"""
NHL pipeline CLI

Commands:
  daily        Run the full daily pipeline (schedule/roster → features → score → site files).
  fetch-odds   Fetch NHL player props odds (SOG + Saves + Points) to nhl/site/data JSON.
  build-sog    Build nhl/site/data/sog_with_market.csv from latest predictions + odds.
  build-saves  Build nhl/site/data/saves_with_market.csv from latest predictions + odds.
  build-points Build nhl/site/data/points_with_market.csv from latest predictions + odds.

Conventions:
  - Operational slate dates use America/New_York.
  - Artifacts:
      exports/                             (SQL exports consumed by models)
      backend/nhl/data/processed/          (model outputs)
      nhl/site/data/                       (site-consumed CSV/JSON)
  - Models live under:
      backend/nhl/models/sog
      backend/nhl/models/saves
      backend/nhl/models/points

  - Daily Run: python -m backend.nhl.cli daily --with-odds    
"""

from __future__ import annotations

from backend.nhl.odds_regions import NHL_ODDS_REGIONS_CSV
from backend.nhl.odds_markets import NHL_COMPREHENSIVE_PROP_MARKETS_CSV

import argparse
import json
import os
import sys
import subprocess
import subprocess as sp
import shutil
import tempfile
from pathlib import Path
import pandas as pd
from typing import Any, Optional, Sequence, Union
from datetime import datetime, timedelta, timezone
import re
import psycopg
import uuid
import time

from backend.nhl.daily_capture import (
    PHASES,
    OddsObservationResult,
    RequestsOddsProvider,
    canonical_game_set_hash,
    capture_odds_observation,
    load_canonical_slate,
    sha256_file,
    verify_package,
)
from backend.nhl.daily_orchestration import (
    DailyRunRecorder,
    LEGACY_SOG_TOI_REASON,
    artifact_identity,
    legacy_sog_toi_population_diagnostic,
    redact_sensitive_text,
    safe_called_process_error,
    verify_roster_observation_reuse,
)
from backend.nhl.prediction_lineage import (
    POINTS_LINES,
    SAVES_LINES,
    prepare_scoring_input,
    validate_prediction_output,
    validate_sog_prediction_artifacts,
    pregame_game_eligibility,
)
from backend.nhl.model_identity import fitted_model_identity
from backend.nhl.sog_feature_input import begin_capture as begin_sog_feature_capture
from backend.nhl.sog_feature_input import finalize_capture as finalize_sog_feature_capture
from backend.nhl.sog_fixed_blend import capture as capture_sog_fixed_blends
from backend.nhl.points_hgb_shadow import capture_from_files as capture_points_hgb_shadow
from backend.nhl.points_hgb_shadow import prepare_authoritative_inputs as prepare_points_hgb_inputs
from backend.nhl.points_hgb_shadow import build_production_prediction_artifact
from backend.nhl.points_hgb_promotion import (
    AUTHORITY_PATH, HGB_AUTHORITY, PHOENIX_AUTHORITY,
    assert_hgb_promotion_ready, selected_production_authority,
)
from backend.nhl.attachment_integrity import (
    AttachmentIntegrityError,
    audit_attachment_files,
    validate_odds_observation,
)
from backend.nhl.sog_attachment_integrity import (
    audit_sog_attachment,
    retain_sog_attachment_package,
)
from backend.nhl.market_attachment_retention import retain_market_attachment_package
from backend.nhl.postgame_learning import (
    current_et_slate, ensure_prior_learning, prior_et_slate,
)


# ---------- bootstrap env ----------

BASE = Path(__file__).resolve().parent

def _load_dotenv_multi():
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    here = Path(__file__).resolve()
    root = here.parents[2]
    for p in (
        root / ".env.local",
        root / ".env",
        root / "backend" / ".env",
        root / "nhl" / ".env",
    ):
        if p.exists():
            load_dotenv(p, override=False)

_load_dotenv_multi()

ROOT = Path(__file__).resolve().parents[2]  # repo root
PY   = os.environ.get("PYTHON", sys.executable)

# Canonical NHL module root
NHL_DIR = ROOT / "backend" / "nhl"

SITE_DIR    = ROOT / "nhl" / "site" / "data"
EXPORTS_DIR = NHL_DIR / "exports"
PROC_DIR    = NHL_DIR / "data" / "processed"
SQL_DIR     = NHL_DIR / "sql"
SCRIPTS_DIR = NHL_DIR / "scripts"
MODELS_DIR  = NHL_DIR / "models"

# Daily artifact organization
EXPORTS_DAILY_NAMES_DIR = EXPORTS_DIR / "daily" / "names"
EXPORTS_DAILY_SOG_DIR   = EXPORTS_DIR / "daily" / "sog_features"
EXPORTS_ODDS_HISTORY_DIR = EXPORTS_DIR / "odds_history"
TMP_DIR = ROOT / "tmp"
SOG_RECONCILE_MONTHLY_CSV = TMP_DIR / "nhl_sog_base_vs_betonline_monthly.csv"
SOG_RECONCILE_MONTHLY_JSON = TMP_DIR / "nhl_sog_base_vs_betonline_monthly.json"
SOG_RECONCILE_MONTHLY_PUBLISHABLE_CSV = TMP_DIR / "nhl_sog_base_vs_betonline_monthly_publishable.csv"
SOG_RECONCILE_ROWS_CSV = TMP_DIR / "nhl_sog_base_vs_betonline_rows.csv"
SOG_RESIDUAL_DATASET_DEFAULT_CSV = ROOT / "backend" / "nhl" / "data" / "analysis" / "sog_poisson_residual_dataset_season_2025.csv"
SOG_RECONCILE_DATASET_PATH = ROOT / "backend" / "nhl" / "data" / "analysis" / "sog_poisson_residual_dataset_reconcile.csv"
ODDS_OBSERVATION_ROOT = ROOT / "artifacts" / "operational" / "nhl" / "odds_observations"
SOG_ATTACHMENT_ROOT = ROOT / "artifacts" / "operational" / "nhl" / "sog_market_attachments"
POINTS_ATTACHMENT_ROOT = ROOT / "artifacts" / "operational" / "nhl" / "points_market_attachments"
SAVES_ATTACHMENT_ROOT = ROOT / "artifacts" / "operational" / "nhl" / "saves_market_attachments"
ROSTER_OBSERVATION_ROOT = ROOT / "artifacts" / "operational" / "nhl" / "roster_observations"
DAILY_RUN_RECEIPT_ROOT = ROOT / "artifacts" / "operational" / "nhl" / "daily_runs"

_ACTIVE_DAILY_RECORDER: DailyRunRecorder | None = None
_ACTIVE_DAILY_LANE = "shared_prerequisites"

DAILY_EXECUTION_GRAPH = (
    "DATABASE_SANITY",
    "FULL_LEAGUE_ROSTER_REFRESH",
    "PRIOR_DATE_OUTCOME_FINALIZATION",
    "CANONICAL_SCHEDULE_AND_SLATE_VALIDATION",
    "SLATE_ROSTER_CAPTURE_AND_NORMALIZATION",
    "FEATURE_AND_PREDICTION_DURABILITY",
    "OPTIONAL_GOVERNED_ODDS_OBSERVATION",
    "OPTIONAL_MARKET_ATTACHMENT",
    "RESEARCH_REFRESH_ARCHIVE_AND_INTEGRITY",
)

for d in (
    SITE_DIR,
    EXPORTS_DIR,
    PROC_DIR,
    TMP_DIR,
    EXPORTS_DAILY_NAMES_DIR,
    EXPORTS_DAILY_SOG_DIR,
    EXPORTS_ODDS_HISTORY_DIR,
):
    d.mkdir(parents=True, exist_ok=True)


def archive_site_artifacts(
    slate: str, *, odds_result: OddsObservationResult | None = None,
    completed_lanes: set[str] | None = None,
) -> None:
    archive_dir = EXPORTS_ODDS_HISTORY_DIR / slate
    archive_dir.mkdir(parents=True, exist_ok=True)

    # The comprehensive runner supplies its completed lane set so a blocked
    # lane can never archive a stale same-date fixed filename.  Standalone
    # callers retain the historical all-lanes behavior.
    standalone_archive = completed_lanes is None
    if standalone_archive:
        completed_lanes = {"legacy_sog", "points", "saves"}
    artifacts: list[Path] = []
    if "legacy_sog" in completed_lanes:
        artifacts.extend([
            SITE_DIR / "sog_with_market.csv",
            SITE_DIR / "unmatched_sog.csv",
            SITE_DIR / "sog_attachment_integrity.json",
            SOG_RECONCILE_MONTHLY_CSV,
            SOG_RECONCILE_MONTHLY_JSON,
            SOG_RECONCILE_MONTHLY_PUBLISHABLE_CSV,
            SOG_RECONCILE_ROWS_CSV,
        ])
        if standalone_archive:
            artifacts.extend([
                PROC_DIR / "sog_predictions_wide_calibrated.csv",
                PROC_DIR / "sog_predictions_wide_defense_surprise_shadow.csv",
            ])
    if "saves" in completed_lanes:
        artifacts.extend([
            SITE_DIR / "saves_with_market.csv",
            SITE_DIR / "unmatched_saves.csv",
            SITE_DIR / "ambiguous_saves_alias_matches.csv",
            SITE_DIR / "saves_attachment_integrity.json",
        ])
    if "points" in completed_lanes:
        artifacts.extend([
            SITE_DIR / "points_with_market.csv",
            SITE_DIR / "unmatched_points.csv",
            SITE_DIR / "points_attachment_integrity.json",
        ])
    # Odds evidence is already immutable inside odds_result.observation_dir and
    # is referenced by hash from the parent receipt.  Do not copy mutable
    # compatibility filenames for comprehensive (explicit lane-set) runs.
    if (
        standalone_archive
        and odds_result is not None
        and odds_result.classification.startswith("CAPTURED_")
    ):
        artifacts.extend([
            SITE_DIR / "odds_nhl_playerprops_today.json",
            SITE_DIR / "events_today.json",
            SITE_DIR / "odds_observation_latest.json",
        ])
        if odds_result.classification == "CAPTURED_NONEMPTY":
            artifacts.append(SITE_DIR / "odds_latest.json")
    copied: list[str] = []

    for src in artifacts:
        if src.exists() and src.stat().st_size > 0:
            dst = archive_dir / src.name
            shutil.copy2(src, dst)
            copied.append(src.name)

    if copied:
        print(f"archive → {archive_dir} ({', '.join(copied)})")
    else:
        print(f"archive → {archive_dir} (no site artifacts copied)")


def refresh_sog_reconcile_artifacts(*, to_date: str) -> None:
    """Refresh row-level SOG reconcile artifacts used by backtests/replays."""
    from_date = (os.environ.get("NHL_SOG_RECONCILE_FROM_DATE") or "2025-10-07").strip()
    to_date = str(to_date).strip() or et_today()
    dataset_csv = (os.environ.get("NHL_SOG_RECONCILE_DATASET_CSV")
                   or str(SOG_RECONCILE_DATASET_PATH)).strip()
    seasons = range(
        infer_nhl_season_from_date_yyyy_mm_dd(from_date),
        infer_nhl_season_from_date_yyyy_mm_dd(to_date) + 1,
    )
    combined: list[pd.DataFrame] = []
    with tempfile.TemporaryDirectory(prefix="nhl_sog_reconcile_") as temp_dir:
        for season in seasons:
            season_path = Path(temp_dir) / f"season_{season}.csv"
            command = [
                PY, SCRIPTS_DIR / "build_sog_poisson_residual_dataset.py",
                "--season", str(season), "--from-date", from_date,
                "--to-date", to_date, "--out-csv", season_path,
            ]
            run(command)
            if season_path.is_file() and season_path.stat().st_size:
                try:
                    frame = pd.read_csv(season_path)
                except pd.errors.EmptyDataError:
                    continue
                if not frame.empty:
                    combined.append(frame)
    dataset = pd.concat(combined, ignore_index=True) if combined else pd.DataFrame()
    if not dataset.empty:
        if dataset.duplicated(["game_id", "player_id"]).any():
            raise RuntimeError("SOG_RECONCILE_SEASON_DATASET_DUPLICATE_GAME_PLAYER")
        dataset = dataset.sort_values(["game_date", "player_id", "game_id"])
    target = Path(dataset_csv)
    target.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(target, index=False)
    run(
        [
            PY,
            SCRIPTS_DIR / "reconcile_sog_base_vs_betonline_by_month.py",
            "--dataset-csv",
            target,
            "--observation-root",
            ROOT / "artifacts" / "operational" / "nhl" / "odds_observations",
            "--from-date",
            from_date,
            "--to-date",
            to_date,
            "--out-csv",
            SOG_RECONCILE_MONTHLY_CSV,
            "--out-json",
            SOG_RECONCILE_MONTHLY_JSON,
            "--out-rows-csv",
            SOG_RECONCILE_ROWS_CSV,
        ]
    )


def refresh_sog_residual_dataset(*, slate: str) -> None:
    """Refresh season-scoped SOG residual dataset used by reconcile/replay scripts."""
    season = infer_nhl_season_from_date_yyyy_mm_dd(str(slate))
    from_date = (os.environ.get("NHL_SOG_DATASET_FROM_DATE") or "").strip()
    to_date = str(slate).strip() or et_today()
    out_csv = (os.environ.get("NHL_SOG_DATASET_CSV")
               or str(SOG_RESIDUAL_DATASET_DEFAULT_CSV.with_name(
                   f"sog_poisson_residual_dataset_season_{season}.csv"))).strip()
    cmd: list[str] = [
        str(PY),
        str(SCRIPTS_DIR / "build_sog_poisson_residual_dataset.py"),
        "--season",
        str(season),
        "--to-date",
        to_date,
        "--out-csv",
        out_csv,
    ]
    if from_date:
        cmd.extend(["--from-date", from_date])
    run(cmd)

# ---------- time helpers (Eastern operational boundary) ----------

def pt_today() -> str:
    """Legacy name retained for callers; NHL operational dates use Eastern time."""
    return current_et_slate()


def pt_yesterday() -> str:
    return prior_et_slate()

def et_today() -> str:
    """Current NHL operational date in Eastern time."""
    return pt_today()

def et_yesterday() -> str:
    """Previous NHL operational date in Eastern time."""
    return pt_yesterday()
    
def infer_nhl_season_from_date_yyyy_mm_dd(date_str: str) -> int:
    # NHL season naming is the single starting year.
    y, m, d = (int(x) for x in date_str.split("-"))
    return y if m >= 9 else (y - 1)

# ---------- guardrails: prevent repeating the same fix/step ----------
def _guard_dir() -> Path:
    d = PROC_DIR / "_guard"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _guard_path(key: str, slate: str | None = None) -> Path:
    safe_key = re.sub(r"[^a-zA-Z0-9_.-]+", "_", key).strip("_")
    safe_slate = re.sub(r"[^0-9-]+", "_", (slate or "global"))
    return _guard_dir() / f"{safe_key}__{safe_slate}.done"

def guard_mark_done(key: str, slate: str | None = None, details: str = "") -> None:
    p = _guard_path(key, slate)
    payload = (details.strip() + "\n") if details else "done\n"
    p.write_text(payload, encoding="utf-8")

def guard_already_done(key: str, slate: str | None = None) -> bool:
    return _guard_path(key, slate).exists()

def guard_require_not_done(key: str, slate: str | None = None) -> None:
    """
    Hard stop if we try to repeat a step that was already marked done.
    Use this for 'assistant-suggested edits' so we don't churn.
    """
    p = _guard_path(key, slate)
    if p.exists():
        msg = p.read_text(encoding="utf-8").strip()
        raise AssertionError(f"[guard] step already done: {key} (slate={slate}). Notes: {msg}")


def _table_has_column(db_url: str, schema: str, table: str, column: str) -> bool:
    with psycopg.connect(db_url, prepare_threshold=None) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None

def guard_clear(key: str, slate: str | None = None) -> None:
    p = _guard_path(key, slate)
    if p.exists():
        p.unlink()

def guard_list(slate: str | None = None) -> list[tuple[str, str, str]]:
    """
    Return [(key, slate, notes)] for .done files under PROC_DIR/_guard.
    If slate is provided, filters to that slate (or 'global').
    """
    out = []
    d = _guard_dir()
    for p in sorted(d.glob("*.done")):
        name = p.name  # key__slate.done
        if "__" not in name:
            continue
        key, rest = name.split("__", 1)
        slate_part = rest.replace(".done", "")
        if slate is not None and slate_part not in {slate, "global"}:
            continue
        notes = p.read_text(encoding="utf-8").strip()
        out.append((key, slate_part, notes))
    return out

def guard_print(slate: str | None = None) -> None:
    rows = guard_list(slate)
    if not rows:
        print("[guard] no recorded steps.")
        return
    print("[guard] recorded steps:")
    for key, sl, notes in rows:
        msg = f"  - {key} (slate={sl})"
        if notes and notes != "done":
            msg += f": {notes}"
        print(msg)

# ---------- shell helpers ----------

def _psql_env() -> dict:
    """
    Build a safe environment for any psql subprocess spawned by cli.py.

    Why:
      - Supabase often enforces a low default statement_timeout (you saw 2min),
        which kills long-running \COPY exports.
      - We also want a consistent schema search_path for all sessions.

    Behavior:
      - Ensures search_path=nhl,public
      - Ensures statement_timeout=0 (unlimited) so exports don't get canceled
      - Keeps any existing PGOPTIONS flags (appends ours)
    """
    env = dict(os.environ)

    # Our required session settings
    required = [
        "-c search_path=nhl,public",
        "-c statement_timeout=0",
        # Optional: fail fast on lock waits instead of "hanging"
        "-c lock_timeout=5000",
    ]
    required_str = " ".join(required)

    # Preserve any existing PGOPTIONS and append ours
    existing = (env.get("PGOPTIONS") or "").strip()
    env["PGOPTIONS"] = (existing + " " + required_str).strip() if existing else required_str

    return env

def run_psql_bytes(db_url: str, sql: str, *, timeout: Optional[int] = None) -> bytes:
    """
    Runs a single SQL command via psql and returns stdout bytes.
    Raises if psql exits non-zero.
    """
    return subprocess.check_output(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )

def is_testing() -> bool:
    return os.environ.get("TESTING") == "1"

def guard_testing_only_slate(slate: str | None, *, cmd_name: str) -> None:
    # Passing an explicit slate is considered "testing mode" behavior.
    if slate and not is_testing():
        raise AssertionError(
            f"[guard] {cmd_name} with an explicit slate is only allowed when TESTING=1. "
            f"Refusing slate={slate} in non-testing runs."
        )

def assert_sog_rollups_present(db_url: str, slate_date: str, *, min_ok_frac: float = 0.25) -> None:
    sql = f"""
    COPY (
      SELECT
        COUNT(*)::int AS n,
        COUNT(*) FILTER (WHERE d10_sog_per60 IS NOT NULL)::int AS n_d10_ok,
        COUNT(*) FILTER (WHERE attempts_d10_per60 IS NOT NULL)::int AS n_att_ok,
        CASE WHEN COUNT(*) = 0 THEN 0
             ELSE (COUNT(*) FILTER (WHERE d10_sog_per60 IS NOT NULL)::numeric / COUNT(*)::numeric)
        END AS frac_ok
      FROM nhl.training_features_nhl_sog_enriched_pregame_v2
      WHERE game_date = DATE '{slate_date}'
    ) TO STDOUT WITH CSV HEADER;
    """
    csv_bytes = run_psql_bytes(db_url, sql)
    lines = csv_bytes.decode("utf-8", errors="replace").splitlines()
    if len(lines) < 2:
        raise AssertionError(f"[sog_rollups_check] empty result for slate_date={slate_date}")

    header = lines[0].split(",")
    vals = lines[1].split(",")
    row = dict(zip(header, vals))

    n = int(row["n"])
    n_d10_ok = int(row["n_d10_ok"])
    frac_ok = float(row["frac_ok"])

    print(f"[sog_rollups_check] slate={slate_date} n={n} n_d10_ok={n_d10_ok} frac_ok={frac_ok:.3f}")

    if n == 0:
        raise AssertionError(f"[sog_rollups_check] no rows for slate_date={slate_date}")
    if frac_ok < min_ok_frac:
        raise AssertionError(
            f"[sog_rollups_check] rollups missing: slate_date={slate_date} "
            f"frac_ok={frac_ok:.3f} (n={n}, n_d10_ok={n_d10_ok})"
        )

_DATABASE_WRITE_CAPABLE_SCRIPTS = {
    "refresh_all_team_rosters.py",
    "seed_goalie_logs_for_date.py",
    "refresh_players_and_roster_today.py",
    "seed_skater_logs_for_date.py",
    "ingest_shiftcharts_for_date.py",
    "backfill_game_manpower_segments.py",
    "fill_pp_toi_minutes_for_date.py",
    "import_schedule_today.py",
    "import_roster_today.py",
    "load_nhl_predictions_generic.py",
    "load_sog_predictions_denali.py",
}


def _command_is_database_write_capable(cmd: Sequence[str]) -> bool:
    if any(Path(token).name in _DATABASE_WRITE_CAPABLE_SCRIPTS for token in cmd):
        return True
    if cmd and Path(cmd[0]).name == "psql":
        if "-f" in cmd:
            return True
        sql_parts = [cmd[index + 1] for index, token in enumerate(cmd[:-1]) if token == "-c"]
        mutation = re.compile(
            r"\b(?:INSERT|UPDATE|DELETE|TRUNCATE|MERGE|REFRESH|CREATE|ALTER|DROP)\b",
            re.I,
        )
        return any(mutation.search(sql) for sql in sql_parts)
    return False


def _structured_child_summary(stdout: str | None) -> dict[str, Any]:
    structured: dict[str, Any] = {}
    for line in (stdout or "").splitlines():
        if line.startswith("NHL_CHILD_SUMMARY_JSON="):
            value = json.loads(line.split("=", 1)[1])
            if not isinstance(value, dict):
                raise RuntimeError("NHL_CHILD_SUMMARY_NOT_OBJECT")
            structured.update(value)
    return structured


def run(
    cmd, *, cwd: Path = ROOT, env: dict | None = None, check: bool = True,
    database_write_capable: bool | None = None,
):
    cmd = [str(c) for c in cmd]
    write_capable = (
        _command_is_database_write_capable(cmd)
        if database_write_capable is None else bool(database_write_capable)
    )
    cmd_for_log = _format_cmd_for_log(cmd)
    print("▶", cmd_for_log)
    e = os.environ.copy()
    if env:
        e.update(env)
    started = time.monotonic()
    try:
        result = sp.run(cmd, cwd=str(cwd), env=e, check=check, text=True, capture_output=True)
        if _ACTIVE_DAILY_RECORDER is not None:
            summary: dict[str, Any] = {
                "lane": _ACTIVE_DAILY_LANE,
                "command_identity": cmd_for_log,
                "exit_status": int(result.returncode),
                "duration_ms": round((time.monotonic() - started) * 1000),
                "status": "COMPLETE",
                "database_write_capable": write_capable,
            }
            summary.update(_structured_child_summary(result.stdout))
            summary["database_write_capable"] = bool(
                write_capable or summary.get("database_write_capable"))
            _ACTIVE_DAILY_RECORDER.record_child(summary)
        return result
    except sp.CalledProcessError as exc:
        if _ACTIVE_DAILY_RECORDER is not None:
            summary = {
                "lane": _ACTIVE_DAILY_LANE,
                "command_identity": cmd_for_log,
                "exit_status": int(exc.returncode),
                "duration_ms": round((time.monotonic() - started) * 1000),
                "status": "FAILED",
                "database_write_capable": write_capable,
            }
            summary.update(_structured_child_summary(exc.stdout))
            summary["database_write_capable"] = bool(
                write_capable or summary.get("database_write_capable"))
            _ACTIVE_DAILY_RECORDER.record_child(summary)
        print(f"[run] COMMAND FAILED: {cmd_for_log}", file=sys.stderr)
        safe_stdout = redact_sensitive_text(exc.stdout) if exc.stdout else ""
        safe_stderr = redact_sensitive_text(exc.stderr) if exc.stderr else ""
        if safe_stdout:
            print("[run] --- stdout ---", file=sys.stderr)
            print(safe_stdout, file=sys.stderr)
        if safe_stderr:
            print("[run] --- stderr ---", file=sys.stderr)
            print(safe_stderr, file=sys.stderr)
        raise safe_called_process_error(exc, cmd) from None

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = raw.strip().lower()
    if not val:
        return default
    return val in {"1", "true", "t", "yes", "y", "on"}

def _append_cli_arg(cmd: list[str], flag: str, value: str | None) -> None:
    if value is None:
        return
    s = str(value).strip()
    if not s:
        return
    cmd.extend([flag, s])


_URL_CRED_RE = re.compile(r"((?:[a-z][a-z0-9+.\-]*):\/\/[^:@\/\s]+:)([^@\/\s]+)(@)", re.I)
_SENSITIVE_ENV_KEY_RE = re.compile(
    r"(?:^|_)(?:TOKEN|SECRET|PASSWORD|PASS|KEY|API_KEY|DB_URL|DATABASE_URL|URL)$", re.I
)


def _redact_token_for_log(token: str) -> str:
    redacted = redact_sensitive_text(token)
    redacted = _URL_CRED_RE.sub(r"\1***\3", redacted)
    if "=" in redacted:
        key, value = redacted.split("=", 1)
        if _SENSITIVE_ENV_KEY_RE.search(key):
            if "://" in value:
                value = _URL_CRED_RE.sub(r"\1***\3", value)
            elif value:
                value = "***"
            redacted = f"{key}={value}"
    return redacted


def _format_cmd_for_log(cmd: Sequence[object]) -> str:
    return " ".join(_redact_token_for_log(str(c)) for c in cmd)

def require_db_url() -> str:
    db = os.environ.get("SUPABASE_DB_URL")
    if not db:
        print("FATAL: SUPABASE_DB_URL missing", file=sys.stderr)
        sys.exit(2)
    return db

def run_psql_file(sql_file: Path, *, vars: dict[str, str] | None = None):
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "--pset", "pager=off", "-v", "ON_ERROR_STOP=1", db]

    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]

    cmd += ["-c", "SET statement_timeout=0;"]
    cmd += ["-f", str(sql_file)]

    run(cmd, env=_psql_env(), database_write_capable=True)

def run_psql_file_to_path(
    sql_file: Path,
    out_path: Path,
    *,
    vars: dict[str, str] | None = None,
) -> None:
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "--pset", "pager=off", "-v", "ON_ERROR_STOP=1", db]

    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]

    cmd += ["-c", "SET statement_timeout=0;"]
    cmd += ["-f", str(sql_file)]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        try:
            sp.run(cmd, env=_psql_env(), check=True, stdout=f)
        except sp.CalledProcessError as exc:
            raise safe_called_process_error(exc, cmd) from None

def run_psql(sql: str) -> str:
    import os
    import subprocess

    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL)")

    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-A", "-F", ",", "-t", "-c", sql]
    try:
        p = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        raise safe_called_process_error(exc, cmd) from None
    return p.stdout

def psql_one_row(db_url: str, sql: str) -> dict:
    wrapped = f"SELECT row_to_json(t) FROM ({sql}) t;"
    res = sp.run(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", wrapped],
        capture_output=True, text=True
    )
    if res.returncode != 0:
        raise RuntimeError(
            "psql_one_row failed.\n"
            f"SQL:\n{wrapped}\n\n"
            f"STDOUT:\n{res.stdout}\n\n"
            f"STDERR:\n{res.stderr}\n"
        )

    out = (res.stdout or "").strip()
    if not out:
        raise RuntimeError(f"psql_one_row: expected 1 row, got 0.\nSQL:\n{wrapped}")

    try:
        return json.loads(out)
    except Exception as e:
        raise RuntimeError(f"psql_one_row: failed to parse JSON:\n{out}") from e

def _pred_game_dates(pred_csv: Path) -> list[str]:
    """Return sorted unique game_date strings found in sog_predictions.csv (if column exists)."""
    if not pred_csv.exists():
        return []
    try:
      
        df = pd.read_csv(pred_csv, usecols=lambda c: c in {"game_date"}, dtype={"game_date": "string"})
        if "game_date" not in df.columns:
            return []
        vals = df["game_date"].dropna().astype(str).unique().tolist()
        vals = sorted(set(v.strip() for v in vals if v and v.strip()))
        return vals
    except Exception:
        return []
    
def require_single_game_date_csv(path: Path, slate: str, *, col: str = "game_date", label: str = "") -> None:
    """
    Guardrail: ensure a CSV contains exactly one unique game_date and it matches slate.
    Fails early so we don't chase downstream join errors.
    """

    if not path.exists() or path.stat().st_size == 0:
        raise AssertionError(f"[guard] missing/empty CSV: {label or path}")

    df = pd.read_csv(path, usecols=lambda c: c == col, dtype={col: "string"})
    if col not in df.columns:
        raise AssertionError(f"[guard] {label or path} missing required column: {col}")

    vals = df[col].dropna().astype(str).map(lambda s: s.strip()).tolist()
    uniq = sorted(set(v for v in vals if v))
    if uniq != [slate]:
        show = uniq[:5]
        raise AssertionError(
            f"[guard] {label or path} {col} mismatch: expected [{slate}] got {show}"
        )

def _run_sog_evaluator() -> None:
    subprocess.check_call([sys.executable, "backend/nhl/scripts/evaluate_sog_predictions.py"])

def psql_stdout(sql_file: Path, *, vars: dict[str, str] | None = None) -> bytes:
    """Run psql on a file that COPY/SELECTs TO STDOUT and return stdout bytes."""
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "--pset", "pager=off", "-q", "-v", "ON_ERROR_STOP=1", db]

    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]

    cmd += ["-c", "SET statement_timeout=0;"]
    cmd += ["-f", str(sql_file)]

    res = sp.run(
        cmd,
        cwd=str(ROOT),
        env=_psql_env(),
        check=False,
        capture_output=True,
        text=False,  # IMPORTANT: keep stdout as bytes
    )

    if res.returncode != 0:
        print("psql FAILED:", _format_cmd_for_log(cmd), file=sys.stderr)
        if res.stderr:
            try:
                print(redact_sensitive_text(
                    res.stderr.decode("utf-8", errors="replace").strip()), file=sys.stderr)
            except Exception:
                print(redact_sensitive_text(str(res.stderr)[:2000]), file=sys.stderr)

        # show tail of stdout too (COPY can emit partial output)
        if res.stdout:
            try:
                tail = b"\n".join(res.stdout.splitlines()[-30:]).decode("utf-8", errors="replace")
                print("psql stdout tail:\n" + redact_sensitive_text(tail), file=sys.stderr)
            except Exception:
                pass

        error = sp.CalledProcessError(res.returncode, cmd, output=res.stdout, stderr=res.stderr)
        raise safe_called_process_error(error, cmd) from None

    return res.stdout

def refresh_sog_denali_rollups_window(db: str, *, start_date: str, end_date: str) -> None:
    """
    Recompute + persist rolling SOG features into:
      nhl.training_features_nhl_sog_enriched_pregame_v2

    This prevents 'frozen' rollups by making the daily runner actively refresh them.

    Window behavior:
      - Updates target rows where game_date in [start_date, end_date]
      - Computes rolling sums from nhl.skater_game_logs_raw joined to nhl.games
      - STRICT: uses only realized games strictly BEFORE end_date (end_date - 1 day)
      - Converts to per60 using TOI minutes
    """
    sql = f"""
    BEGIN;

    WITH params AS (
      SELECT DATE '{start_date}' AS start_date, DATE '{end_date}' AS end_date
    ),
    realized AS (
      SELECT
        l.player_id::bigint AS player_id,
        g.game_id::bigint   AS game_id,
        g.game_date::date   AS game_date,

        -- safe numeric casts (handle '' and NULL)
        COALESCE(NULLIF(BTRIM(l.shots_on_goal::text), ''), '0')::numeric AS sog,
        COALESCE(NULLIF(BTRIM(l.shot_attempts::text), ''), '0')::numeric AS attempts,

        NULLIF(COALESCE(NULLIF(BTRIM(l.toi_minutes::text), ''), '0')::numeric, 0) AS toi_min
      FROM nhl.skater_game_logs_raw l
      JOIN nhl.games g USING (game_id)
      JOIN params p ON TRUE
      -- include enough history before start_date so rolling windows at start_date aren't empty
      -- and exclude end_date itself to avoid any same-day leakage
      WHERE g.game_date BETWEEN (p.start_date - INTERVAL '260 days') AND (p.end_date - INTERVAL '1 day')
    ),
    rolls AS (
      SELECT
        r.player_id,
        r.game_id,
        r.game_date,

        SUM(r.sog)      OVER w5  AS sog_5,
        SUM(r.toi_min)  OVER w5  AS toi_5,

        SUM(r.sog)      OVER w10 AS sog_10,
        SUM(r.attempts) OVER w10 AS att_10,
        SUM(r.toi_min)  OVER w10 AS toi_10,

        SUM(r.sog)      OVER w20 AS sog_20,
        SUM(r.toi_min)  OVER w20 AS toi_20,

        ROW_NUMBER() OVER (
          PARTITION BY r.player_id
          ORDER BY r.game_date DESC, r.game_id DESC
        ) AS rn
      FROM realized r
      WINDOW
        w5  AS (PARTITION BY r.player_id ORDER BY r.game_date, r.game_id ROWS BETWEEN 4  PRECEDING AND CURRENT ROW),
        w10 AS (PARTITION BY r.player_id ORDER BY r.game_date, r.game_id ROWS BETWEEN 9  PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY r.player_id ORDER BY r.game_date, r.game_id ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    ),
    rollups AS (
      SELECT
        player_id,
        CASE WHEN toi_5  IS NULL OR toi_5  <= 0 THEN NULL ELSE (sog_5  / toi_5 ) * 60 END AS d5_sog_per60,
        CASE WHEN toi_10 IS NULL OR toi_10 <= 0 THEN NULL ELSE (sog_10 / toi_10) * 60 END AS d10_sog_per60,
        CASE WHEN toi_20 IS NULL OR toi_20 <= 0 THEN NULL ELSE (sog_20 / toi_20) * 60 END AS d20_sog_per60,
        CASE WHEN toi_10 IS NULL OR toi_10 <= 0 THEN NULL ELSE (att_10 / toi_10) * 60 END AS attempts_d10_per60
      FROM rolls
      WHERE rn = 1
    )
    UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
    SET
      d5_sog_per60       = r.d5_sog_per60,
      d10_sog_per60      = r.d10_sog_per60,
      d20_sog_per60      = r.d20_sog_per60,
      attempts_d10_per60 = r.attempts_d10_per60
    FROM rollups r, params p
    WHERE
      t.player_id = r.player_id
      AND t.game_date BETWEEN p.start_date AND p.end_date;

    COMMIT;
    """

    print(f"↻ Refreshing SOG rollups in-table for {start_date}..{end_date} ...")
    run(["psql", db, "--no-psqlrc", "-v", "ON_ERROR_STOP=1", "-c", sql])
    print("✅ SOG rollups refreshed.")

def export_sog_denali_features(
    db_url: str, slate_date: str, out_path: Path, *,
    require_pairings_coverage: bool = True,
) -> None:
    """
    Export Denali SOG features for a given slate_date into a CSV used by the SOG scorer.

    Behavior:
      - Optionally requires pairings/coverage substrate for the ordinal scorer.
      - Runs backend/nhl/sql/export_sog_denali_pregame.sql via psql.
      - Passes slate_date as a psql variable: -v slate_date=YYYY-MM-DD
      - Script itself does COPY ... TO STDOUT WITH CSV HEADER.
      - (Post guard) Ensures CSV is non-empty and has basic ID columns.
    """
    sql_path = BASE / "sql" / "export_sog_denali_pregame.sql"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # The ordinal scorer consumes pairings/coverage features.  The default
    # Poisson baseline does not; requiring those columns there can suppress
    # otherwise scoreable raw SOG predictions.
    if require_pairings_coverage:
        guard_sql = f"""
        WITH s AS (
          SELECT COUNT(*)::int AS n_rows
          FROM nhl.training_features_nhl_sog_enriched_pregame_v2
          WHERE game_date = DATE '{slate_date}'
        ),
        nn AS (
          SELECT
            COUNT(d10_shiftcharts_coverage_rate)::int AS nn_d10_cov,
            COUNT(d20_shiftcharts_coverage_rate)::int AS nn_d20_cov,
            COUNT(d10_pairings_available)::int        AS nn_d10_avail,
            COUNT(d20_pairings_available)::int        AS nn_d20_avail
          FROM nhl.training_features_nhl_sog_enriched_pregame_v2
          WHERE game_date = DATE '{slate_date}'
        )
        SELECT
          s.n_rows,
          nn.nn_d10_cov, nn.nn_d20_cov,
          nn.nn_d10_avail, nn.nn_d20_avail
        FROM s, nn;
        """

        proc = sp.run(
            ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", guard_sql],
            check=True,
            capture_output=True,
            text=True,
            env=_psql_env(),
        )

        # output format: n_rows|nn_d10_cov|nn_d20_cov|nn_d10_avail|nn_d20_avail
        parts = proc.stdout.strip().split("|")
        if len(parts) != 5:
            raise RuntimeError(f"[guard] unexpected guard output: {proc.stdout!r}")

        n_rows, nn_d10_cov, nn_d20_cov, nn_d10_avail, nn_d20_avail = map(int, parts)

        if n_rows == 0:
            raise RuntimeError(
                f"[guard] no rows in nhl.training_features_nhl_sog_enriched_pregame_v2 for slate_date={slate_date}"
            )

        if nn_d10_cov == 0 or nn_d20_cov == 0 or nn_d10_avail == 0 or nn_d20_avail == 0:
            raise RuntimeError(
                f"[guard] missing pairings/coverage substrate for slate_date={slate_date}: "
                f"n_rows={n_rows} nn_d10_cov={nn_d10_cov} nn_d20_cov={nn_d20_cov} "
                f"nn_d10_avail={nn_d10_avail} nn_d20_avail={nn_d20_avail}. "
                "This should be fixed upstream (fill_sog_pairings_rolling_for_slate.sql / fill_sog_pairings_for_slate.sql)."
            )

    # ------------------------------------------------------------------
    # EXPORT
    # ------------------------------------------------------------------
    with out_path.open("w", encoding="utf-8", newline="") as f:
        sp.run(
            [
                "psql",
                db_url,
                "-v", "ON_ERROR_STOP=1",
                "-v", f"slate_date={slate_date}",
                "-f", str(sql_path),
            ],
            check=True,
            stdout=f,
            env=_psql_env(),
        )

    # ------------------------------------------------------------------
    # POST-EXPORT GUARD: ensure artifact is real + has ID columns
    # ------------------------------------------------------------------
    if (not out_path.exists()) or out_path.stat().st_size < 200:
        raise AssertionError(
            f"[guard] export produced empty/small CSV: {out_path} (slate_date={slate_date})"
        )

    hdr = pd.read_csv(out_path, nrows=1)
    required_cols = ["player_id", "game_id", "game_date"]
    missing = [c for c in required_cols if c not in hdr.columns]
    if missing:
        raise AssertionError(
            f"[guard] {out_path.name} missing columns {missing} (slate_date={slate_date}). "
            f"got={list(hdr.columns)}"
        )

    print(f"✅ Exported SOG Denali features for {slate_date} → {out_path}")

# ---------- names export ----------

def export_names_csv(slate: str) -> Path:
    """
    exports/names_{slate}.csv with columns:
      player_id,full_name,team_id,team_code,game_id,game_date

    Uses backend/nhl/sql/_export_names.sql, which expects:
      -v slate_date=YYYY-MM-DD
    """
    out_path = EXPORTS_DAILY_NAMES_DIR / f"names_{slate}.csv"

    # Base directory for this NHL backend module (backend/nhl)
    nhl_base = Path(__file__).resolve().parent
    sql_path = nhl_base / "sql" / "_export_names.sql"

    # Run the static SQL with a bound slate_date variable and capture CSV bytes
    csv_bytes = psql_stdout(sql_path, vars={"slate_date": slate})
    if not isinstance(csv_bytes, (bytes, bytearray)):
        raise AssertionError(
            f"[guard] psql_stdout must return bytes, got {type(csv_bytes).__name__}"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(csv_bytes)
    require_single_game_date_csv(out_path, slate, label="names export")

    # Guardrail: verify header/shape so we don't proceed with a broken names export
    try:
        df_head = pd.read_csv(out_path, nrows=5)
        need = {"player_id", "game_id", "full_name", "team_id", "team_code", "game_date"}
        miss = sorted(need - set(df_head.columns))
        if miss:
            raise AssertionError(
                f"[export_names_csv] BAD CSV header (missing={miss}). "
                f"got={list(df_head.columns)} file={out_path}"
            )
        if df_head.empty:
            raise AssertionError(f"[export_names_csv] names CSV has no rows: {out_path}")
    except Exception as e:
        raise AssertionError(f"[export_names_csv] names CSV validation failed: {e}") from e

    print(f"[export_names_csv] wrote names CSV → {out_path}")
    return out_path

# ---------- odds fetch ----------


def fetch_odds(
    days_from: int = 1,
    markets: str = NHL_COMPREHENSIVE_PROP_MARKETS_CSV,
    regions: str = NHL_ODDS_REGIONS_CSV,
    odds_format: str = "american",
    *,
    slate: str | None = None,
    season: int | None = None,
    phase: str = "EARLY",
    parent_daily_run_id: str | None = None,
    canonical_games=None,
    observation_root: Path = ODDS_OBSERVATION_ROOT,
    compatibility_dir: Path = SITE_DIR,
    reuse_observation_dir: Path | None = None,
) -> OddsObservationResult:
    """Run one claimed odds observation for the comprehensive daily command."""
    slate = slate or pt_today()
    season = season if season is not None else infer_nhl_season_from_date_yyyy_mm_dd(slate)
    phase = str(phase).upper()
    if phase not in PHASES:
        raise ValueError(f"unsupported odds phase: {phase}")
    if canonical_games is None:
        slate_root = ROOT / "artifacts" / "operational" / "nhl" / "slates" / slate
        canonical_games = load_canonical_slate(
            slate_date=slate,
            raw_schedule_path=slate_root / "raw_schedule_response.json",
            slate_health_path=slate_root / "slate_health.json",
        )
    parent_daily_run_id = parent_daily_run_id or (
        f"nhldaily_{slate.replace('-', '')}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:8]}"
    )
    key = os.environ.get("ODDS_API_KEY", "").strip()
    provider = RequestsOddsProvider(key) if key else None
    latest_pointer_path = None
    configured_pointer = os.environ.get("NHL_ODDS_LATEST_POINTER_PATH")
    if configured_pointer:
        latest_pointer_path = Path(configured_pointer).expanduser()
        default_pointer = (Path(compatibility_dir) / "odds_observation_latest.json").resolve()
        if (latest_pointer_path.name != "odds_observation_latest.json"
                or latest_pointer_path.resolve() == default_pointer
                or parent_daily_run_id not in latest_pointer_path.parts):
            raise ValueError("NHL_ODDS_LATEST_POINTER_PATH_MUST_BE_RUN_SCOPED")
    result = capture_odds_observation(
        root=observation_root,
        season=season,
        slate_date=slate,
        phase=phase,
        parent_daily_run_id=parent_daily_run_id,
        canonical_games=canonical_games,
        provider=provider,
        authorized=bool(key),
        compatibility_dir=compatibility_dir,
        latest_pointer_path=latest_pointer_path,
        days_from=days_from,
        markets=markets,
        regions=regions,
        odds_format=odds_format,
        reuse_observation_dir=reuse_observation_dir,
    )
    print(
        f"ODDS_OBSERVATION classification={result.classification} "
        f"path={result.observation_dir} replayed={str(result.replayed).lower()}"
    )
    return result


def run_optional_odds_observation(*, with_odds: bool, **kwargs) -> OddsObservationResult | None:
    """The single gate through which the comprehensive runner may acquire odds."""
    return fetch_odds(**kwargs) if with_odds else None


def _attachment_market_inputs(
    attachment_lane: str, odds_result: OddsObservationResult | None,
) -> dict[str, Path]:
    """Bind attachment inputs only to the current immutable captured package."""
    if odds_result is None or not odds_result.classification.startswith("CAPTURED_"):
        return {}
    inputs = {"odds_json": odds_result.observation_dir / "raw_response.json"}
    if attachment_lane in {"sog_attachment", "points_attachment"}:
        inputs["events_json"] = odds_result.observation_dir / "events_response.json"
    return inputs


def daily_health_for_odds(*, requested: bool,
                          result: OddsObservationResult | None) -> str:
    if requested and result is not None and result.classification in {
        "FAILED_PROVIDER", "FAILED_MALFORMED_RESPONSE", "FAILED_BUDGET_GUARD",
        "SKIPPED_NO_AUTHORIZATION", "SKIPPED_BUDGET_GUARD",
    }:
        return "READY_WITH_ODDS_WARNING"
    return "READY"

# ---------- builders (CSV for site) ----------

def _require_current_prediction_artifact(
    path: Path, *, slate: str, expected_sha256: str | None,
) -> dict[str, Any]:
    path = Path(path)
    if not path.is_file() or path.stat().st_size == 0:
        raise AssertionError(f"current-run prediction artifact missing/empty: {path}")
    dates = _pred_game_dates(path)
    if len(dates) != 1 or dates[0] != slate:
        raise AssertionError(
            f"current-run prediction slate mismatch: expected {slate}, got {dates} in {path}")
    identity = artifact_identity(path)
    if expected_sha256 is not None and identity["sha256"] != expected_sha256:
        raise AssertionError(f"current-run prediction hash mismatch: {path}")
    return identity


def _write_attachment_integrity_report(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
    identity = artifact_identity(path)
    identity.update(payload.get("counts") or {})
    identity["status"] = payload.get("status")
    identity["attachment_sha256"] = payload.get("attachment_sha256")
    identity["prediction_artifact_sha256"] = payload.get("prediction_artifact_sha256")
    identity["odds_observation_manifest_sha256"] = payload.get(
        "odds_observation_manifest_sha256")
    return identity


def _market_attachment_retention_receipt_outputs(
    package_dir: Path, *, lane: str, parent_daily_run_id: str, manifest_sha256: str,
) -> list[dict[str, Any]]:
    """Expose every retained Points/Saves package object in its daily receipt."""
    package_dir = Path(package_dir).resolve()
    outputs: list[dict[str, Any]] = [{
        "path": str(package_dir), "manifest_sha256": manifest_sha256,
        "parent_daily_run_id": parent_daily_run_id,
        "retention_schema_version": f"NHL_{lane.upper()}_MARKET_ATTACHMENT_RETENTION_V1",
    }]
    names = [f"{lane}_attachment_integrity.json", f"{lane}_with_market.csv",
             f"unmatched_{lane}.csv"]
    if lane == "saves":
        names.append("ambiguous_saves_alias_matches.csv")
    names.extend(["RUN_COMPLETE.json", "SHA256SUMS"])
    outputs.extend(artifact_identity(package_dir / name) for name in names)
    return outputs


def build_sog(slate: str, *, odds_json: Path | None = None,
              events_json: Path | None = None, pred_path: Path | None = None,
              expected_pred_sha256: str | None = None,
              parent_daily_run_id: str | None = None,
              odds_observation_dir: Path | None = None,
              expected_odds_manifest_sha256: str | None = None,
              odds_phase: str | None = None,
              odds_replayed: bool = False,
              feature_input_binding: dict[str, Any] | None = None):
    # Always regenerate (or overwrite) names for this slate and use the returned path
    names_csv = export_names_csv(slate)

    pred_path = Path(pred_path or (PROC_DIR / "sog_predictions_wide_calibrated.csv"))
    _require_current_prediction_artifact(
        pred_path, slate=slate, expected_sha256=expected_pred_sha256)

    if not names_csv.exists() or names_csv.stat().st_size == 0:
        raise AssertionError(f"[build-sog] expected artifact missing/empty: {names_csv}")

    command = [
            PY,
            SCRIPTS_DIR / "build_sog_with_market.py",
            "--pred",       str(pred_path),
            "--names",      str(names_csv),
            "--out",        "nhl/site/data/sog_with_market.csv",
            "--unmatched",  "nhl/site/data/unmatched_sog.csv",
            "--slate-date", slate,
            "--pred-only",
        ]
    if odds_json is not None:
        command.extend(["--odds-json", str(odds_json)])
    if events_json is not None:
        command.extend(["--events-json", str(events_json)])
    run(command)

    # Postcondition: market merge must produce a non-empty output artifact
    out_csv = SITE_DIR / "sog_with_market.csv"
    if not out_csv.exists() or out_csv.stat().st_size == 0:
        raise AssertionError(f"[build-sog] expected artifact missing/empty: {out_csv}")
    
    unmatched_csv = SITE_DIR / "unmatched_sog.csv"
    if not unmatched_csv.exists() or unmatched_csv.stat().st_size == 0:
        raise AssertionError(f"[build-sog] expected artifact missing/empty: {unmatched_csv}")

    if parent_daily_run_id is not None:
        canonical_games_for_sog = load_canonical_slate(
            slate_date=slate,
            raw_schedule_path=ROOT / "artifacts/operational/nhl/slates" / slate / "raw_schedule_response.json",
            slate_health_path=ROOT / "artifacts/operational/nhl/slates" / slate / "slate_health.json",
        )
        canonical_game_ids = {int(game.game_id) for game in canonical_games_for_sog}
        odds_lineage = None
        if odds_json is not None and odds_observation_dir is not None and expected_odds_manifest_sha256:
            odds_lineage = validate_odds_observation(
                observation_dir=Path(odds_observation_dir), odds_json=Path(odds_json),
                expected_manifest_sha256=expected_odds_manifest_sha256,
                expected_parent_daily_run_id=parent_daily_run_id,
                expected_slate_date=slate,
                expected_season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
                expected_phase=odds_phase,
                expected_game_set_hash=canonical_game_set_hash(canonical_game_ids),
                replayed=odds_replayed,
            )
        pred_game_ids = set(pd.to_numeric(pd.read_csv(pred_path).game_id, errors="raise").astype(int))
        game_starts = {int(game.game_id): game.start_time_utc for game in canonical_games_for_sog}
        integrity = audit_sog_attachment(
            prediction_path=pred_path, attachment_path=out_csv,
            unmatched_path=unmatched_csv, slate_date=slate,
            parent_daily_run_id=parent_daily_run_id,
            odds_observation_path=odds_observation_dir,
            odds_observation_manifest_sha256=(
                odds_lineage["odds_observation_manifest_sha256"] if odds_lineage else None),
            names_path=names_csv,
            canonical_game_starts_utc=game_starts,
        )
        if odds_lineage:
            integrity.update(odds_lineage)
            integrity["odds_observation_phase"] = odds_phase
            integrity["odds_observation_season"] = infer_nhl_season_from_date_yyyy_mm_dd(slate)
        else:
            integrity["status"] = "UNAVAILABLE_NO_GOVERNED_ODDS_OBSERVATION"
            integrity["integrity_status"] = "UNAVAILABLE"
            integrity["checks"]["odds_manifest_bound"] = False
        report_path = SITE_DIR / "sog_attachment_integrity.json"
        report_path.write_text(json.dumps(integrity, indent=2, sort_keys=True) + "\n")
        package_path = (
            SOG_ATTACHMENT_ROOT / f"season={infer_nhl_season_from_date_yyyy_mm_dd(slate)}"
            / f"slate_date={slate}" / f"run_id={parent_daily_run_id}"
        )
        package_report, _ = retain_sog_attachment_package(
            package_path=package_path, integrity=integrity,
            attachment_path=out_csv, unmatched_path=unmatched_csv,
            names_path=names_csv, feature_input_binding=feature_input_binding,
        )
        # Keep a compatibility copy in the site directory while the receipt binds
        # the retained, create-only report package as the evidence authority.
        shutil.copy2(package_report, report_path)


def build_saves(slate: str, *, odds_json: Path | None = None,
                pred_path: Path | None = None,
                expected_pred_sha256: str | None = None,
                parent_daily_run_id: str | None = None,
                odds_observation_dir: Path | None = None,
                expected_odds_manifest_sha256: str | None = None,
                odds_phase: str | None = None,
                odds_replayed: bool = False):
    # Ensure names exist (build_saves can be called standalone)
    names_csv = export_names_csv(slate)

    pred_path = Path(pred_path or (PROC_DIR / "saves_predictions.csv"))
    _require_current_prediction_artifact(
        pred_path, slate=slate, expected_sha256=expected_pred_sha256)
    command = [
            PY,
            SCRIPTS_DIR / "build_saves_with_market.py",
            "--pred", pred_path,
            "--names", names_csv,
            "--out", SITE_DIR / "saves_with_market.csv",
            "--unmatched", SITE_DIR / "unmatched_saves.csv",
            "--ambiguous", SITE_DIR / "ambiguous_saves_alias_matches.csv",
            "--integrity-report", SITE_DIR / "saves_attachment_integrity.json",
        ]
    if parent_daily_run_id is not None and expected_pred_sha256 is not None:
        command.extend([
            "--strict-current-run",
            "--parent-run-id", str(parent_daily_run_id),
            "--expected-pred-sha256", str(expected_pred_sha256),
        ])
    if odds_json is not None:
        command.extend(["--odds-json", odds_json])
        command.extend(["--odds-observation-dir", odds_observation_dir])
        command.extend([
            "--expected-odds-manifest-sha256",
            str(expected_odds_manifest_sha256 or ""),
        ])
        command.extend([
            "--odds-season", str(infer_nhl_season_from_date_yyyy_mm_dd(slate)),
        ])
        if odds_phase is not None:
            command.extend(["--odds-phase", str(odds_phase)])
        if odds_replayed:
            command.append("--odds-replayed")
    run(command, env={"SLATE_DATE": slate})


def build_points(slate: str, *, odds_json: Path | None = None,
                 events_json: Path | None = None, pred_path: Path | None = None,
                 expected_pred_sha256: str | None = None,
                 parent_daily_run_id: str | None = None,
                 odds_observation_dir: Path | None = None,
                 expected_odds_manifest_sha256: str | None = None,
                 odds_phase: str | None = None,
                 odds_replayed: bool = False):
    args = [
        PY,
        SCRIPTS_DIR / "build_points_with_market.py",
        "--out",         SITE_DIR / "points_with_market.csv",
        "--unmatched",   SITE_DIR / "unmatched_points.csv",
        "--strict-current-run",
    ]
    if odds_json is not None:
        args += ["--odds-json", odds_json]
    if events_json is not None:
        args += ["--events-json", events_json]

    pred_path = Path(pred_path or (PROC_DIR / "points_predictions.csv"))
    _require_current_prediction_artifact(
        pred_path, slate=slate, expected_sha256=expected_pred_sha256)
    args += ["--pred", pred_path]
    if parent_daily_run_id is not None and expected_pred_sha256 is not None:
        args += [
            "--parent-run-id", str(parent_daily_run_id),
            "--expected-pred-sha256", str(expected_pred_sha256),
        ]

    # Only include names if we actually have them (and don't assume location)
    names_path = export_names_csv(slate)
    if names_path.exists():
        args += ["--names", names_path]

    if odds_json is not None:
        args += [
            "--odds-observation-dir", str(odds_observation_dir),
            "--expected-odds-manifest-sha256", str(expected_odds_manifest_sha256 or ""),
            "--odds-season", str(infer_nhl_season_from_date_yyyy_mm_dd(slate)),
        ]
        if odds_phase is not None:
            args += ["--odds-phase", str(odds_phase)]
        if odds_replayed:
            args.append("--odds-replayed")

    run(args)


def _score_phoenix_points_artifact(*, recorder, prediction_run_dir: Path,
                                   canonical_games, slate: str,
                                   daily_run_id: str, cutoff: str,
                                   output_path: Path) -> dict[str, Any]:
    """Score the existing Phoenix model, returning its bound artifact identity."""
    points_source_csv = EXPORTS_DIR / "train_nhl_points_v2.csv"
    recorder.lane("points").inputs.append(artifact_identity(points_source_csv))
    points_input_csv = prediction_run_dir / "points_scoring_input.csv"
    points_input_identity = prepare_scoring_input(
        source_path=points_source_csv, output_path=points_input_csv,
        canonical_games=canonical_games, slate=slate,
        parent_daily_run_id=daily_run_id,
        feature_input_cutoff_utc=cutoff,
        expected_game_set_hash=recorder.canonical_game_set_hash,
    )
    points_input_identity.update(artifact_identity(points_input_csv))
    recorder.lane("points").inputs.append(points_input_identity)
    score_result = run([
        PY, SCRIPTS_DIR / "score_nhl_points_with_lineage.py",
        "--features-csv", points_input_csv,
        "--model-root", MODELS_DIR / "latest" / "points",
        "--out", output_path,
    ])
    validation = validate_prediction_output(
        path=output_path, lane="points", canonical_games=canonical_games,
        slate=slate, parent_daily_run_id=daily_run_id,
        feature_input_cutoff_utc=cutoff,
        expected_game_set_hash=recorder.canonical_game_set_hash,
        expected_lines=POINTS_LINES,
    )
    identity = artifact_identity(output_path)
    identity.update(validation)
    evidence = _structured_child_summary(score_result.stdout).get("fitted_model_evidence")
    if not evidence or evidence.get("prediction_artifact_sha256") != identity["sha256"]:
        raise RuntimeError("POINTS_FITTED_MODEL_EVIDENCE_MISSING_OR_UNBOUND")
    identity["fitted_model_evidence"] = evidence
    return identity


def _reference_cold_start_sog(recorder: DailyRunRecorder, slate: str) -> None:
    root = ROOT / "artifacts" / "operational" / "nhl" / "sog_prediction_only"
    season = infer_nhl_season_from_date_yyyy_mm_dd(slate)
    recorder.start_lane("cold_start_sog_reference")
    packages = sorted((root / f"season={season}" / f"slate_date={slate}").glob(
        "phase=*/run_id=*/SHA256SUMS"))
    statuses = sorted((root / "status" / slate).glob("prediction_*.json"))
    try:
        outputs = [{
            "path": str(path.parent.resolve()),
            "manifest_sha256": verify_package(path.parent),
        } for path in packages]
        if statuses:
            outputs.append(artifact_identity(statuses[-1]))
        recorder.finish_lane(
            "cold_start_sog_reference",
            status="REFERENCED_EXTERNAL_OWNER" if outputs else "NOT_AVAILABLE_EXTERNAL_OWNER",
            reason="PREDICTION_ONLY_OBSERVER_OWNS_COLD_START_SOG",
            outputs=outputs,
        )
    except Exception as error:
        recorder.fail_lane("cold_start_sog_reference", error, blocking=False)


def _run_independent_daily_lanes(
    *, recorder: DailyRunRecorder, db: str, slate: str, with_odds: bool,
    odds_phase: str, daily_run_id: str, canonical_games,
    saves_export_ready: bool, points_export_ready: bool,
    legacy_sog_prediction: dict[str, Any] | None,
    reuse_odds_observation: Path | None = None,
) -> None:
    """Run lane-local scoring, acquisition, attachment, and integrity stages."""
    global _ACTIVE_DAILY_LANE
    prediction_identities: dict[str, dict[str, Any]] = {}
    prediction_run_dir = PROC_DIR / "daily_runs" / daily_run_id
    prediction_run_dir.mkdir(parents=True, exist_ok=True)
    no_pregame_games = not pregame_game_eligibility(
        canonical_games, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )["eligible_pregame_game_ids"]
    if no_pregame_games:
        for lane_name in ("saves", "points", "points_hgb_shadow", "odds",
                          "sog_attachment", "points_attachment", "saves_attachment",
                          "research_integrity"):
            recorder.finish_lane(lane_name, status="SKIPPED_NO_PREGAME_GAMES",
                                 reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
        _reference_cold_start_sog(recorder, slate)
        return

    if saves_export_ready:
        recorder.start_lane("saves")
        _ACTIVE_DAILY_LANE = "saves"
        try:
            saves_source_csv = EXPORTS_DIR / "train_goalie_saves_v2.csv"
            recorder.lane("saves").inputs.append(artifact_identity(saves_source_csv))
            saves_model_dir = MODELS_DIR / "latest" / "goalie_saves"
            if not saves_model_dir.exists():
                raise RuntimeError(f"SAVES_MODEL_MISSING:{saves_model_dir}")
            saves_cutoff = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            saves_input_csv = prediction_run_dir / "saves_scoring_input.csv"
            saves_input_identity = prepare_scoring_input(
                source_path=saves_source_csv, output_path=saves_input_csv,
                canonical_games=canonical_games, slate=slate,
                parent_daily_run_id=daily_run_id,
                feature_input_cutoff_utc=saves_cutoff,
                expected_game_set_hash=recorder.canonical_game_set_hash,
                constant_feature_values={"start_prob": 1.0},
            )
            saves_input_identity.update(artifact_identity(saves_input_csv))
            recorder.lane("saves").inputs.append(saves_input_identity)
            saves_pred_csv = prediction_run_dir / "saves_predictions.csv"
            saves_score_result = run([
                PY, SCRIPTS_DIR / "score_nhl_saves_with_lineage.py",
                "--model-dir", saves_model_dir,
                "--csv", saves_input_csv,
                "--feature-json", "backend/nhl/features/feature_metadata_nhl.json",
                "--feature-key", "goalie_saves",
                "--line", "18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,30.5",
                "--out", saves_pred_csv,
            ])
            saves_validation = validate_prediction_output(
                path=saves_pred_csv, lane="saves", canonical_games=canonical_games,
                slate=slate, parent_daily_run_id=daily_run_id,
                feature_input_cutoff_utc=saves_cutoff,
                expected_game_set_hash=recorder.canonical_game_set_hash,
                expected_lines=SAVES_LINES,
            )
            prediction_identities["saves"] = artifact_identity(saves_pred_csv)
            prediction_identities["saves"].update(saves_validation)
            saves_evidence = _structured_child_summary(saves_score_result.stdout).get("fitted_model_evidence")
            if not saves_evidence or saves_evidence.get("prediction_artifact_sha256") != prediction_identities["saves"]["sha256"]:
                raise RuntimeError("SAVES_FITTED_MODEL_EVIDENCE_MISSING_OR_UNBOUND")
            prediction_identities["saves"]["fitted_model_evidence"] = saves_evidence
            run([
                PY, SCRIPTS_DIR / "load_nhl_predictions_generic.py",
                "--pred-csv", saves_pred_csv, "--project", "nhl",
                "--prop", "goalie_saves", "--model-family", "phoenix",
                "--model-version", "phoenix_v2", "--feature-hash", "phoenix_v2",
                "--expected-sha256", prediction_identities["saves"]["sha256"],
            ])
            recorder.finish_lane(
                "saves", outputs=[prediction_identities["saves"]],
                database_rows_written=True)
        except Exception as error:
            if str(error) == "SCORING_INPUT_NO_PREGAME_GAMES_REMAIN":
                recorder.finish_lane("saves", status="SKIPPED_NO_PREGAME_GAMES",
                                     reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
            else:
                recorder.fail_lane("saves", error, blocking=False)
    else:
        recorder.finish_lane(
            "saves", status="FAILED_NONBLOCKING", reason="SAVES_FEATURE_EXPORT_FAILED")

    authority = selected_production_authority()
    production_authority = str(authority["production_authority"])
    hgb_capture_result: dict[str, Any] | None = None
    phoenix_shadow_identity: dict[str, Any] | None = None
    phoenix_shadow_error: str | None = None
    if points_export_ready or production_authority == HGB_AUTHORITY:
        recorder.start_lane("points")
        _ACTIVE_DAILY_LANE = "points"
        if production_authority == HGB_AUTHORITY:
            # HGB is an explicitly selected production lane. A missing HGB
            # prerequisite must fail this lane instead of falling back.
            recorder.lane("points").blocking = True
        try:
            recorder.lane("points").inputs.append({"authority_path": str(AUTHORITY_PATH.resolve()),
                "authority_sha256": sha256_file(AUTHORITY_PATH),
                "production_authority": production_authority,
                "shadow_authorities": authority.get("shadow_authorities", []),
                "authority_version": authority["authority_version"]})
            if production_authority == HGB_AUTHORITY:
                promotion = assert_hgb_promotion_ready()
                recorder.lane("points").inputs.append({"promotion_evaluator": promotion["classification"],
                    "promotion_gate_statuses": promotion["gates"]})

            points_cutoff = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            phoenix_pred_path = prediction_run_dir / (
                "points_predictions.csv" if production_authority == PHOENIX_AUTHORITY
                else "points_phoenix_shadow_predictions.csv")
            if points_export_ready:
                try:
                    phoenix_shadow_identity = _score_phoenix_points_artifact(
                        recorder=recorder, prediction_run_dir=prediction_run_dir,
                        canonical_games=canonical_games, slate=slate,
                        daily_run_id=daily_run_id, cutoff=points_cutoff,
                        output_path=phoenix_pred_path,
                    )
                except Exception as phoenix_error:
                    if production_authority == PHOENIX_AUTHORITY:
                        raise
                    phoenix_shadow_error = f"{type(phoenix_error).__name__}:{phoenix_error}"
                if production_authority == PHOENIX_AUTHORITY:
                    if phoenix_shadow_identity is None:
                        raise RuntimeError("PHOENIX_PRODUCTION_SCORING_UNAVAILABLE")
                    prediction_identities["points"] = phoenix_shadow_identity
                    run([
                        PY, SCRIPTS_DIR / "load_nhl_predictions_generic.py",
                        "--pred-csv", phoenix_pred_path, "--project", "nhl",
                        "--prop", "player_points", "--model-family", "phoenix",
                        "--model-version", "phoenix_v2", "--feature-hash", "phoenix_v2",
                        "--expected-sha256", phoenix_shadow_identity["sha256"],
                    ])
            elif production_authority == PHOENIX_AUTHORITY:
                raise RuntimeError("POINTS_FEATURE_EXPORT_FAILED")
            else:
                raise RuntimeError("UNSUPPORTED_POINTS_PRODUCTION_AUTHORITY")

            if production_authority == HGB_AUTHORITY:
                # Phoenix is only a same-run shadow in this state. If its
                # scorer failed, continue with canonical roster identities.
                if phoenix_shadow_identity is None and phoenix_shadow_error is None:
                    phoenix_shadow_error = "PHOENIX_SHADOW_SCORING_UNAVAILABLE"
                try:
                    temporary = tempfile.TemporaryDirectory(prefix="nhl_points_hgb_daily_")
                    logs_path, outcomes_paths, slate_path, source_summary = prepare_points_hgb_inputs(
                        db_url=db, canonical_games=list(canonical_games), slate_date=slate,
                        phoenix_predictions=(Path(phoenix_shadow_identity["path"])
                                             if phoenix_shadow_identity else None),
                        asof_utc=points_cutoff, work_dir=Path(temporary.name))
                    hgb_capture_result = capture_points_hgb_shadow(
                        logs_path=logs_path, outcomes_paths=outcomes_paths, slate_path=slate_path,
                        phoenix_predictions_path=(Path(phoenix_shadow_identity["path"])
                                                  if phoenix_shadow_identity else None),
                        phoenix_prediction_sha256=(phoenix_shadow_identity["sha256"]
                                                   if phoenix_shadow_identity else None),
                        phoenix_model_identity_sha256=(phoenix_shadow_identity["fitted_model_evidence"]["fitted_model_identity_sha256"]
                                                       if phoenix_shadow_identity else None),
                        phoenix_feature_contract_sha256=(phoenix_shadow_identity["fitted_model_evidence"].get("scoring_configuration", {}).get("feature_contract_sha256")
                                                         if phoenix_shadow_identity else None),
                        phoenix_feature_cutoff_utc=(phoenix_shadow_identity.get("feature_input_cutoff_utc")
                                                    if phoenix_shadow_identity else None),
                        slate_date=slate, season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
                        capture_phase=odds_phase, parent_run_id=daily_run_id,
                        feature_cutoff_utc=points_cutoff,
                        output_root=ROOT / "artifacts/operational/nhl/points_hgb_shadow",
                        excluded_started_game_ids=source_summary["excluded_started_game_ids"],
                    )
                    hgb_capture_result["source_summary"] = source_summary
                except Exception as shadow_error:
                    # Reaching this branch means the HGB production prerequisites
                    # failed; it remains a visible blocking Points failure.
                    raise RuntimeError(f"HGB_PRODUCTION_FEATURE_OR_SCORING_FAILED:{shadow_error}") from shadow_error
                finally:
                    temporary.cleanup()

                points_pred_csv = prediction_run_dir / "points_predictions.csv"
                production_identity = build_production_prediction_artifact(
                    capture_path=Path(hgb_capture_result["capture_path"]),
                    output_path=points_pred_csv, canonical_games=list(canonical_games),
                    slate_date=slate, parent_run_id=daily_run_id,
                    feature_cutoff_utc=points_cutoff,
                    canonical_game_set_sha256=recorder.canonical_game_set_hash,
                )
                production_validation = validate_prediction_output(
                    path=points_pred_csv, lane="points", canonical_games=canonical_games,
                    slate=slate, parent_daily_run_id=daily_run_id,
                    feature_input_cutoff_utc=points_cutoff,
                    expected_game_set_hash=recorder.canonical_game_set_hash,
                    expected_lines=POINTS_LINES,
                )
                production_identity.update(production_validation)
                model_identity = hgb_capture_result["model_identity"]
                production_identity["model_family"] = "hist_gradient_boosting"
                production_identity["model_version"] = HGB_AUTHORITY
                production_identity["production_authority"] = HGB_AUTHORITY
                production_identity.update({
                    "hgb_capture_path": hgb_capture_result["capture_path"],
                    "hgb_capture_timestamp_utc": hgb_capture_result["capture_timestamp_utc"],
                    "hgb_feature_artifact_sha256": hgb_capture_result["feature_sha256"],
                    "hgb_model_artifact_sha256": model_identity["model_artifact_sha256"],
                    "feature_contract": model_identity["feature_contract"],
                    "feature_contract_sha256": model_identity["feature_contract_sha256"],
                    "history_contract": model_identity["history_contract"],
                    "history_contract_identity_sha256": model_identity["history_contract_identity_sha256"],
                })
                production_identity["phoenix_shadow"] = ({"status": "COMPLETE",
                    "model": "PHOENIX_POINTS_INCUMBENT_SHADOW",
                    "model_family": "phoenix", "model_version": PHOENIX_AUTHORITY,
                    "feature_contract": "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2",
                    "path": phoenix_shadow_identity["path"],
                    "sha256": phoenix_shadow_identity["sha256"],
                    "fitted_model_identity_sha256": phoenix_shadow_identity["fitted_model_evidence"]["fitted_model_identity_sha256"],
                    "feature_contract_sha256": phoenix_shadow_identity["fitted_model_evidence"].get("scoring_configuration", {}).get("feature_contract_sha256")}
                    if phoenix_shadow_identity else {"status": "FAILED_NONBLOCKING",
                        "model": "PHOENIX_POINTS_INCUMBENT_SHADOW",
                        "model_family": "phoenix", "model_version": PHOENIX_AUTHORITY,
                        "feature_contract": "POINTS_PLAYER_HISTORY_CROSS_SEASON_V2",
                        "reason": phoenix_shadow_error})
                production_identity["fitted_model_evidence"] = {
                    "model_family": "hist_gradient_boosting",
                    "model_version": HGB_AUTHORITY,
                    "fitted_model_identity_sha256": model_identity["model_identity_sha256"],
                    "prediction_artifact_path": str(points_pred_csv.resolve()),
                    "prediction_artifact_sha256": production_identity["sha256"],
                    "scoring_run_id": daily_run_id,
                    "component_artifacts": [
                        {"canonical_artifact_path": "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/nhl_points_count_hgb_v1.joblib",
                         "role": "frozen_fitted_model", "sha256": model_identity["model_artifact_sha256"]},
                        {"canonical_artifact_path": "backend/nhl/scripts/score_nhl_points_hgb_shadow.py",
                         "role": "scorer", "sha256": model_identity["scorer_sha256"]},
                        {"canonical_artifact_path": "backend/nhl/scripts/export_nhl_points_hgb_features.py",
                         "role": "feature_exporter", "sha256": sha256_file(ROOT / "backend/nhl/scripts/export_nhl_points_hgb_features.py")},
                    ],
                    "scoring_configuration": {
                        "feature_contract": model_identity["feature_contract"],
                        "feature_contract_sha256": model_identity["feature_contract_sha256"],
                        "feature_columns": model_identity["feature_columns"],
                        "history_contract": model_identity["history_contract"],
                        "history_contract_identity_sha256": model_identity["history_contract_identity_sha256"],
                        "probability_construction": "POISSON_SURVIVAL_FROM_FROZEN_HGB_EXPECTED_COUNT",
                        "calibration": "NONE", "pava": False, "class_weighting": False,
                        "scored_lines": list(POINTS_LINES),
                    },
                    "source_hgb_capture_prediction_sha256": production_identity["source_hgb_capture_prediction_sha256"],
                }
                prediction_identities["points"] = production_identity
                run([
                    PY, SCRIPTS_DIR / "load_nhl_predictions_generic.py",
                    "--pred-csv", points_pred_csv, "--project", "nhl",
                    "--prop", "player_points", "--model-family", "hist_gradient_boosting",
                    "--model-version", HGB_AUTHORITY,
                    "--feature-hash", f"{HGB_AUTHORITY}:{model_identity['model_identity_sha256']}",
                    "--model-params-json", json.dumps({
                        "fitted_model_identity_sha256": model_identity["model_identity_sha256"],
                        "model_artifact_sha256": model_identity["model_artifact_sha256"],
                        "feature_contract_sha256": model_identity["feature_contract_sha256"],
                        "history_contract": model_identity["history_contract"],
                    }),
                    "--expected-sha256", production_identity["sha256"],
                ])
            recorder.finish_lane(
                "points", outputs=[prediction_identities["points"]],
                database_rows_written=True)
        except Exception as error:
            if (str(error) == "SCORING_INPUT_NO_PREGAME_GAMES_REMAIN"
                    or "HGB_ALL_CANONICAL_GAMES_ALREADY_STARTED" in str(error)):
                recorder.finish_lane("points", status="SKIPPED_NO_PREGAME_GAMES",
                                     reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
                recorder.finish_lane("points_hgb_shadow", status="SKIPPED_NO_PREGAME_GAMES",
                                     reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
            else:
                recorder.fail_lane("points", error,
                                   blocking=True if production_authority == HGB_AUTHORITY else False)
    else:
        recorder.finish_lane(
            "points", status="FAILED_BLOCKING" if production_authority == HGB_AUTHORITY else "FAILED_NONBLOCKING",
            reason="POINTS_FEATURE_EXPORT_FAILED",
        )

    _reference_cold_start_sog(recorder, slate)

    odds_result: OddsObservationResult | None = None
    odds_eligibility = pregame_game_eligibility(
        canonical_games, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    if not odds_eligibility["eligible_pregame_game_ids"]:
        recorder.finish_lane("odds", status="SKIPPED_NO_PREGAME_GAMES",
                             reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
    else:
        recorder.start_lane("odds", inputs=[{
            "canonical_game_set_hash": recorder.canonical_game_set_hash,
            "canonical_game_count": len(recorder.canonical_game_ids),
            "authorized": bool(with_odds),
            "eligible_pregame_game_ids": odds_eligibility["eligible_pregame_game_ids"],
            "started_excluded_game_ids": odds_eligibility["started_excluded_game_ids"],
        }])
        _ACTIVE_DAILY_LANE = "odds"
        try:
            odds_result = run_optional_odds_observation(
                with_odds=with_odds, slate=slate,
                season=infer_nhl_season_from_date_yyyy_mm_dd(slate), phase=odds_phase,
                parent_daily_run_id=daily_run_id, canonical_games=canonical_games,
                reuse_observation_dir=reuse_odds_observation)
            if odds_result is None:
                recorder.finish_lane("odds", status="SKIPPED_NOT_REQUESTED")
            else:
                summary = odds_result.summary
                recorder.odds_observation = {
                    "path": str(odds_result.observation_dir.resolve()),
                    "manifest_sha256": odds_result.manifest_sha256,
                    "classification": odds_result.classification,
                    "request_plan_path": str((odds_result.observation_dir / "request_plan.json").resolve()),
                    "request_plan_sha256": sha256_file(odds_result.observation_dir / "request_plan.json"),
                    "network_attempt_count": int(summary.get("network_attempt_count", 0)),
                    "credits_consumed": summary.get("credits_consumed"),
                    "maximum_credits": summary.get("maximum_credits"),
                }
                odds_health = daily_health_for_odds(requested=with_odds, result=odds_result)
                recorder.finish_lane(
                    "odds", status=("READY_WITH_ODDS_WARNING" if odds_health != "READY" else odds_result.classification),
                    reason=(odds_result.classification if odds_health != "READY" else None),
                    outputs=[recorder.odds_observation],
                    provider_requests=int(summary.get("network_attempt_count", 0)),
                    credits_consumed=summary.get("credits_consumed"),
                )
        except Exception as error:
            recorder.fail_lane("odds", error, blocking=False)

    captured = bool(
        odds_result is not None and odds_result.classification.startswith("CAPTURED_"))
    odds_path = odds_result.observation_dir / "raw_response.json" if captured else None
    odds_lineage: dict[str, Any] = {}
    odds_integrity_error: AttachmentIntegrityError | None = None
    if captured and odds_result is not None and odds_path is not None:
        try:
            odds_lineage = validate_odds_observation(
                observation_dir=odds_result.observation_dir,
                odds_json=odds_path,
                expected_manifest_sha256=odds_result.manifest_sha256,
                expected_parent_daily_run_id=daily_run_id,
                expected_slate_date=slate,
                expected_season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
                expected_phase=odds_phase,
                expected_game_set_hash=recorder.canonical_game_set_hash,
                replayed=odds_result.replayed,
            )
        except AttachmentIntegrityError as error:
            odds_integrity_error = error

    attachment_specs = [
        ("sog_attachment", "legacy_sog", legacy_sog_prediction, build_sog),
        ("saves_attachment", "saves", prediction_identities.get("saves"), build_saves),
        ("points_attachment", "points", prediction_identities.get("points"), build_points),
    ]
    attachment_contexts: dict[str, dict[str, Any]] = {}
    for attachment_lane, prediction_lane, identity, builder in attachment_specs:
        if identity is None or recorder.lane(prediction_lane).status != "COMPLETE":
            recorder.finish_lane(
                attachment_lane, status="SKIPPED_UPSTREAM_LANE_BLOCKED",
                reason=f"{prediction_lane.upper()}_CURRENT_RUN_ARTIFACT_UNAVAILABLE")
            continue
        attachment_inputs = [identity]
        if captured and recorder.odds_observation is not None:
            attachment_inputs.append(recorder.odds_observation)
        recorder.start_lane(attachment_lane, inputs=attachment_inputs)
        _ACTIVE_DAILY_LANE = attachment_lane
        try:
            if prediction_lane in {"saves", "points"} and odds_integrity_error is not None:
                raise odds_integrity_error
            kwargs: dict[str, Any] = {
                "pred_path": Path(identity["path"]),
                "expected_pred_sha256": identity["sha256"],
            }
            kwargs.update(_attachment_market_inputs(attachment_lane, odds_result))
            if attachment_lane == "sog_attachment":
                kwargs.update({
                    "parent_daily_run_id": daily_run_id,
                    "odds_observation_dir": (
                        odds_result.observation_dir if captured and odds_result else None),
                    "expected_odds_manifest_sha256": (
                        odds_result.manifest_sha256 if captured and odds_result else None),
                    "odds_phase": odds_phase,
                    "odds_replayed": bool(odds_result and odds_result.replayed),
                    "feature_input_binding": {
                        key: legacy_sog_prediction[key] for key in (
                            "feature_input_path", "feature_input_sha256",
                            "feature_input_manifest_sha256", "feature_contract",
                            "feature_contract_version", "feature_cutoff_utc",
                            "scorer_sha256", "fitted_model_identity_sha256",
                            "prediction_artifact_sha256", "parent_daily_run_id",
                            "canonical_game_set_hash")
                        if key in legacy_sog_prediction
                    },
                })
            if attachment_lane == "saves_attachment":
                kwargs.update({
                    "parent_daily_run_id": daily_run_id,
                    "odds_observation_dir": (
                        odds_result.observation_dir if captured and odds_result else None),
                    "expected_odds_manifest_sha256": (
                        odds_result.manifest_sha256 if captured and odds_result else None),
                    "odds_phase": odds_phase,
                    "odds_replayed": bool(odds_result and odds_result.replayed),
                })
            if attachment_lane == "points_attachment":
                kwargs.update({
                    "parent_daily_run_id": daily_run_id,
                    "odds_observation_dir": (
                        odds_result.observation_dir if captured and odds_result else None),
                    "expected_odds_manifest_sha256": (
                        odds_result.manifest_sha256 if captured and odds_result else None),
                    "odds_phase": odds_phase,
                    "odds_replayed": bool(odds_result and odds_result.replayed),
                })
            builder(slate, **kwargs)
            prefix = {"sog_attachment": "sog", "saves_attachment": "saves", "points_attachment": "points"}[attachment_lane]
            attachment_path = SITE_DIR / f"{prefix}_with_market.csv"
            outputs = [
                artifact_identity(attachment_path),
                artifact_identity(SITE_DIR / f"unmatched_{prefix}.csv"),
            ]
            if attachment_lane == "sog_attachment":
                package_dir = (
                    SOG_ATTACHMENT_ROOT
                    / f"season={infer_nhl_season_from_date_yyyy_mm_dd(slate)}"
                    / f"slate_date={slate}" / f"run_id={daily_run_id}"
                )
                outputs.extend(artifact_identity(package_dir / name) for name in (
                    "sog_attachment_integrity.json", "sog_with_market.csv",
                    "unmatched_sog.csv", "RUN_COMPLETE.json", "SHA256SUMS",
                ))
            if prediction_lane in {"saves", "points"}:
                expected_odds_manifest = (
                    odds_result.manifest_sha256 if captured and odds_result else None)
                integrity = audit_attachment_files(
                    lane=prediction_lane,
                    prediction_path=Path(identity["path"]),
                    attachment_path=attachment_path,
                    expected_prediction_sha256=identity["sha256"],
                    expected_parent_daily_run_id=daily_run_id,
                    expected_odds_manifest_sha256=expected_odds_manifest,
                )
                integrity.update(odds_lineage)
                integrity["slate_date"] = slate
                integrity["canonical_game_set_hash"] = recorder.canonical_game_set_hash
                integrity["proposition"] = (
                    "player_points" if prediction_lane == "points" else "goalie_saves")
                unmatched_path = SITE_DIR / f"unmatched_{prefix}.csv"
                integrity["unmatched_path"] = str(unmatched_path.resolve())
                integrity["unmatched_sha256"] = sha256_file(unmatched_path)
                ambiguous_path = None
                if attachment_lane == "saves_attachment":
                    ambiguous_path = SITE_DIR / "ambiguous_saves_alias_matches.csv"
                    integrity["ambiguous_inventory_path"] = str(ambiguous_path.resolve())
                    integrity["ambiguous_inventory_sha256"] = sha256_file(ambiguous_path)
                observation_timestamp = None
                if captured and odds_result is not None:
                    observation = json.loads((odds_result.observation_dir
                                              / "observation_summary.json").read_text())
                    observation_timestamp = observation.get("observation_timestamp_utc")
                    integrity["odds_observation_identity"] = (
                        observation.get("invocation_id") or odds_result.observation_dir.name)
                integrity["odds_observation_timestamp_utc"] = observation_timestamp
                integrity["prediction_row_count"] = integrity["counts"]["prediction_row_count"]
                integrity["exact_proposition_key_count"] = integrity["counts"][
                    "unique_prediction_key_count"]
                pred_frame = pd.read_csv(Path(identity["path"]))
                integrity["natural_identity_count"] = int(
                    pred_frame[["game_id", "player_id"]].drop_duplicates().shape[0])
                report_path = SITE_DIR / f"{prefix}_attachment_integrity.json"
                package_root = (POINTS_ATTACHMENT_ROOT if prediction_lane == "points"
                                else SAVES_ATTACHMENT_ROOT)
                package_dir = (package_root
                    / f"season={infer_nhl_season_from_date_yyyy_mm_dd(slate)}"
                    / f"slate_date={slate}" / f"run_id={daily_run_id}")
                starts = {int(game.game_id): str(game.start_time_utc)
                          for game in canonical_games}
                retained_report, retained_manifest_sha = retain_market_attachment_package(
                    lane=prediction_lane, package_path=package_dir, integrity=integrity,
                    prediction_path=Path(identity["path"]), attachment_path=attachment_path,
                    unmatched_path=unmatched_path, ambiguous_path=ambiguous_path,
                    canonical_game_set_sha256=recorder.canonical_game_set_hash,
                    canonical_game_starts_utc=starts)
                shutil.copy2(retained_report, report_path)
                report_identity = _write_attachment_integrity_report(report_path, integrity)
                outputs.append(report_identity)
                if ambiguous_path is not None:
                    outputs.append(artifact_identity(ambiguous_path))
                outputs.extend(_market_attachment_retention_receipt_outputs(
                    package_dir, lane=prediction_lane, parent_daily_run_id=daily_run_id,
                    manifest_sha256=retained_manifest_sha))
                attachment_contexts[attachment_lane] = {
                    "lane": prediction_lane,
                    "prediction_path": Path(identity["path"]),
                    "attachment_path": attachment_path,
                    "expected_prediction_sha256": identity["sha256"],
                    "expected_parent_daily_run_id": daily_run_id,
                    "expected_odds_manifest_sha256": expected_odds_manifest,
                    "expected_counts": integrity["counts"],
                }
            recorder.finish_lane(attachment_lane, outputs=outputs)
        except AttachmentIntegrityError as error:
            recorder.finish_lane(
                attachment_lane, status="FAILED_NONBLOCKING_INTEGRITY",
                reason=redact_sensitive_text(f"{type(error).__name__}:{error}"))
        except Exception as error:
            recorder.fail_lane(attachment_lane, error, blocking=False)

    # HGB is a separate first-class Points shadow. Its failures never block the
    # Phoenix lane or its market attachment, and it does not depend on odds.
    if recorder.lane("points").status == "SKIPPED_NO_PREGAME_GAMES":
        recorder.finish_lane("points_hgb_shadow", status="SKIPPED_NO_PREGAME_GAMES",
                             reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
    elif (recorder.lane("points").status == "COMPLETE"
            and prediction_identities.get("points")
            and production_authority == HGB_AUTHORITY):
        recorder.start_lane("points_hgb_shadow", inputs=[prediction_identities["points"]])
        _ACTIVE_DAILY_LANE = "points_hgb_shadow"
        recorder.finish_lane(
            "points_hgb_shadow", status="COMPLETE",
            reason="HGB_PRODUCTION_PHOENIX_INCUMBENT_SHADOW",
            outputs=[hgb_capture_result or {},
                     {"shadow_role": "PHOENIX_POINTS_INCUMBENT_SHADOW",
                      **(phoenix_shadow_identity or {}),
                      "status": "COMPLETE" if phoenix_shadow_identity else "FAILED_NONBLOCKING",
                      "reason": phoenix_shadow_error}],
        )
    elif (recorder.lane("points").status == "COMPLETE"
            and prediction_identities.get("points")):
        recorder.start_lane("points_hgb_shadow", inputs=[prediction_identities["points"]])
        _ACTIVE_DAILY_LANE = "points_hgb_shadow"
        try:
            phoenix_identity = prediction_identities["points"]
            phoenix_evidence = phoenix_identity["fitted_model_evidence"]
            phoenix_config = phoenix_evidence.get("scoring_configuration", {})
            cutoff = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            with tempfile.TemporaryDirectory(prefix="nhl_points_hgb_daily_") as temporary:
                logs_path, outcomes_paths, slate_path, source_summary = prepare_points_hgb_inputs(
                    db_url=db, canonical_games=list(canonical_games), slate_date=slate,
                    phoenix_predictions=Path(phoenix_identity["path"]), asof_utc=cutoff,
                    work_dir=Path(temporary))
                result = capture_points_hgb_shadow(
                    logs_path=logs_path, outcomes_paths=outcomes_paths, slate_path=slate_path,
                    phoenix_predictions_path=Path(phoenix_identity["path"]),
                    phoenix_prediction_sha256=phoenix_identity["sha256"],
                    phoenix_model_identity_sha256=str(phoenix_evidence.get("fitted_model_identity_sha256") or ""),
                    phoenix_feature_contract_sha256=str(phoenix_config.get("feature_contract_sha256") or ""),
                    phoenix_feature_cutoff_utc=str(phoenix_identity.get("feature_input_cutoff_utc") or ""),
                    slate_date=slate, season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
                    capture_phase=odds_phase, parent_run_id=daily_run_id,
                    feature_cutoff_utc=cutoff,
                    output_root=ROOT / "artifacts/operational/nhl/points_hgb_shadow",
                    excluded_started_game_ids=source_summary["excluded_started_game_ids"],
                )
            result["source_summary"] = source_summary
            recorder.finish_lane(
                "points_hgb_shadow", status="COMPLETE", reason="DETERMINISTIC_REPLAY_PASS",
                outputs=[result],
            )
        except Exception as error:
            message = redact_sensitive_text(f"{type(error).__name__}:{error}")
            if "ALL_CANONICAL_GAMES_ALREADY_STARTED" in message:
                recorder.finish_lane(
                    "points_hgb_shadow", status="SKIPPED_NO_PREGAME_GAMES",
                    reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
            elif "ALREADY_STARTED" in message:
                status = "PREREQUISITES_UNAVAILABLE"
            elif "COHERENCE" in message:
                status = "COHERENCE_FAILED"
            elif "IDENTITY" in message or "MISMATCH" in message or "DUPLICATE" in message:
                status = "IDENTITY_VALIDATION_FAILED"
            elif "FEATURE" in message or "HISTORY" in message:
                status = "FEATURE_EXPORT_FAILED"
            else:
                status = "SCORING_FAILED"
            recorder.finish_lane("points_hgb_shadow", status=status, reason=message)
    else:
        recorder.finish_lane(
            "points_hgb_shadow", status="PREREQUISITES_UNAVAILABLE",
            reason="PHOENIX_SAME_RUN_CONTROL_UNAVAILABLE",
        )
    _ACTIVE_DAILY_LANE = "research_integrity"

    recorder.start_lane("research_integrity")
    _ACTIVE_DAILY_LANE = "research_integrity"
    research_warnings: list[str] = []
    for attachment_lane, context in attachment_contexts.items():
        try:
            independent = audit_attachment_files(**{
                key: value for key, value in context.items() if key != "expected_counts"
            })
            if independent["counts"] != context["expected_counts"]:
                raise AttachmentIntegrityError("ATTACHMENT_INDEPENDENT_COUNT_MISMATCH")
        except AttachmentIntegrityError as error:
            recorder.finish_lane(
                attachment_lane, status="FAILED_NONBLOCKING_INTEGRITY",
                reason=redact_sensitive_text(
                    f"RESEARCH_INTEGRITY:{type(error).__name__}:{error}"))
            research_warnings.append(
                redact_sensitive_text(
                    f"{attachment_lane.upper()}_INTEGRITY:{type(error).__name__}:{error}"))
    if recorder.lane("legacy_sog").status == "COMPLETE":
        for label, callback in (
            ("SOG_RESIDUAL_REFRESH", lambda: refresh_sog_residual_dataset(slate=slate)),
            ("SOG_RECONCILIATION_REFRESH", lambda: refresh_sog_reconcile_artifacts(to_date=slate)),
        ):
            try:
                callback()
            except Exception as error:
                research_warnings.append(redact_sensitive_text(
                    f"{label}:{type(error).__name__}:{error}"))
        try:
            run([
                PY, SCRIPTS_DIR / "sog_integrity_report.py", "--slate-date", slate,
                "--feature-key", "shots_on_goal_denali", "--db-toi-check",
                "--db-toi-source", "nhl.skater_game_logs_raw", "--db-toi-days-back", "30",
            ])
        except Exception as error:
            research_warnings.append(redact_sensitive_text(
                f"SOG_INTEGRITY:{type(error).__name__}:{error}"))
    else:
        research_warnings.append("LEGACY_SOG_RESEARCH_SKIPPED_BLOCKED_LANE")

    completed_lanes = {
        prediction_lane
        for attachment_lane, prediction_lane in (
            ("sog_attachment", "legacy_sog"),
            ("points_attachment", "points"),
            ("saves_attachment", "saves"),
        )
        if recorder.lane(attachment_lane).status == "COMPLETE"
    }
    archive_site_artifacts(slate, odds_result=odds_result, completed_lanes=completed_lanes)
    sanity = f"""
    WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '{slate}')
    SELECT 'games_today' AS which, COUNT(*) FROM nhl.games WHERE game_date = DATE '{slate}'
    UNION ALL SELECT 'roster_rows_today', COUNT(*) FROM nhl.roster_status r WHERE r.game_id IN (SELECT game_id FROM g)
    UNION ALL SELECT 'preds_sog', COUNT(*) FROM nhl.predictions p WHERE p.game_id IN (SELECT game_id FROM g) AND p.prop = 'shots_on_goal'
    UNION ALL SELECT 'preds_saves', COUNT(*) FROM nhl.predictions p WHERE p.game_id IN (SELECT game_id FROM g) AND p.prop = 'goalie_saves'
    UNION ALL SELECT 'preds_points', COUNT(*) FROM nhl.predictions p WHERE p.game_id IN (SELECT game_id FROM g) AND p.prop = 'player_points';
    """
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", sanity])
    recorder.finish_lane(
        "research_integrity",
        status="COMPLETE_WITH_BOUNDED_LIMITS" if research_warnings else "COMPLETE",
        reason=";".join(research_warnings) if research_warnings else None,
    )
    _ACTIVE_DAILY_LANE = "shared_prerequisites"


def _refresh_all_team_rosters_for_daily(
    *, slate: str, reuse_roster_observation: Path | None,
) -> bool:
    """Run the broad roster fetch unless explicit immutable reuse forbids it."""
    if reuse_roster_observation is not None:
        print("ℹ️ explicit roster observation reuse: full-team roster fetch skipped")
        return False
    try:
        run([PY, SCRIPTS_DIR / "refresh_all_team_rosters.py"], env={"SLATE_DATE": slate})
    except Exception as error:
        print("⚠️ full-team roster refresh failed/skipped (continuing): "
              + redact_sensitive_text(error))
    return True


def _roster_refresh_environment(
    *, slate_date: str, reuse_roster_observation: Path | None,
) -> dict[str, str]:
    environment = {"SLATE_DATE": slate_date}
    if reuse_roster_observation is not None:
        environment["NHL_FETCH_DISABLE"] = "1"
    return environment


# ---------- daily pipeline ----------

# --- REPLACE the very top of cmd_daily(with_odds: bool) down through the two print() lines ---
def _cmd_daily_impl(*, with_odds: bool, morning_only: bool, odds_phase: str,
                    daily_run_id: str, recorder: DailyRunRecorder,
                    reuse_roster_observation: Path | None = None,
                    reuse_odds_observation: Path | None = None):
    global _ACTIVE_DAILY_LANE
    db = require_db_url()
    odds_phase = str(odds_phase).upper()
    if odds_phase not in PHASES:
        raise ValueError(f"unsupported daily phase: {odds_phase}")
    print(f"NHL_DAILY_RUN_ID={daily_run_id}")
    print("NHL_DAILY_EXECUTION_GRAPH=" + ">".join(DAILY_EXECUTION_GRAPH))

    # DAILY SHOULD MEAN "TODAY" BY DEFAULT.
    #
    # We only honor SLATE_DATE / YDAY from env if you explicitly opt in by setting:
    #   HONOR_ENV_DATES=1
    #
    # This prevents stale SLATE_DATE from old debugging sessions from silently
    # forcing yesterday (or any prior date) into today's processed outputs.
    honor_env = os.environ.get("HONOR_ENV_DATES") == "1"

    if honor_env:
        # Explicit opt-in behavior (historical/replay runs)
        slate = os.environ.get("SLATE_DATE") or et_today()
        yday = os.environ.get("YDAY")
        if not yday:
            yday = slate if os.environ.get("SLATE_DATE") else et_yesterday()
    else:
        # Default behavior for real daily runs: ignore any stale env values
        os.environ.pop("SLATE_DATE", None)
        os.environ.pop("YDAY", None)
        slate = et_today()
        yday = et_yesterday()

    os.environ["SLATE_DATE"] = slate
    os.environ["YDAY"] = yday

    season = infer_nhl_season_from_date_yyyy_mm_dd(yday)
    recorder.slate_date = slate
    recorder.canonical_season = infer_nhl_season_from_date_yyyy_mm_dd(slate)
    recorder.start_lane("shared_prerequisites")

    print(f"SLATE_DATE (ET): {slate}" + (" (honor env)" if honor_env else ""))
    print(f"PRIOR_DATE (ET): {yday}" + (" (honor env)" if honor_env else ""))

    # --- Daily artifact dirs (your weekly cleanup automation) ---
    DAILY_EXPORTS_DIR = ROOT / "backend" / "nhl" / "exports" / "daily"
    DAILY_NAMES_DIR = DAILY_EXPORTS_DIR / "names"
    DAILY_SOG_FEATURES_DIR = DAILY_EXPORTS_DIR / "sog_features"
    for d in (DAILY_NAMES_DIR, DAILY_SOG_FEATURES_DIR):
        d.mkdir(parents=True, exist_ok=True)

    # 0) DB sanity
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", "SELECT now();"])

    # Complete yesterday's official learning handoff before history updates.
    # Official NHL requests are governed; this path never invokes Odds API.
    prior_date_et = prior_et_slate()
    recorder.prior_learning = ensure_prior_learning(prior_date_et)
    print("NHL_PRIOR_DAY_LEARNING=" + json.dumps(recorder.prior_learning, sort_keys=True))
    if recorder.prior_learning.get("reconciliation_status") in {
        "CREATED", "REUSED_VALID_PACKAGE", "NOT_FINAL", "NO_PACKAGE", "NO_PRIOR_GAMES",
    }:
        pass
    else:
        raise RuntimeError("PRIOR_DAY_LEARNING_STATUS_INVALID")

    # 0b) Full-league roster/player refresh (all teams, not slate-limited).
    # Explicit roster reuse prohibits a second roster-provider acquisition.
    _refresh_all_team_rosters_for_daily(
        slate=slate, reuse_roster_observation=reuse_roster_observation)

    # ============================================================
    # PHASE A: Finalize YDAY into raw/history FIRST (the guardrail)
    # ============================================================

    # A1) Pull yesterday logs + shiftcharts into stage (safe even if yday had no games)
    run([PY, SCRIPTS_DIR / "seed_goalie_logs_for_date.py"],        env={"SLATE_DATE": yday})
    run(
        [PY, SCRIPTS_DIR / "refresh_players_and_roster_today.py"],
        env=_roster_refresh_environment(
            slate_date=yday, reuse_roster_observation=reuse_roster_observation),
    )
    run([PY, SCRIPTS_DIR / "seed_skater_logs_for_date.py"],        env={"SLATE_DATE": yday})
    run([PY, SCRIPTS_DIR / "ingest_shiftcharts_for_date.py"],      env={"SLATE_DATE": yday})

    # A2) Promote stage → raw for yday (idempotent upsert)
    stage_has_blocks = _table_has_column(db, "nhl", "import_skater_logs_stage", "blocks")
    raw_has_blocks = _table_has_column(db, "nhl", "skater_game_logs_raw", "blocks")
    src_blocks = ", s.blocks" if stage_has_blocks and raw_has_blocks else ""
    joined_blocks = ", src.blocks AS blocks" if stage_has_blocks and raw_has_blocks else ""
    insert_blocks = ", blocks" if stage_has_blocks and raw_has_blocks else ""
    select_blocks = ", blocks" if stage_has_blocks and raw_has_blocks else ""
    update_blocks = ",\n      blocks         = COALESCE(EXCLUDED.blocks, nhl.skater_game_logs_raw.blocks)" if stage_has_blocks and raw_has_blocks else ""

    promote_sql = f"""
    WITH src AS (
      SELECT DISTINCT
        s.player_id, s.game_id, s.game_date,
        s.shots_on_goal, s.shot_attempts, s.toi_minutes, s.pp_toi_minutes{src_blocks}
      FROM nhl.import_skater_logs_stage s
      WHERE s.game_date = DATE '{yday}'
    ),
    rs AS (
      SELECT DISTINCT ON (game_id, player_id)
        game_id,
        team_id,
        player_id
      FROM nhl.roster_status
      WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date = DATE '{yday}')
      ORDER BY game_id, player_id, asof_ts DESC
    ),
    g AS (
      SELECT game_id, home_team_id, away_team_id
      FROM nhl.games
      WHERE game_date = DATE '{yday}'
    ),
    joined AS (
      SELECT
        src.player_id,
        src.game_id,
        rs.team_id,
        CASE
          WHEN rs.team_id = g.home_team_id THEN g.away_team_id
          WHEN rs.team_id = g.away_team_id THEN g.home_team_id
          ELSE NULL
        END AS opponent_id,
        (rs.team_id = g.home_team_id) AS is_home,
        src.game_date,
        src.shots_on_goal,
        src.shot_attempts,
        src.toi_minutes,
        src.pp_toi_minutes
        {joined_blocks}
      FROM src
      JOIN rs ON rs.game_id = src.game_id AND rs.player_id = src.player_id
      JOIN g  ON g.game_id  = src.game_id
    )
    INSERT INTO nhl.skater_game_logs_raw
      (player_id, game_id, team_id, opponent_id, is_home, game_date,
       shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes{insert_blocks})
    SELECT
      player_id, game_id, team_id, opponent_id, is_home, game_date,
      shots_on_goal, shot_attempts, toi_minutes, NULLIF(pp_toi_minutes, 0) AS pp_toi_minutes{select_blocks}
    FROM joined
    WHERE opponent_id IS NOT NULL
    ON CONFLICT (player_id, game_id) DO UPDATE SET
      team_id        = EXCLUDED.team_id,
      opponent_id    = EXCLUDED.opponent_id,
      is_home        = EXCLUDED.is_home,
      game_date      = EXCLUDED.game_date,
      shots_on_goal  = EXCLUDED.shots_on_goal,
      shot_attempts  = COALESCE(EXCLUDED.shot_attempts, nhl.skater_game_logs_raw.shot_attempts),
      toi_minutes    = EXCLUDED.toi_minutes,
      pp_toi_minutes = COALESCE(NULLIF(EXCLUDED.pp_toi_minutes, 0), nhl.skater_game_logs_raw.pp_toi_minutes){update_blocks};
    """
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", promote_sql])

    # A3a) Build manpower segments (PP/PK windows) for this slate date (required for PP TOI)
    run([PY, SCRIPTS_DIR / "backfill_game_manpower_segments.py", "--start-date", yday, "--end-date", yday, "--season", str(season)])

    # A3b) Fill PP TOI minutes (depends on manpower segments)
    run([PY, SCRIPTS_DIR / "fill_pp_toi_minutes_for_date.py", "--date", yday, "--commit"])

    # A4) Pairings artifacts for yday (built ONCE)
    run_psql_file(SQL_DIR / "shiftcharts_pairings_for_date.sql", vars={"game_date": yday})

    # A5) Refresh views/materializations now that yday raw/pp_toi/pairings are updated
    refresh_sql = SCRIPTS_DIR / "refresh.sql"
    if refresh_sql.exists():
        run(["psql", db, "-v", "ON_ERROR_STOP=1", "-f", refresh_sql])

    # A6) Evaluate SOG for YDAY (only if actuals exist)
    try:
        run(
            [
                PY,
                SCRIPTS_DIR / "evaluate_sog_predictions.py",
                "--game-date", yday,
            ]
        )
    except Exception as e:
        print(f"⚠️ evaluate_sog_predictions failed/skipped (continuing): {e}")

    # ============================================================
    # PHASE B: Build SLATE pregame features (history through yday)
    # ============================================================

    # 1) Today: schedule & roster
    run([PY, SCRIPTS_DIR / "import_schedule_today.py"], env={"SLATE_DATE": slate})

    # A date-bound completion record distinguishes a valid empty slate from a
    # failed or never-run fetch.  Never proceed from database rows alone.
    slate_health_path = ROOT / "artifacts" / "operational" / "nhl" / "slates" / slate / "slate_health.json"
    if not slate_health_path.exists():
        raise RuntimeError(f"NHL slate health missing for {slate}: {slate_health_path}")
    slate_health = json.loads(slate_health_path.read_text())
    if slate_health.get("slate_date") != slate or not slate_health.get("downstream_ready"):
        raise RuntimeError(f"NHL slate not downstream-ready for {slate}: {slate_health}")
    if slate_health.get("completion_status") == "VALID_EMPTY_SLATE":
        recorder.set_canonical(
            slate_date=slate,
            season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
            game_ids=[],
            game_set_hash=canonical_game_set_hash([]),
        )
        print(f"ℹ️ Valid empty NHL slate for {slate} — skipping scoring/export steps.")
        return
    if slate_health.get("completion_status") != "READY":
        raise RuntimeError(f"Unexpected NHL slate completion status for {slate}: {slate_health}")

    canonical_games = load_canonical_slate(
        slate_date=slate,
        raw_schedule_path=slate_health_path.parent / "raw_schedule_response.json",
        slate_health_path=slate_health_path,
    )
    game_hash = canonical_game_set_hash(game.game_id for game in canonical_games)
    recorder.set_canonical(
        slate_date=slate,
        season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
        game_ids=[game.game_id for game in canonical_games],
        game_set_hash=game_hash,
    )

    # --- EARLY EXIT: no NHL games on this slate date ---
    no_games_sql = f"SELECT COUNT(*) FROM nhl.games WHERE game_date = DATE '{slate}';"
    res = sp.run(
        ["psql", db, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", no_games_sql],
        capture_output=True, text=True, check=True
    )
    game_count = int((res.stdout or "").strip() or "0")
    if game_count == 0:
        print(f"ℹ️ No NHL games for {slate} (ET) — skipping scoring/export steps (prior-date finalization already done).")
        return
    # --- end early exit ---

    recorder.finish_lane("shared_prerequisites", database_rows_written=True)
    recorder.start_lane("roster", inputs=[{
        "canonical_game_set_hash": game_hash,
        "reuse_requested": reuse_roster_observation is not None,
    }])
    _ACTIVE_DAILY_LANE = "roster"
    roster_env = {
        "SLATE_DATE": slate,
        "NHL_DAILY_PHASE": odds_phase,
        "NHL_PARENT_DAILY_RUN_ID": daily_run_id,
        "NHL_ROSTER_OBSERVATION_ROOT": str(ROSTER_OBSERVATION_ROOT),
    }
    if reuse_roster_observation is not None:
        reuse_reference = verify_roster_observation_reuse(
            reuse_roster_observation,
            season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
            slate_date=slate,
            phase=odds_phase,
            canonical_game_ids=[game.game_id for game in canonical_games],
            canonical_game_set_hash=game_hash,
        )
        recorder.roster_observation = reuse_reference
        roster_env["NHL_REUSE_ROSTER_OBSERVATION"] = str(reuse_roster_observation.resolve())
        roster_env["NHL_FETCH_DISABLE"] = "1"
    else:
        run(
            [PY, SCRIPTS_DIR / "import_roster_today.py"],
            env={"SLATE_DATE": slate, "SKIP_ROSTER_STATUS": "1", "SKIP_PLAYERS": "1"},
        )
    roster_result = run(
        [PY, SCRIPTS_DIR / "refresh_players_and_roster_today.py"], env=roster_env)
    structured = next((
        child for child in reversed(recorder.children)
        if child.get("schema_version") in {
            "NHL_ROSTER_CHILD_SUMMARY_V1", "NHL_ROSTER_CHILD_SUMMARY_V2",
        }
    ), None)
    if structured:
        if recorder.roster_observation is None and structured.get("roster_observation"):
            recorder.roster_observation = {
                "path": structured["roster_observation"],
                "manifest_sha256": structured.get("roster_manifest_sha256"),
                "reuse_mode": "NEW_IMMUTABLE_OBSERVATION",
            }
        recorder.finish_lane(
            "roster", status="REUSED" if reuse_roster_observation else "COMPLETE",
            outputs=[recorder.roster_observation] if recorder.roster_observation else [],
            database_stage_summary=structured.get("database_stage_summary") or {},
        )
    else:
        raise RuntimeError("ROSTER_CHILD_SUMMARY_MISSING")
    _ACTIVE_DAILY_LANE = "shared_prerequisites"

    # 2) Seed features for today (SOG + Saves).
    run_psql_file(SQL_DIR / "seed_sog_features_for_slate.sql",    vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_counts_for_slate.sql",      vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "seed_goalie_features_for_slate.sql", vars={"slate_date": slate})

    # 2b) Refresh rolling SOG features into the pregame table
    # Guardrail: rollups may only use realized stats through YDAY.
    rollup_start = os.environ.get("ROLLUP_START_DATE") or (
        datetime.fromisoformat(slate) - timedelta(days=260)
    ).strftime("%Y-%m-%d")
    refresh_sog_denali_rollups_window(db, start_date=rollup_start, end_date=slate)

    # 2c) Fill PP role + TOI-derived features for THIS slate (computed from history < slate)
    run_psql_file(SQL_DIR / "fill_sog_pp_role_for_slate.sql",              vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_toi_features_for_slate.sql",         vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_season_toi_features_for_slate.sql",  vars={"slate_date": slate})

    # 2d) Pairings fill into TODAY's pregame table using the yday-built pairings history
    run_psql_file(SQL_DIR / "fill_sog_pairings_for_slate.sql",             vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_pairings_rolling_for_slate.sql",     vars={"slate_date": slate})

    row = psql_one_row(
        db,
        f"""
        SELECT
        COUNT(*)::int AS n,
        COUNT(*) FILTER (WHERE szn_toi_per_game_5on5 IS NULL)::int AS null_5v5,
        COUNT(*) FILTER (WHERE season_5on5_icetime_per_game IS NULL)::int AS null_season_5v5
        FROM nhl.training_features_nhl_sog_enriched_pregame_v2
        WHERE game_date = DATE '{slate}'
        """
    )

    n = int(row["n"])
    null_5v5 = int(row["null_5v5"])
    null_season_5v5 = int(row["null_season_5v5"])
    sog_gate = legacy_sog_toi_population_diagnostic(
        population_rows=n, null_5v5=null_5v5,
        null_season_5v5=null_season_5v5)
    null_ratio = sog_gate["null_ratio"]

    # Keep the former 20% season-TOI population gate as a diagnostic only.
    # The Poisson scorer has per-row rate/exposure fallbacks and writes an
    # unscored ledger for rows that lack every valid source. A slate-wide
    # threshold must not discard other scoreable players.
    sog_scorer = (os.environ.get("NHL_SOG_SCORER") or "poisson_baseline").strip().lower()
    recorder.start_lane("legacy_sog", inputs=[{
        "slate_date": slate,
        "population_rows": n,
        "null_szn_toi_per_game_5on5": null_5v5,
        "null_season_5on5_icetime_per_game": null_season_5v5,
        "null_ratio": null_ratio,
        "maximum_null_ratio": 0.20,
        "legacy_population_gate_would_block": bool(
            sog_gate["legacy_population_gate_would_block"]),
    }])

    # After seed_sog_features_for_slate + pairings fills

    # 3) Export names (single source of truth)
    try:
        names_path = export_names_csv(slate)
    except Exception as e:
        names_path = None
        print(f"⚠️ names export failed; downstream builders may degrade: {e}")
    # 3.9) Team context rollups for slate
    run_psql_file(SQL_DIR / "upsert_team_context_for_slate.sql", vars={"slate_date": slate})

    # 4) Export feature CSVs for this slate

    # 4a) SOG Denali features → backend/nhl/exports/daily/sog_features/
    sog_feat_path = DAILY_SOG_FEATURES_DIR / f"sog_features_{slate}_denali.csv"
    export_sog_denali_features(
        db, slate, sog_feat_path,
        require_pairings_coverage=(sog_scorer == "ordinal_lgbm"),
    )
    sog_feature_capture = None
    sog_feature_cutoff_utc = datetime.now(timezone.utc)
    sog_cutoff_text = sog_feature_cutoff_utc.isoformat().replace("+00:00", "Z")
    sog_eligibility = pregame_game_eligibility(canonical_games, sog_cutoff_text)
    recorder.set_pregame_eligibility(
        eligible_game_ids=sog_eligibility["eligible_pregame_game_ids"],
        started_excluded_game_ids=sog_eligibility["started_excluded_game_ids"],
        cutoff_utc=sog_cutoff_text,
    )
    _ACTIVE_DAILY_LANE = "legacy_sog"
    if not sog_eligibility["started_excluded_game_ids"]:
        # Preserve the established all-pregame scorer input byte-for-byte.
        sog_scoring_path = sog_feat_path
    else:
        sog_scoring_path = PROC_DIR / "daily_runs" / daily_run_id / "sog_features_eligible.csv"
        sog_scoring_path.parent.mkdir(parents=True, exist_ok=True)
        sog_features = pd.read_csv(sog_feat_path)
        if "game_id" not in sog_features:
            raise RuntimeError("SOG_FEATURE_GAME_ID_MISSING")
        sog_features["game_id"] = pd.to_numeric(sog_features["game_id"], errors="raise").astype(int)
        sog_features = sog_features[sog_features.game_id.isin(sog_eligibility["eligible_pregame_game_ids"])]
        if not sog_features.empty:
            sog_features.to_csv(sog_scoring_path, index=False)
    if sog_scorer == "poisson_baseline":
        if not sog_eligibility["eligible_pregame_game_ids"]:
            sog_feature_capture = None
        else:
            sog_feature_capture = begin_sog_feature_capture(
                source_path=sog_scoring_path,
                root=ROOT / "artifacts" / "operational" / "nhl" / "sog_feature_inputs",
                season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
                slate_date=slate,
                run_id=daily_run_id,
                canonical_game_ids=[int(game.game_id) for game in canonical_games],
                canonical_game_starts_utc={int(game.game_id): str(game.start_time_utc)
                                           for game in canonical_games},
                cutoff_utc=sog_feature_cutoff_utc,
                eligible_game_ids=sog_eligibility["eligible_pregame_game_ids"],
            )

    # 4b) Saves / Points exporters are independent lane inputs.
    saves_export_ready = points_export_ready = False
    try:
        saves_csv = psql_stdout(SQL_DIR / "export_saves_from_denali.sql", vars={"slate_date": slate})
        (EXPORTS_DIR / "train_goalie_saves_v2.csv").write_bytes(saves_csv)
        saves_export_ready = True
    except Exception as error:
        recorder.fail_lane("saves", error, blocking=False)
    try:
        points_csv = psql_stdout(SQL_DIR / "export_points.sql", vars={"slate_date": slate})
        (EXPORTS_DIR / "train_nhl_points_v2.csv").write_bytes(points_csv)
        points_export_ready = True
    except Exception as error:
        recorder.fail_lane("points", error, blocking=False)
    print("exports → sog_features_{slate}_denali.csv, train_goalie_saves_v2.csv, train_nhl_points_v2.csv")

    if morning_only:
        recorder.finish_lane(
            "legacy_sog", status="PREREQUISITES_READY",
            reason="MORNING_ONLY_SCORING_NOT_RUN")
        for lane_name in ("points", "saves"):
            if recorder.lane(lane_name).status != "FAILED_NONBLOCKING":
                recorder.finish_lane(
                    lane_name, status="PREREQUISITES_READY",
                    reason="MORNING_ONLY_SCORING_NOT_RUN")
        recorder.finish_lane(
            "cold_start_sog_reference", status="NOT_INVOKED_EXTERNAL_OWNER",
            reason="PREDICTION_ONLY_OBSERVER_OWNS_COLD_START_SOG")
        recorder.finish_lane(
            "points_hgb_shadow",
            status="PREREQUISITES_READY" if points_export_ready else "PREREQUISITES_UNAVAILABLE",
            reason="MORNING_ONLY_SCORING_NOT_RUN" if points_export_ready else "POINTS_FEATURE_EXPORT_UNAVAILABLE",
            outputs=([artifact_identity(EXPORTS_DIR / "train_nhl_points_v2.csv")]
                     if points_export_ready else []),
        )
        recorder.finish_lane("odds", status="SKIPPED_MORNING_ONLY")
        for lane_name in ("sog_attachment", "points_attachment", "saves_attachment"):
            recorder.finish_lane(lane_name, status="SKIPPED_MORNING_ONLY")
        recorder.finish_lane("research_integrity", status="SKIPPED_MORNING_ONLY")
        print("✅ Morning-only boundary reached: stable upstream state prepared; scoring, markets, candidates, and uploads skipped.")
        return

    prediction_run_dir = PROC_DIR / "daily_runs" / daily_run_id
    prediction_run_dir.mkdir(parents=True, exist_ok=True)
    calibrated_pred_path = prediction_run_dir / "sog_predictions_wide_calibrated.csv"
    sog_calibration_artifact_path = prediction_run_dir / "sog_segmented_calibration_fit.json"
    sog_segmented_calibration_applied = False
    unscored_pred_path = prediction_run_dir / "sog_predictions_unscored.csv"
    recorder.independent_context = {
        "db": db,
        "slate": slate,
        "with_odds": with_odds,
        "odds_phase": odds_phase,
        "daily_run_id": daily_run_id,
        "canonical_games": canonical_games,
        "saves_export_ready": saves_export_ready,
        "points_export_ready": points_export_ready,
        "legacy_sog_prediction": None,
        "reuse_odds_observation": reuse_odds_observation,
    }
    _ACTIVE_DAILY_LANE = "legacy_sog"
    if not sog_eligibility["eligible_pregame_game_ids"]:
        recorder.finish_lane("legacy_sog", status="SKIPPED_NO_PREGAME_GAMES",
                             reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
        recorder.finish_lane("sog_fixed_blend_shadows", status="SKIPPED_NO_PREGAME_GAMES",
                             reason="ALL_CANONICAL_GAMES_ALREADY_STARTED")
        recorder.independent_context["legacy_sog_prediction"] = None
        _run_independent_daily_lanes(recorder=recorder, **recorder.independent_context)
        return
    if sog_scorer == "poisson_baseline":
        sog_score_command = [
            PY,
            SCRIPTS_DIR / "score_sog_poisson_baseline.py",
            "--in", str(sog_scoring_path),
            "--out", str(calibrated_pred_path),
            "--unscored-out", str(unscored_pred_path),
        ]
        if names_path is not None and names_path.is_file():
            sog_score_command.extend(["--names", str(names_path)])
        run(sog_score_command)
        sog_model_family = "poisson_baseline"
        sog_model_version = "baseline_v1"
        sog_feature_hash = "poisson_baseline_v1"
    elif sog_scorer == "ordinal_lgbm":
        ordinal_root = (
            ROOT
            / "backend" / "nhl" / "models" / "latest" / "shots_on_goal"
            / "sog_player_denali_pairings_ordinal_v1__no_shiftcounts"
        )
        ordinal_meta = ordinal_root / "ge_2" / "metadata.json"

        if not ordinal_root.exists():
            raise SystemExit(f"Missing ORDINAL SOG models at {ordinal_root}")
        if not ordinal_meta.exists():
            raise SystemExit(f"Missing ORDINAL feature metadata at {ordinal_meta}")

        run(
            [
                PY,
                SCRIPTS_DIR / "score_sog_denali_pairings_ordinal_lgbm.py",
                "--in",
                str(sog_scoring_path),
                "--out",
                str(calibrated_pred_path),
                "--model-root",
                str(ordinal_root),
                "--feature-meta",
                str(ordinal_meta),
            ]
        )
        sog_model_family = "denali_blend"
        sog_model_version = "phoenix_v2"
        sog_feature_hash = "phoenix_v2"
    else:
        raise SystemExit(f"Unsupported NHL_SOG_SCORER={sog_scorer!r}")

    shadow_default = sog_scorer == "poisson_baseline"
    shadow_enabled = _env_bool("NHL_SOG_DEFENSE_SHADOW_ENABLED", default=shadow_default)
    shadow_required = _env_bool("NHL_SOG_DEFENSE_SHADOW_REQUIRED", default=False)
    if shadow_enabled and sog_scorer == "poisson_baseline":
        shadow_pred_path = prediction_run_dir / "sog_predictions_wide_defense_surprise_shadow.csv"
        shadow_cmd = [
            PY,
            SCRIPTS_DIR / "score_sog_poisson_defense_surprise_shadow.py",
            "--in",
            str(sog_scoring_path),
            "--out",
            str(shadow_pred_path),
            "--slate-date",
            slate,
        ]
        _append_cli_arg(
            shadow_cmd,
            "--alphas",
            os.environ.get("NHL_SOG_DEFENSE_SHADOW_ALPHAS"),
        )
        _append_cli_arg(
            shadow_cmd,
            "--bandwidth",
            os.environ.get("NHL_SOG_DEFENSE_SHADOW_BANDWIDTH"),
        )
        _append_cli_arg(
            shadow_cmd,
            "--goalie-weight",
            os.environ.get("NHL_SOG_DEFENSE_SHADOW_GOALIE_WEIGHT"),
        )
        _append_cli_arg(
            shadow_cmd,
            "--clip-low",
            os.environ.get("NHL_SOG_DEFENSE_SHADOW_CLIP_LOW"),
        )
        _append_cli_arg(
            shadow_cmd,
            "--clip-high",
            os.environ.get("NHL_SOG_DEFENSE_SHADOW_CLIP_HIGH"),
        )
        try:
            run(shadow_cmd)
            print(f"✅ defense surprise shadow written: {shadow_pred_path}")
        except Exception as exc:
            if shadow_required:
                raise RuntimeError(
                    "Defense surprise shadow failed with NHL_SOG_DEFENSE_SHADOW_REQUIRED=1."
                ) from exc
            print(f"⚠️ defense surprise shadow failed; continuing without sidecar: {exc}")

    # ---- GUARD: ordinal predictions must exist + match slate ----
    calib_path = calibrated_pred_path
    if not calib_path.exists() or calib_path.stat().st_size < 200:
        raise AssertionError(f"[daily] missing/empty ordinal predictions: {calib_path}")

    dates = _pred_game_dates(calib_path)
    if not dates:
        raise AssertionError(f"[daily] ordinal CSV has no game_date column or no values: {calib_path}")

    if len(dates) != 1 or dates[0] != slate:
        raise AssertionError(
            f"[daily] ordinal predictions slate mismatch: expected {slate}, got {dates}. "
            f"Refusing to continue."
        )

    # 5a.1) Optional segmented recency calibration (feature-flagged).
    # Default ON for daily runs; set NHL_SOG_SEGMENTED_CALIBRATION_ENABLED=0 to disable quickly.
    seg_cal_default = sog_scorer == "ordinal_lgbm"
    seg_cal_enabled = _env_bool("NHL_SOG_SEGMENTED_CALIBRATION_ENABLED", default=seg_cal_default)
    seg_cal_required = _env_bool("NHL_SOG_SEGMENTED_CALIBRATION_REQUIRED", default=False)
    if seg_cal_enabled:
        seg_cal_cmd = [
            PY,
            SCRIPTS_DIR / "calibrate_sog_segmented_recency.py",
            "--pred-csv", str(calibrated_pred_path),
            "--out-csv", str(calibrated_pred_path),
            "--fitted-artifact-out", str(sog_calibration_artifact_path),
        ]
        _append_cli_arg(
            seg_cal_cmd,
            "--model-family",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_MODEL_FAMILY")
            or os.environ.get("NHL_SOG_MODEL_FAMILY")
            or sog_model_family,
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--model-version",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_MODEL_VERSION")
            or os.environ.get("NHL_SOG_MODEL_VERSION")
            or sog_model_version,
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--lines",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_LINES")
            or os.environ.get("NHL_SOG_LINES"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--lookback-days",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_LOOKBACK_DAYS")
            or os.environ.get("NHL_SOG_CAL_LOOKBACK_DAYS"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--segment-min-rows",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_SEGMENT_MIN_ROWS")
            or os.environ.get("NHL_SOG_CAL_SEGMENT_MIN_ROWS"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--blend-alpha",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_BLEND_ALPHA")
            or os.environ.get("NHL_SOG_CAL_BLEND_ALPHA"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--decay-half-life-days",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_DECAY_HALF_LIFE_DAYS")
            or os.environ.get("NHL_SOG_CAL_DECAY_HALF_LIFE_DAYS"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--asof-date",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_ASOF_DATE"),
        )
        if _env_bool("NHL_SOG_SEGMENTED_CALIBRATION_STRICT", default=False):
            seg_cal_cmd.append("--strict")

        try:
            run(seg_cal_cmd)
            sog_segmented_calibration_applied = sog_calibration_artifact_path.is_file()
            print("✅ segmented SOG calibration applied.")
        except Exception as exc:
            if seg_cal_required:
                raise RuntimeError(
                    "Segmented SOG calibration failed with NHL_SOG_SEGMENTED_CALIBRATION_REQUIRED=1."
                ) from exc
            print(f"⚠️ segmented SOG calibration failed; continuing with ordinal output: {exc}")

    sog_scored_metadata, sog_unscored_metadata = validate_sog_prediction_artifacts(
        scored_path=calibrated_pred_path,
        unscored_path=unscored_pred_path if unscored_pred_path.is_file() else None,
        slate=slate,
        parent_daily_run_id=daily_run_id,
        canonical_game_ids=[game.game_id for game in canonical_games],
        expected_game_set_hash=recorder.canonical_game_set_hash,
        eligible_game_ids=sog_eligibility["eligible_pregame_game_ids"],
    )
    legacy_sog_identity = _require_current_prediction_artifact(
        calibrated_pred_path, slate=slate, expected_sha256=None)
    legacy_sog_identity.update(sog_scored_metadata)
    sog_components = [("scorer", SCRIPTS_DIR / ("score_sog_poisson_baseline.py" if sog_scorer == "poisson_baseline" else "score_sog_denali_pairings_ordinal_lgbm.py"))]
    if sog_scorer == "ordinal_lgbm":
        sog_components.extend((f"ordinal_model:{path.relative_to(ordinal_root).as_posix()}", path)
                              for path in sorted(ordinal_root.rglob("*")) if path.is_file())
        sog_components.append(("ordinal_feature_metadata", ordinal_meta))
    if sog_segmented_calibration_applied:
        sog_components.extend([("segmented_calibration_code", SCRIPTS_DIR / "calibrate_sog_segmented_recency.py"),
                               ("segmented_calibration_fitted_state", sog_calibration_artifact_path)])
    sog_evidence = fitted_model_identity(
        model_family=sog_model_family, model_version=sog_model_version,
        components=sog_components, prediction_path=calibrated_pred_path,
        scoring_run_id=daily_run_id,
        scoring_configuration={"scorer": sog_scorer,
                               "segmented_calibration_applied": sog_segmented_calibration_applied})
    legacy_sog_identity["fitted_model_evidence"] = sog_evidence
    sog_feature_binding = None
    if sog_feature_capture is not None:
        sog_feature_binding = finalize_sog_feature_capture(
            sog_feature_capture,
            scorer_path=SCRIPTS_DIR / "score_sog_poisson_baseline.py",
            model_family=sog_model_family,
            model_version=sog_model_version,
            fitted_model_identity_sha256=str(sog_evidence["fitted_model_identity_sha256"]),
            prediction_path=calibrated_pred_path,
            python_executable=str(PY), names_path=names_path,
        )
        legacy_sog_identity.update(sog_feature_binding)
    sog_outputs = [legacy_sog_identity]
    if sog_segmented_calibration_applied:
        sog_outputs.append(artifact_identity(sog_calibration_artifact_path))
    if unscored_pred_path.is_file() and sog_unscored_metadata is not None:
        unscored_identity = artifact_identity(unscored_pred_path)
        unscored_identity.update(sog_unscored_metadata)
        unscored_identity["parent_daily_run_id"] = daily_run_id
        unscored_identity["canonical_game_count"] = len(recorder.canonical_game_ids)
        unscored_identity["canonical_game_set_hash"] = recorder.canonical_game_set_hash
        sog_outputs.append(unscored_identity)

    # Research-only fixed blends consume this exact production feature snapshot
    # and remain an independent, nonblocking daily lane.
    recorder.start_lane("sog_fixed_blend_shadows", inputs=[{
        "feature_input_sha256": (sog_feature_capture or {}).get("retained_sha256"),
        "parent_daily_run_id": daily_run_id,
        "production_model_identity": "poisson_baseline/baseline_v1",
    }])
    if sog_scorer == "poisson_baseline" and sog_feature_capture is not None:
        try:
            shadow_packages = capture_sog_fixed_blends(
                feature_path=Path(sog_feature_binding["feature_input_path"]),
                production_path=calibrated_pred_path,
                output_root=ROOT / "artifacts" / "operational" / "nhl" / "sog_fixed_blend_shadows",
                run_id=daily_run_id, slate_date=slate,
                season=infer_nhl_season_from_date_yyyy_mm_dd(slate),
                feature_sha256=str(sog_feature_capture["retained_sha256"]),
                cutoff_utc=str(sog_feature_capture["cutoff_utc"]),
                canonical_game_ids=[int(game.game_id) for game in canonical_games],
                scorer_path=SCRIPTS_DIR / "score_sog_poisson_baseline.py",
            )
            recorder.finish_lane("sog_fixed_blend_shadows", outputs=shadow_packages,
                                 reason="PRIMARY_AND_COMPARATOR_IMMUTABLE_CAPTURE_COMPLETE")
        except Exception as error:
            recorder.fail_lane("sog_fixed_blend_shadows", error, blocking=False)
            print(f"⚠️ fixed-blend SOG research shadows failed; production continues: {error}")
    else:
        recorder.finish_lane("sog_fixed_blend_shadows", status="SKIPPED_UNSUPPORTED_PRODUCTION_SCORER",
                             reason="FIXED_BLEND_SHADOW_REQUIRES_POISSON_BASELINE")

    # 5b) Load SOG into nhl.predictions
    run(
        [
            PY,
            SCRIPTS_DIR / "load_sog_predictions_denali.py",
            "--pred-csv",   str(calibrated_pred_path),
            "--project",    "nhl",
            "--prop-type",  "shots_on_goal",
            "--slate-date", slate,
            "--model-family", sog_model_family,
            "--model-version", sog_model_version,
            "--feature-hash", sog_feature_hash,
        ]
    )
    recorder.finish_lane(
        "legacy_sog", outputs=sog_outputs, database_rows_written=True)
    _run_independent_daily_lanes(
        recorder=recorder, db=db, slate=slate, with_odds=with_odds,
        odds_phase=odds_phase, daily_run_id=daily_run_id,
        canonical_games=canonical_games,
        saves_export_ready=saves_export_ready,
        points_export_ready=points_export_ready,
        legacy_sog_prediction=legacy_sog_identity,
        reuse_odds_observation=reuse_odds_observation,
    )
    return


def cmd_daily(with_odds: bool, morning_only: bool = False,
              odds_phase: str = "EARLY",
              reuse_roster_observation: Path | None = None,
              reuse_odds_observation: Path | None = None):
    """Run the comprehensive daily graph and always finalize a parent receipt."""
    global _ACTIVE_DAILY_RECORDER, _ACTIVE_DAILY_LANE
    odds_phase = str(odds_phase).upper()
    started = datetime.now(timezone.utc)
    daily_run_id = os.environ.get("NHL_DAILY_RUN_ID") or (
        f"nhldaily_{started.strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:8]}"
    )
    command = [str(PY), "-m", "backend.nhl.cli", "daily"]
    if with_odds:
        command.append("--with-odds")
    if morning_only:
        command.append("--morning-only")
    command.extend(["--odds-phase", odds_phase])
    if reuse_roster_observation is not None:
        reuse_roster_observation = Path(reuse_roster_observation).resolve()
        command.extend(["--reuse-roster-observation", str(reuse_roster_observation)])
    if reuse_odds_observation is not None:
        if not with_odds:
            raise ValueError("--reuse-odds-observation requires --with-odds")
        reuse_odds_observation = Path(reuse_odds_observation).resolve()
        command.extend(["--reuse-odds-observation", str(reuse_odds_observation)])

    recorder = DailyRunRecorder(
        run_id=daily_run_id, command=command, phase=odds_phase, started_at=started)
    _ACTIVE_DAILY_RECORDER = recorder
    _ACTIVE_DAILY_LANE = "shared_prerequisites"
    pending_error: BaseException | None = None
    receipt_path: Path | None = None
    try:
        # Reuse is an explicit pre-execution contract. Validate against the
        # retained canonical slate before DB access, roster DML, or any provider.
        if reuse_roster_observation is not None or reuse_odds_observation is not None:
            preflight_slate = et_today()
            slate_root = ROOT / "artifacts" / "operational" / "nhl" / "slates" / preflight_slate
            preflight_games = load_canonical_slate(
                slate_date=preflight_slate,
                raw_schedule_path=slate_root / "raw_schedule_response.json",
                slate_health_path=slate_root / "slate_health.json",
            )
            preflight_hash = canonical_game_set_hash(game.game_id for game in preflight_games)
            recorder.set_canonical(
                slate_date=preflight_slate,
                season=infer_nhl_season_from_date_yyyy_mm_dd(preflight_slate),
                game_ids=[game.game_id for game in preflight_games],
                game_set_hash=preflight_hash,
            )
            if reuse_roster_observation is not None:
                recorder.roster_observation = verify_roster_observation_reuse(
                    reuse_roster_observation,
                    season=infer_nhl_season_from_date_yyyy_mm_dd(preflight_slate),
                    slate_date=preflight_slate,
                    phase=odds_phase,
                    canonical_game_ids=[game.game_id for game in preflight_games],
                    canonical_game_set_hash=preflight_hash,
                )
            if reuse_odds_observation is not None:
                capture_odds_observation(
                    root=ODDS_OBSERVATION_ROOT,
                    season=infer_nhl_season_from_date_yyyy_mm_dd(preflight_slate),
                    slate_date=preflight_slate, phase=odds_phase,
                    parent_daily_run_id=daily_run_id,
                    canonical_games=preflight_games, provider=None, authorized=False,
                    reuse_observation_dir=reuse_odds_observation,
                )
        _cmd_daily_impl(
            with_odds=with_odds, morning_only=morning_only,
            odds_phase=odds_phase, daily_run_id=daily_run_id,
            recorder=recorder, reuse_roster_observation=reuse_roster_observation,
            reuse_odds_observation=reuse_odds_observation)
        if recorder.lane("shared_prerequisites").status == "RUNNING":
            recorder.finish_lane("shared_prerequisites")
        if recorder.lane("shared_prerequisites").status == "COMPLETE":
            for lane in recorder.lanes.values():
                if lane.status == "NOT_STARTED":
                    recorder.finish_lane(
                        lane.name, status="SKIPPED_VALID_EMPTY_SLATE",
                        reason="NO_CANONICAL_GAMES")
    except BaseException as error:
        failed_lane = _ACTIVE_DAILY_LANE
        recorder.failure = {
            "lane": failed_lane,
            "error_type": type(error).__name__,
            "error_message": redact_sensitive_text(error),
        }
        # Once shared inputs, roster state, and the independent feature exports
        # are proven, any legacy-SOG exception is lane-local. Continue from the
        # captured context instead of allowing it to terminate Points/Saves/odds.
        if (
            failed_lane == "legacy_sog"
            and recorder.independent_context is not None
            and recorder.lane("shared_prerequisites").status == "COMPLETE"
            and recorder.lane("roster").status in {"COMPLETE", "REUSED"}
        ):
            recorder.fail_lane("legacy_sog", error, blocking=False)
            try:
                _run_independent_daily_lanes(
                    recorder=recorder, **recorder.independent_context)
            except BaseException as continuation_error:
                pending_error = continuation_error
                recorder.fail_lane(
                    "shared_prerequisites",
                    RuntimeError(
                        "INDEPENDENT_CONTINUATION_INTEGRITY_FAILED:"
                        f"{type(continuation_error).__name__}:{continuation_error}"
                    ),
                    blocking=True,
                )
                recorder.failure = {
                    "lane": _ACTIVE_DAILY_LANE,
                    "error_type": type(continuation_error).__name__,
                    "error_message": redact_sensitive_text(continuation_error),
                }
        else:
            pending_error = (
                safe_called_process_error(error)
                if isinstance(error, sp.CalledProcessError)
                else RuntimeError(redact_sensitive_text(error))
            )
            active = recorder.lane(failed_lane)
            if active.blocking:
                recorder.fail_lane(active.name, error, blocking=True)
            else:
                recorder.fail_lane(active.name, error, blocking=False)
                recorder.fail_lane(
                    "shared_prerequisites",
                    RuntimeError(
                        f"UNCONTAINED_LANE_EXCEPTION:{active.name}:"
                        f"{type(error).__name__}:{error}"
                    ),
                    blocking=True,
                )
    finally:
        receipt_root = Path(os.environ.get(
            "NHL_DAILY_RECEIPT_ROOT", str(DAILY_RUN_RECEIPT_ROOT)))
        try:
            receipt_path = recorder.finalize(receipt_root)
            print(
                f"NHL_DAILY_RECEIPT={receipt_path} "
                f"classification={recorder.classification()}")
        finally:
            _ACTIVE_DAILY_RECORDER = None
            _ACTIVE_DAILY_LANE = "shared_prerequisites"
    if pending_error is not None:
        raise pending_error
    return receipt_path


# ---------- entrypoint ----------

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="nhl-cli", description="NHL pipelines")
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("daily", help="Run full daily pipeline")
    d.add_argument("--with-odds", action="store_true", help="Fetch odds inline")
    d.add_argument("--odds-phase", choices=PHASES, default=os.environ.get("NHL_DAILY_PHASE", "EARLY"),
                   help="Governed append-only odds/roster observation phase")
    d.add_argument("--morning-only", action="store_true", help="Prepare stable upstream state only; skip scoring and market-timed work")
    d.add_argument(
        "--reuse-roster-observation", type=Path, default=None,
        help="Explicit verified immutable roster package to reuse; validation failure stops before DB/provider access.",
    )
    d.add_argument(
        "--reuse-odds-observation", type=Path, default=None,
        help="Explicit immutable odds observation package to reuse instead of a fresh provider capture.",
    )

    fo = sub.add_parser("fetch-odds", help="Fetch odds JSON into nhl/site/data")
    fo.add_argument("--days-from", type=int, default=1)
    fo.add_argument("--slate", default=os.environ.get("SLATE_DATE") or pt_today())
    fo.add_argument("--phase", choices=PHASES, default=os.environ.get("NHL_DAILY_PHASE", "EARLY"))

    rr = sub.add_parser("refresh-rosters-all", help="Refresh NHL players/rosters for all teams")
    rr.add_argument("--date", default=os.environ.get("SLATE_DATE") or et_today(), help="YYYY-MM-DD Eastern context date")

    bsog = sub.add_parser("build-sog", help="Build sog_with_market.csv")
    bsog.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    bsv = sub.add_parser("build-saves", help="Build saves_with_market.csv")
    bsv.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    bpts = sub.add_parser("build-points", help="Build points_with_market.csv")
    bpts.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    g = sub.add_parser("guard", help="List/clear one-off guardrail steps")
    gsub = g.add_subparsers(dest="guard_cmd", required=True)

    gl = gsub.add_parser("list", help="List recorded guard steps")
    gl.add_argument("--slate", default=None, help="Optional slate YYYY-MM-DD; filters to slate + global")

    gc = gsub.add_parser("clear", help="Clear a recorded guard step")
    gc.add_argument("key", help="Guard key to clear (e.g., fix_psql_stdout_bytes_vs_str)")
    gc.add_argument("--slate", default="global", help="Slate to clear (default: global)")

    return ap


def main(argv: Sequence[str] | None = None):
    ap = build_arg_parser()

    args = ap.parse_args(argv)

    if args.cmd == "daily":
        cmd_daily(with_odds=args.with_odds, morning_only=args.morning_only,
                  odds_phase=args.odds_phase,
                  reuse_roster_observation=args.reuse_roster_observation,
                  reuse_odds_observation=args.reuse_odds_observation)
    elif args.cmd == "fetch-odds":
        fetch_odds(days_from=args.days_from, slate=args.slate, phase=args.phase)
    elif args.cmd == "refresh-rosters-all":
        run([PY, SCRIPTS_DIR / "refresh_all_team_rosters.py"], env={"SLATE_DATE": args.date})
    elif args.cmd == "guard":
        if args.guard_cmd == "list":
            guard_print(args.slate)
        elif args.guard_cmd == "clear":
            guard_clear(args.key, args.slate)
            print(f"[guard] cleared: {args.key} (slate={args.slate})")
        else:
            ap.print_help()
    elif args.cmd == "build-sog":
        guard_testing_only_slate(args.slate, cmd_name="build-sog")
        build_sog(args.slate)
    elif args.cmd == "build-saves":
        guard_testing_only_slate(args.slate, cmd_name="build-saves")
        build_saves(args.slate)
    elif args.cmd == "build-points":
        guard_testing_only_slate(args.slate, cmd_name="build-points")
        build_points(args.slate)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
