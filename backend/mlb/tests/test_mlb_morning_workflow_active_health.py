from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
import subprocess

from backend.mlb.scripts import audit_mlb_morning_workflow as morning
from backend.mlb.scripts import check_mlb_daily_feature_lineage_health as lineage
from backend.mlb.scripts import hydrate_mlb_pa_foundation_context as pa
from backend.mlb.scripts import build_mlb_predictions_wide as wide
from backend.mlb.shared.mlb_api_v2 import GameLite


def test_pa_context_query_is_validly_terminated_and_returns_rows(monkeypatch):
    captured = {}

    def fake_fetchall(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [{"artifact_date": "2026-08-05", "player_id": 7, "d7_plate_appearances": 12}]

    monkeypatch.setattr(pa, "pg_fetchall", fake_fetchall)
    result = pa._load_pa_context("2026-08-05", "2026-08-05", [7])
    assert result[("2026-08-05", 7)]["d7_plate_appearances"] == 12
    assert "),\nSELECT" not in captured["sql"]
    assert captured["params"] == ([7], "2026-08-05", "2026-08-05")


def test_retired_predictive_lineage_outputs_are_explicit_skips(tmp_path):
    slate = tmp_path / "slate.csv"
    fields = list(dict.fromkeys(
        lineage.CRITICAL_FIELDS
        + lineage.BVP_COMPACT_FIELDS
        + lineage.ROLLING_CONTEXT_FIELDS
        + lineage.MARKET_AUDIT_FIELDS
    ))
    slate.write_text(",".join(fields) + "\n" + ",".join("1" for _ in fields) + "\n", encoding="utf-8")
    args = SimpleNamespace(
        date="2026-08-05",
        slate_output_csv=str(slate),
        lane_selector_csv=str(tmp_path / "lane.csv"),
        ranking_upload_input_csv=str(tmp_path / "ranking.csv"),
        quick_card_csv=str(tmp_path / "quick.csv"),
        ranking_upload_diagnostics_csv=str(tmp_path / "ranking_diag.csv"),
        quick_card_upload_diagnostics_csv=str(tmp_path / "quick_diag.csv"),
        warn_null_threshold=1.0,
        out_json=str(tmp_path / "health.json"),
        latest_json=str(tmp_path / "latest.json"),
        out_md=str(tmp_path / "health.md"),
    )
    assert lineage.run(args) == 0
    payload = __import__("json").loads(Path(args.out_json).read_text())
    skipped = [row for row in payload["artifacts"] if row["status"] == "skip"]
    assert len(skipped) == 5
    assert payload["summary"]["fail_count"] == 0
    assert all(row["issues"] == ["expected_unavailable:NO_QUALIFIED_MLB_MODEL"] for row in skipped)


def test_candidate_navigation_is_skipped_only_without_predictive_authority(tmp_path):
    rows = []
    workbench = tmp_path / "workbench.md"
    workbench.write_text("# Workbench\n", encoding="utf-8")
    other = tmp_path / "other.md"
    other.write_text("# Other\n", encoding="utf-8")
    morning._transition_checks(
        rows,
        home=other,
        ops_brief=other,
        workbench=workbench,
        candidate_csvs=[],
        timing_template=other,
        decision_performance=other,
        predictive_outputs_expected=False,
    )
    row = next(item for item in rows if item.check == "Workbench points to current Candidate CSV")
    assert row.status == "SKIP"
    assert row.severity == "INFO"
    assert "NO_QUALIFIED_MLB_MODEL" in row.detail


def _game(game_id, start):
    return GameLite(
        game_id=game_id, game_date=start.date().isoformat(), game_time=start.isoformat(), game_type="R",
        home_team_id=1, away_team_id=2, home_abbr="A", away_abbr="B",
        sp_home_id=None, sp_away_id=None,
    )


def test_late_slate_requires_current_date_and_every_game_started(monkeypatch):
    now = datetime.now(timezone.utc)
    today = wide._date_et_today()
    games = {("A", "B"): [_game(1, now - timedelta(minutes=1))]}
    assert wide._late_slate_all_games_started(today, games, now.isoformat()) == (True, 1)
    games[("C", "D")] = [_game(2, now + timedelta(minutes=1))]
    assert wide._late_slate_all_games_started(today, games, now.isoformat()) == (False, 2)
    assert wide._late_slate_all_games_started("2026-01-01", games, now.isoformat()) == (False, 0)


def test_late_slate_guard_continues_without_creating_prediction_artifact(tmp_path):
    helper = Path("bin/mlb_predictions_wide_guarded.sh").resolve()
    output = tmp_path / "predictions.csv"
    today = wide._date_et_today()
    diagnostic = (
        "echo '[mlb-wide-pred] ERROR: no lineage-certified pregame rows' >&2; "
        f"echo '[mlb-wide-pred] LATE_SLATE_NO_WORK_CERTIFIED slate_date={today} "
        "scheduled_games=15 started_games=15 certified_rows=0' >&2; exit 2"
    )
    command = (
        f"zsh {helper} --slate-date {today} --output {output} -- zsh -c \"{diagnostic}\"; "
        "echo NEXT_STAGE_EXECUTED; echo FULL_REFRESH_COMPLETED"
    )
    result = subprocess.run(["zsh", "-c", command], text=True, capture_output=True)
    assert result.returncode == 0
    assert "SKIP MLB predictions-wide: NO_ELIGIBLE_PREGAME_ROWS_LATE_SLATE" in result.stdout
    assert "NEXT_STAGE_EXECUTED" in result.stdout
    assert "FULL_REFRESH_COMPLETED" in result.stdout
    assert not output.exists()


def test_late_slate_guard_keeps_unexplained_and_historical_failures_fatal(tmp_path):
    helper = Path("bin/mlb_predictions_wide_guarded.sh").resolve()
    output = tmp_path / "predictions.csv"
    for slate_date, diagnostic in [
        (wide._date_et_today(), "echo missing_source >&2; exit 7"),
        ("2026-01-01", "echo '[mlb-wide-pred] ERROR: no lineage-certified pregame rows' >&2; exit 2"),
    ]:
        result = subprocess.run(
            ["zsh", str(helper), "--slate-date", slate_date, "--output", str(output), "--", "zsh", "-c", diagnostic],
            text=True,
            capture_output=True,
        )
        assert result.returncode != 0
        assert "SKIP MLB predictions-wide" not in result.stdout
