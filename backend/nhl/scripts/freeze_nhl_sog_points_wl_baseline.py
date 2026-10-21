#!/usr/bin/env python3
"""Freeze a W/L baseline for NHL production SOG and Points HGB.

Row-level grades come from the same governed paths the restated performance
summaries use (P2.27): `performance_summary.build_production_sog_reference`
for SOG and `points_hgb_shadow.grade_prior_hgb_capture` for Points HGB, each
selecting every player's latest pregame source through the resilient selector.
Their grading functions are wrapped only to capture the rows they already
grade; nothing is re-selected or re-scored here. Each day's totals are checked
against the governed result before anything is written.

Probability bands reuse the SOG band edges of `analyze_sog_1_5_deep.py`.
Read-only: HGB grade packages are written to a temporary folder; the output is
a new folder under `artifacts/analysis/nhl/sog_points_wl_baseline/<date>/`.
Run from the main checkout, where the retained artifacts live.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import performance_summary  # noqa: E402
from backend.nhl import points_hgb_shadow  # noqa: E402
from backend.nhl.scripts import grade_nhl_points_hgb_shadow  # noqa: E402

OPERATIONAL = ROOT / "artifacts/operational/nhl"
RESTATEMENTS = OPERATIONAL / "postgame_reconciliation/learning_restatements"
# Same edges as backend/nhl/scripts/analyze_sog_1_5_deep.py (probability_band_analysis).
BANDS = ((".00-.30", 0.0, 0.30), (".30-.40", 0.30, 0.40), (".40-.45", 0.40, 0.45),
         (".45-.50", 0.45, 0.50), (".50-.55", 0.50, 0.55), (".55-.60", 0.55, 0.60),
         (".60-.70", 0.60, 0.70), (".70-1.00", 0.70, 1.0000001))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _band(p: float) -> str:
    return next(name for name, lo, hi in BANDS if lo <= p < hi)


def _latest_summary(day: str) -> tuple[Path, dict]:
    paths = sorted(RESTATEMENTS.glob(f"{day}/performance_summary=*/performance_summary.json"))
    if not paths:
        raise FileNotFoundError(f"NO_PERFORMANCE_SUMMARY:{day}")
    loaded = [(p, json.loads(p.read_text())) for p in paths]
    # Revision numbers are per lineage; the newest summary is the current one.
    return max(loaded, key=lambda item: item[1].get("generated_at_utc", ""))


def _package(day: str, identity: str) -> Path:
    for summary in sorted((OPERATIONAL / "postgame_reconciliation" / day).glob("reconciliation=*/summary.json")):
        if json.loads(summary.read_text()).get("substantive_identity") == identity:
            return summary.parent
    raise FileNotFoundError(f"RECONCILIATION_PACKAGE_NOT_FOUND:{day}")


UNPROVEN_MODEL = "UNPROVEN_BEFORE_2026_10_08"


def _load_unproven_sog_run(receipt_path: Path, slate_date: str, *, require_ready: bool = False) -> dict:
    """Load a pre-2026-10-08 run whose receipt binds predictions but no fitted model.

    Runs before commit b6ed889c (2026-10-08) record the prediction file and its
    hash but not the fitted model identity. The receipt package and prediction
    hash are verified; the model identity is recorded as unproven.
    """
    receipt, manifest_sha = performance_summary._verify_daily_receipt(
        receipt_path, slate_date, require_ready=False)
    run_id = str(receipt.get("parent_daily_run_id") or "")
    lane = receipt.get("lanes", {}).get("legacy_sog", {})
    if lane.get("status") != "COMPLETE" or not lane.get("ended_at_utc"):
        raise ValueError("SOG_LANE_NOT_COMPLETE")
    output = next((o for o in lane.get("outputs", [])
                   if str(o.get("path", "")).endswith("sog_predictions_wide_calibrated.csv")), None)
    path = Path(str((output or {}).get("path", "")))
    if not output or not path.is_file() or _sha(path) != output.get("sha256"):
        raise ValueError("SOG_PREDICTION_ARTIFACT_BINDING_MISMATCH")
    predictions = pd.read_csv(path)
    canonical = sorted(map(int, receipt.get("canonical_game_ids", [])))
    if predictions.duplicated(["game_id", "player_id"]).any() or not set(
            map(int, predictions.game_id.unique())) <= set(canonical):
        raise ValueError("SOG_PREDICTION_IDENTITY_INVALID")
    ended = str(lane["ended_at_utc"])
    if not receipt.get("prior_day_learning"):
        # The earliest runs recorded no prior-day link. Verify against the one
        # retained prior-day reconciliation package instead (recorded in the
        # manifest as PRIOR_DAY_LINK_NOT_RECORDED).
        prior = (date.fromisoformat(slate_date) - timedelta(days=1)).isoformat()
        packages = sorted((OPERATIONAL / "postgame_reconciliation" / prior).glob("reconciliation=*"))
        if len(packages) != 1:
            raise ValueError("PRIOR_DAY_PACKAGE_NOT_UNIQUE")
        receipt = {**receipt, "prior_day_learning": {"reconciliation_package": str(packages[0]),
                                                     "link": "PRIOR_DAY_LINK_NOT_RECORDED"}}
    return {
        "run_id": run_id, "ended_at_utc": str(receipt.get("ended_at_utc") or ""),
        "receipt_path": str(Path(receipt_path).resolve()), "receipt_sha": _sha(Path(receipt_path)),
        "prediction_path": str(path), "phase": str(receipt.get("resolved_phase") or "DAILY"),
        "lane_ended_at_utc": ended, "feature_cutoff_utc": str(receipt.get("feature_cutoff_utc") or ended),
        "feature_contract": "LEGACY_SOG_FEATURE_INPUT_UNBOUND",
        "receipt": receipt, "receipt_manifest_sha": manifest_sha,
        "evidence": {"model_family": "poisson_baseline", "model_version": "baseline_v1",
                     "fitted_model_identity_sha256": UNPROVEN_MODEL, "scoring_run_id": run_id},
        "scorer": {"sha256": UNPROVEN_MODEL}, "scorer_path": "backend/nhl/scripts/score_sog_poisson_baseline.py",
        "prediction_sha": str(output["sha256"]), "feature_binding": {},
        "predictions": predictions, "unscored_rows": pd.DataFrame(), "unscored_sha": None,
        "canonical_ids": canonical,
        "eligible_ids": sorted(map(int, receipt.get("eligible_pregame_game_ids") or canonical)),
    }


def sog_rows(day: str, *, unproven: bool = False) -> tuple[pd.DataFrame, dict]:
    summary_path, summary = _latest_summary(day)
    reference = (summary.get("models", {}).get("sog") or {}).get("reference")
    if not reference and not unproven:
        return pd.DataFrame(), {}
    package = _package(day, summary["reconciliation_package"])
    coverage = summary["models"]["sog"].get("market_coverage") or {}
    captured: list[pd.DataFrame] = []
    original = performance_summary.grade_sog_predictions

    def capture(*args, **kwargs):
        graded = original(*args, **kwargs)
        captured.append(graded.copy())
        return graded

    patches = [mock.patch.object(performance_summary, "grade_sog_predictions", capture)]
    if unproven:
        patches.append(mock.patch.object(performance_summary, "_load_production_sog_run", _load_unproven_sog_run))
        coverage = {}
    with contextlib.ExitStack() as stack:
        for patch in patches:
            stack.enter_context(patch)
        display, _ = performance_summary.build_production_sog_reference(
            slate_date=day, package=package, daily_run_root=OPERATIONAL / "daily_runs",
            market_coverage=coverage if coverage.get("daily_run_id") else None)
    rows = captured[-1]
    settled = rows[rows.grading_state.isin(["WIN", "LOSS"])].copy()
    wins, losses = int(settled.grading_state.eq("WIN").sum()), int(settled.grading_state.eq("LOSS").sum())
    governed = display["overall"]
    if (wins, losses) != (governed["wins"], governed["losses"]):
        raise RuntimeError(f"SOG_TOTALS_DO_NOT_MATCH_GOVERNED:{day}:{wins}-{losses}")
    stored = (reference or {}).get("overall") or {}
    settled["slate_date"] = day
    return settled[["slate_date", "game_id", "player_id", "line", "p_over", "selected_side", "grading_state"]], {
        "slate_date": day, "summary": str(summary_path.relative_to(ROOT)),
        "summary_identity": summary.get("summary_identity"), "summary_revision": summary.get("summary_revision"),
        "package_manifest_sha256": _sha(package / "SHA256SUMS"),
        "wins": wins, "losses": losses,
        "matches_stored_summary": (wins, losses) == (stored.get("wins"), stored.get("losses")) if stored else None,
        "model_identity": UNPROVEN_MODEL if unproven else "PROVEN_FITTED_MODEL_EVIDENCE",
        "prior_day_link": ("PRIOR_DAY_LINK_NOT_RECORDED" if unproven and not any(
            (json.loads(Path(OPERATIONAL / "daily_runs" / f"run_id={r['parent_daily_run_id']}" / "parent_receipt.json").read_text())
             .get("prior_day_learning")) for r in display["model_identity"].get("source_daily_runs", [])
            or [{"parent_daily_run_id": display["model_identity"].get("parent_daily_run_id")}]) else "RECORDED"),
        "source_daily_runs": [r["parent_daily_run_id"] for r in display["model_identity"].get("source_daily_runs", [])]
                             or [display["model_identity"].get("parent_daily_run_id")],
    }


def points_rows(day: str) -> tuple[pd.DataFrame, dict]:
    summary_path, summary = _latest_summary(day)
    package = _package(day, summary["reconciliation_package"])
    season = int(day[:4]) if int(day[5:7]) >= 9 else int(day[:4]) - 1
    source = OPERATIONAL / "points_hgb_shadow" / f"season={season}" / f"slate_date={day}"
    if not source.is_dir():
        return pd.DataFrame(), {}
    captured: list[tuple[pd.DataFrame, pd.DataFrame]] = []
    original = grade_nhl_points_hgb_shadow.grade

    def capture(predictions, outcomes):
        captured.append((predictions.copy(), outcomes.copy()))
        return original(predictions, outcomes)

    with tempfile.TemporaryDirectory() as temp:
        shadow_root = Path(temp)
        (shadow_root / f"season={season}").mkdir()
        # Read the real captures; the grade package lands in the temp folder.
        (shadow_root / f"season={season}" / f"slate_date={day}").symlink_to(source)
        with mock.patch.object(grade_nhl_points_hgb_shadow, "grade", capture):
            result = points_hgb_shadow.grade_prior_hgb_capture(
                slate_date=day, reconciliation_package=package, shadow_root=shadow_root,
                daily_run_root=OPERATIONAL / "daily_runs")
    if result.get("status") != "COMPLETE" or result.get("evaluation_role") != "PRODUCTION":
        return pd.DataFrame(), {"slate_date": day, "skipped": result.get("evaluation_role") or result.get("status")}
    predictions, outcomes = captured[-1]
    frame = predictions.merge(outcomes, on=["game_id", "player_id"], how="left")
    points = (pd.to_numeric(frame.official_points, errors="coerce") if "official_points" in frame else
              pd.to_numeric(frame.official_goals, errors="coerce") + pd.to_numeric(frame.official_assists, errors="coerce"))
    final = (frame.official_final.astype(str).str.lower().isin({"true", "t", "1"})
             if "official_final" in frame else pd.Series(True, index=frame.index))
    state = frame["participation_state"] if "participation_state" in frame else pd.Series("PARTICIPATED", index=frame.index)
    valid = frame[final & state.eq("PARTICIPATED") & points.notna()].assign(points=points)
    long = []
    for line, column in ((0.5, "prob_over_0_5"), (1.5, "prob_over_1_5"), (2.5, "prob_over_2_5")):
        part = valid[["game_id", "player_id", column, "points"]].rename(columns={column: "p_over"})
        part["line"] = line
        part["selected_side"] = part.p_over.ge(0.5).map({True: "OVER", False: "UNDER"})
        realized = part.points.ge(line + 0.5).map({True: "OVER", False: "UNDER"})
        part["grading_state"] = (part.selected_side == realized).map({True: "WIN", False: "LOSS"})
        long.append(part)
    rows = pd.concat(long, ignore_index=True)
    record = result["hgb_metrics"]["selected_side_record"]
    wins, losses = int(rows.grading_state.eq("WIN").sum()), int(rows.grading_state.eq("LOSS").sum())
    if (wins, losses) != (record["wins"], record["losses"]):
        raise RuntimeError(f"POINTS_TOTALS_DO_NOT_MATCH_GOVERNED:{day}:{wins}-{losses}")
    rows["slate_date"] = day
    return rows[["slate_date", "game_id", "player_id", "line", "p_over", "selected_side", "grading_state"]], {
        "slate_date": day, "summary": str(summary_path.relative_to(ROOT)),
        "summary_identity": summary.get("summary_identity"),
        "package_manifest_sha256": _sha(package / "SHA256SUMS"),
        "hgb_grade_identity": result.get("official_outcome_identity"),
        "source_captures": len(result.get("source_captures") or []) or 1,
        "wins": wins, "losses": losses,
    }


def _table(rows: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    grouped = rows.groupby(keys, observed=True).grading_state
    out = pd.DataFrame({"wins": grouped.apply(lambda s: int(s.eq("WIN").sum())),
                        "losses": grouped.apply(lambda s: int(s.eq("LOSS").sum()))}).reset_index()
    out["settled"] = out.wins + out.losses
    out["win_rate"] = (out.wins / out.settled).round(4)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2026-10-08")
    parser.add_argument("--end", default="2026-10-20")
    parser.add_argument("--unproven-sog-start", default="2026-10-01",
                        help="first slate of the separate unproven-model SOG section")
    parser.add_argument("--unproven-sog-end", default="2026-10-07")
    parser.add_argument("--output-root", type=Path, default=ROOT / "artifacts/analysis/nhl/sog_points_wl_baseline")
    args = parser.parse_args()
    days = [d.name for d in sorted(RESTATEMENTS.iterdir()) if d.is_dir() and args.start <= d.name <= args.end]
    sog_parts, points_parts, sog_days, points_days = [], [], [], []
    for day in days:
        rows, meta = sog_rows(day)
        if meta:
            sog_parts.append(rows)
            sog_days.append(meta)
        rows, meta = points_rows(day)
        if meta:
            points_days.append(meta)
            if not rows.empty:
                points_parts.append(rows)
    early_parts, early_days = [], []
    for day in [d.name for d in sorted(RESTATEMENTS.iterdir())
                if d.is_dir() and args.unproven_sog_start <= d.name <= args.unproven_sog_end]:
        try:
            rows, meta = sog_rows(day, unproven=True)
        except (RuntimeError, ValueError, FileNotFoundError) as error:
            early_days.append({"slate_date": day, "skipped": f"{type(error).__name__}:{error}"[:200]})
            continue
        early_parts.append(rows)
        early_days.append(meta)
    sog, points = pd.concat(sog_parts, ignore_index=True), pd.concat(points_parts, ignore_index=True)
    early = pd.concat(early_parts, ignore_index=True) if early_parts else pd.DataFrame(columns=sog.columns)
    for frame in (sog, points, early):
        frame["probability_band"] = pd.Categorical(frame.p_over.map(_band), [b[0] for b in BANDS], ordered=True)
    out = args.output_root / date.today().isoformat()
    out.mkdir(parents=True, exist_ok=False)
    tables = {
        "sog_rows.csv": sog, "points_hgb_rows.csv": points,
        "sog_by_line.csv": _table(sog, ["line"]), "sog_by_line_side.csv": _table(sog, ["line", "selected_side"]),
        "sog_by_line_band.csv": _table(sog, ["line", "probability_band"]),
        "points_hgb_by_line.csv": _table(points, ["line"]),
        "points_hgb_by_line_side.csv": _table(points, ["line", "selected_side"]),
        "points_hgb_by_line_band.csv": _table(points, ["line", "probability_band"]),
        "sog_by_day.csv": pd.DataFrame(sog_days), "points_hgb_by_day.csv": pd.DataFrame(points_days),
        # Separate section: model identity unproven (no fitted-model evidence before 2026-10-08).
        "sog_unproven_rows.csv": early, "sog_unproven_by_line.csv": _table(early, ["line"]),
        "sog_unproven_by_line_side.csv": _table(early, ["line", "selected_side"]),
        "sog_unproven_by_line_band.csv": _table(early, ["line", "probability_band"]),
        "sog_unproven_by_day.csv": pd.DataFrame(early_days),
    }
    for name, frame in tables.items():
        frame.to_csv(out / name, index=False)
    manifest = {
        "contract": "NHL_SOG_POINTS_HGB_WL_BASELINE_V1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "window": {"start": args.start, "end": args.end},
        "sog_model": "poisson_baseline/baseline_v1 (production reference, D10)",
        "points_model": "NHL_POINTS_COUNT_HGB_V1 (production-role days only)",
        "selection": "latest pregame source per player via select_latest_atomic_sources (P2.27)",
        "side_rule": "OVER when P(over) >= 0.5, else UNDER",
        "bands": "P(over) edges from backend/nhl/scripts/analyze_sog_1_5_deep.py",
        "sog_days": sog_days, "points_days": points_days,
        "sog_unproven_window": {"start": args.unproven_sog_start, "end": args.unproven_sog_end,
                                "model_identity": UNPROVEN_MODEL,
                                "proof": "receipt package and prediction-file hash only; same selector, pregame checks and grading",
                                "days": early_days},
        "script_sha256": _sha(Path(__file__)),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    files = sorted(p for p in out.iterdir() if p.is_file())
    (out / "SHA256SUMS").write_text("".join(f"{_sha(p)}  {p.name}\n" for p in files))
    print(json.dumps({"output": str(out),
                      "sog": [int(sog.grading_state.eq("WIN").sum()), int(sog.grading_state.eq("LOSS").sum())],
                      "points_hgb": [int(points.grading_state.eq("WIN").sum()), int(points.grading_state.eq("LOSS").sum())],
                      "sog_days": len(sog_days),
                      "sog_unproven": [int(early.grading_state.eq("WIN").sum()), int(early.grading_state.eq("LOSS").sum())],
                      "sog_unproven_days": early_days,
                      "points_days": [d["slate_date"] for d in points_days if "wins" in d],
                      "points_skipped": [d for d in points_days if "skipped" in d]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
