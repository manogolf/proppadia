#!/usr/bin/env python3
"""October 2026 month-end evaluation: published picks vs the updated models.

Operator request 2026-11-01: the weekly evaluation's tables (W-L, probability
quality, line and side, model-vs-market gap bands, confidence bands) for the
predictions made in October, as published and as the models cut over on
2026-11-01 would have made them on the same graded player-games.

- Points: the governed graded rows of the weekly evaluation (row cache,
  ``freeze_nhl_sog_points_wl_baseline.points_rows``). Each row is traced to the
  daily capture that published it (same game, player and P(over 0.5)); that
  capture's stored inputs are rescored by the production goals + assists model
  after adding the own-rate inputs from official outcomes before the slate date
  (``points_component_features``, frozen prior-season totals and league rates).
- Saves: the graded settled starters (``research_nhl_saves_refit.production_2026``)
  against the frozen refit's strict-prior expected saves for the same starters
  (``frozen_holdout_expected_saves.csv``) through ``saves_refit_model.prob_over``.

Retrospective, not live: both models were chosen on 2025 holdouts, and their
research also looked at these October lines. Read-only; writes only under
``artifacts/analysis/nhl/model_evaluation/month_end_2026-10/`` and the report.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_component_features as pcf  # noqa: E402
from backend.nhl import saves_refit_model as srm  # noqa: E402
from backend.nhl import saves_production  # noqa: E402
from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly  # noqa: E402
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_saves_refit as saves_research  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

FIRST, LAST = "2026-10-01", "2026-10-31"
OUT = weekly.OUT_ROOT / "month_end_2026-10"
DOC = weekly.DOCS / "month_end_model_evaluation_2026-10.md"
CAPTURES = weekly.OPERATIONAL / "points_hgb_shadow" / "season=2026"
KEYS = ["game_id", "player_id"]
LINES = ((0.5, "prob_over_0_5"), (1.5, "prob_over_1_5"), (2.5, "prob_over_2_5"))


def published_points() -> pd.DataFrame:
    frames = []
    for day in weekly._days(weekly.POINTS_START, LAST):
        summary = weekly._summary(day)
        if summary:
            r = weekly._cached_rows("points", day, summary["summary_identity"])
            if not r.empty:
                frames.append(r)
    return pd.concat(frames, ignore_index=True)


def capture_features(day: str) -> pd.DataFrame:
    """Every stored capture's inputs and P(over 0.5) for one slate, latest capture first."""
    frames = []
    for pred in sorted((CAPTURES / f"slate_date={day}").glob("run_id=*/phase=*/captured_at=*/predictions.csv"),
                       key=lambda p: p.parent.name, reverse=True):
        feats = pd.read_csv(pred.parent / "features.csv")
        p = pd.read_csv(pred)[KEYS + ["prob_over_0_5"]]
        frames.append(feats.merge(p, on=KEYS, how="inner", validate="one_to_one").assign(capture=pred.parent.name))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def rescore_points(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    contract = json.loads(scorer.CONTRACT_PATH.read_text())
    folder = scorer.CONTRACT_PATH.parent
    prior = pd.read_csv(folder / contract["prior_season_totals"])
    league = pd.read_csv(folder / contract["league_rates"]).set_index("season")
    outcomes, _, _ = freeze.official()
    model = joblib.load(scorer.MODEL_PATH)
    key_rows = rows[rows.line.eq(0.5)][["slate_date", *KEYS, "p_over"]]
    scored, audit = [], {"published_player_games": int(len(key_rows)), "traced": 0, "untraced": 0}
    for day, want in key_rows.groupby("slate_date"):
        caps = capture_features(day)
        if caps.empty:
            audit["untraced"] += len(want)
            continue
        m = want.merge(caps, on=KEYS, how="inner")
        m = m[(m.p_over - m.prob_over_0_5).abs() <= 1e-12].drop_duplicates(KEYS)  # latest matching capture
        audit["traced"] += len(m)
        audit["untraced"] += len(want) - len(m)
        targets = m[KEYS].assign(season=2026, game_date=day)
        comp = pcf.add_features(targets, outcomes[outcomes.game_date.astype(str) < day], prior, league)
        feats = m.drop(columns=[c for c in pcf.FEATURES if c in m]).merge(comp[KEYS + pcf.FEATURES], on=KEYS)
        s = scorer.score(feats, model)
        scored.append(s[KEYS + [c for _, c in LINES] + ["expected_points"]].assign(slate_date=day))
    new = pd.concat(scored, ignore_index=True)
    long = pd.concat([new[["slate_date", *KEYS]].assign(line=line, p_new=new[c]) for line, c in LINES])
    return long, audit


def regrade(rows: pd.DataFrame, p_new: str) -> pd.DataFrame:
    """The same graded rows with the new probability: side, result and pick probability."""
    out = rows.copy()
    over = np.where(out.selected_side.eq("OVER"), out.grading_state.eq("WIN"), out.grading_state.eq("LOSS"))
    out["p_over"] = out[p_new]
    out["selected_side"] = np.where(out.p_over >= 0.5, "OVER", "UNDER")
    out["grading_state"] = np.where(out.selected_side.eq("OVER") == over, "WIN", "LOSS")
    return out


def tables(rows: pd.DataFrame, label: str) -> dict[str, pd.DataFrame]:
    rows = weekly.enrich(rows)
    two = weekly.two_sided_rows(rows)
    q = pd.DataFrame([{"model": label, "level": lvl, **weekly.quality(r)} for lvl, r in (("all", rows), ("two_sided", two))])
    by_side = rows.groupby(["line", "selected_side"]).agg(n=("win", "size"), wins=("win", "sum"), predicted=("p_pick", "mean")).reset_index()
    ts = two.groupby(["line", "selected_side"]).agg(two_n=("win", "size"), two_wins=("win", "sum"), two_predicted=("p_pick", "mean")).reset_index()
    by_side = by_side.merge(ts, on=["line", "selected_side"], how="left").assign(model=label)
    gaps = weekly.gap_table(rows, []).assign(model=label)
    seg = pd.concat([weekly.segments(rows, 1).assign(level="all"), weekly.segments(two, 1).assign(level="two_sided")]).assign(model=label)
    return {"quality": q, "by_side": by_side, "gaps": gaps, "bands": seg}


def saves_rows() -> pd.DataFrame:
    prod = saves_research.production_2026()
    prod = prod[prod.prediction_timestamp_utc.str[:10].between(FIRST, LAST)]
    exp = pd.read_csv(saves_production.FROZEN_DIR / "frozen_holdout_expected_saves.csv")
    exp = exp[exp.season.eq(2026)].rename(columns={"player_id": "goalie_id", "expected_saves": "mu_refit"})
    model = joblib.load(saves_production.FROZEN_DIR / json.loads(saves_production.CONTRACT_PATH.read_text())["fitted_artifact"])
    m = prod.merge(exp[["game_id", "goalie_id", "mu_refit"]], on=["game_id", "goalie_id"], how="inner")
    probs = srm.prob_over(model, m.mu_refit.to_numpy(float))
    p = probs.to_numpy() if isinstance(probs, pd.DataFrame) else np.asarray(probs)
    col = {line: i for i, line in enumerate(model["lines"])}
    m["p_new"] = [float(p[i, col[float(line)]]) for i, line in enumerate(m.line)]
    m = m.merge(saves_research.market_saves(), on=["game_id", "goalie_id", "line"], how="left")
    m["slate_date"] = m.prediction_timestamp_utc.str[:10]
    m["y_over"] = (m.official_saves > m.line).astype(int)
    return m


def saves_tables(m: pd.DataFrame) -> dict[str, pd.DataFrame]:
    out = {}
    for label, col in (("published (Phoenix V2)", "prob_over"), ("refit", "p_new")):
        r = pd.DataFrame({"slate_date": m.slate_date, "game_id": m.game_id, "player_id": m.goalie_id, "line": m.line,
                          "p_over": m[col], "market_p_over": m.market_p_over, "lane": "saves",
                          "two_sided": m.market_p_over.notna()})
        r["selected_side"] = np.where(r.p_over >= 0.5, "OVER", "UNDER")
        r["grading_state"] = np.where(r.selected_side.eq("OVER") == m.y_over.astype(bool), "WIN", "LOSS")
        for name, t in tables(r, label).items():
            out.setdefault(name, []).append(t)
    return {k: pd.concat(v, ignore_index=True) for k, v in out.items()}


def _pct(x) -> str:
    return "" if pd.isna(x) else f"{100 * x:.1f}%"


def render(pts: dict, sv: dict, audit: dict, saves_n: dict) -> str:
    L = ["# NHL month-end evaluation — October 2026, published vs updated models", "",
         "Same graded picks, two probabilities: what was published in October, and what the models cut over on "
         "2026-11-01 give for those player-games (Points goals + assists `8cda6061…`; Saves "
         "`NHL_SAVES_SHOTS_X_SAVE_RATE_V1`). Retrospective: both models were chosen on 2025 holdouts, and their "
         "research also looked at these October lines, so this shows where the bands now sit; it is not a live result. "
         "Points rows are the weekly evaluation's governed rows from 2026-10-10; two-sided lines as in the weekly "
         "report. Saves are the settled starters; \"two-sided\" for Saves means a market no-vig price on that line.", "",
         f"Points coverage: {audit['traced']:,} of {audit['published_player_games']:,} published player-games traced to "
         f"their capture inputs ({audit['untraced']} untraced, left out of both columns). Saves: "
         f"{saves_n['rows']:,} starter-lines ({saves_n['starters']} starters).", "",
         "## Probability quality", "",
         "| Lane | Level | Model | n | W-L | Win % | Mean pick prob | Brier | Log loss |",
         "|---|---|---|---:|---:|---:|---:|---:|---:|"]
    for lane, t in (("points", pts), ("saves", sv)):
        for r in t["quality"].sort_values(["level"], kind="stable").itertuples():
            L.append(f"| {lane} | {r.level} | {r.model} | {r.n:,} | {r.wins}-{r.losses} | {_pct(r.win_rate)} | "
                     f"{_pct(r.mean_pick_probability)} | {r.brier:.4f} | {r.log_loss:.4f} |")
    L += ["", "## By line and side", "", "Win % and the model's mean confidence on its pick; two-sided lines in the last columns.", "",
          "| Lane | Model | Line | Side | W-L | Win % | Predicted | Two-sided W-L | Two-sided win % | Two-sided predicted |",
          "|---|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    for lane, t in (("points", pts), ("saves", sv)):
        for r in t["by_side"].sort_values(["line", "selected_side", "model"]).itertuples():
            tw = "" if pd.isna(r.two_n) else f"{int(r.two_wins)}-{int(r.two_n - r.two_wins)}"
            L.append(f"| {lane} | {r.model} | {r.line} | {r.selected_side} | {int(r.wins)}-{int(r.n - r.wins)} | "
                     f"{_pct(r.wins / r.n)} | {_pct(r.predicted)} | {tw} | "
                     f"{'' if pd.isna(r.two_n) else _pct(r.two_wins / r.two_n)} | {_pct(r.two_predicted)} |")
    L += ["", "## Model vs market by gap (two-sided lines)", "",
          "Gap = model probability on its side minus the market's no-vig probability on that side. The closer log loss forecast better.", "",
          "| Lane | Model | Gap | Side | W-L | Win % | Model | Market | Log loss model | Log loss market |",
          "|---|---|---|---|---:|---:|---:|---:|---:|---:|"]
    for lane, t in (("points", pts), ("saves", sv)):
        for r in t["gaps"].itertuples():
            L.append(f"| {lane} | {r.model} | {r.gap_band} | {r.side} | {r.wins}-{r.losses} | {_pct(r.win_rate)} | "
                     f"{_pct(r.model_p)} | {_pct(r.market_p)} | {r.log_loss_model:.4f} | {r.log_loss_market:.4f} |")
    L += ["", "## Confidence bands (segments with at least 40 picks)", "",
          "Gap = actual win rate minus mean confidence in the pick; negative is overconfident. Flagged at 5+ points either way.", ""]
    for level in ("all", "two_sided"):
        L += [f"{'All lines' if level == 'all' else 'Two-sided lines only'}:", "",
              "| Lane | Model | Line | Side | Band | n | Win % | Predicted | Gap |", "|---|---|---:|---|---|---:|---:|---:|---:|"]
        for lane, t in (("points", pts), ("saves", sv)):
            b = t["bands"]
            b = b[(b.level == level) & (b.n >= weekly.MIN_SEGMENT)].sort_values(["line", "selected_side", "band", "model"])
            for r in b.itertuples():
                flag = " ⚑" if abs(r.gap) >= weekly.FLAG_GAP else ""
                L.append(f"| {lane} | {r.model} | {r.line} | {r.selected_side} | {r.band} | {r.n:,} | {_pct(r.win_rate)} | "
                         f"{_pct(r.predicted)} | {100 * r.gap:+.1f} pts{flag} |")
        L.append("")
    L += ["## Flagged bands: published vs updated", ""]
    for lane, t in (("points", pts), ("saves", sv)):
        b = t["bands"][t["bands"].n >= weekly.MIN_SEGMENT]
        for level in ("all", "two_sided"):
            c = b[b.level == level].assign(flag=lambda f: f.gap.abs() >= weekly.FLAG_GAP).groupby("model").agg(
                segments=("flag", "size"), flagged=("flag", "sum"), worst=("gap", lambda g: g.abs().max()))
            for model, r in c.iterrows():
                L.append(f"- {lane}, {level.replace('_', '-')}, {model}: {int(r.flagged)} of {int(r.segments)} bands "
                         f"flagged; largest miss {100 * r.worst:.1f} pts.")
    L += ["", f"Tables: `{OUT.relative_to(ROOT)}/`. Script: `backend/nhl/scripts/evaluate_nhl_month_end_updated_models.py`.", ""]
    return "\n".join(L)


def main() -> None:
    tempfile.tempdir = str(ROOT / "tmp")
    OUT.mkdir(parents=True, exist_ok=True)
    rows = published_points()
    rows = rows[rows.slate_date.between(FIRST, LAST)].reset_index(drop=True)
    rows["two_sided"] = pd.Series(pd.NA, index=rows.index, dtype="boolean")
    rows["market_p_over"] = np.nan
    loaded: list = []

    def load_candidates() -> list:
        if not loaded:
            loaded.append(weekly.two_sided_lines.candidate_export.load_candidates(weekly.OPERATIONAL)[0])
        return loaded[0]

    for day in weekly._days(weekly.TWO_SIDED_START, LAST):
        on = rows.slate_date.eq(day)
        if on.any():
            keys = weekly._cached_two_sided_keys(day, load_candidates).get("points")
            rows.loc[on, "two_sided"] = weekly.mark_two_sided(rows[on], keys)
            rows.loc[on, "market_p_over"] = weekly.market_p_over(rows[on], keys)
    new, audit = rescore_points(rows)
    rows = rows.merge(new, on=["slate_date", *KEYS, "line"], how="inner")
    rows["lane"] = "points"
    rows.to_csv(OUT / "points_rows_published_and_updated.csv", index=False)
    pts = {}
    for label, frame in (("published", rows), ("goals + assists", regrade(rows, "p_new"))):
        for name, t in tables(frame.drop(columns=["p_new", "expected_points", *[c for _, c in LINES]], errors="ignore"), label).items():
            pts.setdefault(name, []).append(t)
    pts = {k: pd.concat(v, ignore_index=True) for k, v in pts.items()}
    sv_rows = saves_rows()
    sv_rows.to_csv(OUT / "saves_rows_published_and_refit.csv", index=False)
    sv = saves_tables(sv_rows)
    for lane, t in (("points", pts), ("saves", sv)):
        for name, frame in t.items():
            frame.to_csv(OUT / f"{lane}_{name}.csv", index=False)
    saves_n = {"rows": int(len(sv_rows)), "starters": int(sv_rows[["game_id", "goalie_id"]].drop_duplicates().shape[0])}
    DOC.write_text(render(pts, sv, audit, saves_n))
    (OUT / "summary.json").write_text(json.dumps({"window": [FIRST, LAST], "points_trace": audit, "saves": saves_n,
                                                  "points_model_identity": json.loads(scorer.CONTRACT_PATH.read_text())["model_identity_sha256"],
                                                  "saves_model": srm.MODEL_ID, "doc": str(DOC.relative_to(ROOT))}, indent=2) + "\n")
    print(json.dumps({"points_trace": audit, "saves": saves_n}, indent=1))
    print(pd.concat([pts["quality"].assign(lane="points"), sv["quality"].assign(lane="saves")]).to_string(index=False))


if __name__ == "__main__":
    main()
