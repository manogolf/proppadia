#!/usr/bin/env python3
"""Weekly NHL model evaluation: where every model and shadow stands.

Reuses the governed grades only:
- each slate's latest performance summary for the W/L of every model and
  shadow (Moneyline, Puck Line, SOG arms, Points HGB and Phoenix, Saves);
- `freeze_nhl_sog_points_wl_baseline.sog_rows` / `points_rows` for row-level
  production SOG and Points HGB grades, which already check their totals
  against the governed result;
- `backend.nhl.two_sided_lines` to mark which of those rows had a two-sided
  line (same book quoted Over and Under before the prediction), from 2026-10-10.

Adds probability quality (Brier, log loss), calibration by probability band
and the segments whose results differ most from their predicted confidence.
Read-only for grades. Writes `docs/nhl/weekly_model_evaluation_<as_of>.md`,
updates the improvement log (`docs/nhl/model_improvement_log.json` and its
markdown view) from the flagged segments, and writes data under
`artifacts/analysis/nhl/model_evaluation/weekly_<as_of>/`; per-slate rows are
cached by summary identity so later weeks do not regrade the season.
"""
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl import two_sided_lines
from backend.nhl.scripts import freeze_nhl_sog_points_wl_baseline as baseline

ROOT = Path(__file__).resolve().parents[3]
OUT_ROOT = ROOT / "artifacts/analysis/nhl/model_evaluation"
CACHE = OUT_ROOT / "row_cache"
DOCS = ROOT / "docs/nhl"
SOG_START, POINTS_START = "2026-10-08", "2026-10-10"
# First slate with timestamped production SOG and Points HGB candidates for quote matching.
TWO_SIDED_START = "2026-10-10"
OPERATIONAL = ROOT / "artifacts/operational/nhl"
MIN_SEGMENT = 40
FLAG_GAP = 0.05          # flag a segment when results differ from confidence by 5+ points
CLEAR_WEEKS = 3          # weeks in a row unflagged before suggesting an entry is resolved
LOG_JSON = DOCS / "model_improvement_log.json"
LOG_MD = DOCS / "model_improvement_log.md"
MANUAL_STATUSES = {"TESTING", "PROMOTED", "REJECTED", "WATCH", "RESOLVED"}


def _days(start: str, end: str) -> list[str]:
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def _summary(day: str) -> dict | None:
    try:
        return baseline._latest_summary(day)[1]
    except FileNotFoundError:
        return None


def _cached_rows(lane: str, day: str, identity: str) -> pd.DataFrame:
    """Governed rows for one slate, cached by the summary identity they came from."""
    path = CACHE / lane / f"{day}_{identity[:16]}.csv"
    if path.is_file():
        return pd.read_csv(path)
    rows, _ = (baseline.sog_rows if lane == "sog" else baseline.points_rows)(day)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(path, index=False)
    return rows


def _cached_two_sided_keys(day: str, load_candidates) -> dict[str, list | None]:
    """Two-sided keys with the market's no-vig P(over) for one slate.

    Entries are [game_id, player_id, line, market_p_over or null]; cached by
    the slate's retained odds snapshot count (v2 adds the market probability).
    """
    snapshots = len(list((OPERATIONAL / "odds_observations" / "season=2026" / f"slate_date={day}").glob("observation=*")))
    path = CACHE / "two_sided" / f"{day}_{snapshots}_v2.json"
    if path.is_file():
        return json.loads(path.read_text())
    evidence = two_sided_lines.load_evidence(OPERATIONAL, day, candidates=load_candidates())
    keys = {}
    for lane, found in evidence["keys"].items():
        market = (evidence.get("market_p_over") or {}).get(lane) or {}
        keys[lane] = (sorted([int(g), int(p), float(line), market.get((g, p, line))] for g, p, line in found)
                      if found is not None else None)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(keys) + "\n")
    return keys


def mark_two_sided(rows: pd.DataFrame, keys: list | None) -> pd.Series:
    """True/False per row; NA when the slate has no quote evidence for the lane."""
    if keys is None:
        return pd.Series(pd.NA, index=rows.index, dtype="boolean")
    found = {(int(entry[0]), int(entry[1]), float(entry[2])) for entry in keys}
    return pd.Series([(int(g), int(p), float(line)) in found for g, p, line in
                      zip(rows.game_id, rows.player_id, rows.line)], index=rows.index, dtype="boolean")


def market_p_over(rows: pd.DataFrame, keys: list | None) -> pd.Series:
    """The market's no-vig P(over) per row on a two-sided line, else NaN."""
    found = {(int(e[0]), int(e[1]), float(e[2])): e[3] for e in (keys or []) if len(e) > 3 and e[3] is not None}
    return pd.Series([found.get((int(g), int(p), float(line)), np.nan) for g, p, line in
                      zip(rows.game_id, rows.player_id, rows.line)], index=rows.index, dtype=float)


GAP_BANDS = (("model 10+ below", -1.0, -0.10), ("within 10", -0.10, 0.10),
             ("10-15 above", 0.10, 0.15), ("over 15 above", 0.15, 1.0))


def gap_band(gap: pd.Series) -> pd.Series:
    """Model minus market on the selected side, in the bands the operator reviews."""
    band = pd.Series(pd.NA, index=gap.index, dtype="object")
    for label, low, high in GAP_BANDS:
        inside = gap.gt(low) & gap.le(high) if low > -1 else gap.le(high)
        band[inside] = label
    return band


def gap_table(rows: pd.DataFrame, week: list[str]) -> pd.DataFrame:
    """W-L and log loss (model vs market) by lane, gap band and side."""
    if "gap_band" not in rows:
        return pd.DataFrame()
    priced = rows[rows.gap_band.notna()]
    out = []
    for (lane, band, side), g in priced.groupby(["lane", "gap_band", "selected_side"]):
        w = g[g.slate_date.isin(week)]
        y = g.y_over.to_numpy(float)
        ll = lambda p: float(-np.mean(y * np.log(np.clip(p, 1e-6, 1 - 1e-6))  # noqa: E731
                                     + (1 - y) * np.log(np.clip(1 - p, 1e-6, 1 - 1e-6))))
        out.append({"lane": lane, "gap_band": band, "side": side, "n": int(len(g)),
                    "week_wins": int(w.win.sum()), "week_losses": int(len(w) - w.win.sum()),
                    "wins": int(g.win.sum()), "losses": int(len(g) - g.win.sum()),
                    "win_rate": float(g.win.mean()), "model_p": float(g.p_pick.mean()),
                    "market_p": float(g.market_p_pick.mean()),
                    "log_loss_model": ll(g.p_over.to_numpy(float)), "log_loss_market": ll(g.market_p_over.to_numpy(float))})
    order = {label: i for i, (label, _, _) in enumerate(GAP_BANDS)}
    frame = pd.DataFrame(out)
    return frame.sort_values(["lane", "gap_band", "side"], key=lambda s: s.map(order) if s.name == "gap_band" else s) \
        if len(frame) else frame


GRADE_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow/grades"


def moneyline_shadow_records(grade_root: Path = GRADE_ROOT, *, last_day: str | None = None) -> pd.DataFrame:
    """Daily W/L of the cross-market Moneyline shadow model from its graded results.

    Summaries written before the shadow appeared in them (2026-10-14..10-25)
    do not carry it, so the record comes from the grade packages. A game graded
    more than once keeps its latest grade.
    """
    parts = []
    for path in sorted(Path(grade_root).glob("state=*/grade=*/graded_moneyline_shadow_results.csv")):
        frame = pd.read_csv(path)
        if {"slate_date", "game_id", "correct", "evaluation_status", "control_name"} - set(frame.columns):
            continue
        parts.append(frame[["slate_date", "game_id", "correct", "evaluation_status", "control_name",
                            "grading_timestamp_utc"]])
    if not parts:
        return pd.DataFrame(columns=["slate_date", "lane", "model", "wins", "losses"])
    rows = pd.concat(parts, ignore_index=True)
    rows = rows[rows.evaluation_status.eq("REGULAR_SEASON_GRADED") & rows.correct.notna()]
    if last_day:
        rows = rows[rows.slate_date.astype(str).le(last_day)]
    rows = rows.sort_values("grading_timestamp_utc").drop_duplicates(["slate_date", "game_id"], keep="last")
    correct = rows.correct.astype(str).str.lower().isin(["true", "1"])
    out = (rows.assign(wins=correct.astype(int), losses=(~correct).astype(int))
           .groupby(["slate_date", "control_name"], as_index=False)[["wins", "losses"]].sum())
    return out.rename(columns={"control_name": "model"}).assign(lane="moneyline")[
        ["slate_date", "lane", "model", "wins", "losses"]]


def model_records(summaries: dict[str, dict], extra: pd.DataFrame | None = None) -> pd.DataFrame:
    """W/L per model and shadow from the governed summaries."""
    out = []
    for day, s in summaries.items():
        m = s.get("models") or {}
        for lane in ("moneyline", "puck_line"):
            for role, v in (m.get(lane) or {}).items():
                if isinstance(v, dict) and "correct" in v:
                    identity = v.get("model_identity") or {}
                    name = identity.get("model_version") or identity.get("control_name") or identity.get("name") or role
                    out.append((day, lane, name, int(v["correct"]), int(v["incorrect"])))
        for name, v in (m.get("sog") or {}).items():
            if isinstance(v, dict) and "overall" in v and day >= SOG_START:
                label = "production" if name == "reference" else f"shadow: {name}"
                out.append((day, "sog", label, int(v["overall"]["wins"]), int(v["overall"]["losses"])))
        shrunk = ((s.get("sog_shrunk_rate_k180_shadow") or {}).get("latest_pregame") or {}).get("overall") or {}
        if shrunk and day >= SOG_START:
            out.append((day, "sog", "shadow: SHRUNK_180 (latest pregame)", int(shrunk["wins"]), int(shrunk["losses"])))
        hgb_sog = ((s.get("sog_count_hgb_shadow") or {}).get("latest_pregame") or {}).get("overall") or {}
        if hgb_sog and day >= SOG_START:
            out.append((day, "sog", "shadow: HGB count model (latest pregame)", int(hgb_sog["wins"]), int(hgb_sog["losses"])))
        formula = ((s.get("sog_formula_shadow") or {}).get("latest_pregame") or {}).get("overall") or {}
        if formula and day >= SOG_START:
            out.append((day, "sog", "shadow: Poisson formula (latest pregame)", int(formula["wins"]), int(formula["losses"])))
        points = m.get("points") or {}
        evaluation = (points.get("authority_context") or {}).get("hgb_evaluation") or {}
        hgb = (evaluation.get("metrics") or {}).get("selected_side_record") or {}
        hgb_production = evaluation.get("role") == "PRODUCTION" or (not evaluation.get("role") and day >= POINTS_START)
        if hgb and hgb_production:
            out.append((day, "points", "production: HGB", int(hgb["wins"]), int(hgb["losses"])))
            phoenix = evaluation.get("phoenix_same_capture_selected_side_record") or {}
            if phoenix:
                out.append((day, "points", "shadow: Phoenix (same players)", int(phoenix["wins"]), int(phoenix["losses"])))
        saves = (m.get("saves") or {}).get("reference") or {}
        if "overall" in saves:
            out.append((day, "saves", "production", int(saves["overall"]["wins"]), int(saves["overall"]["losses"])))
    frame = pd.DataFrame(out, columns=["slate_date", "lane", "model", "wins", "losses"])
    if extra is not None and len(extra):
        # Summary records come first, so a day the summary already covers keeps them.
        frame = pd.concat([frame, extra[extra.slate_date.isin(list(summaries))]], ignore_index=True)
    # After the Moneyline cutover the same model fills two roles; count it once.
    frame = frame.drop_duplicates(["slate_date", "lane", "model"])
    totals = frame.groupby(["lane", "model"]).wins.transform("sum") + frame.groupby(["lane", "model"]).losses.transform("sum")
    return frame[totals > 0].reset_index(drop=True)


def enrich(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows.copy()
    rows["win"] = rows.grading_state.eq("WIN").astype(int)
    rows["p_pick"] = np.where(rows.selected_side.eq("OVER"), rows.p_over, 1 - rows.p_over)
    rows["y_over"] = np.where(rows.selected_side.eq("OVER"), rows.win, 1 - rows.win)
    rows["band"] = rows.p_over.map(baseline._band)
    if "market_p_over" in rows:
        rows["market_p_pick"] = np.where(rows.selected_side.eq("OVER"), rows.market_p_over, 1 - rows.market_p_over)
        rows["gap"] = rows.p_pick - rows.market_p_pick
        rows["gap_band"] = gap_band(rows.gap)
    return rows


def quality(rows: pd.DataFrame) -> dict:
    if rows.empty:
        return {"n": 0}
    p = np.clip(rows.p_over.to_numpy(float), 1e-6, 1 - 1e-6)
    y = rows.y_over.to_numpy(int)
    return {"n": int(len(rows)), "wins": int(rows.win.sum()), "losses": int(len(rows) - rows.win.sum()),
            "win_rate": float(rows.win.mean()), "mean_pick_probability": float(rows.p_pick.mean()),
            "brier": float(np.mean((p - y) ** 2)),
            "log_loss": float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))}


def segments(rows: pd.DataFrame, min_n: int = MIN_SEGMENT) -> pd.DataFrame:
    """Lane x line x side x band: realized win rate minus mean predicted confidence."""
    g = rows.groupby(["lane", "line", "selected_side", "band"], observed=True)
    out = g.agg(n=("win", "size"), wins=("win", "sum"), win_rate=("win", "mean"), predicted=("p_pick", "mean")).reset_index()
    out["gap"] = out.win_rate - out.predicted
    return out[out.n >= min_n].sort_values("gap")


def update_improvement_log(log: dict, seg: pd.DataFrame, as_of: str) -> tuple[dict, list[str], list[str]]:
    """Add newly flagged segments and record each open entry's state for this week.

    Status values set by hand (TESTING, PROMOTED, REJECTED, WATCH, RESOLVED)
    are never changed here. Returns the log and the ids that are new or
    recurring this week.
    """
    log = {"schema_version": "NHL_MODEL_IMPROVEMENT_LOG_V1", "entries": [], **log}
    entries = {e["key"]: e for e in log["entries"]}
    flagged = seg[(seg.n >= MIN_SEGMENT) & (seg.gap.abs() >= FLAG_GAP)]
    new, recurring = [], []
    seen = set()
    for r in flagged.itertuples():
        level = getattr(r, "level", "all")
        # All-predictions keys keep their original form; two-sided segments are separate entries.
        key = (f"{r.lane}|{r.line}|{r.selected_side}|{r.band}" if level == "all"
               else f"{level}|{r.lane}|{r.line}|{r.selected_side}|{r.band}")
        seen.add(key)
        point = {"as_of": as_of, "n": int(r.n), "win_rate": round(float(r.win_rate), 4),
                 "predicted": round(float(r.predicted), 4), "gap": round(float(r.gap), 4), "flagged": True}
        direction = "overconfident" if r.gap < 0 else "underconfident"
        if key not in entries:
            entry = {"id": f"IMP-{len(entries) + 1:03d}", "key": key, "level": level, "lane": r.lane,
                     "line": float(r.line),
                     "side": r.selected_side, "band": r.band, "first_flagged": as_of, "status": "OPEN",
                     "finding": (f"{'two-sided lines: ' if level == 'two_sided' else ''}"
                                 f"{r.lane} {r.line} {r.selected_side}, P(over) {r.band}: model is {direction} "
                                 f"(wins {100 * r.win_rate:.1f}% vs {100 * r.predicted:.1f}% expected, n={int(r.n)})."),
                     "suggested_checks": ("Check this group's feature inputs against training; compare with the market on "
                                          "two-sided lines; test a calibration layer as a shadow." if r.gap < 0 else
                                          "Model may be too cautious here; check whether the market agrees before "
                                          "trusting larger confidence."),
                     "hypothesis": "", "next_step": "", "history": []}
            entries[key] = entry
            new.append(entry["id"])
        entry = entries[key]
        if not entry["history"] or entry["history"][-1]["as_of"] != as_of:
            entry["history"].append(point)
        if entry["id"] not in new:
            recurring.append(entry["id"])
        entry["last_flagged"] = as_of
        entry["weeks_flagged"] = sum(1 for h in entry["history"] if h.get("flagged"))
    for key, entry in entries.items():
        if key in seen:
            continue
        if not entry["history"] or entry["history"][-1]["as_of"] != as_of:
            entry["history"].append({"as_of": as_of, "flagged": False})
        clear = 0
        for h in reversed(entry["history"]):
            if h.get("flagged"):
                break
            clear += 1
        if entry["status"] == "OPEN" and clear >= CLEAR_WEEKS:
            entry["status"] = "OPEN_CLEAR_3_WEEKS"
    log["entries"] = sorted(entries.values(), key=lambda e: e["id"])
    log["updated_as_of"] = as_of
    return log, new, recurring


def render_log(log: dict) -> str:
    lines = ["# NHL model improvement log", "",
             "Generated by the weekly model evaluation from segments whose results differ from the model's",
             f"confidence by at least {int(FLAG_GAP * 100)} points (n >= {MIN_SEGMENT}), at two levels: all",
             "predictions, and two-sided lines only (marked \"(two-sided)\"). Edit status, hypothesis and",
             "next_step in `model_improvement_log.json`; the weekly run keeps hand-set statuses",
             "(TESTING, PROMOTED, REJECTED, WATCH, RESOLVED). Rules for acting on an entry:",
             "`docs/nhl/model_promotion_rules.md`.", "",
             f"Updated as of {log.get('updated_as_of')}.", "",
             "| Id | Status | Segment | First flagged | Weeks flagged | Latest gap | Finding |",
             "|---|---|---|---|---:|---:|---|"]
    for e in log["entries"]:
        last = next((h for h in reversed(e["history"]) if h.get("flagged")), {})
        level = " (two-sided)" if e.get("level") == "two_sided" else ""
        lines.append(f"| {e['id']} | {e['status']} | {e['lane']} {e['line']} {e['side']} {e['band']}{level} | "
                     f"{e['first_flagged']} | {e.get('weeks_flagged', 0)} | "
                     f"{100 * last.get('gap', 0):+.1f} pts | {e['finding']} |")
    return "\n".join(lines) + "\n"


def _pct(x: float) -> str:
    return f"{100 * x:.1f}%"


def two_sided_rows(rows: pd.DataFrame) -> pd.DataFrame:
    """Rows on a two-sided line; empty when no row carries two-sided evidence."""
    if "two_sided" not in rows:
        return rows.iloc[0:0]
    return rows[rows.two_sided.fillna(False).astype(bool)]


def _wl(frame: pd.DataFrame) -> str:
    return f"{int(frame.win.sum())}-{int(len(frame) - frame.win.sum())}"


def render(as_of: str, week: list[str], records: pd.DataFrame, rows: pd.DataFrame, pulse: str | None,
           log_note: str = "", schedule_note: str = "") -> str:
    wk = rows[rows.slate_date.isin(week)]
    lines = [f"# NHL weekly model evaluation — week ending {as_of}", "",
             *([schedule_note, ""] if schedule_note else []),
             f"Week: {week[0]} to {week[-1]} ({len(set(wk.slate_date))} slates with production rows). "
             f"Season to date: SOG (production and shadows) from {SOG_START}, Points HGB as production from {POINTS_START}, "
             "Moneyline, Puck Line and Saves from 2026-10-01. Governed grades only. "
             f"Two-sided lines from {TWO_SIDED_START}: only picks whose exact line had the same book or prediction "
             "market quoting both Over and Under before the prediction (`backend/nhl/two_sided_lines.py`).", "",
             "## Where every model stands (W-L)", "",
             "| Lane | Model | Week | Win % | Season | Win % |", "|---|---|---:|---:|---:|---:|"]
    for (lane, model), g in records.groupby(["lane", "model"], sort=True):
        w = g[g.slate_date.isin(week)]
        ww, wl, sw, sl = int(w.wins.sum()), int(w.losses.sum()), int(g.wins.sum()), int(g.losses.sum())
        lines.append(f"| {lane} | {model} | {ww}-{wl} | {_pct(ww / (ww + wl)) if ww + wl else '—'} | "
                     f"{sw}-{sl} | {_pct(sw / (sw + sl)) if sw + sl else '—'} |")
    lines += ["", "## Probability quality (production SOG and Points HGB)", "",
              "Brier and log loss on P(over); lower is better. Mean pick probability vs actual win rate shows calibration.", "",
              "| Lane | Window | n | W-L | Win % | Mean pick prob | Brier | Log loss |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    for lane in ("sog", "points"):
        for label, frame in (("week", wk[wk.lane == lane]), ("season", rows[rows.lane == lane])):
            q = quality(frame)
            if q["n"]:
                lines.append(f"| {lane} | {label} | {q['n']} | {q['wins']}-{q['losses']} | {_pct(q['win_rate'])} | "
                             f"{_pct(q['mean_pick_probability'])} | {q['brier']:.4f} | {q['log_loss']:.4f} |")
    two = two_sided_rows(rows)
    wk_two = two[two.slate_date.isin(week)]
    lines += ["", "## Two-sided lines (production SOG and Points HGB)", "",
              "The same measures on picks whose line had a two-sided quote: the level where the model meets the market. "
              "Share = two-sided picks out of all settled picks on slates with quote evidence.", "",
              "| Lane | Window | n | Share | W-L | Win % | Mean pick prob | Brier | Log loss |",
              "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for lane in ("sog", "points"):
        for label, frame, base in (("week", wk_two[wk_two.lane == lane], wk[wk.lane == lane]),
                                   ("season", two[two.lane == lane], rows[rows.lane == lane])):
            q = quality(frame)
            covered = base.two_sided.notna().sum() if "two_sided" in base else 0
            if q["n"]:
                lines.append(f"| {lane} | {label} | {q['n']} | {_pct(q['n'] / covered)} | {q['wins']}-{q['losses']} | "
                             f"{_pct(q['win_rate'])} | {_pct(q['mean_pick_probability'])} | {q['brier']:.4f} | "
                             f"{q['log_loss']:.4f} |")
    lines += ["", "## By line and side (week / season)", "",
              "| Lane | Line | Side | Week W-L | Week two-sided | Season W-L | Season win % | Season two-sided | Two-sided win % |",
              "|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    for (lane, line, side), g in rows.groupby(["lane", "line", "selected_side"]):
        w = g[g.slate_date.isin(week)]
        t = two_sided_rows(g)
        tw = t[t.slate_date.isin(week)]
        lines.append(f"| {lane} | {line} | {side} | {_wl(w)} | {_wl(tw)} | {_wl(g)} | {_pct(g.win.mean())} | "
                     f"{_wl(t)} | {_pct(t.win.mean()) if len(t) else '—'} |")
    gaps = gap_table(rows, week)
    lines += ["", "## Model vs market by gap (two-sided lines, season to date)", "",
              "Gap = model probability on its side minus the market's no-vig probability on that side "
              "(median across books). Lower log loss is better; the closer of the two shows who forecast better.", "",
              "| Lane | Gap | Side | Week W-L | Season W-L | Win % | Model | Market | Log loss model | Log loss market |",
              "|---|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in gaps.itertuples():
        lines.append(f"| {r.lane} | {r.gap_band} | {r.side} | {r.week_wins}-{r.week_losses} | {r.wins}-{r.losses} | "
                     f"{_pct(r.win_rate)} | {_pct(r.model_p)} | {_pct(r.market_p)} | "
                     f"{r.log_loss_model:.4f} | {r.log_loss_market:.4f} |")
    if gaps.empty:
        lines.append("| — | no priced two-sided rows | | | | | | | | |")
    seg = segments(rows)
    lines += ["", f"## Areas for improvement (season, segments with at least {MIN_SEGMENT} picks)", "",
              "Gap = actual win rate minus the model's average confidence in its pick. Negative means overconfident.", "",
              "| Lane | Line | Side | Band | n | Win % | Predicted | Gap |", "|---|---:|---|---|---:|---:|---:|---:|"]
    for r in pd.concat([seg.head(6), seg.tail(3)]).drop_duplicates().itertuples():
        lines.append(f"| {r.lane} | {r.line} | {r.selected_side} | {r.band} | {r.n} | {_pct(r.win_rate)} | {_pct(r.predicted)} | {100 * r.gap:+.1f} pts |")
    seg_two = segments(two)
    lines += ["", f"Two-sided lines only (at least {MIN_SEGMENT} picks):", "",
              "| Lane | Line | Side | Band | n | Win % | Predicted | Gap |", "|---|---:|---|---|---:|---:|---:|---:|"]
    for r in pd.concat([seg_two.head(6), seg_two.tail(3)]).drop_duplicates().itertuples():
        lines.append(f"| {r.lane} | {r.line} | {r.selected_side} | {r.band} | {r.n} | {_pct(r.win_rate)} | {_pct(r.predicted)} | {100 * r.gap:+.1f} pts |")
    if log_note:
        lines += ["", "## Improvement log", "", log_note]
    lines += ["", "## Data health", "", f"Latest player performance pulse: `{pulse}`" if pulse else "Player performance pulse: no revision found.", "",
              "## Not yet covered", "", "- Closing-line value.",
              "- Moneyline and Puck Line probability quality (W-L only above).", ""]
    return "\n".join(lines)


def latest_pulse() -> str | None:
    from backend.nhl.scripts.extend_nhl_player_performance_pulse import verdict_line
    revisions = sorted((ROOT / "artifacts/analysis/nhl/player_performance_pulse/2026-10-07_final/revisions").glob("as_of=*/summary.json"))
    for path in reversed(revisions):
        s = json.loads(path.read_text())
        if s.get("revision") == 3 and "artifact_audit" in s:
            invalid = sum(c.get("INVALID_PRODUCTION_TRANSITION_EVIDENCE", 0) for c in s["production_evidence_counts_by_lane"].values())
            return verdict_line(s, invalid)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--as-of", required=True, help="last slate date in the week, YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()
    tempfile.tempdir = str(ROOT / "tmp")  # repo-only scratch for the governed graders
    Path(tempfile.tempdir).mkdir(exist_ok=True)
    week = _days((date.fromisoformat(args.as_of) - timedelta(days=args.days - 1)).isoformat(), args.as_of)
    summaries = {d: s for d in _days("2026-10-01", args.as_of) if (s := _summary(d))}
    records = model_records(summaries, moneyline_shadow_records(last_day=args.as_of))
    frames = []
    for lane, start in (("sog", SOG_START), ("points", POINTS_START)):
        for day in _days(start, args.as_of):
            if day in summaries:
                r = _cached_rows(lane, day, summaries[day]["summary_identity"])
                if not r.empty:
                    frames.append(r.assign(lane=lane))
    rows = pd.concat(frames, ignore_index=True)
    rows["two_sided"] = pd.Series(pd.NA, index=rows.index, dtype="boolean")
    rows["market_p_over"] = np.nan
    loaded: list = []

    def load_candidates() -> list:
        if not loaded:
            loaded.append(two_sided_lines.candidate_export.load_candidates(OPERATIONAL)[0])
        return loaded[0]

    for day in _days(TWO_SIDED_START, args.as_of):
        on_day = rows.slate_date.eq(day)
        if not on_day.any():
            continue
        keys = _cached_two_sided_keys(day, load_candidates)
        for lane in ("sog", "points"):
            mask = on_day & rows.lane.eq(lane)
            rows.loc[mask, "two_sided"] = mark_two_sided(rows[mask], keys.get(lane))
            rows.loc[mask, "market_p_over"] = market_p_over(rows[mask], keys.get(lane))
    rows = enrich(rows)
    out = OUT_ROOT / f"weekly_{args.as_of}"
    out.mkdir(parents=True, exist_ok=True)
    records.to_csv(out / "model_records.csv", index=False)
    seg = pd.concat([segments(rows, 1).assign(level="all"),
                     segments(two_sided_rows(rows), 1).assign(level="two_sided")], ignore_index=True)
    log = json.loads(LOG_JSON.read_text()) if LOG_JSON.is_file() else {}
    log, new, recurring = update_improvement_log(log, seg, args.as_of)
    LOG_JSON.write_text(json.dumps(log, indent=2) + "\n")
    LOG_MD.write_text(render_log(log))
    open_ids = [e["id"] for e in log["entries"] if e["status"].startswith("OPEN") or e["status"] in {"TESTING", "WATCH"}]
    log_note = (f"New this week: {', '.join(new) or 'none'}. Still flagged: {', '.join(recurring) or 'none'}. "
                f"Open or in test: {len(open_ids)}. Full log: `docs/nhl/model_improvement_log.md`.")
    from backend.nhl.model_evaluation_schedule import load_config, status as schedule_status
    nxt = schedule_status((date.fromisoformat(args.as_of) + timedelta(days=1)).isoformat(), load_config())
    schedule_note = (f"Schedule: automatic weekly build is {'on' if nxt['enabled'] else 'off'}; "
                     f"next report due {(date.fromisoformat(args.as_of) + timedelta(days=8)).isoformat()} "
                     f"(week ending {(date.fromisoformat(args.as_of) + timedelta(days=7)).isoformat()}).")
    report = render(args.as_of, week, records, rows, latest_pulse(), log_note, schedule_note)
    seg.to_csv(out / "segments.csv", index=False)
    gap_table(rows, week).to_csv(out / "gap_bands.csv", index=False)
    doc = DOCS / f"weekly_model_evaluation_{args.as_of}.md"
    doc.write_text(report)
    (out / "summary.json").write_text(json.dumps({
        "as_of": args.as_of, "week": week, "doc": str(doc.relative_to(ROOT)),
        "summaries": {d: s.get("summary_identity") for d, s in summaries.items()},
        "quality": {lane: {"week": quality(rows[(rows.lane == lane) & rows.slate_date.isin(week)]),
                           "season": quality(rows[rows.lane == lane])} for lane in ("sog", "points")},
        "quality_two_sided": {lane: {"week": quality(two_sided_rows(rows[(rows.lane == lane) & rows.slate_date.isin(week)])),
                                     "season": quality(two_sided_rows(rows[rows.lane == lane]))}
                              for lane in ("sog", "points")},
        "two_sided_start": TWO_SIDED_START,
    }, indent=2) + "\n")
    print(f"NHL WEEKLY EVALUATION {args.as_of}: {doc.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
