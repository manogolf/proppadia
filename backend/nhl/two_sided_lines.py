"""Two-sided (executable) line level for the daily performance summary.

A graded prop prediction is on a two-sided line when the same book or
prediction market quoted both Over and Under at its exact line before the
prediction. Quote matching and timing reuse the candidate evaluation export
(`export_nhl_all_candidate_evaluation --price-both-sides`): retained
EARLY/FINAL_PREGAME snapshots only (no CLOSING), and a quote counts only when
its source and observation timestamps precede the prediction timestamp, which
precedes the scheduled start. This is the second W/L level used by the
2026-10-23 executable-line analysis. It is descriptive; it never changes the
all-predictions grade.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.scripts import export_nhl_all_candidate_evaluation as candidate_export

MARKETS = {"sog": "shots_on_goal", "points": "player_points"}
DEFINITION = ("same book or prediction market quoted both Over and Under at the exact line, "
              "in an EARLY/FINAL_PREGAME snapshot whose quote and observation precede the prediction")
KEY_COLUMNS = ("game_id", "player_id", "line")


def load_evidence(operational_root: Path, slate_date: str) -> dict[str, Any]:
    """Return two-sided keys per lane and the export's Points production rows."""
    candidates, _ = candidate_export.load_candidates(Path(operational_root))
    # Production SOG candidates come from the slate's retained production capture.
    candidates = candidate_export.add_captured_sog_arms(
        [row for row in candidates if row["slate_date"] == slate_date], Path(operational_root), slate_date)
    candidates = [row for row in candidates
                  if row["market_key"] in MARKETS.values() and row["model_role"] == "PRODUCTION"]
    scoped, _ = candidate_export.scope_regular_season_pregame(candidates)
    candidates = candidate_export.deduplicate_candidates(scoped)
    quotes, _ = candidate_export.load_retained_quotes(
        Path(operational_root), candidates, price_both_sides=True)
    quotes = candidate_export.deduplicate_quotes(quotes)
    sides: dict[tuple, set[str]] = {}
    for quote in quotes:
        key = (quote["market_key"], int(float(quote["game_id"])), int(float(quote["player_id"])),
               float(quote["market_line"]), str(quote["sportsbook_key"]))
        sides.setdefault(key, set()).add(str(quote["quote_side"]))
    # A lane with no production candidates has no evidence, not zero two-sided lines.
    present = {row["market_key"] for row in candidates}
    keys = {lane: ({key[1:4] for key, quoted in sides.items()
                    if key[0] == market and {"OVER", "UNDER"} <= quoted}
                   if market in present else None)
            for lane, market in MARKETS.items()}
    points = pd.DataFrame([row for row in candidates if row["market_key"] == MARKETS["points"]])
    return {"keys": keys, "points_rows": _export_rows(points),
            "quote_count": len(quotes), "candidate_count": len(candidates)}


def _export_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Settled export candidates as game/player/line/side/WIN-LOSS rows."""
    columns = [*KEY_COLUMNS, "selected_side", "grading_state"]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    settled = frame[frame.outcome_status.eq("SETTLED")].copy()
    settled["line"] = pd.to_numeric(settled.market_line, errors="raise")
    settled["grading_state"] = settled.outcome_side.eq(settled.selected_side).map(
        {True: "WIN", False: "LOSS"})
    return settled[columns]


def _record(rows: pd.DataFrame) -> dict[str, Any]:
    wins = int(rows.grading_state.eq("WIN").sum())
    losses = int(rows.grading_state.eq("LOSS").sum())
    return {"wins": wins, "losses": losses, "settled": wins + losses,
            "win_rate": wins / (wins + losses) if wins + losses else None}


def level(rows: pd.DataFrame, keys: set[tuple[int, int, float]]) -> dict[str, Any]:
    """All-predictions and two-sided W/L overall, by side, by line and line x side."""
    rows = rows[rows.grading_state.isin(["WIN", "LOSS"])].copy()
    rows["line"] = pd.to_numeric(rows.line, errors="raise").astype(float)
    identity = zip(rows.game_id.astype("int64"), rows.player_id.astype("int64"), rows.line)
    rows["two_sided"] = [(int(g), int(p), float(line)) in keys for g, p, line in identity]
    side = rows.selected_side.astype(str).str.upper()

    def both(mask: pd.Series) -> dict[str, Any]:
        return {"all": _record(rows[mask]), "two_sided": _record(rows[mask & rows.two_sided])}

    everything = pd.Series(True, index=rows.index)
    by_line = {}
    for value in sorted(rows.line.unique().tolist()):
        on_line = rows.line.eq(value)
        by_line[f"{value:g}"] = {**both(on_line), "by_side": {
            name: both(on_line & side.eq(name)) for name in ("OVER", "UNDER")}}
    return {
        "status": "AVAILABLE", "definition": DEFINITION,
        "affects_grading_denominator": False,
        **both(everything),
        "by_side": {name: both(side.eq(name)) for name in ("OVER", "UNDER")},
        "by_line": by_line,
    }


def unavailable(reason: str) -> dict[str, Any]:
    return {"status": "UNAVAILABLE", "reason": reason, "definition": DEFINITION,
            "affects_grading_denominator": False}


def render(context: dict[str, Any] | None, title: str) -> list[str]:
    """Markdown table: all predictions next to two-sided lines only."""
    context = context or {}
    if context.get("status") != "AVAILABLE":
        return ["", f"{title} — two-sided lines: {context.get('status', 'UNAVAILABLE')}"
                + (f" ({context['reason']})" if context.get("reason") else "")]

    def cell(record: dict[str, Any]) -> str:
        if not record.get("settled"):
            return "—"
        return f"{record['wins']}-{record['losses']} ({100 * record['win_rate']:.1f}%)"

    lines = ["", f"{title} — two-sided lines only (same book quoted Over and Under before the prediction):",
             "| Line | Side | All predictions | Two-sided lines | Two-sided picks |",
             "|---:|:---|---:|---:|---:|"]
    for line, values in context.get("by_line", {}).items():
        lines.append(f"| {line} | All | {cell(values['all'])} | {cell(values['two_sided'])} | "
                     f"{values['two_sided']['settled']} |")
        for name, sided in values.get("by_side", {}).items():
            if sided["all"]["settled"]:
                lines.append(f"| {line} | {name.title()} | {cell(sided['all'])} | "
                             f"{cell(sided['two_sided'])} | {sided['two_sided']['settled']} |")
    lines.append(f"| **Total** | | **{cell(context['all'])}** | **{cell(context['two_sided'])}** | "
                 f"**{context['two_sided']['settled']}** |")
    return lines
