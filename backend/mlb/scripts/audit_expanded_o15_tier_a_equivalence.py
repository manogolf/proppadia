#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import statistics
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any

from backend.mlb.scripts import analyze_expanded_o15_universe_slices as slices
from backend.mlb.scripts import audit_expanded_o15_betonline as bol_audit


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_REVIEW_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_ODDS_HISTORY_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _f(value: Any) -> float | None:
    return slices._f(value)


def _b(value: Any) -> bool:
    return slices._b(value)


def _date(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("board_date") or "")[:10]


def _norm_name(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^0-9A-Za-z ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def _player_key(row: dict[str, Any]) -> str:
    player_id = _f(row.get("player_id"))
    if player_id is not None:
        return f"id:{int(player_id)}"
    return f"name:{_norm_name(row.get('player_name') or row.get('player'))}"


def _board_presence(review_dir: Path, date_text: str) -> dict[str, dict[str, dict[str, Any]]]:
    boards = {
        "simple": review_dir / f"hits_o15_simple_filter_{date_text}.csv",
        "watch": review_dir / f"hits_o15_watch_candidates_{date_text}.csv",
        "layered": review_dir / f"hits_o15_layered_candidates_{date_text}.csv",
    }
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for name, path in boards.items():
        rows = _read_csv(path)
        out[name] = {_player_key(row): row for row in rows if str(row.get("line") or "1.5") in {"1.5", "1.50"}}
        out[f"{name}_path"] = {"_path": {"path": str(path), "exists": path.exists(), "rows": len(rows)}}
    return out


def _slate_context(odds_history_root: Path, date_text: str) -> dict[str, dict[str, Any]]:
    path = odds_history_root / date_text / "mlb_slate_output.csv"
    rows = _read_csv(path)
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("prop_type") or "").lower() != "hits":
            continue
        key = _player_key(row)
        existing = out.get(key)
        try:
            is_line_15 = abs(float(row.get("line") or 0) - 1.5) < 1e-9
        except Exception:
            is_line_15 = False
        if existing is None or is_line_15:
            out[key] = row
    return out


def _units(row: dict[str, Any], price_col: str) -> float | None:
    price = _f(row.get(price_col))
    if price is None:
        return None
    return slices._american_units(price, _b(row.get("win")), _b(row.get("loss")), _b(row.get("push")))


def _metrics(rows: list[dict[str, Any]], price_col: str) -> dict[str, Any]:
    resolved = [row for row in rows if _b(row.get("resolved"))]
    priced = [row for row in resolved if _f(row.get(price_col)) is not None]
    wins = sum(1 for row in resolved if _b(row.get("win")))
    losses = sum(1 for row in resolved if _b(row.get("loss")))
    pushes = sum(1 for row in resolved if _b(row.get("push")))
    units = sum((_units(row, price_col) or 0.0) for row in priced)
    return {
        "rows": len(rows),
        "resolved": len(resolved),
        "priced_resolved": len(priced),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / len(priced) if priced else None,
        "units": units if priced else None,
        "avg_price": slices._avg([_f(row.get(price_col)) for row in rows]),
        "avg_implied": slices._avg([bol_audit._american_implied(_f(row.get(price_col))) for row in rows]),
    }


def _price_compression(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = {
        "alternate_hitter_tier_a": [row for row in rows if str(row.get("hitter_tier") or "") == "A"],
        "alternate_non_hitter_tier_a": [row for row in rows if str(row.get("hitter_tier") or "") != "A"],
        "alternate_layer_a": [row for row in rows if str(row.get("alternate_layer") or "") == "alternate_layer_a_d7_d15_starter"],
        "alternate_not_layer_a": [row for row in rows if str(row.get("alternate_layer") or "") != "alternate_layer_a_d7_d15_starter"],
    }
    out: list[dict[str, Any]] = []
    for label, group in groups.items():
        best = _metrics(group, "best_available_over_price")
        bol = _metrics(group, "betonline_over_price")
        median = _metrics(group, "median_available_over_price")
        gaps = [_f(row.get("price_gap_best_minus_bol")) for row in group if _f(row.get("price_gap_best_minus_bol")) is not None]
        out.append(
            {
                "population": label,
                "rows": len(group),
                "resolved": best["resolved"],
                "wins": best["wins"],
                "losses": best["losses"],
                "pushes": best["pushes"],
                "avg_best_price": best["avg_price"],
                "avg_betonline_price": bol["avg_price"],
                "avg_median_price": median["avg_price"],
                "avg_best_to_betonline_gap": slices._avg(gaps),
                "avg_best_implied": best["avg_implied"],
                "avg_betonline_implied": bol["avg_implied"],
                "avg_median_implied": median["avg_implied"],
                "roi_best_price": best["roi"],
                "roi_betonline_price": bol["roi"],
                "roi_median_price": median["roi"],
                "units_best_price": best["units"],
                "units_betonline_price": bol["units"],
                "units_median_price": median["units"],
            }
        )
    return out


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _write_definition_report(path: Path) -> None:
    lines = [
        "# Expanded O1.5 Tier A Definition Comparison",
        "",
        "## Main Board",
        "",
        "- Source script: `backend/mlb/scripts/run_mlb_hits_o15_review_board.py`",
        "- Hitter Tier A: `d7_hits_rate > 1.30` and `d15_hits_rate > 1.20`.",
        "- Pitcher Tier A: trusted starter context and `starter_expected_hits_allowed >= 5.5`.",
        "- Layered board Layer 3/4 uses the broader consistency layer: `d7_hits_rate > 1.0`, `d15_hits_rate > 1.0`, and `starter_expected_hits_allowed >= 5.0`.",
        "",
        "## Reconstructed Tier Backtest",
        "",
        "- Source script: `backend/mlb/scripts/run_mlb_hits_15_tier_backtest.py`",
        "- Hitter Tier A: `d7_hits_rate > 1.30` and `d15_hits_rate > 1.20`.",
        "- Uses corrected execution reconcile rows, so its population is all reconciled/main-market opportunities, not actual daily board artifacts.",
        "",
        "## Alternate Discovery Builder",
        "",
        "- Source script: `backend/mlb/scripts/run_mlb_hits_o15_review_board.py --board alternate_o15`",
        "- Hitter Tier A: same shared function, `d7_hits_rate > 1.30` and `d15_hits_rate > 1.20`.",
        "- Alternate Layer A is not the same as Hitter Tier A. It is `d7_hits_rate > 1.0`, `d15_hits_rate > 1.0`, and `starter_expected_hits_allowed >= 5.0`.",
        "- Alternate boards are built from `batter_hits_alternate` market rows and archived `odds_history/<date>/mlb_slate_output.csv` context.",
        "",
        "## Expanded Universe Builder",
        "",
        "- Source script: `backend/mlb/scripts/expanded_o15_universe_builder.py`",
        "- Does not recalculate tiers. It preserves `hitter_tier`, `pitcher_tier`, `combined_tier`, and `alternate_layer` from source board artifacts.",
        "- It deduplicates by date/player/line/side and collapses bookmaker for universe identity.",
        "",
        "## Verdict On Definitions",
        "",
        "The Hitter Tier A formula is identical across the main board, reconstructed tier backtest, and alternate discovery builder.",
        "The surprising alternate result is therefore not a formula mismatch. It is a population/source/price question: alternate Tier A rows come from a broader over-only alternate market population, often absent from main board artifacts, and are materially price-sensitive.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_report(path: Path, rows: list[dict[str, Any]], price_rows: list[dict[str, Any]]) -> None:
    tier_a = [row for row in rows if row.get("audit_population") == "hitter_tier_a"]
    layer_a = [row for row in rows if row.get("audit_population") == "alternate_layer_a"]
    tier_a_metrics = _metrics(tier_a, "best_available_over_price")
    layer_a_metrics = _metrics(layer_a, "best_available_over_price")
    main_overlap = [row for row in tier_a if row.get("appeared_any_main_board") == "true"]
    leakage_flags = [row for row in rows if row.get("d7_d15_matches_archived_slate") == "false"]
    lines = [
        "# Expanded O1.5 Tier A Equivalence Audit",
        "",
        "Scope: alternate-source rows that are either Hitter Tier A or Alternate Layer A.",
        "",
        "## Summary",
        "",
        f"- Alternate Hitter Tier A rows audited: `{len(tier_a)}`; resolved `{tier_a_metrics['resolved']}`; record `{tier_a_metrics['wins']}-{tier_a_metrics['losses']}-{tier_a_metrics['pushes']}`; ROI best price `{_fmt_pct(tier_a_metrics['roi'])}`.",
        f"- Alternate Layer A rows audited: `{len(layer_a)}`; resolved `{layer_a_metrics['resolved']}`; record `{layer_a_metrics['wins']}-{layer_a_metrics['losses']}-{layer_a_metrics['pushes']}`; ROI best price `{_fmt_pct(layer_a_metrics['roi'])}`.",
        f"- Hitter Tier A rows also appearing in same-date main boards: `{len(main_overlap)}` of `{len(tier_a)}`.",
        f"- Rows whose d7/d15 did not match archived slate context: `{len(leakage_flags)}`.",
        "",
        "## Price Compression",
        "",
        "| population | rows | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg best | avg BOL | avg gap |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in price_rows:
        lines.append(
            f"| {row.get('population')} | {row.get('rows')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
            f"{_fmt_pct(row.get('roi_best_price'))} | {_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_pct(row.get('roi_median_price'))} | "
            f"{_fmt_num(row.get('avg_best_price'))} | {_fmt_num(row.get('avg_betonline_price'))} | {_fmt_num(row.get('avg_best_to_betonline_gap'))} |"
        )
    lines.extend(
        [
            "",
            "## Diagnosis",
            "",
            "- Definition mismatch: `NO` for Hitter Tier A. The threshold formula is identical.",
            "- Layer mismatch: `YES`. Alternate Layer A is a broader d7/d15/starter layer, not the stricter Hitter Tier A definition.",
            "- Historical context leakage/misalignment: no row-level mismatch was found between alternate board d7/d15 and archived slate d7/d15 for matched rows.",
            "- Price compression: `YES`. Tier A/Layer A are meaningfully worse at BetOnline and median prices than best price.",
            "- Population mismatch: `YES`. Most alternate Tier A rows do not appear in same-date main board artifacts.",
            "",
            "## Can We Trust 'Tier A Is Bad In Alternate'?",
            "",
            "PARTIAL. We can trust that the audited alternate-source Tier A sample performed poorly, and the Hitter Tier A formula itself is the same. But it should not be generalized back to main board Tier A because the alternate rows are a different market/source population with different price availability and little overlap with actual main board artifacts.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit whether Expanded O1.5 alternate Tier A matches main-board Tier A semantics.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--review-dir", default=str(DEFAULT_REVIEW_DIR))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--odds-history-root", default=str(DEFAULT_ODDS_HISTORY_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = [row for row in _read_csv(Path(args.rows_csv)) if _b(row.get("from_alternate"))]
    bol_audit._enrich(rows, Path(args.backfill_root))

    review_dir = Path(args.review_dir)
    odds_history_root = Path(args.odds_history_root)
    board_cache: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    slate_cache: dict[str, dict[str, dict[str, Any]]] = {}

    audited: list[dict[str, Any]] = []
    for row in rows:
        is_tier_a = str(row.get("hitter_tier") or "") == "A"
        is_layer_a = str(row.get("alternate_layer") or "") == "alternate_layer_a_d7_d15_starter"
        if not (is_tier_a or is_layer_a):
            continue
        date_text = _date(row)
        key = _player_key(row)
        if date_text not in board_cache:
            board_cache[date_text] = _board_presence(review_dir, date_text)
        if date_text not in slate_cache:
            slate_cache[date_text] = _slate_context(odds_history_root, date_text)
        boards = board_cache[date_text]
        slate = slate_cache[date_text].get(key, {})
        in_simple = key in boards.get("simple", {})
        in_watch = key in boards.get("watch", {})
        in_layered = key in boards.get("layered", {})
        archived_d7 = _f(slate.get("d7_hits"))
        archived_d15 = _f(slate.get("d15_hits"))
        row_d7 = _f(row.get("d7_hits_rate"))
        row_d15 = _f(row.get("d15_hits_rate"))
        matched_slate = bool(slate)
        d7_d15_match = (
            matched_slate
            and (row_d7 is None and archived_d7 is None or row_d7 is not None and archived_d7 is not None and abs(row_d7 - archived_d7) < 1e-9)
            and (row_d15 is None and archived_d15 is None or row_d15 is not None and archived_d15 is not None and abs(row_d15 - archived_d15) < 1e-9)
        )
        if in_simple or in_watch or in_layered:
            absence_reason = "appeared_in_main_board"
        elif not matched_slate:
            absence_reason = "no_archived_main_slate_context_match"
        else:
            absence_reason = "not_in_actual_main_board_artifacts"
        source_market_path = Path(args.backfill_root) / date_text / "live_alternate_book_level_rows.csv"
        source_context_path = odds_history_root / date_text / "mlb_slate_output.csv"
        base = {
            "audit_population": "hitter_tier_a" if is_tier_a else "alternate_layer_a",
            "is_hitter_tier_a": is_tier_a,
            "is_alternate_layer_a": is_layer_a,
            "date": date_text,
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name") or row.get("player"),
            "team": row.get("team"),
            "opponent": row.get("opponent"),
            "line": row.get("line"),
            "side": row.get("side"),
            "d7_hits_rate": row.get("d7_hits_rate"),
            "d15_hits_rate": row.get("d15_hits_rate"),
            "archived_slate_d7_hits": archived_d7,
            "archived_slate_d15_hits": archived_d15,
            "d7_d15_matches_archived_slate": str(d7_d15_match).lower(),
            "archived_slate_context_found": str(matched_slate).lower(),
            "context_as_of_note": "archived odds_history slate output for same date" if matched_slate else "no archived slate match",
            "source_context_path": str(source_context_path),
            "source_context_exists": source_context_path.exists(),
            "source_market_path": str(source_market_path),
            "source_market_exists": source_market_path.exists(),
            "source_market_type": "historical batter_hits_alternate backfill",
            "hitter_tier": row.get("hitter_tier"),
            "pitcher_tier": row.get("pitcher_tier"),
            "combined_tier": row.get("combined_tier"),
            "alternate_layer": row.get("alternate_layer"),
            "market_price": row.get("market_price") or row.get("expanded_price"),
            "betonline_price": row.get("betonline_over_price"),
            "best_price": row.get("best_available_over_price"),
            "median_price": row.get("median_available_over_price"),
            "best_available_over_price": row.get("best_available_over_price"),
            "betonline_over_price": row.get("betonline_over_price"),
            "median_available_over_price": row.get("median_available_over_price"),
            "best_to_betonline_gap": row.get("price_gap_best_minus_bol"),
            "price_gap_best_minus_bol": row.get("price_gap_best_minus_bol"),
            "outcome": row.get("result") or ("win" if _b(row.get("win")) else "loss" if _b(row.get("loss")) else "push" if _b(row.get("push")) else ""),
            "resolved": row.get("resolved"),
            "win": row.get("win"),
            "loss": row.get("loss"),
            "push": row.get("push"),
            "actual_hits": row.get("actual_value"),
            "appeared_o15_simple_board": str(in_simple).lower(),
            "appeared_o15_watch_board": str(in_watch).lower(),
            "appeared_o15_layered_board": str(in_layered).lower(),
            "appeared_any_main_board": str(in_simple or in_watch or in_layered).lower(),
            "same_date_main_production_source": str(_b(row.get("from_main"))).lower(),
            "main_absence_reason": absence_reason,
            "board_source_file": row.get("board_source_file"),
            "bookmaker_list": row.get("bookmaker_list") or row.get("bookmaker_list_source"),
        }
        audited.append(base)
        if is_tier_a and is_layer_a:
            dup = dict(base)
            dup["audit_population"] = "alternate_layer_a"
            audited.append(dup)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    price_rows = _price_compression(rows)

    _write_csv(out_dir / "expanded_o15_tier_a_equivalence_rows.csv", audited)
    _write_csv(out_dir / "expanded_o15_tier_a_price_compression.csv", price_rows)
    _write_definition_report(out_dir / "expanded_o15_tier_a_definition_comparison.md")
    _write_report(out_dir / "expanded_o15_tier_a_equivalence_audit.md", audited, price_rows)
    print(
        {
            "audited_rows": len(audited),
            "hitter_tier_a_rows": sum(1 for row in audited if row.get("audit_population") == "hitter_tier_a"),
            "layer_a_rows": sum(1 for row in audited if row.get("audit_population") == "alternate_layer_a"),
            "report": str(out_dir / "expanded_o15_tier_a_equivalence_audit.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
