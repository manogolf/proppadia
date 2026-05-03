#!/usr/bin/env python3
"""Join executed MLB graded wagers to V1 early-steam signal candidates.

CSV-only tracking. The graded file is execution truth; candidate files are signal truth.
No database reads or writes.
"""

from __future__ import annotations

import argparse
import glob
import re
import unicodedata
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PITCHER_MARKETS = {"pitcher_strikeouts", "pitcher_outs"}
PROP_TO_MARKET = {
    "strikeouts_pitching": "pitcher_strikeouts",
    "outs_recorded": "pitcher_outs",
}
PROP_ALIASES = [
    ("hits_runs_rbis", ["hits + runs + rbis", "hits + runs + rbi", "hits runs rbis", "hits runs rbi"]),
    ("runs_rbis", ["runs + rbis", "runs + rbi", "runs rbis", "runs rbi"]),
    ("strikeouts_pitching", ["pitcher strikeouts", "strikeouts pitching", "strikeouts"]),
    ("outs_recorded", ["outs recorded", "pitcher outs", "pitching outs"]),
    ("hits_allowed", ["hits allowed", "pitcher hits"]),
    ("walks_allowed", ["walks allowed"]),
    ("earned_runs", ["earned runs"]),
    ("total_bases", ["total bases"]),
    ("runs_scored", ["runs scored"]),
    ("stolen_bases", ["stolen bases"]),
    ("home_runs", ["home runs"]),
    ("strikeouts_batting", ["batter strikeouts", "hitter strikeouts"]),
    ("doubles", ["doubles"]),
    ("triples", ["triples"]),
    ("singles", ["singles"]),
    ("rbis", ["rbis", "rbi"]),
    ("walks", ["walks"]),
    ("hits", ["hits"]),
]
BET_TAIL_RE = re.compile(r"^(?P<head>.+?)\s+(?P<side>over|under)\s+(?P<line>-?\d+(?:\.\d+)?)\s*$", re.I)


def _clean_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _norm_key(value: Any) -> str:
    return _clean_text(value).lower()


def _norm_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", _clean_text(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace(",", " ")
    keep = [ch if ch.isalnum() or ch.isspace() else " " for ch in text]
    return " ".join("".join(keep).split())


def _num(value: Any) -> float:
    try:
        text = _clean_text(value).replace(",", "")
        if not text:
            return np.nan
        return float(text)
    except Exception:
        return np.nan


def _line_key(value: Any) -> str:
    v = _num(value)
    if pd.isna(v):
        return ""
    return f"{v:.3f}".rstrip("0").rstrip(".")


def _id_key(value: Any) -> str:
    v = pd.to_numeric(value, errors="coerce")
    if pd.isna(v):
        return ""
    return str(int(v))


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _first_col(df: pd.DataFrame, names: list[str]) -> str:
    lookup = {str(c).strip().lower(): str(c) for c in df.columns}
    for name in names:
        key = name.strip().lower()
        if key in lookup:
            return lookup[key]
    return ""


def _grade(value: Any) -> str:
    text = _norm_key(value)
    if text in {"w", "win", "won"}:
        return "win"
    if text in {"l", "loss", "lost"}:
        return "loss"
    if text in {"p", "push", "tie"}:
        return "push"
    if text in {"void", "cancelled", "canceled", "dnp"}:
        return text
    return text or "unknown"


def _infer_prop_type(prop_label: Any, market: Any = "") -> str:
    text = f"{_clean_text(prop_label)} {_clean_text(market)}".lower()
    text = text.replace("+", " + ")
    text = " ".join(text.split())
    for prop_type, aliases in PROP_ALIASES:
        if any(alias in text for alias in aliases):
            return prop_type
    return "unknown"


def _canonical_prop_type(value: Any, market: Any = "") -> str:
    raw = _norm_key(value).replace(" ", "_")
    if raw == "pitcher_outs":
        return "outs_recorded"
    if raw == "pitcher_strikeouts":
        return "strikeouts_pitching"
    if raw in PROP_TO_MARKET or raw in {"strikeouts_pitching"}:
        return raw
    return _infer_prop_type(value, market)


def _market_key(prop_type: Any, market: Any = "") -> str:
    raw_market = _norm_key(prop_type)
    if raw_market in PITCHER_MARKETS:
        return raw_market
    raw = _canonical_prop_type(prop_type, market)
    if raw in PROP_TO_MARKET:
        return PROP_TO_MARKET[raw]
    inferred = _infer_prop_type(prop_type, market)
    return PROP_TO_MARKET.get(inferred, inferred)


def _split_player_prop(head: str) -> tuple[str, str]:
    norm = " ".join(_clean_text(head).split())
    low = re.sub(r"[^a-z0-9]+", " ", norm.lower()).strip()
    aliases: list[tuple[str, str]] = []
    for prop_type, names in PROP_ALIASES:
        for alias in names:
            aliases.append((prop_type, re.sub(r"[^a-z0-9]+", " ", alias.lower()).strip()))
    for prop_type, alias in sorted(aliases, key=lambda item: len(item[1]), reverse=True):
        suffix = f" {alias}"
        if low.endswith(suffix):
            player = low[: -len(suffix)].strip()
            return " ".join(part.capitalize() for part in player.split()), prop_type
        if low == alias:
            return "", prop_type
    return norm, "unknown"


def _parse_bet_market(bet: Any, market: Any) -> tuple[str, str, str, float]:
    bet_text = _clean_text(bet)
    market_text = _clean_text(market)
    player = ""
    prop_type = "unknown"
    side = ""
    line = np.nan

    m = BET_TAIL_RE.match(bet_text)
    if m:
        side = _norm_key(m.group("side"))
        line = _num(m.group("line"))
        player, prop_type = _split_player_prop(m.group("head"))

    if not player or prop_type == "unknown" or pd.isna(line):
        mm = re.match(r"^(?P<head>.+?)\s+over/under\s+(?P<line>-?\d+(?:\.\d+)?)\s*$", market_text, re.I)
        if mm:
            if pd.isna(line):
                line = _num(mm.group("line"))
            m_player, m_prop_type = _split_player_prop(mm.group("head"))
            if not player:
                player = m_player
            if prop_type == "unknown":
                prop_type = m_prop_type

    if prop_type == "unknown":
        prop_type = _infer_prop_type("", market_text or bet_text)
    return player, prop_type, side, line


def _expand_paths(values: list[str]) -> list[Path]:
    paths: list[Path] = []
    for value in values:
        matches = sorted(glob.glob(value))
        if matches:
            paths.extend(Path(m) for m in matches)
        else:
            paths.append(Path(value))
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        key = str(path)
        if key not in seen:
            seen.add(key)
            out.append(path)
    return out


def load_graded(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    if df.empty:
        return pd.DataFrame()

    # Already-normalized output from report_mlb_graded_wagers.py.
    if {"report_date", "player_name", "prop_type", "side", "line", "grade", "pnl"}.issubset(df.columns):
        out = pd.DataFrame(
            {
                "date": df["report_date"].map(_date_key),
                "player_name": df["player_name"].map(_clean_text),
                "player_id": df[_first_col(df, ["player_id", "mlbam_id"])] if _first_col(df, ["player_id", "mlbam_id"]) else "",
                "prop_type": df.apply(lambda r: _canonical_prop_type(r.get("prop_type"), r.get("market")), axis=1),
                "market_key": df.apply(lambda r: _market_key(r.get("prop_type"), r.get("market")), axis=1),
                "side": df["side"].map(_norm_key),
                "line": pd.to_numeric(df["line"], errors="coerce"),
                "bookmaker": df[_first_col(df, ["book", "bookmaker", "bookmaker_key"])] if _first_col(df, ["book", "bookmaker", "bookmaker_key"]) else "",
                "price_taken": df[_first_col(df, ["price_taken", "odds", "station odds"])] if _first_col(df, ["price_taken", "odds", "station odds"]) else np.nan,
                "graded_result": df["grade"].map(_grade),
                "graded_profit": pd.to_numeric(df["pnl"], errors="coerce"),
            }
        )
    else:
        for col in ["Event Date", "Book", "Market", "Bet", "Grade", "$ W/L", "Odds", "Station Odds", "Wager ID"]:
            if col not in df.columns:
                df[col] = pd.NA
        parsed = df.apply(lambda r: _parse_bet_market(r.get("Bet"), r.get("Market")), axis=1, result_type="expand")
        parsed.columns = ["player_name", "prop_type", "side", "line"]
        out = pd.DataFrame(
            {
                "date": df["Event Date"].map(_date_key),
                "player_name": parsed["player_name"].map(_clean_text),
                "player_id": "",
                "prop_type": parsed.apply(lambda r: _canonical_prop_type(r.get("prop_type")), axis=1),
                "market_key": parsed["prop_type"].map(_market_key),
                "side": parsed["side"].map(_norm_key),
                "line": pd.to_numeric(parsed["line"], errors="coerce"),
                "bookmaker": df["Book"].map(_clean_text),
                "price_taken": pd.to_numeric(df["Odds"].where(df["Odds"].notna(), df["Station Odds"]), errors="coerce"),
                "graded_result": df["Grade"].map(_grade),
                "graded_profit": pd.to_numeric(df["$ W/L"], errors="coerce"),
            }
        )
    out["source_graded_file"] = str(path)
    out["player_key"] = out["player_name"].map(_norm_name)
    out["line_key"] = out["line"].map(_line_key)
    out["book_key"] = out["bookmaker"].map(_norm_key)
    out["player_id_key"] = out["player_id"].map(_id_key)
    out["graded_profit"] = pd.to_numeric(out["graded_profit"], errors="coerce").fillna(0.0)
    return out[out["market_key"].isin(PITCHER_MARKETS)].reset_index(drop=True)


def load_candidates(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            raise SystemExit(f"Candidates CSV not found: {path}")
        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            continue
        date_col = _first_col(df, ["date", "game_date", "slate_date"])
        player_col = _first_col(df, ["player_name", "player", "market_player_name"])
        player_id_col = _first_col(df, ["player_id", "candidate_mlbam_id", "profile_pitcher_mlbam_id"])
        market_col = _first_col(df, ["market_key", "market"])
        prop_col = _first_col(df, ["prop_type", "prop"])
        side_col = _first_col(df, ["side", "selected_side"])
        line_col = _first_col(df, ["line", "point"])
        book_col = _first_col(df, ["bookmaker_key", "bookmaker", "book"])
        price_col = _first_col(df, ["price", "current_price", "odds", "selected_american_odds", "second_price", "second_odds"])
        required = {
            "date": date_col,
            "player": player_col,
            "market": market_col,
            "side": side_col,
            "line": line_col,
        }
        missing = [name for name, col in required.items() if not col]
        if missing:
            raise SystemExit(f"Candidates CSV {path} missing required candidate fields: {missing}")
        work = pd.DataFrame(
            {
                "date": df[date_col].map(_date_key),
                "player_name": df[player_col].map(_clean_text),
                "player_id": df[player_id_col] if player_id_col else "",
                "market_key": df[market_col].map(lambda v: _market_key(v, v)),
                "prop_type": df.apply(lambda r: _canonical_prop_type(r.get(prop_col) if prop_col else r.get(market_col), r.get(market_col)), axis=1),
                "side": df[side_col].map(_norm_key),
                "line": pd.to_numeric(df[line_col], errors="coerce"),
                "candidate_bookmaker_key": df[book_col].map(_clean_text) if book_col else "",
                "price": pd.to_numeric(df[price_col], errors="coerce") if price_col else np.nan,
                "first_price": pd.to_numeric(df[_first_col(df, ["first_price", "early_price", "first_odds"])], errors="coerce")
                if _first_col(df, ["first_price", "early_price", "first_odds"])
                else np.nan,
                "second_price": pd.to_numeric(df[_first_col(df, ["second_price", "signal_price", "second_odds", "current_price", "odds"])], errors="coerce")
                if _first_col(df, ["second_price", "signal_price", "second_odds", "current_price", "odds"])
                else np.nan,
                "imp_move_early": pd.to_numeric(df[_first_col(df, ["imp_move_early", "implied_move"])], errors="coerce")
                if _first_col(df, ["imp_move_early", "implied_move"])
                else np.nan,
                "last_3_starts_outs_std": pd.to_numeric(df[_first_col(df, ["last_3_starts_outs_std", "workload_volatility"])], errors="coerce")
                if _first_col(df, ["last_3_starts_outs_std", "workload_volatility"])
                else np.nan,
                "outcome": df[_first_col(df, ["outcome", "grade", "result"])].map(_grade)
                if _first_col(df, ["outcome", "grade", "result"])
                else "",
                "pnl": pd.to_numeric(df[_first_col(df, ["pnl", "profit", "graded_profit"])], errors="coerce")
                if _first_col(df, ["pnl", "profit", "graded_profit"])
                else np.nan,
                "source_candidate_file": str(path),
            }
        )
        frames.append(work)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    out["player_key"] = out["player_name"].map(_norm_name)
    out["line_key"] = out["line"].map(_line_key)
    out["book_key"] = out["candidate_bookmaker_key"].map(_norm_key)
    out["player_id_key"] = out["player_id"].map(_id_key)
    out = out[
        out["market_key"].isin(PITCHER_MARKETS)
        & pd.to_numeric(out["imp_move_early"], errors="coerce").between(0.02, 0.05, inclusive="both")
    ].copy()
    if out["last_3_starts_outs_std"].notna().any():
        out = out[pd.to_numeric(out["last_3_starts_outs_std"], errors="coerce").ge(2.0)].copy()
    return out.reset_index(drop=True)


def _candidate_index(candidates: pd.DataFrame, cols: list[str]) -> dict[tuple[Any, ...], pd.DataFrame]:
    indexed: dict[tuple[Any, ...], pd.DataFrame] = {}
    if candidates.empty:
        return indexed
    for key, group in candidates.groupby(cols, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        indexed[key] = group
    return indexed


def _pick_candidate(matches: pd.DataFrame, graded: pd.Series) -> pd.Series:
    work = matches.copy()
    if "book_key" in work.columns and _clean_text(graded.get("book_key")):
        work["__book_match"] = work["book_key"].eq(graded.get("book_key"))
    else:
        work["__book_match"] = False
    work = work.sort_values(["__book_match", "source_candidate_file"], ascending=[False, True], kind="mergesort")
    return work.iloc[0]


def reconcile(graded: pd.DataFrame, candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if candidates.empty:
        out = graded.copy()
        for col in [
            "candidate_bookmaker_key",
            "first_price",
            "second_price",
            "imp_move_early",
            "last_3_starts_outs_std",
            "source_candidate_file",
        ]:
            out[col] = np.nan if col not in {"candidate_bookmaker_key", "source_candidate_file"} else ""
        out["matched_candidate"] = False
        out["join_strategy"] = ""
        return out, candidates

    indexes = [
        (
            "date_player_id_market_side_line",
            ["date", "player_id_key", "market_key", "side", "line_key"],
            lambda r: (r.get("date"), r.get("player_id_key"), r.get("market_key"), r.get("side"), r.get("line_key")),
        ),
        (
            "date_player_market_side_line",
            ["date", "player_key", "market_key", "side", "line_key"],
            lambda r: (r.get("date"), r.get("player_key"), r.get("market_key"), r.get("side"), r.get("line_key")),
        ),
        (
            "date_player_market_side",
            ["date", "player_key", "market_key", "side"],
            lambda r: (r.get("date"), r.get("player_key"), r.get("market_key"), r.get("side")),
        ),
    ]
    built = [(name, _candidate_index(candidates, cols), key_fn) for name, cols, key_fn in indexes]
    rows: list[dict[str, Any]] = []
    matched_candidate_indices: set[int] = set()
    for _, g in graded.iterrows():
        picked = None
        strategy = ""
        for name, idx, key_fn in built:
            key = key_fn(g)
            if any(_clean_text(v) == "" for v in key):
                continue
            matches = idx.get(tuple(key))
            if matches is not None and not matches.empty:
                picked = _pick_candidate(matches, g)
                strategy = name
                matched_candidate_indices.add(int(picked.name))
                break
        row = {
            "date": g.get("date", ""),
            "player_name": g.get("player_name", ""),
            "player_id": g.get("player_id", ""),
            "market_key": g.get("market_key", ""),
            "prop_type": g.get("prop_type", ""),
            "side": g.get("side", ""),
            "line": g.get("line", np.nan),
            "bookmaker": g.get("bookmaker", ""),
            "price_taken": g.get("price_taken", np.nan),
            "candidate_bookmaker_key": "",
            "first_price": np.nan,
            "second_price": np.nan,
            "imp_move_early": np.nan,
            "last_3_starts_outs_std": np.nan,
            "graded_result": g.get("graded_result", ""),
            "graded_profit": g.get("graded_profit", np.nan),
            "matched_candidate": picked is not None,
            "join_strategy": strategy,
            "source_graded_file": g.get("source_graded_file", ""),
            "source_candidate_file": "",
        }
        if picked is not None:
            row.update(
                {
                    "player_id": picked.get("player_id") or row["player_id"],
                    "candidate_bookmaker_key": picked.get("candidate_bookmaker_key", ""),
                    "first_price": picked.get("first_price", np.nan),
                    "second_price": picked.get("second_price", np.nan),
                    "imp_move_early": picked.get("imp_move_early", np.nan),
                    "last_3_starts_outs_std": picked.get("last_3_starts_outs_std", np.nan),
                    "source_candidate_file": picked.get("source_candidate_file", ""),
                }
            )
        rows.append(row)
    unmatched_candidates = candidates.drop(index=list(matched_candidate_indices), errors="ignore").copy()
    return pd.DataFrame(rows), unmatched_candidates


def build_missed_candidates(unmatched_candidates: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        "player_name",
        "market_key",
        "side",
        "line",
        "bookmaker_key",
        "price",
        "first_price",
        "second_price",
        "imp_move_early",
        "last_3_starts_outs_std",
        "outcome",
        "pnl",
        "reason",
    ]
    if unmatched_candidates.empty:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(
        {
            "date": unmatched_candidates.get("date", ""),
            "player_name": unmatched_candidates.get("player_name", ""),
            "market_key": unmatched_candidates.get("market_key", ""),
            "side": unmatched_candidates.get("side", ""),
            "line": unmatched_candidates.get("line", np.nan),
            "bookmaker_key": unmatched_candidates.get("candidate_bookmaker_key", ""),
            "price": unmatched_candidates.get("price", np.nan),
            "first_price": unmatched_candidates.get("first_price", np.nan),
            "second_price": unmatched_candidates.get("second_price", np.nan),
            "imp_move_early": unmatched_candidates.get("imp_move_early", np.nan),
            "last_3_starts_outs_std": unmatched_candidates.get("last_3_starts_outs_std", np.nan),
            "outcome": unmatched_candidates.get("outcome", ""),
            "pnl": unmatched_candidates.get("pnl", np.nan),
            "reason": "not_executed",
        }
    )
    return out[columns]


def _summary_block(rows: pd.DataFrame, group_cols: list[str], scope: str) -> pd.DataFrame:
    matched = rows[rows["matched_candidate"]].copy()
    if matched.empty:
        return pd.DataFrame(columns=["scope", *group_cols, "rows", "wins", "losses", "pushes", "profit_units", "roi", "win_rate"])
    matched["is_win"] = matched["graded_result"].eq("win")
    matched["is_loss"] = matched["graded_result"].eq("loss")
    matched["is_push"] = matched["graded_result"].eq("push")
    out = (
        matched.groupby(group_cols, dropna=False)
        .agg(
            rows=("matched_candidate", "size"),
            wins=("is_win", "sum"),
            losses=("is_loss", "sum"),
            pushes=("is_push", "sum"),
            profit_units=("graded_profit", "sum"),
        )
        .reset_index()
    )
    resolved = out["wins"] + out["losses"]
    out["roi"] = np.where(out["rows"] > 0, out["profit_units"] / out["rows"], np.nan)
    out["win_rate"] = np.where(resolved > 0, out["wins"] / resolved, np.nan)
    out.insert(0, "scope", scope)
    return out


def build_summary(rows: pd.DataFrame, candidates: pd.DataFrame, unmatched_candidates: pd.DataFrame) -> pd.DataFrame:
    total = int(len(rows))
    matched = rows[rows["matched_candidate"]].copy()
    v1_candidate_rows = int(len(candidates))
    missed_candidate_count = int(len(unmatched_candidates))
    wins = int(matched["graded_result"].eq("win").sum())
    losses = int(matched["graded_result"].eq("loss").sum())
    pushes = int(matched["graded_result"].eq("push").sum())
    profit = float(pd.to_numeric(matched["graded_profit"], errors="coerce").fillna(0.0).sum())
    resolved = wins + losses
    matched_roi = profit / len(matched) if len(matched) else np.nan

    candidate_pnl = pd.to_numeric(candidates.get("pnl", pd.Series(dtype="float64")), errors="coerce")
    has_candidate_pnl = bool(v1_candidate_rows and candidate_pnl.notna().any())
    hypothetical_profit = float(candidate_pnl.fillna(0.0).sum()) if has_candidate_pnl else np.nan
    hypothetical_roi = hypothetical_profit / v1_candidate_rows if has_candidate_pnl and v1_candidate_rows else np.nan
    execution_vs_signal_gap = matched_roi - hypothetical_roi if pd.notna(matched_roi) and pd.notna(hypothetical_roi) else np.nan
    overall = pd.DataFrame(
        [
            {
                "scope": "overall",
                "total_graded_wagers": total,
                "matched_v1_candidates": int(len(matched)),
                "unmatched_graded_wagers": int(total - len(matched)),
                "v1_candidate_rows": v1_candidate_rows,
                "v1_candidates_not_executed": missed_candidate_count,
                "execution_rate": int(len(matched)) / v1_candidate_rows if v1_candidate_rows else np.nan,
                "missed_candidate_count": missed_candidate_count,
                "missed_candidate_rate": missed_candidate_count / v1_candidate_rows if v1_candidate_rows else np.nan,
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "profit_units": profit,
                "roi": matched_roi,
                "win_rate": wins / resolved if resolved else np.nan,
                "matched_candidate_profit_units": profit,
                "matched_candidate_roi": matched_roi,
                "hypothetical_all_candidate_profit_units": hypothetical_profit,
                "hypothetical_all_candidate_roi": hypothetical_roi,
                "execution_vs_signal_gap": execution_vs_signal_gap,
            }
        ]
    )
    blocks = [
        overall,
        _summary_block(rows, ["market_key"], "by_market_key"),
        _summary_block(rows, ["side"], "by_side"),
        _summary_block(rows, ["market_key", "side"], "by_market_key_side"),
        _summary_block(rows, ["date"], "by_date"),
    ]
    return pd.concat(blocks, ignore_index=True, sort=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Example: python backend/mlb/scripts/reconcile_mlb_v1_signal_tracking.py "
            "--graded-bets-csv tmp/graded/8rainstation_daily_2026-04-22_mlb_player_props.csv "
            "--candidates-csv 'tmp/mlb_v1_candidates_*.csv'"
        ),
    )
    parser.add_argument("--graded-bets-csv", required=True)
    parser.add_argument("--candidates-csv", action="append", required=True, help="Repeatable path or glob.")
    parser.add_argument("--out-csv", default="tmp/mlb_v1_signal_tracking.csv")
    parser.add_argument("--summary-csv", default="tmp/mlb_v1_signal_tracking_summary.csv")
    parser.add_argument("--missed-candidates-csv", default="tmp/mlb_v1_signal_missed_candidates.csv")
    args = parser.parse_args()

    graded_path = Path(args.graded_bets_csv)
    if not graded_path.exists():
        raise SystemExit(
            "Graded bets CSV not found. Expected a raw 8rainstation MLB player props CSV with columns like "
            "'Event Date', 'Market', 'Bet', 'Book', 'Grade', '$ W/L', 'Odds', or a normalized graded rows CSV "
            "with 'report_date', 'player_name', 'prop_type', 'side', 'line', 'grade', 'pnl'. "
            "Example: --graded-bets-csv tmp/graded/8rainstation_daily_2026-04-22_mlb_player_props.csv"
        )

    candidate_paths = _expand_paths(args.candidates_csv)
    missing = [str(path) for path in candidate_paths if not path.exists()]
    if missing:
        raise SystemExit(f"Candidate CSV path/glob did not resolve to existing files: {missing}")

    graded = load_graded(graded_path)
    candidates = load_candidates(candidate_paths)
    rows, unmatched_candidates = reconcile(graded, candidates)
    missed_candidates = build_missed_candidates(unmatched_candidates)
    summary = build_summary(rows, candidates, unmatched_candidates)

    out_csv = Path(args.out_csv)
    summary_csv = Path(args.summary_csv)
    missed_candidates_csv = Path(args.missed_candidates_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    missed_candidates_csv.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(out_csv, index=False)
    summary.to_csv(summary_csv, index=False)
    missed_candidates.to_csv(missed_candidates_csv, index=False)

    overall = summary[summary["scope"].eq("overall")].iloc[0].to_dict()
    print(
        "[mlb-v1-signal-tracking] "
        f"graded_wagers={int(overall.get('total_graded_wagers', 0) or 0)} "
        f"matched={int(overall.get('matched_v1_candidates', 0) or 0)} "
        f"unmatched={int(overall.get('unmatched_graded_wagers', 0) or 0)} "
        f"candidate_rows={int(overall.get('v1_candidate_rows', 0) or 0)} "
        f"not_executed={int(overall.get('v1_candidates_not_executed', 0) or 0)} "
        f"execution_rate={float(overall.get('execution_rate', np.nan)):.3f} "
        f"profit={float(overall.get('profit_units', 0) or 0):.3f} "
        f"roi={float(overall.get('roi', np.nan)):.3f} "
        f"out_csv={out_csv} summary_csv={summary_csv} missed_candidates_csv={missed_candidates_csv}"
    )
    if not rows.empty:
        print("[mlb-v1-signal-tracking] matched rows")
        print(rows[rows["matched_candidate"]].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
