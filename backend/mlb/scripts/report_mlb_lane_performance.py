#!/usr/bin/env python3
"""Report MLB lane performance across recent outcome-backed reconcile dates."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd


DEFAULT_EXEC_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_QUICK_ROOT = Path("backend/mlb/exports/quick_card")
DEFAULT_V1_ROOT = Path("backend/mlb/exports/v1_wagers")
DEFAULT_TOP_RANK_ROOT = Path("backend/mlb/exports/top_rank")
DEFAULT_DAILY_OUT = Path("backend/mlb/exports/model_performance/lane_performance_daily.csv")
DEFAULT_SUMMARY_OUT = Path("backend/mlb/exports/model_performance/lane_performance_summary_30d.csv")

KEY_COLUMNS = ["date_norm", "player_name_norm", "prop_type_norm", "side_norm", "line_norm"]
RECONCILE_REQUIRED = {
    "game_date",
    "player_name",
    "line",
    "actual_over_outcome",
    "actual_under_outcome",
    "pnl_over_1u",
    "pnl_under_1u",
}

PROP_TO_MARKET = {
    "hits": "batter_hits",
    "total_bases": "batter_total_bases",
    "hits_runs_rbis": "batter_hits_runs_rbis",
    "runs_scored": "batter_runs_scored",
    "rbis": "batter_rbis",
    "walks": "batter_walks",
    "doubles": "batter_doubles",
    "strikeouts_batting": "batter_strikeouts",
    "hits_allowed": "pitcher_hits_allowed",
    "earned_runs": "pitcher_earned_runs",
    "walks_allowed": "pitcher_walks",
    "strikeouts_pitching": "pitcher_strikeouts",
    "outs_recorded": "pitcher_outs",
}
MARKET_TO_PROP = {v: k for k, v in PROP_TO_MARKET.items()}
MARKET_TO_PROP.update(
    {
        "pitcher_outs": "outs_recorded",
        "outs_recorded": "outs_recorded",
        "pitching_outs": "outs_recorded",
        "pitcher_strikeouts": "strikeouts_pitching",
    }
)


def _clean(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _norm_name(value: Any) -> str:
    text = _clean(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _date_norm(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _side_norm(value: Any) -> str:
    text = _clean(value).lower()
    if text.startswith("o"):
        return "over"
    if text.startswith("u"):
        return "under"
    return text


def _line_norm(value: Any) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return ""
    return f"{float(number):.3f}".rstrip("0").rstrip(".")


def _prop_type_norm(value: Any) -> str:
    text = _clean(value).lower().replace(" ", "_")
    return MARKET_TO_PROP.get(text, text)


def _market_to_prop(value: Any) -> str:
    text = _clean(value).lower().replace(" ", "_")
    return MARKET_TO_PROP.get(text, text)


def _available_dates(exec_root: Path, limit: int) -> list[str]:
    if not exec_root.exists():
        return []
    dates: list[str] = []
    for path in exec_root.iterdir():
        if not path.is_dir():
            continue
        name = path.name
        if re.fullmatch(r"20\d\d-\d\d-\d\d", name) and (path / "reconcile_rows.csv").exists():
            dates.append(name)
    return sorted(dates)[-int(limit) :]


def _read_csv(path: Path, required: set[str]) -> tuple[pd.DataFrame | None, str | None]:
    if not path.exists():
        return None, f"missing_file:{path}"
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        return None, f"read_error:{path}:{type(exc).__name__}"
    missing = sorted(required - set(df.columns))
    if missing:
        return None, f"missing_columns:{path}:{','.join(missing)}"
    return df, None


def _prepare_reconcile(path: Path) -> tuple[pd.DataFrame | None, str | None]:
    rec, err = _read_csv(path, RECONCILE_REQUIRED)
    if err or rec is None:
        return None, err

    market_col = "market_key" if "market_key" in rec.columns else "prop_type"
    if market_col not in rec.columns:
        return None, f"missing_columns:{path}:market_key_or_prop_type"

    sides: list[pd.DataFrame] = []
    for side in ("over", "under"):
        frame = rec.copy()
        frame["date_norm"] = frame["game_date"].map(_date_norm)
        frame["player_name_norm"] = frame["player_name"].map(_norm_name)
        if market_col == "market_key":
            frame["prop_type_norm"] = frame[market_col].map(_market_to_prop)
        else:
            frame["prop_type_norm"] = frame[market_col].map(_prop_type_norm)
        frame["side_norm"] = side
        frame["line_norm"] = frame["line"].map(_line_norm)
        frame["result"] = frame["actual_over_outcome" if side == "over" else "actual_under_outcome"].map(
            lambda v: _clean(v).lower()
        )
        frame["pnl"] = pd.to_numeric(frame["pnl_over_1u" if side == "over" else "pnl_under_1u"], errors="coerce")
        sides.append(frame)

    out = pd.concat(sides, ignore_index=True)
    out = out[out["result"].isin({"win", "loss", "push"})].copy()
    out["bet_win"] = out["result"].eq("win")
    out = out[KEY_COLUMNS + ["bet_win", "result", "pnl"]].drop_duplicates(KEY_COLUMNS, keep="first")
    return out, None


def _prepare_lane(path: Path, lane: str) -> tuple[pd.DataFrame | None, str | None]:
    required = {"date", "player_name", "side", "line"}
    if lane in {"quick_card", "top_rank"}:
        required.add("prop_type")
    elif lane == "v1":
        required.add("market_key")
    else:
        return None, f"unknown_lane:{lane}"

    df, err = _read_csv(path, required)
    if err or df is None:
        return None, err

    out = df.copy()
    out["date_norm"] = out["date"].map(_date_norm)
    out["player_name_norm"] = out["player_name"].map(_norm_name)
    if lane == "v1":
        out["prop_type_norm"] = out["market_key"].map(_market_to_prop)
    else:
        out["prop_type_norm"] = out["prop_type"].map(_prop_type_norm)
    out["side_norm"] = out["side"].map(_side_norm)
    out["line_norm"] = out["line"].map(_line_norm)
    out = out[KEY_COLUMNS].drop_duplicates(KEY_COLUMNS, keep="first")
    return out, None


def _metrics(date_text: str, lane: str, joined: pd.DataFrame, skipped_reason: str = "") -> dict[str, Any]:
    if skipped_reason:
        return {
            "date": date_text,
            "lane": lane,
            "bets": 0,
            "wins": 0,
            "losses": 0,
            "pushes": 0,
            "profit_units": np.nan,
            "win_rate": np.nan,
            "roi": np.nan,
            "status": "skipped",
            "skipped_reason": skipped_reason,
        }

    resolved = joined[joined["result"].isin({"win", "loss", "push"})].copy()
    bets = int(len(resolved))
    wins = int(resolved["result"].eq("win").sum())
    losses = int(resolved["result"].eq("loss").sum())
    pushes = int(resolved["result"].eq("push").sum())
    profit = float(pd.to_numeric(resolved["pnl"], errors="coerce").sum()) if bets else 0.0
    return {
        "date": date_text,
        "lane": lane,
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "profit_units": profit,
        "win_rate": wins / bets if bets else np.nan,
        "roi": profit / bets if bets else np.nan,
        "status": "ok",
        "skipped_reason": "",
    }


def _score_lane(date_text: str, lane: str, lane_df: pd.DataFrame, rec: pd.DataFrame) -> dict[str, Any]:
    joined = lane_df.merge(rec, on=KEY_COLUMNS, how="left")
    return _metrics(date_text, lane, joined)


def _summary(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    ok = daily[daily["status"].eq("ok")].copy()
    for lane, group in ok.groupby("lane", dropna=False):
        total_bets = int(pd.to_numeric(group["bets"], errors="coerce").sum())
        total_wins = int(pd.to_numeric(group["wins"], errors="coerce").sum())
        total_losses = int(pd.to_numeric(group["losses"], errors="coerce").sum())
        total_pushes = int(pd.to_numeric(group["pushes"], errors="coerce").sum())
        profit = float(pd.to_numeric(group["profit_units"], errors="coerce").sum()) if total_bets else 0.0
        days = int(len(group))
        rows.append(
            {
                "lane": lane,
                "dates": days,
                "total_bets": total_bets,
                "wins": total_wins,
                "losses": total_losses,
                "pushes": total_pushes,
                "profit_units": profit,
                "win_rate": total_wins / total_bets if total_bets else np.nan,
                "roi": profit / total_bets if total_bets else np.nan,
                "avg_bets_per_day": total_bets / days if days else np.nan,
                "positive_days": int((pd.to_numeric(group["profit_units"], errors="coerce") > 0).sum()),
                "negative_days": int((pd.to_numeric(group["profit_units"], errors="coerce") < 0).sum()),
            }
        )
    return pd.DataFrame(rows)


def build_report(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = _available_dates(args.execution_root, args.days)
    rows: list[dict[str, Any]] = []

    for date_text in dates:
        rec_path = args.execution_root / date_text / "reconcile_rows.csv"
        rec, rec_err = _prepare_reconcile(rec_path)
        if rec_err or rec is None:
            for lane in ("quick_card", "quick_card_hits_under", "v1", "top_rank"):
                rows.append(_metrics(date_text, lane, pd.DataFrame(), rec_err or "missing_reconcile"))
            print(f"[lane-performance] skip date={date_text} reason={rec_err}")
            continue

        lane_paths = {
            "quick_card": args.quick_card_root / date_text / "quick_card.csv",
            "v1": args.v1_root / date_text / "wagers.csv",
            "top_rank": args.top_rank_root / date_text / "top_rank.csv",
        }
        prepared: dict[str, pd.DataFrame] = {}
        for lane, path in lane_paths.items():
            lane_df, err = _prepare_lane(path, lane)
            if err or lane_df is None:
                rows.append(_metrics(date_text, lane, pd.DataFrame(), err or "missing_lane"))
                print(f"[lane-performance] skip date={date_text} lane={lane} reason={err}")
                continue
            prepared[lane] = lane_df
            rows.append(_score_lane(date_text, lane, lane_df, rec))

        quick = prepared.get("quick_card")
        if quick is None:
            rows.append(
                _metrics(
                    date_text,
                    "quick_card_hits_under",
                    pd.DataFrame(),
                    f"quick_card_unavailable:{lane_paths['quick_card']}",
                )
            )
        else:
            sub = quick[quick["prop_type_norm"].eq("hits") & quick["side_norm"].eq("under")].copy()
            rows.append(_score_lane(date_text, "quick_card_hits_under", sub, rec))

    daily = pd.DataFrame(rows)
    summary = _summary(daily)
    return daily, summary


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", "--days-back", dest="days", type=int, default=30)
    parser.add_argument("--execution-root", type=Path, default=DEFAULT_EXEC_ROOT)
    parser.add_argument("--quick-card-root", type=Path, default=DEFAULT_QUICK_ROOT)
    parser.add_argument("--v1-root", type=Path, default=DEFAULT_V1_ROOT)
    parser.add_argument("--top-rank-root", type=Path, default=DEFAULT_TOP_RANK_ROOT)
    parser.add_argument("--out-daily-csv", type=Path, default=DEFAULT_DAILY_OUT)
    parser.add_argument("--out-summary-csv", type=Path, default=DEFAULT_SUMMARY_OUT)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    daily, summary = build_report(args)

    args.out_daily_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(args.out_daily_csv, index=False)
    summary.to_csv(args.out_summary_csv, index=False)

    print(f"[lane-performance] wrote daily={args.out_daily_csv} rows={len(daily)}")
    print(f"[lane-performance] wrote summary={args.out_summary_csv} rows={len(summary)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
