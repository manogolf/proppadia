#!/usr/bin/env python3
"""Grade frozen model-v2 MLB tool upload rows against outcome-backed reconcile rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


MARKET_TO_PROP = {
    "batter_hits": "hits",
    "batter_runs": "runs_scored",
    "batter_rbis": "rbis",
    "batter_bases": "total_bases",
    "batter_total_bases": "total_bases",
    "batter_h+r+rbi": "hits_runs_rbis",
    "batter_hits_runs_rbis": "hits_runs_rbis",
    "batter_walks": "walks",
    "batter_strikeouts": "strikeouts_batting",
    "pitcher_hits": "hits_allowed",
    "pitcher_hits_allowed": "hits_allowed",
    "pitcher_strikeouts": "strikeouts_pitching",
    "pitcher_outs": "outs_recorded",
    "outs_recorded": "outs_recorded",
    "outs recorded": "outs_recorded",
}

TEAM_ALIASES = {
    "AZ": "ARI",
    "CHW": "CWS",
    "CWS": "CWS",
    "SF": "SF",
    "SFG": "SF",
    "SD": "SD",
    "SDP": "SD",
    "KC": "KC",
    "KCR": "KC",
    "TB": "TB",
    "TBR": "TB",
    "WAS": "WSH",
    "WSH": "WSH",
}


def _norm_text(v: Any) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return str(v).strip()


def _norm_key(v: Any) -> str:
    return _norm_text(v).lower()


def _norm_side(v: Any) -> str:
    side = _norm_key(v)
    if side in {"o", "over"}:
        return "over"
    if side in {"u", "under"}:
        return "under"
    return side


def _norm_team(v: Any) -> str:
    team = _norm_text(v).upper()
    return TEAM_ALIASES.get(team, team)


def _date_key(v: Any) -> str:
    raw = _norm_text(v)
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 8:
        dt = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(raw, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _to_float(v: Any) -> float | None:
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        raw = _norm_text(v).replace(",", "")
        if not raw:
            return None
        return float(raw)
    except Exception:
        return None


def _to_int_text(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        return str(int(float(v)))
    except Exception:
        return _norm_text(v)


def _market_to_prop(v: Any) -> str:
    market = _norm_key(v)
    return MARKET_TO_PROP.get(market, market)


def _american_profit(price: Any, won: bool) -> float | None:
    px = _to_float(price)
    if px is None:
        return None
    if not won:
        return -1.0
    if px > 0:
        return px / 100.0
    if px < 0:
        return 100.0 / abs(px)
    return None


def _best_price_index(df: pd.DataFrame, side: str) -> int:
    price_col = "price_over_american" if side == "over" else "price_under_american"
    prices = pd.to_numeric(df.get(price_col), errors="coerce")
    if prices.notna().any():
        return int(prices.fillna(-999999).idxmax())
    return int(df.index[0])


def _load_upload(path: Path, source_lane: str, source_file: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"DATE", "HOME", "AWAY", "MARKET", "SELECTOR", "POINT", "SIDE", "WIN %"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{path} missing required upload columns: {missing}")
    out = df.copy()
    out["upload_row_id"] = [f"{source_lane}_{i}" for i in range(len(out))]
    out["source_lane"] = source_lane
    out["source_file"] = source_file
    out["date_norm"] = out["DATE"].map(_date_key)
    out["player_id_norm"] = out["SELECTOR"].map(_to_int_text)
    out["player_name_norm"] = out["SELECTOR"].map(_norm_key)
    out["prop_type_norm"] = out["MARKET"].map(_market_to_prop)
    out["side_norm"] = out["SIDE"].map(_norm_side)
    out["line_norm"] = pd.to_numeric(out["POINT"], errors="coerce").round(4)
    out["home_norm"] = out["HOME"].map(_norm_team)
    out["away_norm"] = out["AWAY"].map(_norm_team)
    out["uploaded_win_prob"] = pd.to_numeric(out["WIN %"], errors="coerce")
    return out


def _prepare_reconcile(path: Path) -> pd.DataFrame:
    rec = pd.read_csv(path, low_memory=False)
    required = {"game_date", "player_id", "player_name", "prop_type", "line"}
    missing = sorted(required - set(rec.columns))
    if missing:
        raise SystemExit(f"{path} missing required reconcile columns: {missing}")
    rec = rec.copy()
    rec["date_norm"] = rec["game_date"].map(_date_key)
    rec["player_id_norm"] = rec["player_id"].map(_to_int_text)
    rec["player_name_norm"] = rec["player_name"].map(_norm_key)
    rec["prop_type_norm"] = rec["prop_type"].map(_norm_key)
    rec["line_norm"] = pd.to_numeric(rec["line"], errors="coerce").round(4)
    rec["home_norm"] = rec.get("home_team_code", "").map(_norm_team)
    rec["away_norm"] = rec.get("away_team_code", "").map(_norm_team)
    return rec


def _grade_uploads(upload: pd.DataFrame, rec: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped = {
        key: group
        for key, group in rec.groupby(["date_norm", "player_id_norm", "prop_type_norm", "line_norm"], dropna=False)
    }
    for _, row in upload.iterrows():
        key = (row["date_norm"], row["player_id_norm"], row["prop_type_norm"], row["line_norm"])
        matches = grouped.get(key, pd.DataFrame())
        side = row["side_norm"]
        base = {
            "upload_row_id": row["upload_row_id"],
            "source_lane": row["source_lane"],
            "source_file": row["source_file"],
            "date": row["date_norm"],
            "home": row.get("HOME", ""),
            "away": row.get("AWAY", ""),
            "market": row.get("MARKET", ""),
            "prop_type": row["prop_type_norm"],
            "selector": row.get("SELECTOR", ""),
            "point": row.get("POINT", np.nan),
            "line": row["line_norm"],
            "side": side,
            "uploaded_win_prob": row.get("uploaded_win_prob", np.nan),
            "matched": False,
            "resolved": False,
            "match_count": int(len(matches)),
            "join_strategy": "date_player_id_prop_line",
        }
        if matches.empty:
            base.update({"result": "missing", "pnl": np.nan, "missing_reason": "no_reconcile_match"})
            rows.append(base)
            continue

        idx = _best_price_index(matches, side)
        m = matches.loc[idx]
        result_col = "actual_over_outcome" if side == "over" else "actual_under_outcome"
        price_col = "price_over_american" if side == "over" else "price_under_american"
        pnl_col = "pnl_over_1u" if side == "over" else "pnl_under_1u"
        result = _norm_key(m.get(result_col, ""))
        pnl = _to_float(m.get(pnl_col))
        if pnl is None and result in {"win", "loss"}:
            pnl = _american_profit(m.get(price_col), result == "win")
        resolved = result in {"win", "loss", "push"}
        home_away_match = (
            row["home_norm"] in {m.get("home_norm", ""), m.get("away_norm", "")}
            and row["away_norm"] in {m.get("home_norm", ""), m.get("away_norm", "")}
        )
        base.update(
            {
                "matched": True,
                "resolved": resolved,
                "result": result if result else "unresolved",
                "pnl": pnl,
                "missing_reason": "" if resolved else "matched_unresolved",
                "player_id": m.get("player_id", ""),
                "player_name": m.get("player_name", ""),
                "bookmaker_key": m.get("bookmaker_key", ""),
                "price": m.get(price_col, np.nan),
                "actual_value": m.get("actual_value", np.nan),
                "reconcile_home": m.get("home_team_code", ""),
                "reconcile_away": m.get("away_team_code", ""),
                "home_away_match": bool(home_away_match),
                "reconcile_snapshot_run_tag": m.get("snapshot_run_tag", ""),
                "reconcile_slate_source_file": m.get("slate_source_file", ""),
            }
        )
        rows.append(base)
    return pd.DataFrame(rows)


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    resolved = df[df["resolved"]]
    wins = int((resolved["result"] == "win").sum())
    losses = int((resolved["result"] == "loss").sum())
    pushes = int((resolved["result"] == "push").sum())
    profit = float(pd.to_numeric(resolved["pnl"], errors="coerce").fillna(0).sum())
    risk_bets = wins + losses + pushes
    return {
        "uploaded_rows": int(len(df)),
        "matched_rows": int(df["matched"].sum()),
        "matched_resolved_rows": int(len(resolved)),
        "missing_rows": int((~df["matched"]).sum() + (df["matched"] & ~df["resolved"]).sum()),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": (wins / (wins + losses)) if (wins + losses) else None,
        "profit_units": profit,
        "roi": (profit / risk_bets) if risk_bets else None,
    }


def _summaries_by_source(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {str(source): _summarize(group) for source, group in df.groupby("source_file", dropna=False)}


def _key_set(df: pd.DataFrame, *, upload: bool) -> set[tuple[str, str, str, str, float]]:
    if df.empty:
        return set()
    if upload:
        return set(
            zip(
                df["date_norm"].astype(str),
                df["player_id_norm"].astype(str),
                df["prop_type_norm"].astype(str),
                df["side_norm"].astype(str),
                pd.to_numeric(df["line_norm"], errors="coerce").round(4),
            )
        )
    player_col = "player_id" if "player_id" in df.columns else "selector"
    prop_col = "prop_type"
    side_col = "side"
    line_col = "line"
    date_col = "date"
    work = df.copy()
    return set(
        zip(
            work[date_col].map(_date_key).astype(str),
            work[player_col].map(_to_int_text).astype(str),
            work[prop_col].map(_norm_key).astype(str),
            work[side_col].map(_norm_side).astype(str),
            pd.to_numeric(work[line_col], errors="coerce").round(4),
        )
    )


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    overall = summary["overall"]
    lines = [
        f"# Graded Model V2 Uploads: {summary['date']}",
        "",
        "## Overall",
        f"- Uploaded rows: `{overall['uploaded_rows']}`",
        f"- Matched/resolved rows: `{overall['matched_resolved_rows']}`",
        f"- Missing/unresolved rows: `{overall['missing_rows']}`",
        f"- Win rate: `{overall['win_rate']:.2%}`" if overall["win_rate"] is not None else "- Win rate: `n/a`",
        f"- ROI: `{overall['roi']:.2%}`" if overall["roi"] is not None else "- ROI: `n/a`",
        f"- Units: `{overall['profit_units']:.3f}`",
        "",
        "## By Source",
    ]
    for source, stats in summary["by_source"].items():
        win_rate = "n/a" if stats["win_rate"] is None else f"{stats['win_rate']:.2%}"
        roi = "n/a" if stats["roi"] is None else f"{stats['roi']:.2%}"
        lines.append(
            f"- `{source}`: rows `{stats['uploaded_rows']}`, resolved `{stats['matched_resolved_rows']}`, "
            f"win rate `{win_rate}`, ROI `{roi}`, units `{stats['profit_units']:.3f}`"
        )
    cmp_summary = summary["selector_postgame_comparison"]
    lines.extend(
        [
            "",
            "## Selector Postgame Comparison",
            f"- Rows in selector postgame but not upload: `{cmp_summary['selector_postgame_not_upload']}`",
            f"- Rows in upload but not selector postgame: `{cmp_summary['upload_not_selector_postgame']}`",
        ]
    )
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--ranking-upload-csv", default="")
    parser.add_argument("--quick-card-upload-csv", default="")
    parser.add_argument("--reconcile-csv", default="")
    parser.add_argument("--selector-csv", default="")
    parser.add_argument("--out-csv", default="")
    parser.add_argument("--summary-json", default="")
    parser.add_argument("--summary-md", default="")
    args = parser.parse_args()

    date_value = _date_key(args.date)
    if not date_value:
        raise SystemExit(f"Invalid --date: {args.date}")
    compact = date_value.replace("-", "")
    upload_root = Path("backend/mlb/exports/model_v2/upload")
    upload_date_root = upload_root / date_value
    if args.ranking_upload_csv:
        ranking_path = Path(args.ranking_upload_csv)
    else:
        dated_ranking = upload_date_root / f"ranking_tool_upload_{date_value}.csv"
        legacy_ranking = upload_root / f"ranking_tool_upload_{date_value}.csv"
        ranking_path = dated_ranking if dated_ranking.exists() else legacy_ranking
    if args.quick_card_upload_csv:
        quick_path = Path(args.quick_card_upload_csv)
    else:
        dated_quick = upload_date_root / f"quick_card_tool_upload_{date_value}.csv"
        legacy_quick = upload_root / f"quick_card_tool_upload_{date_value}.csv"
        quick_path = dated_quick if dated_quick.exists() else legacy_quick
    reconcile_path = Path(args.reconcile_csv or f"artifacts/analysis/mlb/execution_vs_model/{date_value}/reconcile_rows.csv")
    if args.selector_csv:
        selector_path = Path(args.selector_csv)
    else:
        dated_selector = Path(
            f"backend/mlb/exports/model_v2/lanes/today/{date_value}/hits_lane_selector_{date_value}.csv"
        )
        legacy_selector = Path(f"backend/mlb/exports/model_v2/lanes/today/hits_lane_selector_{date_value}.csv")
        selector_path = dated_selector if dated_selector.exists() else legacy_selector
    out_csv = Path(args.out_csv or upload_date_root / f"graded_uploads_{date_value}.csv")
    summary_json = Path(args.summary_json or upload_date_root / f"graded_uploads_{date_value}_summary.json")
    summary_md = Path(args.summary_md or upload_date_root / f"graded_uploads_{date_value}_summary.md")

    for path in [ranking_path, quick_path, reconcile_path]:
        if not path.exists():
            raise SystemExit(f"Missing required input: {path}")

    uploads = pd.concat(
        [
            _load_upload(ranking_path, "ranking_upload", "ranking_tool_upload"),
            _load_upload(quick_path, "quick_card_upload", "quick_card_tool_upload"),
        ],
        ignore_index=True,
    )
    rec = _prepare_reconcile(reconcile_path)
    graded = _grade_uploads(uploads, rec)

    selector_comparison: dict[str, Any] = {
        "selector_csv": str(selector_path),
        "selector_exists": selector_path.exists(),
        "selector_postgame_rows": 0,
        "selector_postgame_not_upload": None,
        "upload_not_selector_postgame": None,
    }
    if selector_path.exists():
        selector = pd.read_csv(selector_path)
        upload_keys = _key_set(uploads, upload=True)
        selector_keys = _key_set(selector, upload=False)
        selector_comparison.update(
            {
                "selector_postgame_rows": int(len(selector)),
                "selector_postgame_not_upload": int(len(selector_keys - upload_keys)),
                "upload_not_selector_postgame": int(len(upload_keys - selector_keys)),
            }
        )

    summary = {
        "date": date_value,
        "ranking_upload_csv": str(ranking_path),
        "quick_card_upload_csv": str(quick_path),
        "reconcile_csv": str(reconcile_path),
        "out_csv": str(out_csv),
        "overall": _summarize(graded),
        "by_source": _summaries_by_source(graded),
        "selector_postgame_comparison": selector_comparison,
    }

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    graded.to_csv(out_csv, index=False)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    _write_markdown(summary_md, summary)

    print(f"Wrote {out_csv}")
    print(f"Wrote {summary_json}")
    print(f"Wrote {summary_md}")
    print(json.dumps(summary["overall"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
