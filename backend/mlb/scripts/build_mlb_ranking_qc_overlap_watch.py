#!/usr/bin/env python3
"""Build visibility-only ranking-vs-Quick-Card overlap watch artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_LANE_ROOT = Path("backend/mlb/exports/model_v2/lanes/today")
DEFAULT_RECON_ROOT = Path("backend/mlb/exports/model_v2/reconcile")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/v2_qc_diagnostics")


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(dt) else pd.Timestamp(dt).date().isoformat()


def _norm(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    return "" if text in {"", "nan", "none", "null", "<na>"} else text


def _id_key(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return str(int(val)) if pd.notna(val) else ""


def _line_key(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "" if pd.isna(val) else f"{float(val):.3f}".rstrip("0").rstrip(".")


def _fmt_pct(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "n/a" if pd.isna(val) else f"{val * 100:.1f}%"


def _fmt_num(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "n/a" if pd.isna(val) else f"{val:.2f}"


def _md_table(df: pd.DataFrame, cols: list[str], n: int | None = None) -> str:
    work = df[cols].copy()
    if n is not None:
        work = work.head(n)
    for col in work.columns:
        if col in {"wr", "roi", "roi_without_best_day", "roi_without_best_2_days"}:
            work[col] = work[col].map(_fmt_pct)
        elif col in {"units", "avg_odds", "median_odds", "best_day_units", "worst_day_units"}:
            work[col] = work[col].map(_fmt_num)
    work = work.fillna("n/a").astype(str)
    lines = ["| " + " | ".join(work.columns) + " |", "| " + " | ".join(["---"] * len(work.columns)) + " |"]
    for _, row in work.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in work.columns) + " |")
    return "\n".join(lines)


def _side_price(row: pd.Series) -> float:
    side = _norm(row.get("side"))
    col = "odds_over" if side == "over" else "odds_under" if side == "under" else ""
    return pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0] if col else np.nan


def _prep_selection(path: Path, source: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    raw = pd.read_csv(path, low_memory=False)
    if raw.empty:
        return raw
    out = raw.copy()
    out["selection_source"] = source
    out["date_key"] = out["date"].map(_date_key)
    out["player_id_key"] = out.get("player_id", pd.Series("", index=out.index)).map(_id_key)
    out["player_name_key"] = out.get("player_name", out.get("player", pd.Series("", index=out.index))).map(_norm)
    out["player_key"] = out["player_id_key"].where(out["player_id_key"].ne(""), out["player_name_key"])
    out["market_key"] = out.get("prop_type", pd.Series("", index=out.index)).map(_norm)
    out["line_key"] = out.get("line", pd.Series("", index=out.index)).map(_line_key)
    out["side_key"] = out.get("side", pd.Series("", index=out.index)).map(_norm)
    out["home_key"] = out.get("home_team_code", out.get("home_upload", pd.Series("", index=out.index))).map(_norm)
    out["away_key"] = out.get("away_team_code", out.get("away_upload", pd.Series("", index=out.index))).map(_norm)
    out["game_key"] = out["away_key"] + "@" + out["home_key"]
    out["selected_price"] = out.apply(_side_price, axis=1)
    out = out[out["date_key"].ne("") & out["player_key"].ne("") & out["market_key"].ne("") & out["line_key"].ne("")]
    keys = ["date_key", "player_key", "market_key", "line_key", "side_key", "game_key", "selection_source"]
    return out.drop_duplicates(keys, keep="last").reset_index(drop=True)


def load_selections(lane_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    ranking_frames: list[pd.DataFrame] = []
    quick_frames: list[pd.DataFrame] = []
    for date_dir in sorted(lane_root.glob("20??-??-??")):
        date = date_dir.name
        ranking = _prep_selection(date_dir / f"hits_lane_selector_{date}_ranking_upload_input.csv", "ranking")
        quick = _prep_selection(date_dir / f"quick_card_hits_{date}.csv", "quick_card")
        if not ranking.empty:
            ranking_frames.append(ranking)
        if not quick.empty:
            quick_frames.append(quick)
    ranking_all = pd.concat(ranking_frames, ignore_index=True, sort=False) if ranking_frames else pd.DataFrame()
    quick_all = pd.concat(quick_frames, ignore_index=True, sort=False) if quick_frames else pd.DataFrame()
    return ranking_all, quick_all


def classify_overlap(ranking: pd.DataFrame, quick: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if ranking.empty:
        return ranking, quick
    exact_keys = ["date_key", "player_key", "market_key", "line_key", "side_key", "game_key"]
    prop_keys = ["date_key", "player_key", "market_key", "line_key"]
    player_market_keys = ["date_key", "player_key", "market_key"]
    player_date_keys = ["date_key", "player_key"]

    exact = ranking[exact_keys].drop_duplicates().merge(quick[exact_keys].drop_duplicates(), on=exact_keys, how="inner")
    out = ranking.copy().merge(exact.assign(exact_overlap=True), on=exact_keys, how="left")
    out["exact_overlap"] = out["exact_overlap"].eq(True)

    q_prop = quick[prop_keys + ["side_key"]].drop_duplicates().rename(columns={"side_key": "quick_side"})
    tmp = out.merge(q_prop, on=prop_keys, how="left")
    tmp["opposite"] = tmp["quick_side"].notna() & tmp["quick_side"].ne(tmp["side_key"])
    out["opposite_side_same_prop_line"] = tmp.groupby(tmp.index)["opposite"].any().reindex(out.index, fill_value=False).astype(bool)

    q_line = quick[player_market_keys + ["line_key"]].drop_duplicates().rename(columns={"line_key": "quick_line"})
    tmp = out.merge(q_line, on=player_market_keys, how="left")
    tmp["diff_line"] = tmp["quick_line"].notna() & tmp["quick_line"].ne(tmp["line_key"])
    out["same_player_market_different_line"] = tmp.groupby(tmp.index)["diff_line"].any().reindex(out.index, fill_value=False).astype(bool)

    q_player = quick[player_date_keys + ["market_key", "line_key"]].drop_duplicates().rename(columns={"market_key": "quick_market", "line_key": "quick_line"})
    tmp = out.merge(q_player, on=player_date_keys, how="left")
    tmp["diff_market_line"] = tmp["quick_market"].notna() & (tmp["quick_market"].ne(tmp["market_key"]) | tmp["quick_line"].ne(tmp["line_key"]))
    out["same_player_date_different_market_line"] = tmp.groupby(tmp.index)["diff_market_line"].any().reindex(out.index, fill_value=False).astype(bool)

    q_out = quick.copy().merge(exact.assign(exact_overlap=True), on=exact_keys, how="left")
    q_out["exact_overlap"] = q_out["exact_overlap"].eq(True)
    return out, q_out


def _load_perf(recon_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(recon_root.glob("*/actual_wagers_by_source_*.csv")):
        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            continue
        df = df[df.get("source_category", pd.Series("", index=df.index)).isin(["v2_ranking", "quick_card"])].copy()
        if df.empty:
            continue
        df["date_key"] = df["date"].map(_date_key)
        df["player_id_key"] = df.get("player_id", pd.Series("", index=df.index)).map(_id_key)
        df["player_name_key"] = df.get("player_name", pd.Series("", index=df.index)).map(_norm)
        df["player_key"] = df["player_id_key"].where(df["player_id_key"].ne(""), df["player_name_key"])
        df["market_key"] = df.get("prop_type", pd.Series("", index=df.index)).map(_norm)
        df["line_key"] = df.get("line", pd.Series("", index=df.index)).map(_line_key)
        df["side_key"] = df.get("side", pd.Series("", index=df.index)).map(_norm)
        df["result_key"] = df.get("result", pd.Series("", index=df.index)).map(_norm)
        df["units_num"] = pd.to_numeric(df.get("units"), errors="coerce")
        df["price_num"] = pd.to_numeric(df.get("price"), errors="coerce")
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out[out["result_key"].isin(["win", "loss", "push"])]
    keys = ["source_category", "date_key", "player_key", "market_key", "line_key", "side_key"]
    return out.drop_duplicates(keys, keep="last")


def _selection_perf(perf: pd.DataFrame, ranking: pd.DataFrame, quick: pd.DataFrame) -> pd.DataFrame:
    if perf.empty:
        return pd.DataFrame()
    keys = ["source_category", "date_key", "player_key", "market_key", "line_key", "side_key"]
    conflict_keys = ["date_key", "player_key", "market_key", "line_key"]
    r = ranking.copy()
    r["source_category"] = "v2_ranking"
    r["bucket"] = np.where(r["exact_overlap"], "exact_dual_lane_overlap", "ranking_only")
    q = quick.copy()
    q["source_category"] = "quick_card"
    q["bucket"] = np.where(q["exact_overlap"], "exact_dual_lane_overlap", "quick_card_only")
    base = pd.concat([r[keys + ["bucket"]], q[keys + ["bucket"]]], ignore_index=True, sort=False).drop_duplicates(keys + ["bucket"])
    conflicts = r.loc[r["opposite_side_same_prop_line"], conflict_keys].drop_duplicates()
    if not conflicts.empty:
        conflict_base = base.merge(conflicts, on=conflict_keys, how="inner").copy()
        conflict_base["bucket"] = "opposite_side_conflict"
        base = pd.concat([base, conflict_base], ignore_index=True, sort=False).drop_duplicates(keys + ["bucket"])
    joined = base.merge(perf[keys + ["result_key", "units_num", "price_num"]], on=keys, how="left")
    return joined[joined["result_key"].isin(["win", "loss", "push"])].copy()


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(rows))
    wins = int(rows["result_key"].eq("win").sum()) if bets else 0
    losses = int(rows["result_key"].eq("loss").sum()) if bets else 0
    pushes = int(rows["result_key"].eq("push").sum()) if bets else 0
    decisions = wins + losses
    units = float(pd.to_numeric(rows.get("units_num"), errors="coerce").sum(skipna=True)) if bets else 0.0
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / decisions if decisions else np.nan,
        "roi": units / bets if bets else np.nan,
        "units": units,
        "avg_odds": float(pd.to_numeric(rows.get("price_num"), errors="coerce").mean(skipna=True)) if bets else np.nan,
        "median_odds": float(pd.to_numeric(rows.get("price_num"), errors="coerce").median(skipna=True)) if bets else np.nan,
    }


def _perf_summary(rows: pd.DataFrame) -> pd.DataFrame:
    summaries = []
    if rows.empty:
        return pd.DataFrame()
    for bucket, group in rows.groupby("bucket", dropna=False):
        daily = group.groupby("date_key", dropna=False).apply(lambda g: pd.Series(_metrics(g)), include_groups=False).reset_index()
        best = daily.sort_values("units", ascending=False).head(1)
        worst = daily.sort_values("units", ascending=True).head(1)
        best_days = daily.sort_values("units", ascending=False).head(2)["date_key"].astype(str).tolist()
        base = _metrics(group)
        without_best = group[~group["date_key"].eq(str(best["date_key"].iloc[0]))] if not best.empty else group
        without_best2 = group[~group["date_key"].isin(best_days)] if best_days else group
        summaries.append(
            {
                "bucket": bucket,
                **base,
                "best_day": str(best["date_key"].iloc[0]) if not best.empty else "",
                "best_day_units": float(best["units"].iloc[0]) if not best.empty else 0.0,
                "worst_day": str(worst["date_key"].iloc[0]) if not worst.empty else "",
                "worst_day_units": float(worst["units"].iloc[0]) if not worst.empty else 0.0,
                "roi_without_best_day": _metrics(without_best)["roi"],
                "roi_without_best_2_days": _metrics(without_best2)["roi"],
            }
        )
    return pd.DataFrame(summaries)


def build_watch(args: argparse.Namespace) -> dict[str, Any]:
    lane_root = Path(args.lane_root)
    recon_root = Path(args.reconcile_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ranking, quick = load_selections(lane_root)
    ranking, quick = classify_overlap(ranking, quick)
    dates = sorted(set(ranking.get("date_key", pd.Series(dtype=str)).dropna()) | set(quick.get("date_key", pd.Series(dtype=str)).dropna()))
    daily_rows = []
    for date in dates:
        r = ranking[ranking["date_key"].eq(date)]
        q = quick[quick["date_key"].eq(date)]
        daily_rows.append(
            {
                "date": date,
                "ranking_selected_count": int(len(r)),
                "quick_card_selected_count": int(len(q)),
                "exact_overlap_count": int(r["exact_overlap"].sum()) if not r.empty else 0,
                "ranking_only_count": int((~r["exact_overlap"]).sum()) if not r.empty else 0,
                "quick_card_only_count": int((~q["exact_overlap"]).sum()) if not q.empty else 0,
                "opposite_side_same_player_market_line_conflicts": int(r["opposite_side_same_prop_line"].sum()) if not r.empty else 0,
                "same_player_different_line_overlaps": int(r["same_player_market_different_line"].sum()) if not r.empty else 0,
            }
        )
    daily = pd.DataFrame(daily_rows)

    perf = _selection_perf(_load_perf(recon_root), ranking, quick)
    perf_summary = _perf_summary(perf)

    latest_reconcile = max([_date_key(path.parent.name) for path in recon_root.glob("*/actual_wagers_by_source_*.csv")], default="")
    latest_overlap = max(dates, default="")
    stale = bool(latest_reconcile and latest_overlap and latest_overlap < latest_reconcile)

    csv_path = out_dir / "ranking_vs_quick_card_overlap_watch.csv"
    json_path = out_dir / "ranking_vs_quick_card_overlap_watch.json"
    md_path = out_dir / "ranking_vs_quick_card_overlap_watch.md"
    daily.to_csv(csv_path, index=False)

    payload = {
        "latest_reconcile_date_found": latest_reconcile,
        "latest_overlap_date_included": latest_overlap,
        "stale": stale,
        "totals": daily.sum(numeric_only=True).to_dict() if not daily.empty else {},
        "performance": perf_summary.to_dict(orient="records"),
        "outputs": {"csv": str(csv_path), "json": str(json_path), "md": str(md_path)},
    }
    json_path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")

    lines = [
        "# Ranking vs Quick Card Overlap Watch",
        "",
        "Visibility only. Uses intended selected-side rows, not paired upload rows.",
        "",
        f"- Latest reconcile date found: `{latest_reconcile or 'none'}`",
        f"- Latest overlap date included: `{latest_overlap or 'none'}`",
        f"- Stale: `{str(stale).lower()}`",
        "",
        "## Daily Overlap",
        "",
        _md_table(daily, list(daily.columns)),
        "",
        "## Resolved Performance",
        "",
        _md_table(perf_summary, ["bucket", "bets", "wins", "losses", "pushes", "wr", "roi", "units", "avg_odds", "median_odds", "best_day", "best_day_units", "worst_day", "worst_day_units", "roi_without_best_day", "roi_without_best_2_days"]) if not perf_summary.empty else "No resolved performance rows.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ranking-vs-Quick-Card overlap watch artifacts.")
    parser.add_argument("--lane-root", default=str(DEFAULT_LANE_ROOT))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECON_ROOT))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    return parser.parse_args()


def main() -> int:
    payload = build_watch(parse_args())
    outputs = payload.get("outputs", {})
    print(f"Wrote {outputs.get('csv')}")
    print(f"Wrote {outputs.get('json')}")
    print(f"Wrote {outputs.get('md')}")
    print(
        "freshness "
        f"latest_reconcile={payload.get('latest_reconcile_date_found') or 'none'} "
        f"latest_overlap={payload.get('latest_overlap_date_included') or 'none'} "
        f"stale={payload.get('stale')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
