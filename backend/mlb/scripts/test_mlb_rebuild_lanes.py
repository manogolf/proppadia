#!/usr/bin/env python3
"""Walk-forward scenario tester for MLB rebuild lanes.

Purpose:
- test deterministic model/fade/hybrid lanes on reconciled market rows
- score scenarios with strict robustness criteria
- highlight whether any lane is resilient beyond single-prop dependence
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


DEFAULT_SCENARIOS = (
    "fade_only:all_props:20:0",
    "fade_only:all_props:20:2",
    "fade_only:all_props:20:4",
    "fade_only:all_props:20:6",
    "fade_only:all_props:40:0",
    "fade_only:all_props:40:2",
    "fade_only:all_props:40:4",
    "fade_only:all_props:40:6",
    "hybrid_best_side:all_props:20:0",
    "hybrid_best_side:all_props:20:2",
    "hybrid_best_side:all_props:20:4",
    "hybrid_best_side:all_props:20:6",
    "hybrid_best_side:all_props:40:0",
    "hybrid_best_side:all_props:40:2",
    "hybrid_best_side:all_props:40:4",
    "hybrid_best_side:all_props:40:6",
    "model_only:all_props:20:0",
    "model_only:all_props:40:0",
)

DEFAULT_SCOPE_PROPS = {
    "all_props": (),
    "core12": (
        "hits",
        "total_bases",
        "hits_runs_rbis",
        "runs_rbis",
        "rbis",
        "runs_scored",
        "strikeouts_batting",
        "walks",
        "singles",
        "doubles",
        "strikeouts_pitching",
        "outs_recorded",
    ),
    "hits_tb": ("hits", "total_bases"),
    "tb_only": ("total_bases",),
    "hits_only": ("hits",),
}


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    mode: str
    scope: str
    min_rows_train: int
    roi_floor_train_pct: float


def _bucket10(odds: float) -> str | None:
    if pd.isna(odds):
        return None
    o = int(round(float(odds)))
    if o >= 201:
        return ">=+201"
    if 101 <= o <= 200:
        low = ((o - 101) // 10) * 10 + 101
        return f"+{low}..+{low + 9}"
    if -99 <= o <= 100:
        return "-99..+100"
    if -299 <= o <= -100:
        abs_o = abs(o)
        low_abs = ((abs_o - 100) // 10) * 10 + 100
        return f"-{low_abs + 9}..-{low_abs}"
    return "<=-300"


def _parse_scenarios(items: Sequence[str]) -> list[Scenario]:
    out: list[Scenario] = []
    for raw in items:
        val = str(raw).strip()
        if not val:
            continue
        parts = [p.strip() for p in val.split(":")]
        if len(parts) != 4:
            raise ValueError(f"invalid scenario '{val}' (expected mode:scope:min_rows:roi_floor_pct)")
        mode, scope, min_rows, roi_floor = parts
        scenario_id = f"{mode}_{scope}_{min_rows}_{roi_floor.replace('.', 'p')}"
        out.append(
            Scenario(
                scenario_id=scenario_id,
                mode=mode,
                scope=scope,
                min_rows_train=max(0, int(min_rows)),
                roi_floor_train_pct=float(roi_floor),
            )
        )
    return out


def _split_csv(raw: str) -> list[str]:
    return [x.strip() for x in str(raw or "").split(",") if x.strip()]


def _prepare_rows(
    *,
    rows_csv: Path,
    from_date: str,
    to_date: str,
    bookmaker: str,
    require_two_sided: bool,
) -> pd.DataFrame:
    cols = [
        "game_date",
        "game_id",
        "prop_type",
        "bookmaker_key",
        "model_pick_side",
        "pnl_model_pick_1u",
        "pnl_over_1u",
        "pnl_under_1u",
        "price_over_american",
        "price_under_american",
    ]
    df = pd.read_csv(rows_csv, usecols=cols, low_memory=False)
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    start = pd.to_datetime(from_date, errors="coerce").date()
    end = pd.to_datetime(to_date, errors="coerce").date()
    if pd.isna(start) or pd.isna(end):
        raise ValueError("invalid from/to dates")

    work = df[df["game_date"].notna()].copy()
    work = work[(work["game_date"] >= start) & (work["game_date"] <= end)].copy()
    if bookmaker:
        work = work[work["bookmaker_key"].astype(str).str.lower().eq(str(bookmaker).strip().lower())].copy()

    if require_two_sided:
        work = work[work["price_over_american"].notna() & work["price_under_american"].notna()].copy()

    # Require resolved outcomes for both sides so model/fade/hybrid are directly comparable.
    work = work[
        work["pnl_over_1u"].notna()
        & work["pnl_under_1u"].notna()
        & work["pnl_model_pick_1u"].notna()
    ].copy()

    pick = work["model_pick_side"].astype(str).str.lower().str.strip()
    work = work[pick.isin(["over", "under"])].copy()
    work["model_side"] = pick[pick.isin(["over", "under"])]
    work["fade_side"] = np.where(work["model_side"].eq("over"), "under", "over")

    work["model_pnl"] = pd.to_numeric(work["pnl_model_pick_1u"], errors="coerce")
    work["model_odds"] = np.where(
        work["model_side"].eq("over"),
        pd.to_numeric(work["price_over_american"], errors="coerce"),
        pd.to_numeric(work["price_under_american"], errors="coerce"),
    )
    work["fade_pnl"] = np.where(
        work["model_side"].eq("over"),
        pd.to_numeric(work["pnl_under_1u"], errors="coerce"),
        pd.to_numeric(work["pnl_over_1u"], errors="coerce"),
    )
    work["fade_odds"] = np.where(
        work["model_side"].eq("over"),
        pd.to_numeric(work["price_under_american"], errors="coerce"),
        pd.to_numeric(work["price_over_american"], errors="coerce"),
    )
    work["model_bucket"] = pd.Series(work["model_odds"]).map(_bucket10)
    work["fade_bucket"] = pd.Series(work["fade_odds"]).map(_bucket10)
    work = work[work["model_bucket"].notna() & work["fade_bucket"].notna()].copy()
    return work.reset_index(drop=True)


def _scope_mask(df: pd.DataFrame, scope: str, scope_override: dict[str, list[str]]) -> pd.Series:
    props = scope_override.get(scope) or DEFAULT_SCOPE_PROPS.get(scope) or ()
    if not props:
        return pd.Series(True, index=df.index)
    return df["prop_type"].astype(str).isin(set(props))


def _lane_stats(train: pd.DataFrame, side: str) -> pd.DataFrame:
    bcol = f"{side}_bucket"
    pcol = f"{side}_pnl"
    return (
        train.groupby(["prop_type", bcol], dropna=False)[pcol]
        .agg(rows="count", roi="mean")
        .reset_index()
        .rename(columns={bcol: "bucket"})
    )


def _choose_lanes(train: pd.DataFrame, scenario: Scenario) -> dict[tuple[str, str], tuple[str, float]]:
    mode = scenario.mode
    min_rows = int(scenario.min_rows_train)
    roi_floor = float(scenario.roi_floor_train_pct)
    out: dict[tuple[str, str], tuple[str, float]] = {}

    if mode == "fade_only":
        s = _lane_stats(train, "fade")
        s = s[(s["rows"] >= min_rows) & (s["roi"] * 100.0 >= roi_floor)]
        for _, r in s.iterrows():
            out[(str(r["prop_type"]), str(r["bucket"]))] = ("fade", float(r["roi"]))
        return out

    if mode == "model_only":
        s = _lane_stats(train, "model")
        s = s[(s["rows"] >= min_rows) & (s["roi"] * 100.0 >= roi_floor)]
        for _, r in s.iterrows():
            out[(str(r["prop_type"]), str(r["bucket"]))] = ("model", float(r["roi"]))
        return out

    if mode != "hybrid_best_side":
        raise ValueError(f"unsupported mode '{mode}'")

    sm = _lane_stats(train, "model").rename(columns={"rows": "rows_model", "roi": "roi_model"})
    sf = _lane_stats(train, "fade").rename(columns={"rows": "rows_fade", "roi": "roi_fade"})
    joined = sm.merge(sf, on=["prop_type", "bucket"], how="outer")
    for _, r in joined.iterrows():
        choices: list[tuple[str, float]] = []
        if pd.notna(r.get("rows_model")) and float(r["rows_model"]) >= min_rows and float(r["roi_model"]) * 100.0 >= roi_floor:
            choices.append(("model", float(r["roi_model"])))
        if pd.notna(r.get("rows_fade")) and float(r["rows_fade"]) >= min_rows and float(r["roi_fade"]) * 100.0 >= roi_floor:
            choices.append(("fade", float(r["roi_fade"])))
        if not choices:
            continue
        choices = sorted(choices, key=lambda x: x[1], reverse=True)
        out[(str(r["prop_type"]), str(r["bucket"]))] = choices[0]
    return out


def _apply_day(day: pd.DataFrame, lane_map: dict[tuple[str, str], tuple[str, float]]) -> list[dict[str, object]]:
    picked: list[dict[str, object]] = []
    for _, r in day.iterrows():
        prop = str(r["prop_type"])
        model_bucket = str(r["model_bucket"])
        fade_bucket = str(r["fade_bucket"])

        chosen_side: str | None = None
        train_lane_roi: float | None = None

        k_model = (prop, model_bucket)
        if k_model in lane_map and lane_map[k_model][0] == "model":
            chosen_side = "model"
            train_lane_roi = lane_map[k_model][1]
        else:
            k_fade = (prop, fade_bucket)
            if k_fade in lane_map and lane_map[k_fade][0] == "fade":
                chosen_side = "fade"
                train_lane_roi = lane_map[k_fade][1]

        if chosen_side is None:
            # fallback when map key exists but side tag differs due cross-side collision.
            if k_model in lane_map:
                chosen_side = lane_map[k_model][0]
                train_lane_roi = lane_map[k_model][1]
            elif (prop, fade_bucket) in lane_map:
                chosen_side = lane_map[(prop, fade_bucket)][0]
                train_lane_roi = lane_map[(prop, fade_bucket)][1]

        if chosen_side is None:
            continue

        pnl = float(r["model_pnl"]) if chosen_side == "model" else float(r["fade_pnl"])
        picked.append(
            {
                "game_date": str(r["game_date"]),
                "game_id": int(r["game_id"]),
                "prop_type": prop,
                "side": chosen_side,
                "pnl_1u": pnl,
                "train_lane_roi": float(train_lane_roi) if train_lane_roi is not None else np.nan,
            }
        )
    return picked


def _summarize_picks(
    picks: pd.DataFrame,
    *,
    min_prop_bets: int,
    min_positive_props: int,
    max_prop_pnl_share_pct: float,
) -> tuple[dict[str, object], pd.DataFrame]:
    if picks.empty:
        summary = {
            "bets": 0,
            "pnl_1u": 0.0,
            "roi_pct": np.nan,
            "positive_props_meeting_floor": 0,
            "max_prop_pnl_share_pct": np.nan,
            "roi_excluding_total_bases_pct": np.nan,
            "pass_overall": False,
            "fail_reasons": ["no_bets"],
        }
        return summary, pd.DataFrame(columns=["prop_type", "bets", "pnl_1u", "roi_pct"])

    bets = int(len(picks))
    pnl = float(picks["pnl_1u"].sum())
    roi_pct = float(pnl / bets * 100.0) if bets > 0 else np.nan

    by_prop = (
        picks.groupby("prop_type", dropna=False)["pnl_1u"]
        .agg(bets="count", pnl_1u="sum")
        .reset_index()
    )
    by_prop["roi_pct"] = by_prop["pnl_1u"] / by_prop["bets"] * 100.0

    floor = int(min_prop_bets)
    pos_props = by_prop[(by_prop["bets"] >= floor) & (by_prop["roi_pct"] > 0.0)]
    positive_props_count = int(len(pos_props))

    if pnl > 0:
        share = by_prop["pnl_1u"] / pnl * 100.0
        max_share = float(share.max()) if not share.empty else np.nan
    else:
        max_share = np.nan

    ex_tb = picks[picks["prop_type"].astype(str) != "total_bases"]
    if ex_tb.empty:
        roi_ex_tb = np.nan
    else:
        roi_ex_tb = float(ex_tb["pnl_1u"].sum() / len(ex_tb) * 100.0)

    failures: list[str] = []
    if not (bets > 0 and pnl > 0 and roi_pct > 0):
        failures.append("non_positive_overall_roi")
    if positive_props_count < int(min_positive_props):
        failures.append("insufficient_positive_props")
    if not np.isnan(max_share) and max_share > float(max_prop_pnl_share_pct):
        failures.append("prop_pnl_concentration_too_high")
    if np.isnan(roi_ex_tb) or roi_ex_tb <= 0:
        failures.append("non_positive_roi_excluding_total_bases")

    summary = {
        "bets": bets,
        "pnl_1u": pnl,
        "roi_pct": roi_pct,
        "positive_props_meeting_floor": positive_props_count,
        "min_prop_bets_floor": floor,
        "min_positive_props_required": int(min_positive_props),
        "max_prop_pnl_share_pct": max_share,
        "max_prop_pnl_share_allowed_pct": float(max_prop_pnl_share_pct),
        "roi_excluding_total_bases_pct": roi_ex_tb,
        "pass_overall": len(failures) == 0,
        "fail_reasons": failures,
    }
    by_prop = by_prop.sort_values(["pnl_1u", "bets"], ascending=[False, False]).reset_index(drop=True)
    return summary, by_prop


def _run_scenario(
    *,
    rows: pd.DataFrame,
    scenario: Scenario,
    warmup_days: int,
    scope_override: dict[str, list[str]],
    min_prop_bets: int,
    min_positive_props: int,
    max_prop_pnl_share_pct: float,
) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    scoped = rows[_scope_mask(rows, scenario.scope, scope_override)].copy()
    dates = sorted(pd.Series(scoped["game_date"]).dropna().unique().tolist())
    test_dates = dates[int(max(0, warmup_days)) :]

    daily_rows: list[dict[str, object]] = []
    pick_rows: list[dict[str, object]] = []
    for d in test_dates:
        train = scoped[scoped["game_date"] < d]
        day = scoped[scoped["game_date"] == d]
        lane_map = _choose_lanes(train, scenario)
        day_picks = _apply_day(day, lane_map)
        if day_picks:
            pick_rows.extend(day_picks)
        bets = len(day_picks)
        pnl = float(sum(float(r["pnl_1u"]) for r in day_picks)) if bets else 0.0
        daily_rows.append(
            {
                "scenario_id": scenario.scenario_id,
                "mode": scenario.mode,
                "scope": scenario.scope,
                "game_date": str(d),
                "bets": int(bets),
                "pnl_1u": pnl,
                "roi_pct": float(pnl / bets * 100.0) if bets else np.nan,
                "model_bets": int(sum(1 for r in day_picks if r["side"] == "model")),
                "fade_bets": int(sum(1 for r in day_picks if r["side"] == "fade")),
            }
        )

    daily = pd.DataFrame(daily_rows)
    picks = pd.DataFrame(pick_rows)
    summary_metrics, by_prop = _summarize_picks(
        picks,
        min_prop_bets=min_prop_bets,
        min_positive_props=min_positive_props,
        max_prop_pnl_share_pct=max_prop_pnl_share_pct,
    )
    active_days = int((daily["bets"] > 0).sum()) if not daily.empty else 0
    positive_days = int((daily["pnl_1u"] > 0).sum()) if not daily.empty else 0

    summary = {
        "scenario_id": scenario.scenario_id,
        "mode": scenario.mode,
        "scope": scenario.scope,
        "min_rows_train": int(scenario.min_rows_train),
        "roi_floor_train_pct": float(scenario.roi_floor_train_pct),
        "warmup_days": int(warmup_days),
        "active_days": active_days,
        "positive_days": positive_days,
        "positive_day_rate_pct": float(positive_days / active_days * 100.0) if active_days else np.nan,
        "date_min": (str(min(dates)) if dates else None),
        "date_max": (str(max(dates)) if dates else None),
        **summary_metrics,
    }
    return summary, daily, by_prop


def main() -> int:
    ap = argparse.ArgumentParser(description="Run MLB rebuild lane walk-forward tests with strict pass/fail criteria.")
    ap.add_argument("--rows-csv", default="tmp/mlb_base_vs_market_rows.csv")
    ap.add_argument("--from-date", default="2026-03-25")
    ap.add_argument("--to-date", default=pd.Timestamp.utcnow().strftime("%Y-%m-%d"))
    ap.add_argument("--bookmaker", default="betonlineag")
    ap.add_argument("--require-two-sided", action="store_true", default=True)
    ap.add_argument("--warmup-days", type=int, default=7)
    ap.add_argument("--scenario", action="append", default=[], help="mode:scope:min_rows:roi_floor_pct")
    ap.add_argument(
        "--scope-override",
        action="append",
        default=[],
        help="scope=prop1,prop2,...",
    )
    ap.add_argument("--min-prop-bets", type=int, default=30)
    ap.add_argument("--min-positive-props", type=int, default=4)
    ap.add_argument("--max-prop-pnl-share-pct", type=float, default=50.0)
    ap.add_argument("--out-summary-csv", default="tmp/analysis/mlb_rebuild_test_summary.csv")
    ap.add_argument("--out-daily-csv", default="tmp/analysis/mlb_rebuild_test_daily.csv")
    ap.add_argument("--out-by-prop-csv", default="tmp/analysis/mlb_rebuild_test_by_prop.csv")
    ap.add_argument("--out-json", default="tmp/analysis/mlb_rebuild_test_latest.json")
    args = ap.parse_args()

    scenarios = _parse_scenarios(args.scenario if args.scenario else list(DEFAULT_SCENARIOS))
    scope_override: dict[str, list[str]] = {}
    for raw in args.scope_override:
        item = str(raw).strip()
        if not item or "=" not in item:
            continue
        key, val = item.split("=", 1)
        scope_override[key.strip()] = _split_csv(val)

    rows = _prepare_rows(
        rows_csv=Path(args.rows_csv).expanduser(),
        from_date=str(args.from_date),
        to_date=str(args.to_date),
        bookmaker=str(args.bookmaker),
        require_two_sided=bool(args.require_two_sided),
    )

    summaries: list[dict[str, object]] = []
    daily_all: list[pd.DataFrame] = []
    by_prop_all: list[pd.DataFrame] = []
    for sc in scenarios:
        summary, daily, by_prop = _run_scenario(
            rows=rows,
            scenario=sc,
            warmup_days=int(args.warmup_days),
            scope_override=scope_override,
            min_prop_bets=int(args.min_prop_bets),
            min_positive_props=int(args.min_positive_props),
            max_prop_pnl_share_pct=float(args.max_prop_pnl_share_pct),
        )
        summaries.append(summary)
        if not daily.empty:
            daily_all.append(daily)
        if not by_prop.empty:
            bp = by_prop.copy()
            bp.insert(0, "scenario_id", sc.scenario_id)
            by_prop_all.append(bp)

    summary_df = pd.DataFrame(summaries).sort_values(
        ["pass_overall", "roi_pct", "bets"],
        ascending=[False, False, False],
        na_position="last",
    )
    daily_df = pd.concat(daily_all, ignore_index=True) if daily_all else pd.DataFrame()
    by_prop_df = pd.concat(by_prop_all, ignore_index=True) if by_prop_all else pd.DataFrame()

    out_summary = Path(args.out_summary_csv).expanduser()
    out_daily = Path(args.out_daily_csv).expanduser()
    out_prop = Path(args.out_by_prop_csv).expanduser()
    out_json = Path(args.out_json).expanduser()
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_daily.parent.mkdir(parents=True, exist_ok=True)
    out_prop.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(out_summary, index=False)
    daily_df.to_csv(out_daily, index=False)
    by_prop_df.to_csv(out_prop, index=False)

    passes = summary_df[summary_df["pass_overall"] == True]  # noqa: E712
    payload = {
        "ok": True,
        "status": "pass",
        "rows_csv": str(args.rows_csv),
        "from_date": str(args.from_date),
        "to_date": str(args.to_date),
        "bookmaker": str(args.bookmaker),
        "require_two_sided": bool(args.require_two_sided),
        "warmup_days": int(args.warmup_days),
        "scenario_count": int(len(summary_df)),
        "pass_count": int(len(passes)),
        "top_pass_scenarios": passes.head(10).to_dict(orient="records"),
        "top_all_scenarios": summary_df.head(10).to_dict(orient="records"),
        "outputs": {
            "summary_csv": str(out_summary),
            "daily_csv": str(out_daily),
            "by_prop_csv": str(out_prop),
            "json": str(out_json),
        },
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
