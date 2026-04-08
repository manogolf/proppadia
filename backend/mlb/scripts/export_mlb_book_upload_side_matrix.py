#!/usr/bin/env python3
"""Build a tool-ready MLB book-upload CSV from RED-mode model/fade odds buckets.

This script:
1) Builds a side matrix by joining model and fade bucket ROI tables.
2) Uses today's slate + market odds snapshot for a target bookmaker.
3) Chooses model/fade side per row based on the side-matrix bucket preference.
4) Writes a standard book-upload CSV (LEAGUE, DATE, ..., WIN %).

No EV/gap policy filters are applied here.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from backend.mlb.scripts import export_mlb_book_upload as ex

BUCKET_ORDER: Sequence[str] = (
    ">=+201",
    "+151..+200",
    "+131..+150",
    "+121..+130",
    "+111..+120",
    "+101..+110",
    "-99..+100",
    "-109..-100",
    "-119..-110",
    "-139..-120",
    "-159..-140",
    "-179..-160",
    "-199..-180",
    "-219..-200",
    "-249..-220",
    "-299..-250",
    "<=-300",
)


def _bucket_from_american(odds: float) -> str:
    o = int(round(float(odds)))
    if o >= 201:
        return ">=+201"
    if 151 <= o <= 200:
        return "+151..+200"
    if 131 <= o <= 150:
        return "+131..+150"
    if 121 <= o <= 130:
        return "+121..+130"
    if 111 <= o <= 120:
        return "+111..+120"
    if 101 <= o <= 110:
        return "+101..+110"
    if -99 <= o <= 100:
        return "-99..+100"
    if -109 <= o <= -100:
        return "-109..-100"
    if -119 <= o <= -110:
        return "-119..-110"
    if -139 <= o <= -120:
        return "-139..-120"
    if -159 <= o <= -140:
        return "-159..-140"
    if -179 <= o <= -160:
        return "-179..-160"
    if -199 <= o <= -180:
        return "-199..-180"
    if -219 <= o <= -200:
        return "-219..-200"
    if -249 <= o <= -220:
        return "-249..-220"
    if -299 <= o <= -250:
        return "-299..-250"
    return "<=-300"


def _load_bucket_table(path: Path, *, side_prefix: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"bucket table not found: {path}")

    df = pd.read_csv(path)
    required = {"odds_bucket", "rows", "roi_pct"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"bucket table missing required columns {missing}: {path}")

    out = df[["odds_bucket", "rows", "roi_pct"]].copy()
    out["odds_bucket"] = out["odds_bucket"].astype(str)
    out["rows"] = pd.to_numeric(out["rows"], errors="coerce").fillna(0).astype(int)
    out["roi_pct"] = pd.to_numeric(out["roi_pct"], errors="coerce")

    out = out.rename(
        columns={
            "rows": f"{side_prefix}_rows",
            "roi_pct": f"{side_prefix}_roi_pct",
        }
    )
    return out


def _build_side_matrix(model_buckets: pd.DataFrame, fade_buckets: pd.DataFrame) -> pd.DataFrame:
    merged = model_buckets.merge(fade_buckets, on="odds_bucket", how="outer")

    merged["model_rows"] = pd.to_numeric(merged.get("model_rows"), errors="coerce").fillna(0).astype(int)
    merged["fade_rows"] = pd.to_numeric(merged.get("fade_rows"), errors="coerce").fillna(0).astype(int)
    merged["model_roi_pct"] = pd.to_numeric(merged.get("model_roi_pct"), errors="coerce")
    merged["fade_roi_pct"] = pd.to_numeric(merged.get("fade_roi_pct"), errors="coerce")

    preferred_side: List[str] = []
    preferred_roi: List[float] = []
    status: List[str] = []

    for _, row in merged.iterrows():
        m_roi = row.get("model_roi_pct")
        f_roi = row.get("fade_roi_pct")
        m_ok = pd.notna(m_roi)
        f_ok = pd.notna(f_roi)

        if m_ok and f_ok:
            if float(m_roi) >= float(f_roi):
                side = "model"
                roi = float(m_roi)
            else:
                side = "fade"
                roi = float(f_roi)
        elif m_ok:
            side = "model"
            roi = float(m_roi)
        elif f_ok:
            side = "fade"
            roi = float(f_roi)
        else:
            side = "none"
            roi = float("nan")

        preferred_side.append(side)
        preferred_roi.append(roi)
        status.append("play" if pd.notna(roi) and float(roi) > 0.0 else "bench")

    merged["preferred_side"] = preferred_side
    merged["preferred_roi_pct"] = preferred_roi
    merged["status"] = status

    merged["_bucket_rank"] = merged["odds_bucket"].map({b: i for i, b in enumerate(BUCKET_ORDER)})
    merged = merged.sort_values(["_bucket_rank", "odds_bucket"]).drop(columns=["_bucket_rank"]).reset_index(drop=True)

    return merged[
        [
            "odds_bucket",
            "model_rows",
            "model_roi_pct",
            "fade_rows",
            "fade_roi_pct",
            "preferred_side",
            "preferred_roi_pct",
            "status",
        ]
    ]


def _resolve_slate_date(*, merged: pd.DataFrame, explicit_slate_date: str) -> str:
    explicit = str(explicit_slate_date or "").strip()
    if explicit:
        return explicit
    if "slate_date" not in merged.columns:
        raise RuntimeError("slate output is missing slate_date column; pass --slate-date")
    vals = [str(v).strip() for v in merged["slate_date"].dropna().tolist() if str(v).strip()]
    if not vals:
        raise RuntimeError("unable to infer slate_date from slate output; pass --slate-date")
    return vals[0]


def _resolve_odds_snapshot(*, explicit: str, odds_root: Path, slate_date: str) -> Path:
    raw = str(explicit or "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.exists():
            raise FileNotFoundError(f"odds snapshot not found: {p}")
        return p

    p = odds_root / slate_date / "odds_mlb_playerprops.json"
    if not p.exists():
        raise FileNotFoundError(
            f"odds snapshot missing at {p}; pass --odds-snapshot-json or ensure daily snapshot exists"
        )
    return p


def _select_rows_from_side_matrix(
    *,
    candidates: pd.DataFrame,
    side_matrix: pd.DataFrame,
    market_map: Dict[str, str],
    league: str,
    section: str,
    allowed_statuses: Iterable[str],
    selection_mode: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    sm = side_matrix.copy()
    sm["odds_bucket"] = sm["odds_bucket"].astype(str)
    sm["preferred_side"] = sm["preferred_side"].astype(str).str.lower().str.strip()
    sm["status"] = sm["status"].astype(str).str.lower().str.strip()
    sm["preferred_roi_pct"] = pd.to_numeric(sm["preferred_roi_pct"], errors="coerce")
    sm_map = {r["odds_bucket"]: r for _, r in sm.iterrows()}
    status_set = {str(x).strip().lower() for x in allowed_statuses if str(x).strip()}

    work = candidates.copy()
    for col in ("model_prob_over", "model_prob_under", "price_over_american", "price_under_american"):
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["model_prob_over", "model_prob_under", "price_over_american", "price_under_american"])

    rows: List[Dict[str, object]] = []
    details: List[Dict[str, object]] = []
    stats = {
        "candidate_rows": int(len(work)),
        "qualified_candidate_rows": 0,
        "selected_rows": 0,
        "selected_model_rows": 0,
        "selected_fade_rows": 0,
        "rows_with_both_selected": 0,
        "skipped_no_play_bucket": 0,
    }

    for _, r in work.iterrows():
        p_over = float(r["model_prob_over"])
        p_under = float(r["model_prob_under"])

        if not (0.0 < p_over < 1.0 and 0.0 < p_under < 1.0):
            continue

        model_side = "over" if p_over >= p_under else "under"
        fade_side = "under" if model_side == "over" else "over"

        model_odds = float(r["price_over_american"]) if model_side == "over" else float(r["price_under_american"])
        fade_odds = float(r["price_under_american"]) if model_side == "over" else float(r["price_over_american"])

        model_bucket = _bucket_from_american(model_odds)
        fade_bucket = _bucket_from_american(fade_odds)

        options: List[Dict[str, object]] = []

        model_pref = sm_map.get(model_bucket)
        if model_pref is not None:
            pref_side = str(model_pref.get("preferred_side") or "").strip().lower()
            pref_status = str(model_pref.get("status") or "").strip().lower()
            pref_roi = pd.to_numeric(model_pref.get("preferred_roi_pct"), errors="coerce")
            if pref_side == "model" and pref_status in status_set and pd.notna(pref_roi):
                options.append(
                    {
                        "pick_type": "model",
                        "bucket": model_bucket,
                        "bucket_preferred_side": pref_side,
                        "bucket_status": pref_status,
                        "bucket_preferred_roi_pct": float(pref_roi),
                        "selected_side": model_side,
                        "selected_market_odds": float(model_odds),
                    }
                )

        fade_pref = sm_map.get(fade_bucket)
        if fade_pref is not None:
            pref_side = str(fade_pref.get("preferred_side") or "").strip().lower()
            pref_status = str(fade_pref.get("status") or "").strip().lower()
            pref_roi = pd.to_numeric(fade_pref.get("preferred_roi_pct"), errors="coerce")
            if pref_side == "fade" and pref_status in status_set and pd.notna(pref_roi):
                options.append(
                    {
                        "pick_type": "fade",
                        "bucket": fade_bucket,
                        "bucket_preferred_side": pref_side,
                        "bucket_status": pref_status,
                        "bucket_preferred_roi_pct": float(pref_roi),
                        "selected_side": fade_side,
                        "selected_market_odds": float(fade_odds),
                    }
                )

        if not options:
            stats["skipped_no_play_bucket"] += 1
            continue
        stats["qualified_candidate_rows"] += 1

        # Highest preferred ROI wins; break ties in favor of model.
        options = sorted(
            options,
            key=lambda x: (
                float(x["bucket_preferred_roi_pct"]),
                1 if str(x["pick_type"]) == "model" else 0,
            ),
            reverse=True,
        )
        if selection_mode == "best":
            selected_options = [options[0]]
        else:
            selected_options = options
        if len(selected_options) > 1:
            stats["rows_with_both_selected"] += 1

        prop_type = ex._canonical_prop_type(r.get("prop_type"))
        market = ex._normalize_upload_market(
            raw_market=r.get("market_key"),
            prop_type=prop_type,
            market_map=market_map,
        )
        date_str = pd.to_datetime(r["game_date"]).strftime("%Y%m%d")

        for chosen in selected_options:
            chosen_type = str(chosen["pick_type"])
            chosen_side = str(chosen["selected_side"])
            chosen_prob = p_over if chosen_side == "over" else p_under
            fair = ex._prob_to_fair_american(chosen_prob)
            if fair is None:
                continue

            rows.append(
                {
                    "LEAGUE": str(league).strip() or "MLB",
                    "DATE": date_str,
                    "HOME": ex._normalize_upload_team_code(r["home_team_code"]),
                    "AWAY": ex._normalize_upload_team_code(r["away_team_code"]),
                    "DOUBLEHEADER": "",
                    "SECTION": str(section).strip() or "player_prop",
                    "MARKET": market,
                    "SELECTOR": int(r["player_id"]),
                    "POINT": float(r["line"]),
                    "SIDE": chosen_side,
                    "WIN %": int(fair),
                }
            )
            details.append(
                {
                    "game_date": str(r.get("game_date")),
                    "home_team_code": ex._normalize_upload_team_code(r["home_team_code"]),
                    "away_team_code": ex._normalize_upload_team_code(r["away_team_code"]),
                    "player_id": int(r["player_id"]),
                    "player_name": str(r.get("player_name") or ""),
                    "prop_type": str(prop_type or ""),
                    "market_key": str(r.get("market_key") or ""),
                    "line": float(r["line"]),
                    "selection_mode": selection_mode,
                    "pick_type": chosen_type,
                    "selected_side": chosen_side,
                    "selected_market_odds": float(chosen["selected_market_odds"]),
                    "selected_bucket": str(chosen["bucket"]),
                    "bucket_preferred_side": str(chosen["bucket_preferred_side"]),
                    "bucket_preferred_roi_pct": float(chosen["bucket_preferred_roi_pct"]),
                    "bucket_status": str(chosen["bucket_status"]),
                    "model_side": model_side,
                    "fade_side": fade_side,
                    "price_over_american": float(r["price_over_american"]),
                    "price_under_american": float(r["price_under_american"]),
                    "model_prob_over": p_over,
                    "model_prob_under": p_under,
                    "fair_odds_written_win_pct": int(fair),
                }
            )

            stats["selected_rows"] += 1
            if chosen_type == "model":
                stats["selected_model_rows"] += 1
            else:
                stats["selected_fade_rows"] += 1

    if not rows:
        raise RuntimeError("side-matrix selection produced zero output rows")

    out = pd.DataFrame(
        rows,
        columns=[
            "LEAGUE",
            "DATE",
            "HOME",
            "AWAY",
            "DOUBLEHEADER",
            "SECTION",
            "MARKET",
            "SELECTOR",
            "POINT",
            "SIDE",
            "WIN %",
        ],
    )
    out = out.drop_duplicates(
        subset=["DATE", "HOME", "AWAY", "SECTION", "MARKET", "SELECTOR", "POINT", "SIDE"]
    ).reset_index(drop=True)
    details_df = pd.DataFrame(details)
    if not details_df.empty:
        details_df = details_df.drop_duplicates(
            subset=["game_date", "player_id", "prop_type", "line", "selected_side", "selected_market_odds"]
        ).reset_index(drop=True)
    return out, details_df, stats


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Create a book-upload CSV from model/fade bucket preferences (side matrix)."
    )
    ap.add_argument("--model-buckets-csv", default="tmp/analysis/mlb_red_mode_odds_bucket_by_bucket.csv")
    ap.add_argument("--fade-buckets-csv", default="tmp/analysis/mlb_red_mode_fade_odds_bucket_by_bucket.csv")
    ap.add_argument("--side-matrix-out-csv", default="tmp/analysis/mlb_red_mode_side_matrix.csv")

    ap.add_argument("--slate-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--slate-date", default="")
    ap.add_argument("--odds-snapshot-json", default="")
    ap.add_argument("--odds-history-root", default="backend/mlb/exports/odds_history")
    ap.add_argument("--bookmaker", default="betonlineag")

    ap.add_argument("--league", default="MLB")
    ap.add_argument("--section", default="player_prop")
    ap.add_argument("--allowed-statuses", default="play")
    ap.add_argument(
        "--selection-mode",
        choices=["best", "all-qualified"],
        default="all-qualified",
        help="best=emit only one side per row; all-qualified=emit model/fade rows that each qualify by their own market bucket.",
    )

    ap.add_argument("--out-csv", default="backend/mlb/data/processed/mlb_book_upload_side_matrix.csv")
    ap.add_argument("--dated-out-csv", default="")
    ap.add_argument("--details-out-csv", default="")
    args = ap.parse_args()

    model_buckets_csv = Path(args.model_buckets_csv).expanduser()
    fade_buckets_csv = Path(args.fade_buckets_csv).expanduser()
    side_matrix_out_csv = Path(args.side_matrix_out_csv).expanduser()
    slate_csv = Path(args.slate_csv).expanduser()
    odds_root = Path(args.odds_history_root).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    dated_out_csv = Path(str(args.dated_out_csv or "").strip()).expanduser() if str(args.dated_out_csv or "").strip() else None
    details_out_csv = Path(str(args.details_out_csv or "").strip()).expanduser() if str(args.details_out_csv or "").strip() else None

    if not slate_csv.exists():
        raise FileNotFoundError(f"slate csv not found: {slate_csv}")

    model_buckets = _load_bucket_table(model_buckets_csv, side_prefix="model")
    fade_buckets = _load_bucket_table(fade_buckets_csv, side_prefix="fade")
    side_matrix = _build_side_matrix(model_buckets, fade_buckets)

    side_matrix_out_csv.parent.mkdir(parents=True, exist_ok=True)
    side_matrix.to_csv(side_matrix_out_csv, index=False)

    merged = ex._load_slate_output(slate_csv)
    slate_date = _resolve_slate_date(merged=merged, explicit_slate_date=str(args.slate_date or ""))
    odds_snapshot = _resolve_odds_snapshot(
        explicit=str(args.odds_snapshot_json or ""),
        odds_root=odds_root,
        slate_date=slate_date,
    )

    market_map = ex._load_market_map(
        arg_json="",
        env_json=str(os.environ.get("MLB_BOOK_UPLOAD_MARKET_MAP_JSON", "") or ""),
    )

    prop_types = sorted(
        {
            ex._canonical_prop_type(p)
            for p in merged.get("prop_type", pd.Series([], dtype=object)).dropna().tolist()
            if str(p).strip()
        }
    )
    if not prop_types:
        raise RuntimeError("no prop_type values found in slate output")

    plan_df = pd.DataFrame(
        {
            "prop_type": prop_types,
            "bookmaker_key": str(args.bookmaker),
            "side": "over",
            "action": "enable",
        }
    )

    candidates = ex._build_policy_candidate_rows(
        merged=merged,
        plan_df=plan_df,
        odds_snapshot_json=odds_snapshot,
        market_map=market_map,
        include_all_books=False,
    )
    if candidates.empty:
        raise RuntimeError(
            f"no candidate rows found using bookmaker={args.bookmaker} and odds snapshot={odds_snapshot}"
        )

    statuses = [s.strip().lower() for s in str(args.allowed_statuses or "").split(",") if s.strip()]
    if not statuses:
        statuses = ["play"]

    out_df, details_df, stats = _select_rows_from_side_matrix(
        candidates=candidates,
        side_matrix=side_matrix,
        market_map=market_map,
        league=str(args.league),
        section=str(args.section),
        allowed_statuses=statuses,
        selection_mode=str(args.selection_mode).strip().lower(),
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False)
    if dated_out_csv is not None:
        dated_out_csv.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(dated_out_csv, index=False)
    if details_out_csv is not None:
        details_out_csv.parent.mkdir(parents=True, exist_ok=True)
        details_df.to_csv(details_out_csv, index=False)

    print(f"[mlb-side-matrix] slate_date={slate_date}")
    print(f"[mlb-side-matrix] odds_snapshot={odds_snapshot}")
    print(f"[mlb-side-matrix] bookmaker={args.bookmaker}")
    print(f"[mlb-side-matrix] selection_mode={args.selection_mode}")
    print(f"[mlb-side-matrix] side_matrix={side_matrix_out_csv}")
    print(
        "[mlb-side-matrix] candidates={candidate_rows} qualified={qualified_rows} selected={selected_rows} model={model_rows} fade={fade_rows} both_sides={both_rows} skipped_no_play_bucket={skipped}".format(
            candidate_rows=stats["candidate_rows"],
            qualified_rows=stats["qualified_candidate_rows"],
            selected_rows=stats["selected_rows"],
            model_rows=stats["selected_model_rows"],
            fade_rows=stats["selected_fade_rows"],
            both_rows=stats["rows_with_both_selected"],
            skipped=stats["skipped_no_play_bucket"],
        )
    )
    print(f"[mlb-side-matrix] wrote={out_csv}")
    if dated_out_csv is not None:
        print(f"[mlb-side-matrix] wrote_dated={dated_out_csv}")
    if details_out_csv is not None:
        print(f"[mlb-side-matrix] wrote_details={details_out_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
