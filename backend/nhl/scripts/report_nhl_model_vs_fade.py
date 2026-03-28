#!/usr/bin/env python3
"""Report NHL SOG model-picked results vs opposite-side fade using card prices.

For each graded row, this script attempts to join the same-day card row by
(player short key, side, line). On matched rows, fade pnl is computed using:
  - opposite outcome of graded result
  - opposite-side card price (price_over / price_under)
  - same stake amount
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from glob import glob
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


GRADED_RE = re.compile(r"nhl_sog_graded_(\d{4}-\d{2}-\d{2})\.csv$")


def _norm_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\\s]", " ", s)
    s = re.sub(r"\\s+", " ", s).strip()
    return s


def _short_key(name: str) -> str:
    parts = _norm_name(name).split()
    if not parts:
        return ""
    return f"{parts[0][0]} {parts[-1]}"


def _to_num(v: Any) -> float | None:
    try:
        s = str(v if v is not None else "").strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _profit(amount: float, price_american: float, outcome: str) -> float:
    out = str(outcome or "").strip().lower()
    if out == "push":
        return 0.0
    if out != "win":
        return -float(amount)
    price = float(price_american)
    if price > 0:
        return float(amount) * (price / 100.0)
    return float(amount) * (100.0 / abs(price))


def _opp_side(side: str) -> str:
    s = str(side or "").strip().lower()
    if s == "over":
        return "under"
    if s == "under":
        return "over"
    return ""


def _opp_grade(grade: str) -> str:
    g = str(grade or "").strip().lower()
    if g == "win":
        return "loss"
    if g == "loss":
        return "win"
    if g == "push":
        return "push"
    return ""


def _find_card_for_date(cards_dir: Path, day: str) -> Path | None:
    exact = cards_dir / f"nhl_sog_card_{day}.csv"
    if exact.exists():
        return exact
    cands = [Path(p) for p in glob(str(cards_dir / f"nhl_sog_card_{day}*.csv"))]
    if not cands:
        return None
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]


def _prep_card(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    need = {"full_name", "model_pick", "line"}
    miss = sorted([c for c in need if c not in df.columns])
    if miss:
        raise RuntimeError(f"card csv missing required columns {miss}: {path}")
    out = df.copy()
    if "price_over" not in out.columns:
        out["price_over"] = np.nan
    if "price_under" not in out.columns:
        out["price_under"] = np.nan
    # Older cards may only carry price_side; backfill whichever side is present.
    if "price_side" in out.columns:
        side_tmp = out["model_pick"].astype(str).str.lower().str.strip()
        ps = pd.to_numeric(out["price_side"], errors="coerce")
        out.loc[side_tmp.eq("over") & out["price_over"].isna(), "price_over"] = ps
        out.loc[side_tmp.eq("under") & out["price_under"].isna(), "price_under"] = ps

    out["player_key"] = out["full_name"].astype(str).map(_short_key)
    out["side"] = out["model_pick"].astype(str).str.lower().str.strip()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["price_over"] = pd.to_numeric(out["price_over"], errors="coerce")
    out["price_under"] = pd.to_numeric(out["price_under"], errors="coerce")
    out = out[out["side"].isin(["over", "under"]) & out["line"].notna()].copy()
    return out[["player_key", "side", "line", "price_over", "price_under"]].drop_duplicates().reset_index(drop=True)


def _load_and_join(graded_glob: str, cards_dir: Path) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for fp in sorted(Path(p) for p in glob(graded_glob)):
        m = GRADED_RE.match(fp.name)
        if not m:
            continue
        day = m.group(1)
        card_path = _find_card_for_date(cards_dir, day)
        if card_path is None:
            continue

        g = pd.read_csv(fp).copy()
        need = {"player_name", "side", "line", "grade", "amount", "pnl"}
        miss = sorted([c for c in need if c not in g.columns])
        if miss:
            raise RuntimeError(f"graded csv missing required columns {miss}: {fp}")

        g["date"] = day
        g["player_key"] = g["player_name"].astype(str).map(_short_key)
        g["side"] = g["side"].astype(str).str.lower().str.strip()
        g["line"] = pd.to_numeric(g["line"], errors="coerce")
        g["amount"] = pd.to_numeric(g["amount"], errors="coerce")
        g["pnl"] = pd.to_numeric(g["pnl"], errors="coerce")
        g["grade"] = g["grade"].astype(str).str.lower().str.strip()
        g = g[g["side"].isin(["over", "under"]) & g["line"].notna()].copy()

        c = _prep_card(card_path)
        j = g.merge(c, on=["player_key", "side", "line"], how="left")
        j["card_path"] = str(card_path)
        rows.append(j)

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _calc_payload(df: pd.DataFrame, min_bets_alert: int) -> tuple[dict[str, Any], pd.DataFrame]:
    if df.empty:
        payload = {
            "counts": {"rows_input": 0, "rows_matched": 0},
            "overall": {
                "model_bets": 0,
                "fade_bets": 0,
                "model_roi": None,
                "fade_roi": None,
                "delta_fade_minus_model": None,
                "fade_beating_model_alert": False,
            },
            "segments": [],
        }
        return payload, pd.DataFrame()

    out = df.copy()
    out["matched"] = False
    out["fade_side"] = out["side"].map(_opp_side)
    out["fade_grade"] = out["grade"].map(_opp_grade)
    out["fade_price"] = np.where(out["side"].eq("over"), out["price_under"], out["price_over"])
    out["matched"] = out["fade_price"].notna()

    valid_grade = out["grade"].isin(["win", "loss", "push"])
    out["model_pnl"] = np.where(valid_grade & out["pnl"].notna(), out["pnl"], np.nan)
    out["fade_pnl"] = np.nan

    mask_fade = out["matched"] & out["amount"].notna() & out["fade_price"].notna() & out["fade_grade"].isin(["win", "loss", "push"])
    if mask_fade.any():
        out.loc[mask_fade, "fade_pnl"] = [
            _profit(float(a), float(p), str(g))
            for a, p, g in zip(
                out.loc[mask_fade, "amount"],
                out.loc[mask_fade, "fade_price"],
                out.loc[mask_fade, "fade_grade"],
            )
        ]

    paired = out[out["model_pnl"].notna() & out["fade_pnl"].notna() & out["amount"].notna()].copy()
    paired["segment"] = paired["side"].astype(str) + ":" + paired["line"].map(lambda x: f"{float(x):.1f}")

    def _roi(pnl: pd.Series, amt: pd.Series) -> float | None:
        staked = float(amt.sum())
        if staked <= 0:
            return None
        return float(float(pnl.sum()) / staked)

    model_roi = _roi(paired["model_pnl"], paired["amount"]) if not paired.empty else None
    fade_roi = _roi(paired["fade_pnl"], paired["amount"]) if not paired.empty else None
    delta = None
    if model_roi is not None and fade_roi is not None:
        delta = float(fade_roi - model_roi)

    seg_rows: list[dict[str, Any]] = []
    for seg, g in paired.groupby("segment", dropna=False):
        mroi = _roi(g["model_pnl"], g["amount"])
        froi = _roi(g["fade_pnl"], g["amount"])
        d = None if (mroi is None or froi is None) else float(froi - mroi)
        seg_rows.append(
            {
                "segment": str(seg),
                "bets": int(len(g)),
                "model_roi": mroi,
                "fade_roi": froi,
                "delta_fade_minus_model": d,
                "fade_beating_model_alert": bool(len(g) >= int(min_bets_alert) and d is not None and d > 0),
            }
        )
    seg_df = pd.DataFrame(seg_rows).sort_values(["delta_fade_minus_model", "bets"], ascending=[False, False]) if seg_rows else pd.DataFrame()

    payload = {
        "counts": {
            "rows_input": int(len(out)),
            "rows_matched": int(out["matched"].sum()),
            "rows_paired": int(len(paired)),
        },
        "overall": {
            "model_bets": int(len(paired)),
            "fade_bets": int(len(paired)),
            "model_roi": model_roi,
            "fade_roi": fade_roi,
            "delta_fade_minus_model": delta,
            "fade_beating_model_alert": bool(len(paired) >= int(min_bets_alert) and delta is not None and delta > 0),
        },
        "segments": [] if seg_df.empty else seg_df.to_dict(orient="records"),
    }
    return payload, out


def main() -> int:
    ap = argparse.ArgumentParser(description="NHL SOG model-vs-fade report from graded rows + card prices.")
    ap.add_argument("--graded-glob", default="tmp/graded/nhl_sog_graded_*.csv")
    ap.add_argument("--cards-dir", default="tmp/cards")
    ap.add_argument("--min-bets-alert", type=int, default=20)
    ap.add_argument("--out-json", default="tmp/analysis/nhl_model_vs_fade_summary.json")
    ap.add_argument("--out-segments-csv", default="tmp/analysis/nhl_model_vs_fade_by_segment.csv")
    ap.add_argument("--out-rows-csv", default="tmp/analysis/nhl_model_vs_fade_rows.csv")
    args = ap.parse_args()

    cards_dir = Path(args.cards_dir).expanduser()
    out_json = Path(args.out_json).expanduser()
    out_segments_csv = Path(args.out_segments_csv).expanduser()
    out_rows_csv = Path(args.out_rows_csv).expanduser()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_segments_csv.parent.mkdir(parents=True, exist_ok=True)
    out_rows_csv.parent.mkdir(parents=True, exist_ok=True)

    df = _load_and_join(args.graded_glob, cards_dir)
    payload, rows = _calc_payload(df, min_bets_alert=int(args.min_bets_alert))
    payload["inputs"] = {
        "graded_glob": str(args.graded_glob),
        "cards_dir": str(cards_dir),
        "min_bets_alert": int(args.min_bets_alert),
    }
    payload["outputs"] = {
        "summary_json": str(out_json),
        "segments_csv": str(out_segments_csv),
        "rows_csv": str(out_rows_csv),
    }

    pd.DataFrame(payload.get("segments") or []).to_csv(out_segments_csv, index=False)
    rows.to_csv(out_rows_csv, index=False)
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
