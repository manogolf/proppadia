#!/usr/bin/env python3
"""NHL odds line movement through the day (read-only; NHL Backlog P2.28).

Question: are earlier prices better or worse than later ones, and does the
price on the side our model would take improve or worsen if we wait?

Source: every retained 2026 odds snapshot under
`artifacts/operational/nhl/odds_observations/season=2026` except CLOSING
(10-13 minutes before puck; not a market the operator can bet). Markets: SOG
and Points main lines (player_shots_on_goal, player_points) and Moneyline
(h2h). Each offer (slate, game, book, market, player, line, side) seen in an
earlier snapshot is compared with the same offer in the slate's last
pre-puck snapshot taken at least 30 minutes later.

Measures, in implied-probability points of the offered price (positive =
the price got worse for a bettor on that side): mean change, share worse /
better / unchanged, change in the book's two-sided hold, and how often an
earlier offer is gone later. Grouped by hours before puck at the earlier
snapshot and by book group (sportsbooks, prediction markets, BetOnline).

Model side: production SOG / Points HGB p_over per (game, player, line) from
the slate's latest market-attachment run (sog_with_market.csv /
points_with_market.csv); Moneyline V3 and V2 home-win probability from the
slate's latest cross-market capture. OVER (home) when the probability is at
least 0.5.
"""
from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OBS = ROOT / "artifacts/operational/nhl/odds_observations/season=2026"
OUT = ROOT / "artifacts/analysis/nhl/odds_line_movement/2026-10-26"
MARKETS = {"player_shots_on_goal": "SOG", "player_points": "POINTS", "h2h": "MONEYLINE"}
PREDICTION_MARKETS = {"kalshi", "novig", "prophetx", "polymarket", "polymarket_us", "betopenly"}
MIN_GAP_HOURS = 0.5
BUCKETS = [(10, 99, ">=10h"), (6, 10, "6-10h"), (3, 6, "3-6h"), (1, 3, "1-3h"), (0, 1, "<1h")]


def book_group(book: str) -> str:
    if book in PREDICTION_MARKETS:
        return "prediction_markets"
    return "betonline" if book == "betonlineag" else "sportsbooks"


def implied(price: pd.Series) -> pd.Series:
    p = price.astype(float)
    return np.where(p < 0, -p / (-p + 100), 100 / (p + 100))


def norm_name(name) -> str:
    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode()
    return " ".join(text.lower().replace(".", "").replace("'", "").split())


def load_snapshots() -> pd.DataFrame:
    frames = []
    for obs in sorted(OBS.glob("slate_date=*/observation=*")):
        summary = obs / "observation_summary.json"
        rows_path = obs / "normalized_odds.jsonl"
        if not summary.is_file() or not rows_path.is_file():
            continue
        meta = json.loads(summary.read_text())
        phase = meta.get("phase") or meta.get("odds_phase") or meta.get("observation_phase")
        if phase == "CLOSING" or meta.get("classification") != "CAPTURED_NONEMPTY":
            continue
        binding = {}
        bind_path = obs / "game_binding.jsonl"
        if bind_path.is_file():
            for line in bind_path.read_text().splitlines():
                b = json.loads(line)
                if b.get("binding_status") == "MATCHED":
                    binding[b["provider_event_id"]] = int(b["canonical_game_id"])
        rows = pd.read_json(rows_path, lines=True)
        rows = rows[rows.market.isin(MARKETS)]
        if rows.empty:
            continue
        rows = rows.assign(slate=obs.parent.name.split("=")[1], phase=phase, snapshot=obs.name,
                           obs_utc=pd.Timestamp(meta["observation_timestamp_utc"]),
                           game_id=rows.provider_event_id.map(binding))
        frames.append(rows)
    d = pd.concat(frames, ignore_index=True)
    d = d[d.game_id.notna()].copy()
    d["game_id"] = d.game_id.astype("int64")
    d["commence"] = pd.to_datetime(d.commence_time, utc=True)
    d["hours_to_puck"] = (d.commence - d.obs_utc).dt.total_seconds() / 3600
    d = d[d.hours_to_puck > 0.1]
    d["lane"] = d.market.map(MARKETS)
    d["player"] = d.description.map(norm_name)
    d["point"] = d.point.fillna(0.0)
    d["side"] = np.where(d.lane.eq("MONEYLINE"),
                         np.where(d.outcome.eq(d.home_team), "HOME", "AWAY"), d.outcome.str.upper())
    d["group"] = d.bookmaker.map(book_group)
    d["implied"] = implied(d.price)
    d["pt_hour"] = d.obs_utc.dt.tz_convert("America/Los_Angeles").dt.hour
    # two-sided hold per offer pair in the same snapshot
    pair = ["snapshot", "game_id", "bookmaker", "market", "player", "point"]
    hold = d.groupby(pair).implied.agg(["sum", "size"]).reset_index()
    hold = hold[hold["size"].eq(2)].rename(columns={"sum": "hold_sum"})[pair + ["hold_sum"]]
    d = d.merge(hold, on=pair, how="left")
    key = ["slate", "game_id", "bookmaker", "market", "player", "point", "side"]
    return d.drop_duplicates(key + ["snapshot"]).reset_index(drop=True)


def pairs_vs_latest(d: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Each earlier offer against the same offer in the game's last pre-puck snapshot."""
    key = ["slate", "game_id", "bookmaker", "market", "player", "point", "side"]
    last_snap = d.groupby(["slate", "game_id"]).obs_utc.max().rename("last_obs").reset_index()
    d = d.merge(last_snap, on=["slate", "game_id"])
    late = d[d.obs_utc.eq(d.last_obs)][key + ["implied", "hold_sum", "price"]]
    early = d[(d.last_obs - d.obs_utc).dt.total_seconds() / 3600 >= MIN_GAP_HOURS]
    m = early.merge(late, on=key, how="left", suffixes=("", "_late"), indicator=True)
    m["gone_later"] = m["_merge"].eq("left_only")
    m["bucket"] = pd.cut(m.hours_to_puck, [b[0] for b in BUCKETS][::-1] + [99], right=False,
                         labels=[b[2] for b in BUCKETS][::-1])
    matched = m[~m.gone_later].copy()
    matched["delta_pp"] = (matched.implied_late - matched.implied) * 100
    matched["hold_delta_pp"] = (matched.hold_sum_late - matched.hold_sum) * 100
    return m, matched


def summarize(m: pd.DataFrame, matched: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    g = matched.groupby(by, observed=True)
    out = g.agg(pairs=("delta_pp", "size"), mean_change_pp=("delta_pp", "mean"),
                median_change_pp=("delta_pp", "median"),
                worse_share=("delta_pp", lambda s: float((s > 0.05).mean())),
                better_share=("delta_pp", lambda s: float((s < -0.05).mean())),
                unchanged_share=("delta_pp", lambda s: float((s.abs() <= 0.05).mean())),
                hold_change_pp=("hold_delta_pp", "mean"))
    gone = m.groupby(by, observed=True).gone_later.mean().rename("gone_later_share")
    return out.join(gone).reset_index()


def model_sides() -> pd.DataFrame:
    parts = []
    for lane, folder, name in (("SOG", "sog_market_attachments", "sog_with_market.csv"),
                               ("POINTS", "points_market_attachments", "points_with_market.csv")):
        for slate_dir in sorted((ROOT / "artifacts/operational/nhl" / folder / "season=2026").glob("slate_date=*")):
            runs = sorted(slate_dir.glob(f"run_id=*/{name}"))
            if not runs:
                continue
            f = pd.read_csv(runs[-1])  # latest run of the slate (run ids sort by time)
            pcol = "p_over" if "p_over" in f else next((c for c in f.columns if c.startswith("p_over") or c == "prob_over"), None)
            name_col = "full_name" if "full_name" in f else "player_name"
            if pcol is None or name_col not in f or "line" not in f:
                continue
            f = f[[name_col, "game_id", "line", pcol]].dropna()
            parts.append(pd.DataFrame({"slate": slate_dir.name.split("=")[1], "lane": lane,
                                       "game_id": f.game_id.astype("int64"), "player": f[name_col].map(norm_name),
                                       "point": f.line.astype(float), "p_model": f[pcol].astype(float),
                                       "model_run": runs[-1].parent.name}))
    return pd.concat(parts, ignore_index=True).drop_duplicates(["slate", "lane", "game_id", "player", "point"], keep="last")


def moneyline_sides() -> pd.DataFrame:
    parts = []
    root = ROOT / "artifacts/operational/nhl/cross_market_shadow/season=2026"
    for slate_dir in sorted(root.glob("slate_date=*")):
        files = sorted(slate_dir.glob("run_type=*/state=*/moneyline_production_predictions.csv"),
                       key=lambda p: p.stat().st_mtime)
        if not files:
            continue
        f = pd.read_csv(files[-1])
        for model, col in (("V3", "challenger_home_win_probability"), ("V2", "v2_home_win_probability")):
            if col in f:
                parts.append(pd.DataFrame({"slate": slate_dir.name.split("=")[1], "model": model,
                                           "game_id": f.game_id.astype("int64"), "p_home": f[col].astype(float)}))
    return pd.concat(parts, ignore_index=True)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    d = load_snapshots()
    m, matched = pairs_vs_latest(d)
    inventory = {"snapshots": int(d.snapshot.nunique()), "slates": int(d.slate.nunique()),
                 "first_slate": d.slate.min(), "last_slate": d.slate.max(),
                 "offers": int(len(d)), "early_offers_compared": int(len(m)), "matched_pairs": int(len(matched))}
    tables = {
        "by_lane_group": summarize(m, matched, ["lane", "group"]),
        "by_lane_bucket": summarize(m, matched, ["lane", "bucket"]),
        "by_lane_group_bucket": summarize(m, matched, ["lane", "group", "bucket"]),
        "by_early_pt_hour": summarize(m, matched, ["lane", "pt_hour"]),
    }
    matched["week"] = pd.to_datetime(matched.slate).dt.to_period("W-SUN").astype(str)
    m["week"] = pd.to_datetime(m.slate).dt.to_period("W-SUN").astype(str)
    tables["by_week"] = summarize(m, matched, ["lane", "week"])
    # model side, props
    sides = model_sides()
    props = matched[matched.lane.isin(["SOG", "POINTS"])].merge(sides, on=["slate", "lane", "game_id", "player", "point"])
    props["model_side"] = np.where(props.p_model >= 0.5, "OVER", "UNDER")
    props["on_model_side"] = props.side.eq(props.model_side)
    tables["props_model_side"] = summarize(props.assign(gone_later=False), props, ["lane", "on_model_side", "group"])
    tables["props_model_side_by_line"] = summarize(props.assign(gone_later=False), props, ["lane", "point", "model_side", "on_model_side"])
    # model side, moneyline
    ml = matched[matched.lane.eq("MONEYLINE")]
    mls = moneyline_sides()
    ml = ml.merge(mls, on=["slate", "game_id"])
    ml["model_side"] = np.where(ml.p_home >= 0.5, "HOME", "AWAY")
    ml["on_model_side"] = ml.side.eq(ml.model_side)
    tables["moneyline_model_side"] = summarize(ml.assign(gone_later=False), ml, ["model", "on_model_side", "group"])
    for name, table in tables.items():
        table.to_csv(OUT / f"{name}.csv", index=False)
    inventory["props_pairs_with_model_side"] = int(len(props))
    inventory["moneyline_pairs_with_model_side"] = int(len(ml))
    (OUT / "summary.json").write_text(json.dumps(inventory, indent=2) + "\n")
    pd.set_option("display.width", 220); pd.set_option("display.max_rows", 200)
    print(json.dumps(inventory, indent=1))
    for name, table in tables.items():
        print(f"\n== {name}"); print(table.round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
