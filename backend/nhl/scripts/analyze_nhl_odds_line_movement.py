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

Prices at or below -10000 are placeholders (no offer) and are dropped.

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

06:00 PT trial (P2.28): on slates with an EARLY_TRIAL snapshot, each 06:00
offer is also compared with the same offer in each game's last MORNING
(before 10:00 PT), MIDDAY (10:00-13:59), LATE (14:00+) daily snapshot and its
CLOSING snapshot. CLOSING appears only there, as a reference; it never enters
the movement tables. Coverage by time of day reports, per snapshot, the share
of its games with any line and the line depth (players per game against the
slate's most) per market and book group.

Mean changes carry 95% intervals clustered by game. Output goes to
`artifacts/analysis/nhl/odds_line_movement/<--as-of>/` (default today);
`--through` caps the last slate.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OBS = ROOT / "artifacts/operational/nhl/odds_observations/season=2026"
OUT_ROOT = ROOT / "artifacts/analysis/nhl/odds_line_movement"
MARKETS = {"player_shots_on_goal": "SOG", "player_points": "POINTS",
           "player_total_saves": "SAVES", "h2h": "MONEYLINE"}
PREDICTION_MARKETS = {"kalshi", "novig", "prophetx", "polymarket", "polymarket_us", "betopenly"}
MIN_GAP_HOURS = 0.5
# Prices at or below this are placeholders, not offers (Novig posts -99900 for
# a side with no liquidity); they are dropped before pairing.
PLACEHOLDER_PRICE = -10000
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


def load_snapshots(include_closing: bool = False) -> pd.DataFrame:
    frames = []
    for obs in sorted(OBS.glob("slate_date=*/observation=*")):
        summary = obs / "observation_summary.json"
        rows_path = obs / "normalized_odds.jsonl"
        if not summary.is_file() or not rows_path.is_file():
            continue
        meta = json.loads(summary.read_text())
        phase = meta.get("phase") or meta.get("odds_phase") or meta.get("observation_phase")
        if (phase == "CLOSING" and not include_closing) or meta.get("classification") != "CAPTURED_NONEMPTY":
            continue
        binding = {}
        bind_path = obs / "game_binding.jsonl"
        if bind_path.is_file():
            for line in bind_path.read_text().splitlines():
                b = json.loads(line)
                if b.get("binding_status") == "MATCHED":
                    binding[b["provider_event_id"]] = int(b["canonical_game_id"])
        rows = pd.read_json(rows_path, lines=True)
        rows = rows[rows.market.isin(MARKETS) & (rows.price > PLACEHOLDER_PRICE)]
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
    d["snap_class"] = np.select(
        [d.phase.eq("EARLY_TRIAL"), d.phase.eq("CLOSING"), d.pt_hour < 10, d.pt_hour < 14],
        ["0600", "CLOSING", "MORNING", "MIDDAY"], "LATE")
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


def cluster_ci(values: pd.Series, clusters: pd.Series) -> tuple[float, float]:
    """95% interval for a mean, standard error clustered by game."""
    x = values.to_numpy(float)
    n, g = len(x), clusters.nunique()
    if n == 0 or g < 2:
        return (np.nan, np.nan)
    resid = pd.Series(x - x.mean()).groupby(clusters.to_numpy()).sum()
    se = np.sqrt((resid ** 2).sum() * g / (g - 1)) / n
    return (x.mean() - 1.96 * se, x.mean() + 1.96 * se)


def summarize(m: pd.DataFrame, matched: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    g = matched.groupby(by, observed=True)
    out = g.agg(pairs=("delta_pp", "size"), mean_change_pp=("delta_pp", "mean"),
                median_change_pp=("delta_pp", "median"),
                worse_share=("delta_pp", lambda s: float((s > 0.05).mean())),
                better_share=("delta_pp", lambda s: float((s < -0.05).mean())),
                unchanged_share=("delta_pp", lambda s: float((s.abs() <= 0.05).mean())),
                hold_change_pp=("hold_delta_pp", "mean"))
    ci = g[["delta_pp", "game_id"]].apply(lambda t: pd.Series(cluster_ci(t.delta_pp, t.game_id), index=["ci_low_pp", "ci_high_pp"]))
    games = g.game_id.nunique().rename("games")
    gone = m.groupby(by, observed=True).gone_later.mean().rename("gone_later_share")
    return out.join(ci).join(games).join(gone).reset_index()


def trial_pairs(d: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """06:00 EARLY_TRIAL offers against each game's last snapshot of each later class."""
    key = ["slate", "game_id", "bookmaker", "market", "player", "point", "side"]
    trial_slates = d.loc[d.phase.eq("EARLY_TRIAL"), "slate"].unique()
    d = d[d.slate.isin(trial_slates)]
    base = d[d.snap_class.eq("0600")]
    frames = []
    for cls in ("MORNING", "MIDDAY", "LATE", "CLOSING"):
        later = d[d.snap_class.eq(cls)]
        last = later.groupby(["slate", "game_id"]).obs_utc.max().rename("cls_obs").reset_index()
        later = later.merge(last, on=["slate", "game_id"])
        later = later[later.obs_utc.eq(later.cls_obs)][key + ["implied", "hold_sum"]]
        games = later[["slate", "game_id"]].drop_duplicates()
        b = base.merge(games, on=["slate", "game_id"])  # only games that have this class
        m = b.merge(later, on=key, how="outer", suffixes=("", "_late"), indicator=True)
        m["later_class"] = cls
        frames.append(m)
    m = pd.concat(frames, ignore_index=True)
    m["lane"] = m.market.map(MARKETS)
    m["group"] = m.bookmaker.map(book_group)
    m["gone_later"] = m["_merge"].eq("left_only")
    m["new_later"] = m["_merge"].eq("right_only")
    matched = m[m["_merge"].eq("both")].copy()
    matched["delta_pp"] = (matched.implied_late - matched.implied) * 100
    matched["hold_delta_pp"] = (matched.hold_sum_late - matched.hold_sum) * 100
    return m, matched


def trial_coverage(m: pd.DataFrame) -> pd.DataFrame:
    """Offers posted at 06:00 vs the later class, and games with any line."""
    by = ["lane", "group", "later_class"]
    offers = m.groupby(by).agg(offers_0600=("new_later", lambda s: int((~s).sum())),
                               offers_later=("gone_later", lambda s: int((~s).sum())),
                               gone_by_later=("gone_later", "sum"), new_by_later=("new_later", "sum"))
    offers["later_offers_already_at_0600"] = 1 - offers.new_by_later / offers.offers_later
    games_0600 = m[~m.new_later].groupby(by).game_id.nunique().rename("games_with_line_0600")
    games_later = m[~m.gone_later].groupby(by).game_id.nunique().rename("games_with_line_later")
    games_all = m.groupby("later_class").game_id.nunique().rename("games_in_class")
    out = offers.join(games_0600).join(games_later).reset_index().merge(games_all.reset_index(), on="later_class")
    return out


def coverage_by_time(d: pd.DataFrame) -> pd.DataFrame:
    """Per snapshot and market: share of its games with a line, and line depth vs the slate's most."""
    games = d.groupby("snapshot").game_id.nunique().rename("snapshot_games")
    # CLOSING snapshots hold one game each; depth compares players per game.
    per = (d[d.lane.ne("MONEYLINE")].groupby(["slate", "snapshot", "snap_class", "pt_hour", "lane", "group", "game_id"])
           .player.nunique().rename("players").reset_index())
    best = per.groupby(["slate", "lane", "group", "game_id"]).players.max().rename("best")
    per = per.join(best, on=["slate", "lane", "group", "game_id"])
    per["depth"] = per.players / per.best
    snap = per.groupby(["slate", "snapshot", "snap_class", "pt_hour", "lane", "group"]).agg(
        games_with_line=("game_id", "nunique"), depth=("depth", "mean")).reset_index()
    snap = snap.join(games, on="snapshot")
    # snapshots where the market/group is entirely absent count as zero coverage
    grid = d[["slate", "snapshot", "snap_class", "pt_hour"]].drop_duplicates().merge(
        snap[["lane", "group"]].drop_duplicates(), how="cross")
    grid = grid.join(games, on="snapshot")
    snap = grid.merge(snap.drop(columns="snapshot_games"), how="left",
                      on=["slate", "snapshot", "snap_class", "pt_hour", "lane", "group"]).fillna(
        {"games_with_line": 0, "depth": 0})
    snap["game_share"] = snap.games_with_line / snap.snapshot_games
    snap["window"] = np.where(snap.snap_class.eq("CLOSING"), "CLOSING", snap.pt_hour.astype(str).str.zfill(2) + ":00")
    return (snap.groupby(["lane", "group", "window"]).agg(
        snapshots=("snapshot", "nunique"), slates=("slate", "nunique"),
        game_share=("game_share", "mean"), all_games_share=("game_share", lambda s: float((s >= 0.999).mean())),
        depth=("depth", "mean")).reset_index())


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
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--as-of", default=date.today().isoformat(), help="output folder name (YYYY-MM-DD)")
    ap.add_argument("--through", help="last slate date to include (default: all retained slates)")
    args = ap.parse_args()
    OUT = OUT_ROOT / args.as_of
    OUT.mkdir(parents=True, exist_ok=True)
    d_all = load_snapshots(include_closing=True)
    if args.through:
        d_all = d_all[d_all.slate.le(args.through)]
    d = d_all[d_all.phase.ne("CLOSING")].reset_index(drop=True)
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
    # 06:00 trial: 06:00 against MORNING / MIDDAY / LATE and CLOSING (reference only)
    tm, tmatched = trial_pairs(d_all)
    tables["trial_coverage"] = trial_coverage(tm)
    tables["trial_price"] = summarize(tm[~tm.new_later], tmatched, ["lane", "group", "later_class"])
    tprops = tmatched[tmatched.lane.isin(["SOG", "POINTS"])].merge(sides, on=["slate", "lane", "game_id", "player", "point"])
    tprops["on_model_side"] = tprops.side.eq(np.where(tprops.p_model >= 0.5, "OVER", "UNDER"))
    tprops = tprops[tprops.on_model_side]
    tables["trial_model_side"] = summarize(tprops.assign(gone_later=False), tprops, ["lane", "group", "later_class"])
    tables["coverage_by_time"] = coverage_by_time(d_all)
    inventory["trial_slates"] = sorted(d_all.loc[d_all.phase.eq("EARLY_TRIAL"), "slate"].unique().tolist())
    inventory["trial_pairs"] = int(len(tmatched))
    inventory["trial_model_side_pairs"] = int(len(tprops))
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
