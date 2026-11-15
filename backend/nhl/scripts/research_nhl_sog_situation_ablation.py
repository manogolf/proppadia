#!/usr/bin/env python3
"""SOG situation-split ablation (NHL Backlog P2.52, first step). Research only.

Read-only on the database (research login); never changes production. Compares
the production SOG recipe (V2 features, BASE_KW) with two added feature sets:

  B, situation split: even-strength and power-play shot rates kept apart
    (EV SOG per EV minute, PP SOG per PP minute, both shrunk to the prior
    season's league rate), EV/PP/SH minutes averages, and a structural expected
    count = EV rate x EV minutes + PP rate x PP minutes.
  C, B + tonight's PP minutes: the player's share of his team's PP time (last
    10) x the PP minutes expected tonight from his team's PP time drawn and the
    opponent's PP time conceded (last 20 team games each).

Situation counts come from the NHL stats API per player-game (`skater/timeonice`
for EV/PP/SH seconds, `skater/powerplay` for PP shots, `skater/penaltykill` for
SH shots; EV shots = total - PP - SH), captured with SHA-256 manifests; the
database has situation shots for only part of 2025-26 and none of 2026-27.

D, components only: the component total (struct_lambda_tonight) used directly as
the expected count, with no fitted model, to show what the parts alone achieve.

`--fetch` captures the missing PP/PK reports (resumable). The default run fits
A/B/C before 2026-01-01 and scores 2026-01..04 (holdout), fits on seasons
2024-2025 and scores the graded production SOG player-games 2026-10-25..11-14
(live), and writes paired log loss, Brier by line, PP bands and the
P(over 1.5) .45-.60 band.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_sog_hgb_count_model as research  # noqa: E402
from backend.nhl.scripts.prepare_nhl_pp_toi_stats_api_correction import sha256_bytes  # noqa: E402
from backend.nhl.scripts.research_nhl_sog_shrunk_rate import score  # noqa: E402
from backend.nhl.sog_hgb_features import BASE_KW, FEATURES, _roll, fit, predict  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_situation_ablation_2026-11-15"
TOI_EVIDENCE = ROOT / "artifacts/analysis/nhl/pp_toi_stats_api_correction_2026-11-15"
REPORTS = {"powerplay": "https://api.nhle.com/stats/rest/en/skater/powerplay",
           "penaltykill": "https://api.nhle.com/stats/rest/en/skater/penaltykill"}
HOLDOUT_START, HOLDOUT_END = "2026-01-01", "2026-05-01"
LIVE_START, LIVE_END = "2026-10-25", "2026-11-14"
PRIOR_MINUTES_EV, PRIOR_MINUTES_PP = 180.0, 30.0
SKATERS_ON_PP = 5.0

SITUATION = ["ev_sog60_20", "ev_toi_avg_10", "pp_sog60_shrunk", "pp_toi_avg_10_off", "sh_toi_avg_10",
             "ev_rate_shrunk", "struct_lambda"]
TONIGHT = ["pp_share_10", "team_pp_min_20", "opp_pp_conceded_20", "exp_pp_min", "struct_lambda_tonight"]
SETS = {"A_production": FEATURES, "B_situation": FEATURES + SITUATION, "C_tonight_pp": FEATURES + SITUATION + TONIGHT}


def fetch(dates: list[str]) -> None:
    import requests
    session = requests.Session()
    session.headers["User-Agent"] = "research-client/1.0"
    for name, url in REPORTS.items():
        folder = OUT / "official" / name
        folder.mkdir(parents=True, exist_ok=True)
        manifest_path = OUT / f"official_{name}_manifest.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
        for day in dates:
            path = folder / f"{day}.json"
            if day in manifest and path.is_file() and sha256_bytes(path.read_bytes()) == manifest[day]["sha256"]:
                continue
            params = {"isAggregate": "false", "isGame": "true", "limit": "-1",
                      "cayenneExp": f'gameTypeId=2 and gameDate>="{day}" and gameDate<="{day} 23:59:59"'}
            for attempt in range(5):
                try:
                    resp = session.get(url, params=params, timeout=60)
                    resp.raise_for_status()
                    payload = json.loads(resp.content)
                    break
                except Exception:
                    if attempt == 4:
                        raise
                    time.sleep(2 * (attempt + 1))
            if len(payload["data"]) != payload["total"]:
                raise RuntimeError(f"OFFICIAL_RESPONSE_TRUNCATED:{name}:{day}")
            path.write_bytes(resp.content)
            manifest[day] = {"url": url, "params": params, "sha256": sha256_bytes(resp.content),
                             "rows": len(payload["data"])}
            manifest_path.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
            time.sleep(0.25)


def _load(folder: Path, manifest: Path, cols: dict[str, str]) -> pd.DataFrame:
    m = json.loads(manifest.read_text())
    rows = []
    for day, entry in sorted(m.items()):
        body = (folder / f"{day}.json").read_bytes()
        if sha256_bytes(body) != entry["sha256"]:
            raise RuntimeError(f"EVIDENCE_HASH_MISMATCH:{folder.name}:{day}")
        for r in json.loads(body)["data"]:
            rows.append({"game_id": int(r["gameId"]), "player_id": int(r["playerId"]),
                         **{new: r.get(old) for old, new in cols.items()}})
    return pd.DataFrame(rows).drop_duplicates(["game_id", "player_id"])


def situation_table() -> pd.DataFrame:
    toi = _load(TOI_EVIDENCE / "official", TOI_EVIDENCE / "official_manifest.json",
                {"evTimeOnIce": "ev_s", "ppTimeOnIce": "pp_s", "shTimeOnIce": "sh_s", "teamAbbrev": "team"})
    pp = _load(OUT / "official" / "powerplay", OUT / "official_powerplay_manifest.json", {"ppShots": "pp_shots"})
    pk = _load(OUT / "official" / "penaltykill", OUT / "official_penaltykill_manifest.json", {"shShots": "sh_shots"})
    out = toi.merge(pp, on=["game_id", "player_id"], how="left").merge(pk, on=["game_id", "player_id"], how="left")
    for c in ("pp_shots", "sh_shots"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
    for c in ("ev_s", "pp_s", "sh_s"):
        out[c.replace("_s", "_min")] = pd.to_numeric(out[c], errors="coerce") / 60
    return out[["game_id", "player_id", "team", "ev_min", "pp_min", "sh_min", "pp_shots", "sh_shots"]]


def add_features(frame: pd.DataFrame, sit: pd.DataFrame) -> pd.DataFrame:
    """Strict-prior situation features on the production frame (rows already sorted by player and date)."""
    f = frame.merge(sit, on=["game_id", "player_id"], how="left")
    f = f.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    f["ev_shots"] = (f.shots_on_goal - f.pp_shots.fillna(0) - f.sh_shots.fillna(0)).clip(lower=0)
    f.loc[f.ev_min.isna(), "ev_shots"] = np.nan
    p = "player_id"
    ev_s20, ev_t20 = _roll(f, p, "ev_shots", 20, "sum"), _roll(f, p, "ev_min", 20, "sum")
    f["ev_sog60_20"] = (ev_s20 / ev_t20 * 60).where(ev_t20 > 0)
    f["ev_toi_avg_10"] = _roll(f, p, "ev_min", 10, "mean")
    f["pp_toi_avg_10_off"] = _roll(f, p, "pp_min", 10, "mean")
    f["sh_toi_avg_10"] = _roll(f, p, "sh_min", 10, "mean")
    # league situation rates from the prior season, by position group
    lg = f.groupby(["season", "group"]).agg(es=("ev_shots", "sum"), et=("ev_min", "sum"),
                                             ps=("pp_shots", "sum"), pt=("pp_min", "sum")).reset_index()
    lg["lg_ev60"], lg["lg_pp60"] = lg.es / lg.et * 60, lg.ps / lg.pt * 60
    lg["season"] += 1
    f = f.merge(lg[["season", "group", "lg_ev60", "lg_pp60"]], on=["season", "group"], how="left")
    f = f.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    pp_s40, pp_t40 = _roll(f, p, "pp_shots", 40, "sum").fillna(0), _roll(f, p, "pp_min", 40, "sum").fillna(0)
    f["pp_sog60_shrunk"] = (pp_s40 + f.lg_pp60 * PRIOR_MINUTES_PP / 60) / (pp_t40 + PRIOR_MINUTES_PP) * 60
    ev_s, ev_t = ev_s20.fillna(0), ev_t20.fillna(0)
    f["ev_rate_shrunk"] = (ev_s + f.lg_ev60 * PRIOR_MINUTES_EV / 60) / (ev_t + PRIOR_MINUTES_EV) * 60
    f["struct_lambda"] = (f.ev_rate_shrunk * f.ev_toi_avg_10 + f.pp_sog60_shrunk * f.pp_toi_avg_10_off) / 60
    # team PP minutes per game (skater PP minutes / 5), drawn by a team and conceded by its opponent
    team_pp = (sit.groupby(["game_id", "team"]).pp_min.sum() / SKATERS_ON_PP).rename("team_pp").reset_index()
    games = sit.groupby("game_id").team.unique()
    pairs = [(g, t, [o for o in ts if o != t][0]) for g, ts in games.items() if len(ts) == 2 for t in ts]
    tg = pd.DataFrame(pairs, columns=["game_id", "team", "opp"]).merge(team_pp, on=["game_id", "team"])
    tg = tg.merge(team_pp.rename(columns={"team": "opp", "team_pp": "opp_pp"}), on=["game_id", "opp"])
    dates = f.drop_duplicates("game_id")[["game_id", "game_date"]]
    tg = tg.merge(dates, on="game_id").sort_values(["team", "game_date", "game_id"])
    tg["team_pp_min_20"] = _roll(tg, "team", "team_pp", 20, "mean")
    tg["pp_conceded_20"] = _roll(tg, "team", "opp_pp", 20, "mean")      # PP time this team's opponents drew
    f = f.merge(tg[["game_id", "team", "opp", "team_pp_min_20", "team_pp"]], on=["game_id", "team"], how="left")
    conceded = tg[["game_id", "team", "pp_conceded_20"]].rename(columns={"team": "opp", "pp_conceded_20": "opp_pp_conceded_20"})
    f = f.merge(conceded, on=["game_id", "opp"], how="left")
    f = f.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    share_num, share_den = _roll(f, p, "pp_min", 10, "sum"), _roll(f, p, "team_pp", 10, "sum")
    f["pp_share_10"] = (share_num / share_den).where(share_den > 0)
    f["exp_pp_min"] = f.pp_share_10 * (f.team_pp_min_20 + f.opp_pp_conceded_20) / 2
    f["struct_lambda_tonight"] = (f.ev_rate_shrunk * f.ev_toi_avg_10 + f.pp_sog60_shrunk * f.exp_pp_min) / 60
    return f


def _fit(frame: pd.DataFrame, cols: list[str]):
    from sklearn.ensemble import HistGradientBoostingRegressor
    return HistGradientBoostingRegressor(**BASE_KW).fit(frame[cols].astype(float).to_numpy(),
                                                       frame.shots_on_goal.to_numpy(int))


def compare(frame: pd.DataFrame, cols: dict[str, str], name: str) -> dict:
    y = frame.shots_on_goal.to_numpy(int)
    out = {"rows": int(len(frame)), "scores": {k: score(y, frame[c].to_numpy()) for k, c in cols.items()}}
    base = cols["A_production"]
    out["paired_vs_A"] = {k: research.paired_ci(frame, c, base) for k, c in cols.items() if c != base}
    bands = frame.assign(pp_band=pd.cut(frame.pp_toi_avg_10.fillna(0), [-0.01, 0.25, 1, 2, 3, 99],
                                        labels=["0-0.25", "0.25-1", "1-2", "2-3", "3+"]))
    rows = []
    for band, g in bands.groupby("pp_band", observed=True):
        yy = g.shots_on_goal.to_numpy(int)
        row = {"pp_band": band, "n": len(g), "actual": float(yy.mean())}
        for k, c in cols.items():
            lam = g[c].clip(0.02).to_numpy()
            row[f"{k}_mean"], row[f"{k}_ll"] = float(lam.mean()), float(-poisson.logpmf(yy, lam).mean())
        rows.append(row)
    pd.DataFrame(rows).to_csv(OUT / f"{name}_by_pp_band.csv", index=False)
    rows = []
    for k, c in cols.items():
        p = poisson.sf(1, frame[c].clip(0.02).to_numpy())
        for lo, hi in ((0.45, 0.5), (0.5, 0.55), (0.55, 0.6), (0.45, 0.6)):
            m = (p >= lo) & (p < hi)
            rows.append({"model": k, "band": f"{lo:.2f}-{hi:.2f}", "n": int(m.sum()),
                         "model_p": float(p[m].mean()) if m.any() else None,
                         "actual": float((y[m] >= 2).mean()) if m.any() else None})
    pd.DataFrame(rows).to_csv(OUT / f"{name}_imp034_band.csv", index=False)
    return out


def component_targets(frame: pd.DataFrame) -> list[dict]:
    """Each component against its own observed target in the same player-game."""
    out = []
    for name, pred, actual in (("EV minutes (last-10 mean)", "ev_toi_avg_10", "ev_min"),
                               ("PP minutes (last-10 mean)", "pp_toi_avg_10_off", "pp_min"),
                               ("PP minutes (tonight: share x expected team PP)", "exp_pp_min", "pp_min"),
                               ("Total minutes (last-10 mean)", "toi_avg_10", "toi_minutes")):
        g = frame[[pred, actual]].dropna()
        out.append({"component": name, "n": int(len(g)), "mean_pred": float(g[pred].mean()),
                    "mean_actual": float(g[actual].mean()), "mae": float((g[pred] - g[actual]).abs().mean()),
                    "corr": float(g[pred].corr(g[actual]))})
    return out


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    if "--fetch" in sys.argv[1:]:
        manifest = json.loads((TOI_EVIDENCE / "official_manifest.json").read_text())
        fetch(sorted(manifest))
        print("FETCH COMPLETE")
        return 0
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    frame["game_id"], frame["player_id"] = frame.game_id.astype("int64"), frame.player_id.astype("int64")
    frame = add_features(frame, situation_table())
    summary: dict = {"sets": SETS, "coverage": {}}
    train_all = frame[frame.season.isin([2024, 2025]) & frame.toi_avg_10.notna() & frame.shrunk_lambda_180.notna()]
    summary["coverage"]["training_rows"] = int(len(train_all))
    summary["coverage"]["situation_known_share"] = float(train_all.ev_min.notna().mean())
    cut = pd.Timestamp(HOLDOUT_START)
    test = train_all[(train_all.game_date >= cut) & (train_all.game_date < pd.Timestamp(HOLDOUT_END))].copy()
    early = train_all[train_all.game_date < cut]
    cols = {}
    for name, feats in SETS.items():
        test[name] = _fit(early, feats).predict(test[feats].astype(float).to_numpy())
        cols[name] = name
    test["D_components"] = test.struct_lambda_tonight.fillna(test.shrunk_lambda_180)
    cols["D_components"] = "D_components"
    summary["holdout"] = compare(test, cols, "holdout")
    summary["holdout"]["component_targets"] = component_targets(test)
    live = frame[(frame.season == 2026) & (frame.game_date >= pd.Timestamp(LIVE_START))
                 & (frame.game_date <= pd.Timestamp(LIVE_END)) & frame.toi_avg_10.notna()
                 & frame.shrunk_lambda_180.notna()].copy()
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    graded = []
    for day in weekly._days(LIVE_START, LIVE_END):
        s = weekly._summary(day)
        if s:
            r = weekly._cached_rows("sog", day, s["summary_identity"])
            if not r.empty:
                graded.append(r[r.line.eq(1.5)][["game_id", "player_id"]])
    graded = pd.concat(graded).astype("int64").drop_duplicates()
    live = live.merge(graded, on=["game_id", "player_id"], how="inner")
    for name, feats in SETS.items():
        live[name] = _fit(train_all, feats).predict(live[feats].astype(float).to_numpy())
    live["D_components"] = live.struct_lambda_tonight.fillna(live.shrunk_lambda_180)
    summary["live"] = compare(live, cols, "live")
    summary["live"]["component_targets"] = component_targets(live)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(json.dumps({k: summary[k] for k in ("coverage",)}, indent=1))
    for part in ("holdout", "live"):
        s = summary[part]
        print(part, s["rows"], {k: round(v["poisson_log_loss"], 5) for k, v in s["scores"].items()},
              {k: (round(v["mean"], 5), [round(x, 5) for x in v["ci95"]]) for k, v in s["paired_vs_A"].items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
