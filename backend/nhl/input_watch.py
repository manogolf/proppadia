"""Daily input watch: surface model-input problems before anyone asks (2026-10-28).

Runs at the end of each daily run, after production SOG, Points HGB and the
odds observation. Read-only: no database access, no provider requests, no
change to any prediction. It writes
``artifacts/operational/nhl/input_watch/slate_date=<d>/run_id=<r>/`` with
``input_watch.json`` and ``input_watch.md``.

1. Input sanity: impossible or substituted values in each player's inputs,
   and Points inputs outside the range the frozen model saw in training
   (``backend/nhl/config/points_hgb_training_feature_ranges.json``).
2. Disagreement explainer: the day's largest gaps between our probability on
   the selected side and the median same-book no-vig market probability, on
   two-sided lines quoted before the watch ran. Quote matching reuses
   ``export_nhl_all_candidate_evaluation.load_retained_quotes``. Each pick
   lists the inputs that explain it: rule reasons from the 2026-10-28 study
   (NHL Backlog P2.29, P2.37) and inputs unusual against players the market
   prices the same.
3. Source health: the prior date's shot-attempt source diagnostic.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.scripts import export_nhl_all_candidate_evaluation as candidate_export

ROOT = Path(__file__).resolve().parents[2]
POINTS_RANGES_PATH = ROOT / "backend/nhl/config/points_hgb_training_feature_ranges.json"
OUTPUT_ROOT = ROOT / "artifacts/operational/nhl/input_watch"
SCHEMA = "NHL_INPUT_WATCH_V1"
TOP_N = 10
DRIFT_SHARE = 0.10       # flag a feature when this share of players is outside training p01-p99
IMPUTED_SHARE = 0.02     # flag substituted attempts above this share of players
PEER_WINDOW = 0.05       # peers: same line, market P(over) within +/- this
PEER_MIN = 8
PEER_Z = 1.5
POINTS_FEATURES = [
    "is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
    "current_season_points_prior", "current_season_games_prior", "mean_toi_last10",
    "mean_pp_toi_last10", "team_d10_sf_per_game", "last10_team_sog_share",
]
POINTS_LINES = ((0.5, "prob_over_0_5"), (1.5, "prob_over_1_5"), (2.5, "prob_over_2_5"))
SOG_LINES = ((1.5, "p_over_1_5"), (2.5, "p_over_2_5"), (3.5, "p_over_3_5"))
FEATURE_LABELS = {
    "d10_sog_per60": "shots/60", "attempts_d10_per60": "attempts/60",
    "player_points_last10": "points/game last 10", "current_season_games_prior": "games played",
    "current_season_points_prior": "season points", "mean_toi_last10": "TOI",
    "mean_pp_toi_last10": "PP TOI", "team_d10_sf_per_game": "team shots/game",
    "last10_team_sog_share": "team shot share", "season_ppg": "season points/game",
    "feature_sog60_10": "shots/60 (10)", "feature_sog60_season": "shots/60 season",
    "feature_toi_avg_10": "TOI (10)", "feature_pp_toi_avg_10": "PP TOI (10)",
    "feature_att60_10": "attempts/60 (10)", "feature_games_season": "games played",
    "feature_team_sf_10": "team shots/game", "feature_opp_sa_10": "opponent shots against",
}


def _check(name: str, mask: pd.Series, frame: pd.DataFrame, *, flag: bool, detail: str) -> dict[str, Any]:
    hits = frame[mask.fillna(False)]
    examples = hits.get("player_name", hits.player_id.astype(str)).astype(str).head(5).tolist()
    return {"check": name, "count": int(len(hits)), "flag": bool(flag and len(hits)),
            "detail": detail, "examples": examples}


def points_sanity(features: pd.DataFrame, ranges: dict[str, Any]) -> list[dict[str, Any]]:
    f = features.copy()
    for column in POINTS_FEATURES:
        f[column] = pd.to_numeric(f[column], errors="coerce")
    n = max(len(f), 1)
    rates = ["d10_sog_per60", "attempts_d10_per60", "player_points_last10", "mean_toi_last10"]
    per_game_share = (f.d10_sog_per60 * f.mean_toi_last10 / 60) / f.team_d10_sf_per_game.replace(0, np.nan)
    imputed = f.attempts_d10_per60.isna() & f.d10_sog_per60.notna()
    checks = [
        _check("ATTEMPTS_BELOW_SOG", f.attempts_d10_per60 < f.d10_sog_per60, f, flag=True,
               detail="attempt rate below shot rate (impossible)"),
        _check("TEAM_SHOTS_IMPLAUSIBLE", ~f.team_d10_sf_per_game.between(15, 45), f, flag=True,
               detail="team shots per game outside 15-45"),
        _check("SHOT_SHARE_INVALID", ~f.last10_team_sog_share.between(0, 0.5), f, flag=True,
               detail="team shot share outside 0-0.5"),
        _check("PP_TOI_ABOVE_TOI", f.mean_pp_toi_last10 > f.mean_toi_last10, f, flag=True,
               detail="power-play TOI above total TOI"),
        _check("ATTEMPTS_UNKNOWN_IMPUTED", imputed, f, flag=imputed.sum() / n > IMPUTED_SHARE,
               detail="attempt rate unknown; scored from shot rate x 1.993 (P2.37)"),
        _check("NO_HISTORY_ZERO_FILLED", f[rates].isna().all(axis=1), f, flag=False,
               detail="no recent games; frozen recipe fills zero (as in training)"),
        _check("SHOT_SHARE_DEFLATED_BY_MISSED_GAMES",
               f.last10_team_sog_share < 0.75 * per_game_share, f, flag=False,
               detail="share counts team games the player missed"),
    ]
    for column in POINTS_FEATURES:
        bounds = (ranges.get("features") or {}).get(column)
        if not bounds or column == "is_home":
            continue
        outside = f[column].notna() & ~f[column].between(bounds["p01"], bounds["p99"])
        checks.append(_check(f"OUT_OF_TRAINING_RANGE:{column}", outside, f,
                             flag=outside.sum() / n > DRIFT_SHARE,
                             detail=f"outside training p01-p99 [{bounds['p01']:g}, {bounds['p99']:g}]"))
    return checks


def sog_sanity(features: pd.DataFrame) -> list[dict[str, Any]]:
    f = features.copy()
    n = max(len(f), 1)
    num = lambda c: pd.to_numeric(f[c], errors="coerce")  # noqa: E731
    checks = [
        _check("NEGATIVE_RATE", (num("feature_sog60_10") < 0) | (num("feature_att60_10") < 0), f, flag=True,
               detail="negative shot or attempt rate"),
        _check("ATTEMPTS_BELOW_SOG", num("feature_att60_10") < num("feature_sog60_10"), f, flag=True,
               detail="attempt rate below shot rate (impossible)"),
        _check("TEAM_SHOTS_IMPLAUSIBLE", ~num("feature_team_sf_10").between(15, 45), f, flag=True,
               detail="team shots per game outside 15-45"),
        _check("PP_TOI_ABOVE_TOI", num("feature_pp_toi_avg_10") > num("feature_toi_avg_10"), f, flag=True,
               detail="power-play TOI above total TOI"),
    ]
    for column in [c for c in f.columns if c.startswith("feature_")]:
        missing = num(column).isna()
        checks.append(_check(f"MISSING:{column}", missing, f, flag=missing.sum() / n > DRIFT_SHARE,
                             detail="input missing"))
    return checks


def _market(quotes: list[dict[str, Any]]) -> pd.DataFrame:
    """Median same-book no-vig P(selected) per candidate on two-sided lines."""
    if not quotes:
        return pd.DataFrame(columns=["candidate_key", "market_p", "books", "selected_price"])
    q = pd.DataFrame(quotes)
    q["imp"] = pd.to_numeric(q.implied_probability, errors="coerce")
    q["is_selected"] = q.quote_side.eq(q.selected_side)
    wide = q.pivot_table(index=["candidate_key", "sportsbook_key"], columns="is_selected",
                         values=["imp", "american_price"], aggfunc="first").dropna()
    if wide.empty:
        return pd.DataFrame(columns=["candidate_key", "market_p", "books", "selected_price"])
    wide.columns = [f"{a}_{'sel' if b else 'opp'}" for a, b in wide.columns]
    wide["nv"] = wide.imp_sel / (wide.imp_sel + wide.imp_opp)
    return wide.groupby("candidate_key").agg(
        market_p=("nv", "median"), books=("nv", "size"),
        selected_price=("american_price_sel", "median")).reset_index()


def _candidates(rows: pd.DataFrame, *, market_key: str, lines, slate_date: str,
                starts: dict[int, str], now_utc: str, run_id: str) -> list[dict[str, Any]]:
    out = []
    for row in rows.itertuples(index=False):
        for line, column in lines:
            p_over = float(getattr(row, column))
            side = "OVER" if p_over >= 0.5 else "UNDER"
            candidate = {
                "canonical_season": 2026, "slate_date": slate_date, "game_id": str(int(row.game_id)),
                "player_id": str(int(row.player_id)), "goalie_id": "", "market_key": market_key,
                "market_line": str(float(line)), "selected_side": side, "model_role": "PRODUCTION",
                "model_family": market_key, "model_version": "input_watch",
                "prediction_identity": f"{run_id}:{int(row.game_id)}:{int(row.player_id)}:{line}",
                "probability_over": p_over, "probability_under": 1 - p_over,
                # Diagnostic: quotes observed before the watch ran count.
                "prediction_timestamp_utc": now_utc,
                "scheduled_start_utc": starts.get(int(row.game_id), ""),
                "parent_run_id": run_id,
            }
            candidate["candidate_key"] = candidate_export.candidate_key(candidate)
            out.append(candidate)
    return out


def _peer_reasons(row: pd.Series, frame: pd.DataFrame, columns: list[str]) -> list[str]:
    peers = frame[(frame.line == row.line) & (frame.market_p_over - row.market_p_over).abs().le(PEER_WINDOW)
                  & frame.player_id.ne(row.player_id)]
    if len(peers) < PEER_MIN:
        return []
    found = []
    for column in columns:
        value, values = row.get(column), pd.to_numeric(peers[column], errors="coerce").dropna()
        if pd.isna(value) or len(values) < PEER_MIN or values.std() == 0:
            continue
        z = (float(value) - values.mean()) / values.std()
        if abs(z) >= PEER_Z:
            found.append((abs(z), f"{FEATURE_LABELS.get(column, column)} {'high' if z > 0 else 'low'} "
                                  f"vs market peers ({float(value):.3g} vs {values.mean():.3g})"))
    return [text for _, text in sorted(found, reverse=True)[:2]]


def _points_reasons(row: pd.Series, slate: pd.DataFrame, ranges: dict[str, Any]) -> list[str]:
    reasons = []
    if pd.isna(row.attempts_d10_per60) and pd.notna(row.d10_sog_per60):
        reasons.append("attempts unknown (scored from shot rate)")
    if row.current_season_games_prior < 5:
        reasons.append(f"only {int(row.current_season_games_prior)} games played")
    per_game = (row.d10_sog_per60 * row.mean_toi_last10 / 60) / row.team_d10_sf_per_game \
        if row.team_d10_sf_per_game else np.nan
    if pd.notna(per_game) and row.last10_team_sog_share < 0.75 * per_game:
        reasons.append("shot share understated by missed games")
    if row.get("is_defense") == 1:
        reasons.append("defenseman (position is not a model input)")
    if row.season_ppg >= 1.5:
        reasons.append(f"{row.season_ppg:.2f} points/game: near the model's ceiling")
    if row.season_ppg >= 0.7 and row.mean_toi_last10 < slate.mean_toi_last10.median() - 3:
        reasons.append(f"low TOI ({row.mean_toi_last10:.1f}) for his scoring")
    if row.season_ppg >= 0.6 and row.last10_team_sog_share < slate.last10_team_sog_share.quantile(0.33):
        reasons.append("scores without shooting (low shot share)")
    if row.selected_side == "OVER" and row.last10_team_sog_share >= 0.10:
        reasons.append("high shot share (model rewards shooters)")
    for column in POINTS_FEATURES:
        bounds = (ranges.get("features") or {}).get(column)
        value = row.get(column)
        if bounds and column != "is_home" and pd.notna(value) and not bounds["p01"] <= value <= bounds["p99"]:
            reasons.append(f"{FEATURE_LABELS.get(column, column)} {value:.3g} outside training range")
    return reasons


def explain_disagreements(*, points: pd.DataFrame | None, sog: pd.DataFrame | None, slate_date: str,
                          run_id: str, operational_root: Path, now_utc: str,
                          ranges: dict[str, Any]) -> dict[str, Any]:
    starts: dict[int, str] = {}
    if points is not None and "game_start_utc" in points:
        starts = {int(g): str(s).replace(" ", "T") for g, s in zip(points.game_id, points.game_start_utc)}
    lanes: dict[str, tuple] = {}
    if points is not None and len(points):
        lanes["points"] = (points, "player_points", POINTS_LINES)
    if sog is not None and len(sog):
        lanes["sog"] = (sog, "shots_on_goal", SOG_LINES)
    candidates = []
    for frame, market_key, lines in lanes.values():
        candidates += _candidates(frame, market_key=market_key, lines=lines, slate_date=slate_date,
                                  starts=starts, now_utc=now_utc, run_id=run_id)
    quotes, _ = candidate_export.load_retained_quotes(operational_root, candidates, price_both_sides=True)
    market = _market(candidate_export.deduplicate_quotes(quotes))
    cand = pd.DataFrame(candidates).merge(market, on="candidate_key", how="inner")
    out: dict[str, Any] = {"two_sided_candidates": int(len(cand)), "lanes": {}}
    for lane, (frame, market_key, _lines) in lanes.items():
        part = cand[cand.market_key.eq(market_key)].copy()
        if part.empty:
            out["lanes"][lane] = []
            continue
        part["game_id"] = part.game_id.astype("int64")
        part["player_id"] = part.player_id.astype("int64")
        part["line"] = part.market_line.astype(float)
        part["model_p"] = np.where(part.selected_side.eq("OVER"), part.probability_over, part.probability_under)
        part["gap"] = part.model_p - part.market_p
        part["market_p_over"] = np.where(part.selected_side.eq("OVER"), part.market_p, 1 - part.market_p)
        frame = frame.copy()
        frame["game_id"] = frame.game_id.astype("int64")
        frame["player_id"] = frame.player_id.astype("int64")
        part = part.merge(frame, on=["game_id", "player_id"], how="left", suffixes=("", "_feature"))
        if lane == "points":
            part["season_ppg"] = part.current_season_points_prior / part.current_season_games_prior.replace(0, np.nan)
            peer_columns = ["season_ppg", "player_points_last10", "mean_toi_last10", "mean_pp_toi_last10",
                            "d10_sog_per60", "attempts_d10_per60", "last10_team_sog_share",
                            "current_season_games_prior", "team_d10_sf_per_game"]
        else:
            peer_columns = ["feature_sog60_10", "feature_sog60_season", "feature_toi_avg_10",
                            "feature_pp_toi_avg_10", "feature_att60_10", "feature_games_season",
                            "feature_team_sf_10", "feature_opp_sa_10"]
        top = part.sort_values("gap", ascending=False).head(TOP_N)
        picks = []
        for _, row in top.iterrows():
            reasons = (_points_reasons(row, frame.assign(
                season_ppg=frame.current_season_points_prior / frame.current_season_games_prior.replace(0, np.nan)),
                ranges) if lane == "points" else [])
            if lane == "sog":
                if row.get("feature_games_season", 99) < 5:
                    reasons.append(f"only {int(row.feature_games_season)} games played")
                if row.get("feature_is_defense") == 1:
                    reasons.append("defenseman")
                missing = [FEATURE_LABELS.get(c, c) for c in peer_columns if pd.isna(row.get(c))]
                if missing:
                    reasons.append("missing: " + ", ".join(missing))
            reasons += _peer_reasons(row, part, peer_columns)
            picks.append({
                "player": str(row.get("player_name") or row.player_id), "player_id": int(row.player_id),
                "game_id": int(row.game_id), "line": float(row.line), "side": row.selected_side,
                "model_p": round(float(row.model_p), 4), "market_p": round(float(row.market_p), 4),
                "gap": round(float(row.gap), 4), "books": int(row.books),
                "book_price": None if pd.isna(row.selected_price) else int(row.selected_price),
                "reasons": reasons or ["no input stands out; check the market (news, lineup)"],
            })
        out["lanes"][lane] = picks
    return out


def attempt_source_health(operational_root: Path, prior_date: str | None, run_id: str) -> dict[str, Any]:
    if not prior_date:
        return {"status": "NOT_APPLICABLE"}
    path = (operational_root / "shot_attempts_source_diagnostics" / f"slate_date={prior_date}"
            / f"run_id={run_id}" / "attempt_source_diagnostic.json")
    if not path.is_file():
        return {"status": "NO_DIAGNOSTIC_FOR_THIS_RUN", "prior_date": prior_date}
    diagnostic = json.loads(path.read_text())
    summary = diagnostic.get("source_summary") or {}
    failed = diagnostic.get("failed_source_responses") or {}
    return {"status": "READ", "prior_date": prior_date,
            "games_without_attempts": sorted(failed),
            "game_failure_reasons": {game: value.get("reason") for game, value in failed.items()},
            "unresolved_skaters": int(summary.get("pbp_unresolved_skaters", 0) or 0),
            "conflicts": int(summary.get("confirmed_discrepancy_count", 0) or 0),
            "flag": bool(failed)}


def render(result: dict[str, Any]) -> str:
    lines = [f"# NHL input watch — {result['slate_date']}", "",
             f"Run `{result['run_id']}`; {result['flag_count']} flag(s). Read-only diagnostic; "
             "predictions are unchanged.", ""]
    for lane in ("points", "sog"):
        checks = result["sanity"].get(lane) or []
        flagged = [c for c in checks if c["flag"]]
        notes = [c for c in checks if c["count"] and not c["flag"]]
        lines.append(f"## {lane.upper()} input sanity ({result['players'].get(lane, 0)} players)")
        lines += [f"- **FLAG {c['check']}**: {c['count']} ({c['detail']}); e.g. {', '.join(c['examples'])}"
                  for c in flagged] or ["- No flags."]
        lines += [f"- {c['check']}: {c['count']} ({c['detail']})" for c in notes]
        lines.append("")
    explainer = result.get("disagreements") or {}
    if explainer.get("status") == "UNAVAILABLE":
        lines += ["## Biggest model-market disagreements", f"Unavailable: {explainer.get('reason')}", ""]
    for lane, picks in (explainer.get("lanes") or {}).items():
        lines += [f"## Biggest {lane.upper()} disagreements (model above the market on its side)", "",
                  "| Player | Line | Side | Model | Market | Gap | Why |", "|---|---:|---|---:|---:|---:|---|"]
        for p in picks:
            lines.append(f"| {p['player']} | {p['line']:g} | {p['side']} | {100 * p['model_p']:.1f}% | "
                         f"{100 * p['market_p']:.1f}% | {100 * p['gap']:+.1f} | {'; '.join(p['reasons'])} |")
        lines.append("")
    health = result.get("source_health") or {}
    if health.get("status") == "READ":
        lines += ["## Source health (prior date shot attempts)",
                  f"Games without attempts: {', '.join(map(str, health['games_without_attempts'])) or 'none'}; "
                  f"unresolved skaters {health['unresolved_skaters']}; conflicts {health['conflicts']}.", ""]
    return "\n".join(lines)


def run_input_watch(*, slate_date: str, run_id: str, prediction_run_dir: Path,
                    points_capture_dir: Path | None, operational_root: Path,
                    prior_date: str | None = None, output_root: Path = OUTPUT_ROOT,
                    now_utc: str | None = None) -> dict[str, Any]:
    now_utc = now_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    ranges = json.loads(POINTS_RANGES_PATH.read_text())
    points = sog = None
    sog_features_path = Path(prediction_run_dir) / "sog_hgb_features.csv"
    sog_predictions_path = Path(prediction_run_dir) / "sog_predictions_wide_calibrated.csv"
    if sog_features_path.is_file() and sog_predictions_path.is_file():
        predictions = pd.read_csv(sog_predictions_path)
        keep = [c for c in ("game_id", "player_id", "player_name", "p_over_1_5", "p_over_2_5", "p_over_3_5")
                if c in predictions]
        sog = predictions[keep].merge(pd.read_csv(sog_features_path).drop(columns=["poisson_source"], errors="ignore"),
                                      on=["game_id", "player_id"], how="left")
    if points_capture_dir and (Path(points_capture_dir) / "features.csv").is_file():
        features = pd.read_csv(Path(points_capture_dir) / "features.csv")
        predictions = pd.read_csv(Path(points_capture_dir) / "predictions.csv")
        keys = ["game_id", "player_id"]
        points = features.merge(
            predictions[keys + [c for c in predictions.columns if c not in features.columns]],
            on=keys, how="inner")
        if sog is not None and "feature_is_defense" in sog:
            points = points.merge(sog[["game_id", "player_id", "feature_is_defense"]].rename(
                columns={"feature_is_defense": "is_defense"}), on=["game_id", "player_id"], how="left")
    names_path = ROOT / "backend/nhl/exports/daily/names" / f"names_{slate_date}.csv"
    if names_path.is_file():
        names = pd.read_csv(names_path, usecols=["player_id", "full_name"]).drop_duplicates("player_id")
        named = dict(zip(names.player_id.astype("int64"), names.full_name))
        for frame in (points, sog):
            if frame is not None:
                frame["player_name"] = frame.player_id.astype("int64").map(named).fillna(
                    frame["player_name"] if "player_name" in frame else frame.player_id.astype(str))
    sanity = {"points": points_sanity(points, ranges) if points is not None else [],
              "sog": sog_sanity(sog) if sog is not None else []}
    try:
        disagreements = explain_disagreements(points=points, sog=sog, slate_date=slate_date, run_id=run_id,
                                              operational_root=Path(operational_root), now_utc=now_utc,
                                              ranges=ranges)
        disagreements["status"] = "COMPLETE"
    except Exception as error:  # the watch never blocks the run
        disagreements = {"status": "UNAVAILABLE", "reason": f"{type(error).__name__}:{error}"}
    health = attempt_source_health(Path(operational_root), prior_date, run_id)
    flags = [f"{lane}:{c['check']}" for lane, checks in sanity.items() for c in checks if c["flag"]]
    if health.get("flag"):
        flags.append("source:SHOT_ATTEMPTS_GAMES_DROPPED")
    result = {"schema_version": SCHEMA, "slate_date": slate_date, "run_id": run_id, "generated_at_utc": now_utc,
              "players": {"points": 0 if points is None else int(len(points)),
                          "sog": 0 if sog is None else int(len(sog))},
              "sanity": sanity, "disagreements": disagreements, "source_health": health,
              "flags": flags, "flag_count": len(flags), "database_writes": 0, "provider_requests": 0}
    out = Path(output_root) / f"slate_date={slate_date}" / f"run_id={run_id}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "input_watch.json").write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
    (out / "input_watch.md").write_text(render(result) + "\n")
    sums = "".join(f"{hashlib.sha256((out / name).read_bytes()).hexdigest()}  {name}\n"
                   for name in ("input_watch.json", "input_watch.md"))
    (out / "SHA256SUMS").write_text(sums)
    result["path"] = str(out)
    return result
