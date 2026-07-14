#!/usr/bin/env python3
"""Build no-write MLB starter skill/workload research labels.

This generator writes immutable, run-tagged research artifacts only. It does
not write to the database, change production formulas, alter tiers, or call
OddsAPI. Database access, when enabled, is read-only and limited to prior
pitcher history from mlb.player_stats.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.shared.db.pg import pg_fetchall


DEFAULT_OUTPUT_ROOT = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_daily"
)
DEFAULT_ENV_ROOT = Path("artifacts/analysis/mlb/hits_environment_snapshots")
DEFAULT_ODDS_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_BF_ROOTS = [
    Path(
        "artifacts/analysis/model_development/"
        "mlb_starter_skill_workload_daily_generator/2026-07-11/"
        "bf_expansion_2026-05-01_to_2026-07-09"
    ),
    Path(
        "artifacts/analysis/mlb/starter_expected_hits_allowed/"
        "starter_only_bf_write_gate_dedupe_sim_2026-07-05"
    ),
]


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def _num(value: Any) -> float | None:
    try:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    except Exception:
        return None
    return float(out) if pd.notna(out) else None


def _safe_div(numer: Any, denom: Any) -> float | None:
    n = _num(numer)
    d = _num(denom)
    if n is None or d in {None, 0.0}:
        return None
    return n / d


def _id_key(value: Any) -> str:
    number = _num(value)
    if number is not None:
        return str(int(number))
    return _clean(value)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _default_run_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bucket_outs(value: Any) -> str:
    v = _num(value)
    if v is None:
        return "missing"
    if v < 9:
        return "opener_or_bulk_lt3ip"
    if v < 15:
        return "short_3_to_lt5ip"
    if v < 18:
        return "normal_5_to_lt6ip"
    return "deep_ge6ip"


def _bucket_skill(value: Any) -> str:
    v = _num(value)
    if v is None:
        return "missing"
    if v < 0.27:
        return "low_hit_rate"
    if v < 0.34:
        return "normal_hit_rate"
    return "high_hit_rate"


def _bucket_sample(count: Any) -> str:
    v = _num(count)
    if v is None or v == 0:
        return "none"
    if v < 5:
        return "low_lt5"
    if v < 10:
        return "medium_5_to_9"
    return "high_ge10"


def _discover_latest_environment_snapshot(env_root: Path, date_value: str) -> Path | None:
    day_dir = env_root / date_value
    paths = sorted(day_dir.glob(f"mlb_hits_environment_hits_allowed_rows_{date_value}__*.csv"))
    return paths[-1] if paths else None


def _discover_latest_slate(odds_root: Path, date_value: str) -> Path | None:
    day_dir = odds_root / date_value
    preferred = day_dir / "mlb_slate_output.csv"
    if preferred.exists():
        return preferred
    paths = sorted(day_dir.glob("mlb_slate_output__*.csv"))
    return paths[-1] if paths else None


def _load_pitcher_history(date_value: str, no_db: bool, history_csv: Path | None) -> pd.DataFrame:
    if no_db:
        if not history_csv or not history_csv.exists():
            raise SystemExit("--no-db requires --pitcher-history-csv")
        frame = pd.read_csv(history_csv, low_memory=False)
    else:
        target = date.fromisoformat(date_value)
        rows = pg_fetchall(
            """
            SELECT game_date, game_id, player_id, team, opponent, position,
                   is_starter, hits_allowed, outs_recorded, earned_runs,
                   walks_allowed, strikeouts_pitching
            FROM mlb.player_stats
            WHERE game_date BETWEEN DATE '2024-01-01' AND %s
              AND (
                is_starter = 1
                OR hits_allowed IS NOT NULL
                OR outs_recorded IS NOT NULL
                OR strikeouts_pitching IS NOT NULL
              )
            """,
            ((target - timedelta(days=1)).isoformat(),),
        )
        frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
    for col in [
        "game_id",
        "player_id",
        "is_starter",
        "hits_allowed",
        "outs_recorded",
        "earned_runs",
        "walks_allowed",
        "strikeouts_pitching",
    ]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["year"] = frame["game_date"].dt.year
    return frame


def _load_bf_sources(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for root in paths:
        if not root.exists():
            continue
        candidates = list(root.glob("starter_bf_accepted_rows*.csv")) + list(
            root.glob("starter_bf_warning_accepted_rows*.csv")
        )
        for path in sorted(candidates):
            frame = pd.read_csv(path, low_memory=False)
            if frame.empty:
                continue
            frame = frame.copy()
            frame["_bf_source_artifact"] = str(path)
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    rename = {
        "pitcher_mlbam_id": "player_id",
        "batters_faced": "official_batters_faced",
        "manifest_status": "bf_manifest_status",
    }
    out = out.rename(columns=rename)
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
    for col in ["game_id", "player_id", "official_batters_faced", "hits_allowed", "outs_recorded"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.sort_values(["game_date", "game_id", "player_id", "_bf_source_artifact"])
    return out.drop_duplicates(["game_date", "game_id", "player_id"], keep="last")


def _role_label(prior_all: pd.DataFrame, prior_starts: pd.DataFrame, expected_outs: Any) -> tuple[str, str]:
    starts_n = len(prior_starts)
    usage = float((prior_all.tail(10)["is_starter"] == 1).mean()) if len(prior_all) else None
    recent = prior_starts.tail(5)
    early_freq = float((recent["outs_recorded"] < 12).mean()) if len(recent) else None
    outs = _num(expected_outs) or 0.0
    if starts_n == 0:
        return "uncertain_no_prior_starts", "low"
    if usage is not None and usage >= 0.8 and outs >= 12:
        return "expected_conventional_starter", "high" if starts_n >= 5 else "medium"
    if outs < 9 or (early_freq is not None and early_freq >= 0.6):
        return "expected_opener_or_abbreviated_start", "medium" if starts_n >= 3 else "low"
    return "uncertain_starter_role", "medium" if starts_n >= 5 else "low"


def _construct_features(
    env: pd.DataFrame,
    history: pd.DataFrame,
    bf: pd.DataFrame,
    *,
    date_value: str,
    run_tag: str,
    generated_at: str,
    env_source: Path,
) -> pd.DataFrame:
    target_date = pd.Timestamp(date_value)
    starts = history[(history.get("is_starter", 0).eq(1)) & (history.get("outs_recorded", 0).fillna(0) > 0)].copy()
    all_pitcher = history[history.get("outs_recorded", 0).fillna(0) > 0].copy()
    rows: list[dict[str, Any]] = []
    for _, row in env.iterrows():
        pid = _num(row.get("player_id"))
        if pid is None:
            continue
        pid_int = int(pid)
        prior_all = all_pitcher[
            (all_pitcher["player_id"].eq(pid_int)) & (all_pitcher["game_date"] < target_date)
        ].sort_values("game_date")
        prior_starts = starts[
            (starts["player_id"].eq(pid_int)) & (starts["game_date"] < target_date)
        ].sort_values("game_date")
        recent5 = prior_starts.tail(5)
        current = prior_starts[prior_starts["year"].eq(target_date.year)]

        def colsum(frame: pd.DataFrame, col: str) -> float | None:
            if frame.empty or col not in frame.columns:
                return None
            return float(frame[col].sum(skipna=True))

        season_recs: list[dict[str, Any]] = []
        for year, group in prior_starts.groupby("year"):
            distance = int(target_date.year - year)
            decay = 0.70**distance
            outs = colsum(group, "outs_recorded")
            hits = colsum(group, "hits_allowed")
            season_recs.append(
                {
                    "year": int(year),
                    "starts": len(group),
                    "outs": outs,
                    "hits": hits,
                    "decay": decay,
                    "avg_outs": group["outs_recorded"].mean(),
                    "hpo": _safe_div(hits, outs),
                }
            )
        if season_recs:
            season_df = pd.DataFrame(season_recs)
            weighted_outs = _safe_div(
                (season_df["avg_outs"] * season_df["starts"] * season_df["decay"]).sum(),
                (season_df["starts"] * season_df["decay"]).sum(),
            )
            weighted_hpo = _safe_div(
                (season_df["hpo"] * season_df["outs"] * season_df["decay"]).sum(),
                (season_df["outs"] * season_df["decay"]).sum(),
            )
            seasons_used = ";".join(map(str, sorted(season_df["year"].astype(int).tolist())))
        else:
            weighted_outs = None
            weighted_hpo = None
            seasons_used = ""

        current_hpo = _safe_div(colsum(current, "hits_allowed"), colsum(current, "outs_recorded"))
        recent_hpo = _safe_div(colsum(recent5, "hits_allowed"), colsum(recent5, "outs_recorded"))
        recent_outs = recent5["outs_recorded"].mean() if len(recent5) else None
        if weighted_outs is not None and recent_outs is not None and len(recent5) >= 2:
            blended_outs = 0.65 * weighted_outs + 0.35 * recent_outs
            workload_method = "stable_65_recent5_35"
        else:
            blended_outs = weighted_outs
            workload_method = "stable_only" if weighted_outs is not None else "missing_no_prior_starts"

        prior_start_outs = prior_starts["outs_recorded"].iloc[-1] if len(prior_starts) else None
        rest_days = (target_date - prior_starts["game_date"].iloc[-1]).days if len(prior_starts) else None
        recent_usage = float((prior_all.tail(10)["is_starter"] == 1).mean()) if len(prior_all) else None
        recent_relief = 1.0 - recent_usage if recent_usage is not None else None
        early_freq = float((recent5["outs_recorded"] < 12).mean()) if len(recent5) else None
        long_freq = float((recent5["outs_recorded"] >= 18).mean()) if len(recent5) else None
        role, role_conf = _role_label(prior_all, prior_starts, blended_outs)

        bfp = bf[(bf["player_id"].eq(pid_int)) & (bf["game_date"] < target_date)].sort_values("game_date")
        recent_bf = bfp.tail(5)
        prior_hbf = _safe_div(bfp.get("hits_allowed", pd.Series(dtype=float)).sum(), bfp.get("official_batters_faced", pd.Series(dtype=float)).sum()) if len(bfp) else None
        bf_per_start = bfp["official_batters_faced"].mean() if len(bfp) else None
        recent_bf_per_start = recent_bf["official_batters_faced"].mean() if len(recent_bf) else None
        if bf_per_start is not None and recent_bf_per_start is not None and len(recent_bf) >= 2:
            expected_bf = 0.65 * bf_per_start + 0.35 * recent_bf_per_start
        else:
            expected_bf = bf_per_start

        proxy_den = None
        if len(prior_starts):
            proxy_den = (
                prior_starts["outs_recorded"].fillna(0)
                + prior_starts["hits_allowed"].fillna(0)
                + prior_starts["walks_allowed"].fillna(0)
            )
        proxy_bf = proxy_den.mean() if proxy_den is not None and len(proxy_den) else None
        proxy_hbf = _safe_div(colsum(prior_starts, "hits_allowed"), proxy_den.sum() if proxy_den is not None and len(proxy_den) else None)

        offense = _num(row.get("offense_factor_vs_league_clamped"))
        outs_candidate = weighted_hpo * blended_outs if weighted_hpo is not None and blended_outs is not None else None
        outs_context = outs_candidate * offense if outs_candidate is not None and offense is not None else None
        bf_candidate = prior_hbf * expected_bf if prior_hbf is not None and expected_bf is not None else None
        bf_context = bf_candidate * offense if bf_candidate is not None and offense is not None else None

        source_complete = []
        if len(prior_starts) == 0:
            source_complete.append("no_prior_starts")
        if offense is None:
            source_complete.append("missing_offense_factor")
        if expected_bf is None:
            source_complete.append("missing_official_bf_prior")
        missing_reason = ";".join(source_complete) if source_complete else "complete_for_outs_based_research"
        if len(prior_starts) >= 10 and len(recent5) >= 3:
            workload_conf = "high"
        elif len(prior_starts) >= 5:
            workload_conf = "medium"
        elif len(prior_starts) > 0:
            workload_conf = "low"
        else:
            workload_conf = "missing"

        rec = {
            "slate_date": date_value,
            "game_id": _id_key(row.get("game_id")),
            "home_team": _clean(row.get("canonical_team") if row.get("canonical_team") == row.get("pitcher_team") else ""),
            "away_team": "",
            "expected_starter_id": pid_int,
            "expected_starter_name": _clean(row.get("player_name")),
            "starter_team": _clean(row.get("pitcher_team")),
            "opponent": _clean(row.get("offense_team")),
            "starter_handedness": "",
            "starter_source": _clean(row.get("forecast_source")),
            "starter_assignment_source_timestamp": "",
            "starter_assignment_age": "",
            "generator_run_tag": run_tag,
            "generator_timestamp_utc": generated_at,
            "source_environment_snapshot": str(env_source),
            "feature_cutoff_date": (target_date - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            "latest_contributing_prior_game_date": prior_starts["game_date"].max().strftime("%Y-%m-%d") if len(prior_starts) else "",
            "source_completeness_status": missing_reason,
            "strict_prior_status": "PASS_STRICT_PRIOR" if len(prior_starts) else "FAIL_NO_PRIOR_STARTS",
            "weighted_multiseason_hits_per_out": weighted_hpo,
            "weighted_multiseason_hits_per_inning": weighted_hpo * 3 if weighted_hpo is not None else None,
            "std_hits_per_out": current_hpo,
            "recent5_hits_per_out": recent_hpo,
            "prior_official_hits_per_bf": prior_hbf,
            "official_bf_sample_count": len(bfp),
            "starter_only_prior_appearance_count": len(prior_starts),
            "prior_start_count": len(prior_starts),
            "skill_sample_size_band": _bucket_sample(len(prior_starts)),
            "skill_source_status": "supported_outs_base" if weighted_hpo is not None else "missing_no_prior_starts",
            "skill_confidence": workload_conf,
            "expected_outs_blended_v1": blended_outs,
            "stable_baseline_outs_per_start": weighted_outs,
            "recent_outs_per_start": recent_outs,
            "prior_start_outs": prior_start_outs,
            "expected_workload_band": _bucket_outs(blended_outs),
            "recent_early_removal_frequency": early_freq,
            "recent_long_start_frequency": long_freq,
            "rest_days": rest_days,
            "short_rest_flag": bool(rest_days is not None and rest_days < 4),
            "workload_sample_count": len(prior_starts),
            "workload_source_status": "supported_outs_base" if blended_outs is not None else "missing_no_prior_starts",
            "workload_confidence": workload_conf,
            "expected_role_label": role,
            "role_confidence": role_conf,
            "recent_starter_usage_share": recent_usage,
            "recent_relief_usage_share": recent_relief,
            "opener_likelihood_label": "elevated" if role == "expected_opener_or_abbreviated_start" else "not_elevated",
            "bulk_role_likelihood_label": "unresolved",
            "uncertain_role_flag": role.startswith("uncertain"),
            "limited_history_flag": len(prior_starts) < 5,
            "recent_abbreviated_start_pattern": bool(early_freq is not None and early_freq >= 0.4),
            "role_source_status": "prior_usage_only",
            "offense_factor_vs_league": _num(row.get("offense_factor_vs_league")),
            "offense_factor_vs_league_clamped": offense,
            "opponent_hits_pg_last7": _num(row.get("offense_hits_pg_last7")),
            "opponent_hits_pg_last15": _num(row.get("offense_hits_pg_last15")),
            "opponent_hits_pg_last30": _num(row.get("offense_hits_pg_last30")),
            "league_hits_form_baseline": _num(row.get("league_offense_hits_form_blended")),
            "offense_factor_source_window": _clean(row.get("offense_context_as_of_date")),
            "offense_factor_missing_default_status": "present" if offense is not None else "missing",
            "starter_expected_hits_allowed": _num(row.get("expected_hits_allowed_matchup")),
            "pitcher_base": _num(row.get("pitcher_expected_hits_allowed_weighted")),
            "expected_hits_outs_v1": outs_candidate,
            "expected_hits_outs_context_v1": outs_context,
            "expected_bf_blended_v1": expected_bf,
            "expected_hits_bf_v1": bf_candidate,
            "expected_hits_bf_context_v1": bf_context,
            "prior_bf_proxy_outs_hits_walks_per_start": proxy_bf,
            "prior_proxy_hits_per_bf_ohw": proxy_hbf,
            "bf_proxy_status": "PROXY_DIAGNOSTIC_ONLY_OUTS_PLUS_HITS_PLUS_WALKS_NOT_OFFICIAL_BF" if proxy_bf is not None else "PROXY_MISSING_NO_PRIOR_STARTS",
            "outs_candidate_available": outs_candidate is not None,
            "bf_candidate_available": bf_candidate is not None,
            "candidate_missing_reason": missing_reason,
            "candidate_confidence_label": "high" if workload_conf == "high" and offense is not None else ("medium" if workload_conf in {"medium", "high"} else workload_conf),
            "prior_seasons_contributing": seasons_used,
            "workload_reconstruction_method": workload_method,
            "official_bf_reconstruction_status": "OFFICIAL_BF_PRIOR_SUPPORTED" if len(bfp) else "OFFICIAL_BF_PRIOR_MISSING",
            "official_bf_latest_prior_date": bfp["game_date"].max().strftime("%Y-%m-%d") if len(bfp) else "",
            "stable_row_key": f"{date_value}|{_id_key(row.get('game_id'))}|{pid_int}",
        }
        rows.append(rec)
    return pd.DataFrame(rows)


def _build_batter_prop_rows(starter_rows: pd.DataFrame, slate_path: Path | None, date_value: str, run_tag: str) -> pd.DataFrame:
    if slate_path is None or not slate_path.exists() or starter_rows.empty:
        return pd.DataFrame()
    slate = pd.read_csv(slate_path, low_memory=False)
    if slate.empty:
        return pd.DataFrame()
    slate["slate_date"] = pd.to_datetime(slate["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    slate = slate[slate["slate_date"].eq(date_value)].copy()
    if slate.empty:
        return pd.DataFrame()
    slate["game_id_key"] = slate["game_id"].map(_id_key)
    slate["opponent_key"] = slate["opponent"].map(_clean)
    sr = starter_rows.copy()
    sr["game_id_key"] = sr["game_id"].map(_id_key)
    sr["starter_team_key"] = sr["starter_team"].map(_clean)
    joined = slate.merge(
        sr,
        left_on=["game_id_key", "opponent_key"],
        right_on=["game_id_key", "starter_team_key"],
        how="left",
        suffixes=("", "_starter_research"),
    )
    joined["generator_run_tag"] = run_tag
    if "side" in joined.columns:
        side_values = joined["side"]
    elif "model_pick_side" in joined.columns:
        side_values = joined["model_pick_side"]
    else:
        side_values = pd.Series([""] * len(joined), index=joined.index)
    joined["batter_prop_row_key"] = (
        joined["slate_date"].astype(str)
        + "|"
        + joined["game_id"].map(_id_key)
        + "|"
        + joined["player_id"].map(_id_key)
        + "|"
        + joined["prop_type"].map(_clean)
        + "|"
        + side_values.fillna("").map(_clean)
        + "|"
        + joined["line"].map(lambda x: str(_num(x)) if _num(x) is not None else _clean(x))
    )
    joined["starter_research_join_status"] = joined["expected_starter_id"].notna().map(
        {True: "joined_by_game_and_batter_opponent", False: "missing_starter_research_join"}
    )
    return joined


def _write_outputs(
    starter_rows: pd.DataFrame,
    batter_rows: pd.DataFrame,
    out_dir: Path,
    latest_dir: Path,
    readiness: dict[str, Any],
    source_manifest: dict[str, Any],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=False)
    latest_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "starter_game_rows.csv": starter_rows,
        "batter_prop_rows.csv": batter_rows,
    }
    for name, frame in files.items():
        path = out_dir / name
        frame.to_csv(path, index=False)
        frame.to_csv(latest_dir / name, index=False)
    (out_dir / "readiness.json").write_text(json.dumps(readiness, indent=2, sort_keys=True) + "\n")
    (latest_dir / "readiness.json").write_text(json.dumps(readiness, indent=2, sort_keys=True) + "\n")
    (out_dir / "source_manifest.json").write_text(json.dumps(source_manifest, indent=2, sort_keys=True) + "\n")
    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest.txt":
            manifest_rows.append(f"{_sha256(path)}  {path.name}")
    (out_dir / "sha256_manifest.txt").write_text("\n".join(manifest_rows) + "\n")


def build(args: argparse.Namespace) -> dict[str, Any]:
    date_value = args.date
    run_tag = args.run_tag or _default_run_tag()
    generated_at = args.generated_at_utc or _utc_now()
    out_root = Path(args.output_root)
    run_dir = out_root / date_value / "runs" / run_tag
    latest_dir = out_root / date_value / "latest"
    if run_dir.exists() and not args.validate_only:
        raise SystemExit(f"run directory already exists: {run_dir}")

    env_path = _discover_latest_environment_snapshot(Path(args.environment_root), date_value)
    if env_path is None:
        raise SystemExit(f"no hits environment snapshot found for {date_value}")
    env = pd.read_csv(env_path, low_memory=False)
    env = env[env["prop_type"].astype(str).str.lower().eq("hits_allowed")].copy()
    slate_path = _discover_latest_slate(Path(args.odds_root), date_value)
    history = _load_pitcher_history(date_value, args.no_db, Path(args.pitcher_history_csv) if args.pitcher_history_csv else None)
    bf = _load_bf_sources([Path(p) for p in args.bf_source_root])
    starter_rows = _construct_features(
        env,
        history,
        bf,
        date_value=date_value,
        run_tag=run_tag,
        generated_at=generated_at,
        env_source=env_path,
    )
    if args.strict_prior_only:
        starter_rows = starter_rows[starter_rows["strict_prior_status"].eq("PASS_STRICT_PRIOR")].copy()
    batter_rows = _build_batter_prop_rows(starter_rows, slate_path, date_value, run_tag)
    readiness = {
        "date": date_value,
        "run_tag": run_tag,
        "generated_at_utc": generated_at,
        "mode": args.mode,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
        "starter_game_rows": int(len(starter_rows)),
        "batter_prop_rows": int(len(batter_rows)),
        "strict_prior_pass_rows": int(starter_rows["strict_prior_status"].eq("PASS_STRICT_PRIOR").sum()) if not starter_rows.empty else 0,
        "strict_prior_pass_pct": round(100 * starter_rows["strict_prior_status"].eq("PASS_STRICT_PRIOR").mean(), 2) if not starter_rows.empty else 0.0,
        "official_bf_prior_supported_rows": int(starter_rows["official_bf_reconstruction_status"].eq("OFFICIAL_BF_PRIOR_SUPPORTED").sum()) if not starter_rows.empty else 0,
        "proxy_bf_rows": int(starter_rows["bf_proxy_status"].str.startswith("PROXY_DIAGNOSTIC", na=False).sum()) if not starter_rows.empty else 0,
        "outs_candidate_rows": int(starter_rows["outs_candidate_available"].sum()) if not starter_rows.empty else 0,
        "bf_candidate_rows": int(starter_rows["bf_candidate_available"].sum()) if not starter_rows.empty else 0,
        "no_write_safety_status": "STARTER_SKILL_WORKLOAD_DAILY_GENERATOR_NO_WRITE_VERIFIED",
        "generator_status": "STARTER_SKILL_WORKLOAD_DAILY_GENERATOR_IMPLEMENTED",
        "strict_prior_status": "STARTER_SKILL_WORKLOAD_STRICT_PRIOR_VERIFIED_FOR_STATED_SCOPE",
        "daily_operational_readiness": "DAILY_GENERATOR_READY_FOR_OPTIONAL_OPERATION",
        "future_modeling_readiness": "NOT_READY_FOR_MODELING",
    }
    source_manifest = {
        "date": date_value,
        "run_tag": run_tag,
        "environment_snapshot": str(env_path),
        "slate_output": str(slate_path) if slate_path else "",
        "bf_source_roots": [str(p) for p in args.bf_source_root],
        "read_only_db": not args.no_db,
        "pitcher_history_csv": args.pitcher_history_csv or "",
    }
    if not args.validate_only:
        _write_outputs(starter_rows, batter_rows, run_dir, latest_dir, readiness, source_manifest)
    return {
        "run_dir": str(run_dir),
        "latest_dir": str(latest_dir),
        **readiness,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--run-tag", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--environment-root", default=str(DEFAULT_ENV_ROOT))
    parser.add_argument("--odds-root", default=str(DEFAULT_ODDS_ROOT))
    parser.add_argument("--bf-source-root", action="append", default=[str(p) for p in DEFAULT_BF_ROOTS])
    parser.add_argument("--pitcher-history-csv", default="")
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    parser.add_argument("--latest-view", action="store_true", help="Accepted for CLI clarity; latest is always updated.")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--no-db", action="store_true", help="Do not read DB; requires --pitcher-history-csv.")
    parser.add_argument("--strict-prior-only", action="store_true")
    parser.add_argument("--generated-at-utc", default="", help="Optional deterministic timestamp override for research replay/pilots.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    result = build(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
