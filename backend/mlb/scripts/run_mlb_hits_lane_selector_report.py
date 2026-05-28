#!/usr/bin/env python3
"""One-command daily runner/report for the MLB hits lane selector."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd


LANE_ROOT = Path("backend/mlb/exports/model_v2/lanes/today")
UPLOAD_ROOT = Path("backend/mlb/exports/model_v2/upload")
RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DAILY_SCRIPT = Path("backend/mlb/scripts/run_mlb_hits_lane_selector_daily.py")
RESULTS_SCRIPT = Path("backend/mlb/scripts/compare_hits_lane_selector_to_results.py")
QUICK_CARD_UPLOAD_SCRIPT = Path("backend/mlb/scripts/export_quick_card_hits_tool_upload.py")
PRODUCTION_REGIMES_CSV = Path("artifacts/analysis/mlb/hits_environment_persistence/recurring_team_environment_regimes.csv")
BACKFILL_2026_REGIMES_CSV = Path("artifacts/analysis/mlb/hits_environment_persistence_backfill_2026/recurring_team_environment_regimes.csv")
HOSTILE_WATCH_MD = Path("artifacts/analysis/mlb/hits_environment_persistence/hostile_hits_environment_watch.md")
HOSTILE_WATCH_CSV = Path("artifacts/analysis/mlb/hits_environment_persistence/hostile_hits_environment_watch.csv")
HOSTILE_WATCH_JSON = Path("artifacts/analysis/mlb/hits_environment_persistence/hostile_hits_environment_watch.json")
PRODUCTION_FAVORITES_BREAKDOWN_JSON = Path(
    "artifacts/analysis/mlb/hits_environment_persistence/v2_favorites_environment_breakdown_summary.json"
)
BACKFILL_FAVORITES_BREAKDOWN_JSON = Path(
    "artifacts/analysis/mlb/hits_environment_persistence_backfill_2026/v2_favorites_environment_breakdown_summary.json"
)

TEAM_ALIASES = {
    "AZ": "ARI",
    "ARI": "ARI",
    "ATH": "OAK",
    "OAK": "OAK",
    "CHW": "CWS",
    "CWS": "CWS",
    "KCR": "KC",
    "KC": "KC",
    "SDP": "SD",
    "SD": "SD",
    "SFG": "SF",
    "SF": "SF",
    "TBR": "TB",
    "TB": "TB",
    "WAS": "WSH",
    "WSH": "WSH",
}


def _lane_date_dir(date_value: str) -> Path:
    return LANE_ROOT / date_value


def _upload_date_dir(date_value: str) -> Path:
    return UPLOAD_ROOT / date_value


def _dated_or_legacy(date_value: str, filename: str) -> Path:
    dated = _lane_date_dir(date_value) / filename
    if dated.exists():
        return dated
    return LANE_ROOT / filename


def _upload_dated_or_legacy(date_value: str, filename: str) -> Path:
    dated = _upload_date_dir(date_value) / filename
    if dated.exists():
        return dated
    return UPLOAD_ROOT / filename


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        raise SystemExit(f"Invalid --date: {value}")
    return dt.date().isoformat()


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(len(pd.read_csv(path, low_memory=False)))
    except Exception:
        return 0


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "n/a"


def _fmt_units(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):+.2f}"
    except Exception:
        return "n/a"


def _norm_team(value: Any) -> str:
    text = str(value if value is not None else "").strip().upper()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return TEAM_ALIASES.get(text, text)


def _norm_side(value: Any) -> str:
    text = str(value if value is not None else "").strip().lower()
    if text in {"o", "over"}:
        return "over"
    if text in {"u", "under"}:
        return "under"
    return text


def _resolve_environment_regimes(args: argparse.Namespace) -> tuple[Path | None, str]:
    if getattr(args, "environment_regimes_csv", ""):
        path = Path(args.environment_regimes_csv)
        source = args.environment_source or (
            "backfill_2026" if "backfill" in str(path) else "production_history"
        )
        return (path if path.exists() else None), source
    if PRODUCTION_REGIMES_CSV.exists():
        return PRODUCTION_REGIMES_CSV, "production_history"
    if BACKFILL_2026_REGIMES_CSV.exists():
        return BACKFILL_2026_REGIMES_CSV, "backfill_2026"
    return None, ""


def _load_regime_lookup(regimes_csv: Path | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not regimes_csv or not regimes_csv.exists():
        return pd.DataFrame(), pd.DataFrame()
    regimes = pd.read_csv(regimes_csv, low_memory=False)
    if regimes.empty or "team" not in regimes.columns or "role" not in regimes.columns:
        return pd.DataFrame(), pd.DataFrame()
    regimes = regimes.copy()
    regimes["team_key"] = regimes["team"].map(_norm_team)
    keep = [
        "team_key",
        "regime",
        "same_side_rate",
        "residual_avg",
        "current_signed_streak",
        "environment_volatility",
        "stability_score",
        "rolling_stability_rank",
    ]
    keep = [c for c in keep if c in regimes.columns]
    offense = regimes[regimes["role"].eq("offense_environment")][keep].drop_duplicates("team_key", keep="first")
    staff = regimes[regimes["role"].eq("pitcher_staff_environment")][keep].drop_duplicates("team_key", keep="first")
    return offense, staff


def _opponent_team(row: pd.Series) -> str:
    player_team = _norm_team(row.get("player_team"))
    home = _norm_team(row.get("home_team_code") or row.get("home_upload") or row.get("home_raw"))
    away = _norm_team(row.get("away_team_code") or row.get("away_upload") or row.get("away_raw"))
    if player_team and home and player_team == home:
        return away
    if player_team and away and player_team == away:
        return home
    return away or home


def _hostile_flag(side: Any, offense_regime: Any) -> bool:
    side_key = _norm_side(side)
    regime = str(offense_regime if offense_regime is not None else "").strip()
    if side_key == "over" and regime == "recurring_underperformer":
        return True
    if side_key == "under" and regime == "recurring_overperformer":
        return True
    return False


def _side_price(row: pd.Series) -> float:
    side = _norm_side(row.get("side"))
    col = "odds_over" if side == "over" else "odds_under" if side == "under" else ""
    if not col:
        return float("nan")
    val = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
    return float(val) if pd.notna(val) else float("nan")


def _id_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.notna(parsed):
            return str(int(float(parsed)))
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def _load_wide_team_lookup(date_value: str) -> pd.DataFrame:
    path = Path("backend/mlb/exports/odds_history") / date_value / "mlb_predictions_wide_calibrated.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        header = pd.read_csv(path, nrows=0).columns
        usecols = [c for c in ["player_id", "prop_type", "team", "opponent", "game_date"] if c in header]
        wide = pd.read_csv(path, usecols=usecols, low_memory=False)
    except Exception:
        return pd.DataFrame()
    if wide.empty:
        return pd.DataFrame()
    wide["date_key"] = wide.get("game_date", pd.Series(date_value, index=wide.index)).map(_date_key)
    wide = wide[wide["date_key"].eq(date_value)].copy()
    wide["player_id_key"] = wide.get("player_id", pd.Series("", index=wide.index)).map(_id_text)
    wide["prop_type_key"] = wide.get("prop_type", pd.Series("", index=wide.index)).astype(str).str.strip().str.lower()
    wide["wide_player_team"] = wide.get("team", pd.Series("", index=wide.index)).map(_norm_team)
    wide["wide_opponent_team"] = wide.get("opponent", pd.Series("", index=wide.index)).map(_norm_team)
    return wide[
        ["date_key", "player_id_key", "prop_type_key", "wide_player_team", "wide_opponent_team"]
    ].drop_duplicates(["date_key", "player_id_key", "prop_type_key"], keep="last")


def _environment_diagnostics(
    *,
    selector_csv: Path,
    out_csv: Path,
    regimes_csv: Path | None,
    environment_source: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not selector_csv.exists():
        return pd.DataFrame(), {
            "available": False,
            "reason": f"missing selector csv: {selector_csv}",
            "out_csv": str(out_csv),
        }
    selected = pd.read_csv(selector_csv, low_memory=False)
    if selected.empty:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        selected.to_csv(out_csv, index=False)
        return selected, {"available": True, "rows": 0, "out_csv": str(out_csv), "environment_source": environment_source}
    offense, staff = _load_regime_lookup(regimes_csv)
    work = selected.copy()
    date_value = _date_key(work["date"].dropna().iloc[0]) if "date" in work.columns and work["date"].notna().any() else ""
    work["date_key"] = work.get("date", pd.Series(date_value, index=work.index)).map(_date_key)
    work["player_id_key"] = work.get("player_id", pd.Series("", index=work.index)).map(_id_text)
    work["prop_type_key"] = work.get("prop_type", pd.Series("", index=work.index)).astype(str).str.strip().str.lower()
    wide = _load_wide_team_lookup(date_value) if date_value else pd.DataFrame()
    if not wide.empty:
        work = work.merge(wide, on=["date_key", "player_id_key", "prop_type_key"], how="left")
    work["offense_team"] = work.get("player_team", pd.Series("", index=work.index)).map(_norm_team)
    if "wide_player_team" in work.columns:
        work["offense_team"] = work["offense_team"].where(work["offense_team"].ne(""), work["wide_player_team"].fillna(""))
    work["opponent_pitcher_staff_team"] = work.apply(_opponent_team, axis=1)
    if "wide_opponent_team" in work.columns:
        work["opponent_pitcher_staff_team"] = work["opponent_pitcher_staff_team"].where(
            work["opponent_pitcher_staff_team"].ne(""), work["wide_opponent_team"].fillna("")
        )
    work["side_key"] = work.get("side", pd.Series("", index=work.index)).map(_norm_side)
    work["selected_side_price"] = work.apply(_side_price, axis=1)
    work["environment_source"] = environment_source or ""
    if not offense.empty:
        off = offense.rename(
            columns={
                "team_key": "offense_team",
                "regime": "offense_environment_regime",
                "same_side_rate": "environment_same_side_rate",
                "residual_avg": "environment_residual_avg",
                "current_signed_streak": "environment_current_signed_streak",
                "environment_volatility": "offense_environment_volatility",
                "stability_score": "offense_environment_stability_score",
                "rolling_stability_rank": "offense_environment_stability_rank",
            }
        )
        work = work.merge(off, on="offense_team", how="left")
    else:
        work["offense_environment_regime"] = pd.NA
        work["environment_same_side_rate"] = pd.NA
        work["environment_residual_avg"] = pd.NA
        work["environment_current_signed_streak"] = pd.NA
    if not staff.empty:
        st = staff.rename(
            columns={
                "team_key": "opponent_pitcher_staff_team",
                "regime": "pitcher_staff_environment_regime",
                "same_side_rate": "pitcher_staff_environment_same_side_rate",
                "residual_avg": "pitcher_staff_environment_residual_avg",
                "current_signed_streak": "pitcher_staff_environment_current_signed_streak",
                "environment_volatility": "pitcher_staff_environment_volatility",
                "stability_score": "pitcher_staff_environment_stability_score",
                "rolling_stability_rank": "pitcher_staff_environment_stability_rank",
            }
        )
        work = work.merge(st, on="opponent_pitcher_staff_team", how="left")
    else:
        work["pitcher_staff_environment_regime"] = pd.NA
    work["offense_environment_regime"] = work["offense_environment_regime"].fillna("unclassified")
    work["pitcher_staff_environment_regime"] = work["pitcher_staff_environment_regime"].fillna("unclassified")
    work["hostile_environment_flag"] = [
        _hostile_flag(side, regime)
        for side, regime in zip(work["side_key"], work["offense_environment_regime"], strict=False)
    ]
    work["non_hostile_environment_flag"] = ~work["hostile_environment_flag"].astype(bool)
    work["v2_ranking_lane_flag"] = ~work.get("source_lane", pd.Series("", index=work.index)).astype(str).eq("quick_card_hits")
    work["favorite_flag"] = pd.to_numeric(work["selected_side_price"], errors="coerce").lt(0)
    v2_favorites = work[work["v2_ranking_lane_flag"] & work["favorite_flag"]].copy()
    by_lane_hostile = (
        work.groupby(["source_lane", "hostile_environment_flag"], dropna=False)
        .size()
        .reset_index(name="rows")
        .to_dict(orient="records")
    )
    summary = {
        "available": True,
        "environment_source": environment_source,
        "regimes_csv": str(regimes_csv) if regimes_csv else "",
        "out_csv": str(out_csv),
        "rows": int(len(work)),
        "matched_offense_regime_rows": int(work["offense_environment_regime"].ne("unclassified").sum()),
        "matched_pitcher_staff_regime_rows": int(work["pitcher_staff_environment_regime"].ne("unclassified").sum()),
        "v2_ranking_favorites_hostile": int(v2_favorites["hostile_environment_flag"].sum()),
        "v2_ranking_favorites_non_hostile": int((~v2_favorites["hostile_environment_flag"].astype(bool)).sum()),
        "by_source_lane_and_hostile_flag": by_lane_hostile,
    }
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    work.to_csv(out_csv, index=False)
    return work, summary


def _focus_metric(summary: dict[str, Any], key: str) -> dict[str, Any]:
    focus = summary.get("focus_population") or {}
    metric = focus.get(key) or {}
    return {
        "bets": int(metric.get("bets") or 0),
        "wins": int(metric.get("wins") or 0),
        "losses": int(metric.get("losses") or 0),
        "pushes": int(metric.get("pushes") or 0),
        "win_rate": metric.get("win_rate"),
        "roi": metric.get("roi"),
        "units": metric.get("units"),
    }


def _load_historical_observed_performance() -> dict[str, Any]:
    source = ""
    summary = {}
    if PRODUCTION_FAVORITES_BREAKDOWN_JSON.exists():
        source = str(PRODUCTION_FAVORITES_BREAKDOWN_JSON)
        summary = _load_json(PRODUCTION_FAVORITES_BREAKDOWN_JSON)
    elif BACKFILL_FAVORITES_BREAKDOWN_JSON.exists():
        source = str(BACKFILL_FAVORITES_BREAKDOWN_JSON)
        summary = _load_json(BACKFILL_FAVORITES_BREAKDOWN_JSON)
    return {
        "source": source,
        "hostile_favorites": _focus_metric(summary, "all_hostile"),
        "non_hostile_favorites": _focus_metric(summary, "all_non_hostile"),
        "current_sample_size": int((summary.get("focus_population") or {}).get("all_v2_favorites", {}).get("bets") or 0),
    }


def _watch_date_from_path(path: Path) -> str:
    name = path.name
    parts = name.split("_")
    for part in parts:
        try:
            return _date_key(part)
        except SystemExit:
            continue
    return path.parent.name


def _actual_reconcile_for_date(date_value: str) -> Path:
    return Path("backend/mlb/exports/model_v2/reconcile") / date_value / f"actual_wagers_by_source_{date_value}.csv"


def _match_actuals_to_environment(date_value: str, env: pd.DataFrame) -> pd.DataFrame:
    actual_path = _actual_reconcile_for_date(date_value)
    if not actual_path.exists() or env.empty:
        return pd.DataFrame()
    actual = pd.read_csv(actual_path, low_memory=False)
    if actual.empty:
        return pd.DataFrame()
    actual = actual[
        actual.get("row_type", pd.Series("", index=actual.index)).eq("actual_wager")
        & actual.get("source_category", pd.Series("", index=actual.index)).eq("v2_ranking")
    ].copy()
    if actual.empty:
        return pd.DataFrame()
    actual["date_key"] = actual.get("date", pd.Series(date_value, index=actual.index)).map(_date_key)
    actual["player_id_key"] = actual.get("player_id", pd.Series("", index=actual.index)).map(_id_text)
    actual["player_name_key"] = actual.get("player_name", pd.Series("", index=actual.index)).astype(str).str.lower().str.strip()
    actual["prop_type_key"] = actual.get("prop_type", pd.Series("", index=actual.index)).astype(str).str.lower().str.strip()
    actual["side_key"] = actual.get("side", pd.Series("", index=actual.index)).map(_norm_side)
    actual["line_key"] = pd.to_numeric(actual.get("line"), errors="coerce").round(3)
    actual["price_num"] = pd.to_numeric(actual.get("price"), errors="coerce")
    actual = actual[actual["price_num"].lt(0) & actual["date_key"].eq(date_value)].copy()
    if actual.empty:
        return pd.DataFrame()
    actual["_actual_row_id"] = range(len(actual))

    env_work = env.copy()
    env_work["date_key"] = env_work.get("date", pd.Series(date_value, index=env_work.index)).map(_date_key)
    env_work["player_id_key"] = env_work.get("player_id", pd.Series("", index=env_work.index)).map(_id_text)
    env_work["player_name_key"] = env_work.get("player_name", pd.Series("", index=env_work.index)).astype(str).str.lower().str.strip()
    env_work["prop_type_key"] = env_work.get("prop_type", pd.Series("", index=env_work.index)).astype(str).str.lower().str.strip()
    env_work["side_key"] = env_work.get("side", pd.Series("", index=env_work.index)).map(_norm_side)
    env_work["line_key"] = pd.to_numeric(env_work.get("line"), errors="coerce").round(3)
    env_work = env_work[env_work.get("v2_ranking_lane_flag", pd.Series(False, index=env_work.index)).astype(bool)].copy()
    if env_work.empty:
        return pd.DataFrame()

    env_cols = [
        "date_key",
        "player_id_key",
        "player_name_key",
        "prop_type_key",
        "side_key",
        "line_key",
        "hostile_environment_flag",
        "offense_team",
        "offense_environment_regime",
        "source_lane",
    ]
    env_small = env_work[[c for c in env_cols if c in env_work.columns]].drop_duplicates(
        ["date_key", "player_id_key", "prop_type_key", "side_key", "line_key"], keep="first"
    )
    merged = actual.merge(
        env_small,
        on=["date_key", "player_id_key", "prop_type_key", "side_key", "line_key"],
        how="left",
        suffixes=("", "_env"),
    )
    missing = merged["hostile_environment_flag"].isna()
    if missing.any():
        env_name = env_work[[c for c in env_cols if c in env_work.columns]].drop_duplicates(
            ["date_key", "player_name_key", "prop_type_key", "side_key", "line_key"], keep="first"
        )
        fallback_left = actual[actual["_actual_row_id"].isin(merged.loc[missing, "_actual_row_id"])].copy()
        fallback = fallback_left.merge(
            env_name,
            on=["date_key", "player_name_key", "prop_type_key", "side_key", "line_key"],
            how="left",
            suffixes=("", "_env"),
        )
        fallback = fallback.set_index("_actual_row_id")
        for col in ["hostile_environment_flag", "offense_team", "offense_environment_regime", "source_lane"]:
            if col in fallback.columns:
                merged.loc[missing, col] = merged.loc[missing, "_actual_row_id"].map(fallback[col]).values
    return merged[merged["hostile_environment_flag"].notna()].copy()


def _summarize_actual_env(actual_env: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    if actual_env.empty:
        return {}, {}
    out = {}
    for flag, group in actual_env.groupby("hostile_environment_flag", dropna=False):
        key = "hostile" if bool(flag) else "non_hostile"
        result = group.get("result", pd.Series("", index=group.index)).astype(str).str.lower()
        wins = int(result.eq("win").sum())
        losses = int(result.eq("loss").sum())
        pushes = int(result.eq("push").sum())
        bets = int(len(group))
        units = float(pd.to_numeric(group.get("units"), errors="coerce").fillna(0).sum())
        out[key] = {
            "bets": bets,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": wins / (wins + losses) if wins + losses else None,
            "roi": units / bets if bets else None,
            "units": units,
        }
    return out.get("hostile", {}), out.get("non_hostile", {})


def _trend_rows(current_date: str, current_env: pd.DataFrame) -> list[dict[str, Any]]:
    paths = sorted(LANE_ROOT.glob("*/hits_lane_selector_*_environment_diagnostics.csv"))
    by_date: dict[str, pd.DataFrame] = {}
    for path in paths:
        date_value = _watch_date_from_path(path)
        try:
            frame = current_env.copy() if date_value == current_date else pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        if not frame.empty:
            by_date[date_value] = frame
    if current_date and current_date not in by_date and not current_env.empty:
        by_date[current_date] = current_env.copy()

    rows = []
    for date_value, frame in sorted(by_date.items()):
        v2_favs = frame[
            frame.get("v2_ranking_lane_flag", pd.Series(False, index=frame.index)).astype(bool)
            & frame.get("favorite_flag", pd.Series(False, index=frame.index)).astype(bool)
        ].copy()
        hostile_count = int(v2_favs.get("hostile_environment_flag", pd.Series(False, index=v2_favs.index)).astype(bool).sum())
        non_hostile_count = int(len(v2_favs) - hostile_count)
        actual_env = _match_actuals_to_environment(date_value, frame)
        hostile_perf, non_hostile_perf = _summarize_actual_env(actual_env)
        rows.append(
            {
                "date": date_value,
                "hostile_count": hostile_count,
                "non_hostile_count": non_hostile_count,
                "outcomes_available": bool(not actual_env.empty),
                "hostile_bets": hostile_perf.get("bets"),
                "hostile_roi": hostile_perf.get("roi"),
                "hostile_units": hostile_perf.get("units"),
                "non_hostile_bets": non_hostile_perf.get("bets"),
                "non_hostile_roi": non_hostile_perf.get("roi"),
                "non_hostile_units": non_hostile_perf.get("units"),
            }
        )
    return rows


def _write_hostile_watch(
    *,
    date_value: str,
    environment_rows: pd.DataFrame,
    environment_summary: dict[str, Any],
) -> dict[str, Any]:
    current = environment_rows.copy()
    current_v2_favorites = current[
        current.get("v2_ranking_lane_flag", pd.Series(False, index=current.index)).astype(bool)
        & current.get("favorite_flag", pd.Series(False, index=current.index)).astype(bool)
    ].copy()
    current_hostile = current_v2_favorites[
        current_v2_favorites.get("hostile_environment_flag", pd.Series(False, index=current_v2_favorites.index)).astype(bool)
    ].copy()
    hostile_cols = [
        "player_name",
        "player_id",
        "source_lane",
        "side",
        "line",
        "selected_side_price",
        "offense_team",
        "opponent_pitcher_staff_team",
        "home_team_code",
        "away_team_code",
        "offense_environment_regime",
        "pitcher_staff_environment_regime",
    ]
    hostile_rows = current_hostile[[c for c in hostile_cols if c in current_hostile.columns]].to_dict(orient="records")
    historical = _load_historical_observed_performance()
    trend = _trend_rows(date_value, current)

    payload = {
        "last_updated_date": date_value,
        "current_slate_watch": {
            "date": date_value,
            "v2_ranking_favorites_hostile_count": int(environment_summary.get("v2_ranking_favorites_hostile", 0)),
            "v2_ranking_favorites_non_hostile_count": int(environment_summary.get("v2_ranking_favorites_non_hostile", 0)),
            "hostile_rows": hostile_rows,
            "source_lane_counts_by_hostile_flag": environment_summary.get("by_source_lane_and_hostile_flag", []),
            "environment_diagnostics_csv": environment_summary.get("out_csv", ""),
            "environment_source": environment_summary.get("environment_source", ""),
        },
        "historical_observed_performance": {
            **historical,
            "last_updated_date": date_value,
        },
        "recent_daily_trend": trend,
        "guardrail_note": {
            "visibility_only": True,
            "production_filter": False,
            "note": "Visibility only. No production filter. Sample still developing.",
        },
    }

    HOSTILE_WATCH_JSON.parent.mkdir(parents=True, exist_ok=True)
    HOSTILE_WATCH_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    pd.DataFrame(trend).to_csv(HOSTILE_WATCH_CSV, index=False)

    hist_hostile = historical.get("hostile_favorites", {})
    hist_non = historical.get("non_hostile_favorites", {})
    lines = [
        "# Hostile Hits Environment Watch",
        "",
        "Visibility only. No production filter. Sample still developing.",
        "",
        "## Current Slate Watch",
        f"- Date: `{date_value}`",
        f"- Environment source: `{environment_summary.get('environment_source', '')}`",
        f"- v2 ranking favorites hostile: `{environment_summary.get('v2_ranking_favorites_hostile', 0)}`",
        f"- v2 ranking favorites non-hostile: `{environment_summary.get('v2_ranking_favorites_non_hostile', 0)}`",
        f"- Diagnostics CSV: `{environment_summary.get('out_csv', '')}`",
        "",
        "### Hostile Rows",
    ]
    if hostile_rows:
        for row in hostile_rows:
            game = f"{row.get('away_team_code', '')}@{row.get('home_team_code', '')}"
            lines.append(
                f"- `{row.get('player_name')}` `{row.get('source_lane')}` `{row.get('side')}` "
                f"`{row.get('line')}` price `{row.get('selected_side_price')}` "
                f"offense `{row.get('offense_team')}` game `{game}`"
            )
    else:
        lines.append("- None.")
    lines.extend(["", "### Source Lane Counts"])
    for row in environment_summary.get("by_source_lane_and_hostile_flag", []):
        lines.append(f"- `{row.get('source_lane')}` hostile=`{row.get('hostile_environment_flag')}`: `{row.get('rows')}`")
    lines.extend(
        [
            "",
            "## Historical Observed Performance",
            f"- Source: `{historical.get('source', '') or 'unavailable'}`",
            f"- Hostile favorites: bets `{hist_hostile.get('bets', 0)}`, WR `{_fmt_pct(hist_hostile.get('win_rate'))}`, ROI `{_fmt_pct(hist_hostile.get('roi'))}`, units `{_fmt_units(hist_hostile.get('units'))}`",
            f"- Non-hostile favorites: bets `{hist_non.get('bets', 0)}`, WR `{_fmt_pct(hist_non.get('win_rate'))}`, ROI `{_fmt_pct(hist_non.get('roi'))}`, units `{_fmt_units(hist_non.get('units'))}`",
            f"- Current sample size: `{historical.get('current_sample_size', 0)}`",
            f"- Last updated date: `{date_value}`",
            "",
            "## Recent Daily Trend",
        ]
    )
    for row in trend[-14:]:
        roi_text = (
            f"hostile ROI `{_fmt_pct(row.get('hostile_roi'))}`, non-hostile ROI `{_fmt_pct(row.get('non_hostile_roi'))}`"
            if row.get("outcomes_available")
            else "results unavailable"
        )
        lines.append(
            f"- `{row.get('date')}`: hostile `{row.get('hostile_count')}`, "
            f"non-hostile `{row.get('non_hostile_count')}`, {roi_text}"
        )
    lines.extend(
        [
            "",
            "## Guardrail",
            "- Visibility only.",
            "- No production filter.",
            "- Sample still developing.",
        ]
    )
    HOSTILE_WATCH_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def _metric_by_group(results: dict[str, Any], group: str) -> list[dict[str, Any]]:
    return [m for m in results.get("metrics", []) if m.get("group") == group]


def _write_md(
    *,
    path: Path,
    date_value: str,
    selector_summary: dict[str, Any],
    upload_diag: dict[str, Any],
    upload_rows: int,
    quick_upload_rows: int,
    quick_rows: int,
    results_summary: dict[str, Any],
    selector_proc: subprocess.CompletedProcess[str] | None,
    results_proc: subprocess.CompletedProcess[str] | None,
    environment_summary: dict[str, Any] | None = None,
) -> None:
    counts = selector_summary.get("counts_by_lane", {})
    identity = selector_summary.get("upload_identity_validation", {})
    overall_results = next(iter(_metric_by_group(results_summary, "overall")), {})
    quick_warning = selector_summary.get("quick_card_warning", "")
    lines = [
        f"# Hits Lane Selector Daily Report - {date_value}",
        "",
        "## Selector",
        f"- Mode: `{selector_summary.get('mode', 'unknown')}`",
        f"- Note: `{selector_summary.get('note', '')}`",
        f"- Selector rows: `{selector_summary.get('total_selected', 0)}`",
        f"- Ranking upload input rows: `{upload_diag.get('ranking_upload_input_rows', 0)}`",
        f"- Ranking upload rows: `{upload_rows}`",
        f"- Quick Card upload rows: `{quick_upload_rows}`",
        f"- Combined tool-upload rows: `{upload_rows + quick_upload_rows}`",
        f"- Quick Card rows: `{quick_rows}`",
        f"- Quick Card sent to ranking upload: `{upload_diag.get('quick_card_lane', {}).get('sent_to_ranking_upload')}`",
        f"- Quick Card source existed before: `{selector_summary.get('quick_card_source_exists_before')}`",
        f"- Quick Card builder ran: `{selector_summary.get('quick_card_builder_ran')}`",
        f"- Quick Card source exists after: `{selector_summary.get('quick_card_source_exists_after')}`",
        f"- Quick Card hits rows: `{selector_summary.get('quick_card_hits_rows', quick_rows)}`",
        f"- Quick Card warning: `{quick_warning}`",
        "",
        "## Rows By Lane",
    ]
    for lane, row in counts.items():
        lines.append(f"- `{lane}`: `{row.get('count', 0)}` rows | avg odds `{row.get('avg_odds')}`")

    environment_summary = environment_summary or {}
    if environment_summary.get("available"):
        lines.extend(
            [
                "",
                "## Environment Regime Diagnostics",
                f"- Environment source: `{environment_summary.get('environment_source', '')}`",
                f"- Diagnostics CSV: `{environment_summary.get('out_csv', '')}`",
                f"- Selector rows with offense regime: `{environment_summary.get('matched_offense_regime_rows', 0)}` of `{environment_summary.get('rows', 0)}`",
                f"- Selector rows with pitcher-staff regime: `{environment_summary.get('matched_pitcher_staff_regime_rows', 0)}` of `{environment_summary.get('rows', 0)}`",
                f"- v2 ranking favorites hostile: `{environment_summary.get('v2_ranking_favorites_hostile', 0)}`",
                f"- v2 ranking favorites non-hostile: `{environment_summary.get('v2_ranking_favorites_non_hostile', 0)}`",
                "",
                "## Environment Counts By Lane",
            ]
        )
        for row in environment_summary.get("by_source_lane_and_hostile_flag", []):
            lines.append(
                f"- `{row.get('source_lane')}` hostile=`{row.get('hostile_environment_flag')}`: `{row.get('rows')}` rows"
            )
    elif environment_summary:
        lines.extend(["", "## Environment Regime Diagnostics", f"- Unavailable: `{environment_summary.get('reason', 'unknown')}`"])

    lines.extend(
        [
            "",
            "## Upload Diagnostics",
            f"- Excluded low-sample rows: `{upload_diag.get('excluded_low_sample', 0)}`",
            f"- Excluded unmapped rows: `{upload_diag.get('excluded_unmapped_bucket', 0)}`",
            f"- Excluded missing required fields: `{upload_diag.get('excluded_missing_required_fields', 0)}`",
            f"- Would pass with allow-low-sample: `{upload_diag.get('would_pass_with_allow_low_sample_upload', 0)}`",
            "",
            "## Upload Identity",
            f"- Raw HOME/AWAY teams: `{identity.get('raw_teams', [])}`",
            f"- Normalized HOME/AWAY teams: `{identity.get('upload_teams', [])}`",
            f"- Team match ok true: `{identity.get('team_match_ok_true', 0)}`",
            f"- Team match ok false: `{identity.get('team_match_ok_false', 0)}`",
            f"- Team normalizer: `{identity.get('team_normalizer', '')}`",
            f"- Team alias map: `{identity.get('team_alias_map', {})}`",
        ]
    )

    if results_summary:
        lines.extend(
            [
                "",
                "## Results",
                f"- Resolved rows: `{results_summary.get('rows_with_resolved_pnl', 0)}`",
                f"- Missing outcome rows: `{results_summary.get('missing_outcome_rows', 0)}`",
                f"- Win rate: `{_fmt_pct(overall_results.get('win_rate'))}`",
                f"- ROI: `{_fmt_pct(overall_results.get('roi'))}`",
                f"- Units: `{_fmt_units(overall_results.get('units'))}`",
                "",
                "## Results By Lane",
            ]
        )
        for metric in _metric_by_group(results_summary, "by_lane"):
            lines.append(
                f"- `{metric.get('value')}`: `{metric.get('bets')}` bets | "
                f"WR `{_fmt_pct(metric.get('win_rate'))}` | ROI `{_fmt_pct(metric.get('roi'))}` | "
                f"units `{_fmt_units(metric.get('units'))}`"
            )
    else:
        note = selector_summary.get("note") or "Outcomes unavailable or results summary not produced."
        lines.extend(["", "## Results", f"- {note}"])

    lines.extend(
        [
            "",
            "## Commands",
            f"- Selector command status: `{selector_proc.returncode if selector_proc else 'skipped'}`",
            f"- Results command status: `{results_proc.returncode if results_proc else 'skipped'}`",
            "",
            "Lane rules unchanged: UNDER 0.5 top decile, OVER bucket 9, Quick Card separated from ranking upload.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    date_value = _date_key(args.date)
    date_dir = _lane_date_dir(date_value)
    selector_csv = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}.csv")
    selector_summary_json = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}_summary.json")
    upload_diag_json = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}_upload_diagnostics.json")
    quick_card_csv = _dated_or_legacy(date_value, f"quick_card_hits_{date_value}.csv")
    upload_csv = _upload_dated_or_legacy(date_value, f"ranking_tool_upload_{date_value}.csv")
    quick_upload_csv = _upload_date_dir(date_value) / f"quick_card_tool_upload_{date_value}.csv"
    quick_upload_diag_csv = _upload_date_dir(date_value) / f"quick_card_tool_upload_diagnostics_{date_value}.csv"
    results_json = _dated_or_legacy(date_value, f"hits_lane_selector_{date_value}_results_summary.json")
    md_path = date_dir / f"hits_lane_selector_{date_value}_daily_report.md"

    selector_proc: subprocess.CompletedProcess[str] | None = None
    if not args.skip_run_selector:
        cmd = [sys.executable, str(DAILY_SCRIPT), "--date", date_value]
        if args.allow_low_sample_upload:
            cmd.append("--allow-low-sample-upload")
        if args.drop_team_mismatch_upload:
            cmd.append("--drop-team-mismatch-upload")
        selector_proc = _run(cmd)
        if selector_proc.returncode != 0:
            print(selector_proc.stdout)
            print(selector_proc.stderr, file=sys.stderr)
            raise SystemExit(selector_proc.returncode)
        selector_csv = date_dir / f"hits_lane_selector_{date_value}.csv"
        selector_summary_json = date_dir / f"hits_lane_selector_{date_value}_summary.json"
        upload_diag_json = date_dir / f"hits_lane_selector_{date_value}_upload_diagnostics.json"
        quick_card_csv = date_dir / f"quick_card_hits_{date_value}.csv"
        results_json = date_dir / f"hits_lane_selector_{date_value}_results_summary.json"
        upload_csv = _upload_date_dir(date_value) / f"ranking_tool_upload_{date_value}.csv"

    selector_summary = _load_json(selector_summary_json)
    upload_diag = _load_json(upload_diag_json)
    upload_rows = _csv_rows(upload_csv)
    quick_rows = _csv_rows(quick_card_csv)
    if args.skip_quick_card_upload:
        quick_upload_proc = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    else:
        quick_upload_proc = _run(
            [
                sys.executable,
                str(QUICK_CARD_UPLOAD_SCRIPT),
                "--date",
                date_value,
                "--in-csv",
                str(quick_card_csv),
                "--out-csv",
                str(quick_upload_csv),
                "--diagnostics-csv",
                str(quick_upload_diag_csv),
            ]
        )
        if quick_upload_proc.returncode != 0:
            print(quick_upload_proc.stdout)
            print(quick_upload_proc.stderr, file=sys.stderr)
            raise SystemExit(quick_upload_proc.returncode)
    quick_upload_rows = _csv_rows(quick_upload_csv)
    mode = selector_summary.get("mode") or ("postgame" if (RECONCILE_ROOT / date_value / "reconcile_rows.csv").exists() else "pregame")

    regimes_csv, environment_source = _resolve_environment_regimes(args)
    environment_diagnostics_csv = date_dir / f"hits_lane_selector_{date_value}_environment_diagnostics.csv"
    environment_rows, environment_summary = _environment_diagnostics(
        selector_csv=selector_csv,
        out_csv=environment_diagnostics_csv,
        regimes_csv=regimes_csv,
        environment_source=environment_source,
    )
    watch_summary = {}
    if environment_summary.get("available"):
        watch_summary = _write_hostile_watch(
            date_value=date_value,
            environment_rows=environment_rows,
            environment_summary=environment_summary,
        )

    results_proc: subprocess.CompletedProcess[str] | None = None
    if mode != "pregame" and (RECONCILE_ROOT / date_value / "reconcile_rows.csv").exists():
        results_proc = _run([sys.executable, str(RESULTS_SCRIPT), "--date", date_value])
        if results_proc.returncode != 0:
            print(results_proc.stdout)
            print(results_proc.stderr, file=sys.stderr)
        results_summary = _load_json(results_json)
    else:
        results_summary = {}

    _write_md(
        path=md_path,
        date_value=date_value,
        selector_summary=selector_summary,
        upload_diag=upload_diag,
        upload_rows=upload_rows,
        quick_upload_rows=quick_upload_rows,
        quick_rows=quick_rows,
        results_summary=results_summary,
        selector_proc=selector_proc,
        results_proc=results_proc,
        environment_summary=environment_summary,
    )

    overall = next(iter(_metric_by_group(results_summary, "overall")), {})
    print(f"Hits lane selector report: {date_value}")
    print(f"mode={mode}")
    if mode == "pregame":
        print("note=no outcomes available")
    print(f"selector_rows={selector_summary.get('total_selected', 0)}")
    print(f"rows_by_lane={json.dumps(selector_summary.get('counts_by_lane', {}), sort_keys=True)}")
    print(f"ranking_upload_input_rows={upload_diag.get('ranking_upload_input_rows', 0)}")
    print(f"ranking_upload_rows={upload_rows}")
    print(f"quick_card_upload_rows={quick_upload_rows}")
    print(f"combined_tool_upload_rows={upload_rows + quick_upload_rows}")
    print(f"excluded_low_sample={upload_diag.get('excluded_low_sample', 0)}")
    print(f"excluded_unmapped={upload_diag.get('excluded_unmapped_bucket', 0)}")
    print(f"quick_card_rows={quick_rows}")
    print(f"quick_card_source_exists_before={selector_summary.get('quick_card_source_exists_before')}")
    print(f"quick_card_builder_ran={selector_summary.get('quick_card_builder_ran')}")
    print(f"quick_card_source_exists_after={selector_summary.get('quick_card_source_exists_after')}")
    print(f"quick_card_hits_rows={selector_summary.get('quick_card_hits_rows', quick_rows)}")
    if selector_summary.get("quick_card_warning"):
        print(f"quick_card_warning={selector_summary.get('quick_card_warning')}")
    print(f"quick_card_sent_to_ranking_upload={upload_diag.get('quick_card_lane', {}).get('sent_to_ranking_upload')}")
    if environment_summary.get("available"):
        print(f"environment_source={environment_summary.get('environment_source')}")
        print(f"environment_diagnostics_csv={environment_summary.get('out_csv')}")
        print(
            "environment_v2_ranking_favorites "
            f"hostile={environment_summary.get('v2_ranking_favorites_hostile', 0)} "
            f"non_hostile={environment_summary.get('v2_ranking_favorites_non_hostile', 0)}"
        )
        print(
            "environment_by_source_lane_and_hostile_flag="
            + json.dumps(environment_summary.get("by_source_lane_and_hostile_flag", []), sort_keys=True)
        )
        print(f"hostile_environment_watch_md={HOSTILE_WATCH_MD}")
        print(f"hostile_environment_watch_csv={HOSTILE_WATCH_CSV}")
        print(f"hostile_environment_watch_json={HOSTILE_WATCH_JSON}")
    elif environment_summary:
        print(f"environment_diagnostics_unavailable={environment_summary.get('reason')}")
    identity = selector_summary.get("upload_identity_validation", {})
    print(f"raw_home_away_teams={json.dumps(identity.get('raw_teams', []))}")
    print(f"normalized_home_away_teams={json.dumps(identity.get('upload_teams', []))}")
    print(
        "team_match_ok "
        f"true={identity.get('team_match_ok_true', 0)} false={identity.get('team_match_ok_false', 0)}"
    )
    print(f"team_match_false_rows={json.dumps(identity.get('false_rows', []), sort_keys=True)}")
    print(f"team_normalizer={identity.get('team_normalizer', '')}")
    print(f"team_alias_map={json.dumps(identity.get('team_alias_map', {}), sort_keys=True)}")
    if results_summary:
        print(
            "results "
            f"resolved={results_summary.get('rows_with_resolved_pnl', 0)} "
            f"win_rate={_fmt_pct(overall.get('win_rate'))} "
            f"roi={_fmt_pct(overall.get('roi'))} "
            f"units={_fmt_units(overall.get('units'))}"
        )
        by_lane = {
            m.get("value"): {
                "bets": m.get("bets"),
                "win_rate": _fmt_pct(m.get("win_rate")),
                "roi": _fmt_pct(m.get("roi")),
                "units": _fmt_units(m.get("units")),
            }
            for m in _metric_by_group(results_summary, "by_lane")
        }
        print(f"results_by_lane={json.dumps(by_lane, sort_keys=True)}")
    else:
        print("results unavailable")
    print(f"markdown_report={md_path}")
    print("confirmed_no_lane_rule_changes=true")

    return {
        "date": date_value,
        "markdown_report": str(md_path),
        "selector_rows": selector_summary.get("total_selected", 0),
        "ranking_upload_rows": upload_rows,
        "quick_card_upload_rows": quick_upload_rows,
        "combined_tool_upload_rows": upload_rows + quick_upload_rows,
        "environment_diagnostics_csv": environment_summary.get("out_csv"),
        "environment_summary": environment_summary,
        "hostile_environment_watch": {
            "md": str(HOSTILE_WATCH_MD),
            "csv": str(HOSTILE_WATCH_CSV),
            "json": str(HOSTILE_WATCH_JSON),
            "current_hostile_count": (
                watch_summary.get("current_slate_watch", {}).get("v2_ranking_favorites_hostile_count")
                if watch_summary
                else None
            ),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run/report MLB hits lane selector in one command.")
    parser.add_argument("--date", required=True)
    parser.add_argument("--skip-run-selector", action="store_true")
    parser.add_argument("--skip-quick-card-upload", action="store_true")
    parser.add_argument("--allow-low-sample-upload", action="store_true")
    parser.add_argument("--drop-team-mismatch-upload", action="store_true")
    parser.add_argument("--environment-regimes-csv", default="")
    parser.add_argument("--environment-source", choices=["", "production_history", "backfill_2026"], default="")
    return parser.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
