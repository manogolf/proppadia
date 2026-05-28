#!/usr/bin/env python3
"""Cross MLB v2 results against descriptive hits environment regimes."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


TEAM_ALIASES = {
    "ARI": "ARI",
    "AZ": "ARI",
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

BATTER_PROPS = {
    "hits",
    "total_bases",
    "runs_scored",
    "rbis",
    "hits_runs_rbis",
    "walks",
    "strikeouts_batting",
    "singles",
    "doubles",
    "home_runs",
}

PITCHER_PROPS = {"hits_allowed", "strikeouts_pitching", "outs_recorded", "earned_runs"}


def _norm_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value).strip()


def _norm_team(value: Any) -> str:
    text = _norm_text(value).upper()
    return TEAM_ALIASES.get(text, text)


def _norm_side(value: Any) -> str:
    text = _norm_text(value).lower()
    if text in {"o", "over"}:
        return "over"
    if text in {"u", "under"}:
        return "under"
    return text


def _date_key(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _id_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    try:
        return str(int(float(value)))
    except Exception:
        return _norm_text(value)


def _norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _norm_text(value).lower()).strip()


def _to_float(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(parsed) if pd.notna(parsed) else np.nan


def _load_reconcile_rows(root: Path) -> pd.DataFrame:
    paths = sorted(root.glob("*/actual_wagers_by_source_*.csv"))
    frames = []
    for path in paths:
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty:
            continue
        df["reconcile_source_file"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out[out["source_category"].isin(["v2_ranking", "quick_card"])].copy()
    out = out[out["row_type"].isin(["actual_wager", "upload_not_wagered"])].copy()
    if out.empty:
        return out
    out["date"] = out["date"].map(_date_key)
    out["player_id_key"] = out["player_id"].map(_id_text)
    out["player_name_key"] = out["player_name"].map(_norm_name)
    out["prop_type_key"] = out["prop_type"].map(lambda v: _norm_text(v).lower())
    out["side_key"] = out["side"].map(_norm_side)
    out["home_key"] = out["home"].map(_norm_team)
    out["away_key"] = out["away"].map(_norm_team)
    out["price_num"] = pd.to_numeric(out.get("price"), errors="coerce")
    out["units_num"] = pd.to_numeric(out.get("units"), errors="coerce")
    out["result_key"] = out["result"].map(lambda v: _norm_text(v).lower())
    return out


def _actual_wagers_by_source_dates(root: Path) -> list[str]:
    dates = {_date_key(path.parent.name) for path in root.glob("*/actual_wagers_by_source_*.csv")}
    return sorted(date for date in dates if date)


def _build_freshness(reconcile_root: Path, detail: pd.DataFrame, total_rows_loaded: int) -> dict[str, Any]:
    available_dates = _actual_wagers_by_source_dates(reconcile_root)
    included_dates: list[str] = []
    if "date" in detail:
        included_dates = sorted(
            {
                _date_key(value)
                for value in detail["date"].dropna().astype(str).tolist()
                if _date_key(value)
            }
        )

    latest_available = available_dates[-1] if available_dates else None
    latest_included = included_dates[-1] if included_dates else None
    stale = bool(latest_available and (not latest_included or latest_included < latest_available))
    warning = None
    if stale:
        warning = (
            "Interaction analysis is stale relative to available actual_wagers_by_source files: "
            f"latest included={latest_included or 'none'}, latest available={latest_available}."
        )

    return {
        "latest_actual_wagers_by_source_date_found": latest_available,
        "latest_interaction_date_included": latest_included,
        "total_rows_loaded": int(total_rows_loaded),
        "actual_wagers_by_source_dates_found": available_dates,
        "interaction_dates_included": included_dates,
        "is_stale_relative_to_reconcile": stale,
        "warning": warning,
    }


def _wide_paths_for_dates(root: Path, dates: set[str]) -> list[Path]:
    paths: list[Path] = []
    for date in sorted(dates):
        day_root = root / date
        if not day_root.exists():
            continue
        paths.extend(sorted(day_root.glob("mlb_predictions_wide_calibrated*.csv")))
    return paths


def _load_wide_lookup(root: Path, dates: set[str]) -> pd.DataFrame:
    frames = []
    cols = [
        "player_id",
        "player_name",
        "team",
        "opponent",
        "home_team_code",
        "away_team_code",
        "prop_type",
        "game_date",
        "game_id",
    ]
    for path in _wide_paths_for_dates(root, dates):
        try:
            header = pd.read_csv(path, nrows=0).columns
            usecols = [col for col in cols if col in header]
            df = pd.read_csv(path, usecols=usecols)
        except Exception:
            continue
        if df.empty:
            continue
        df["wide_source_file"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    out["date"] = out["game_date"].map(_date_key)
    out["player_id_key"] = out["player_id"].map(_id_text)
    out["player_name_key"] = out["player_name"].map(_norm_name)
    out["prop_type_key"] = out["prop_type"].map(lambda v: _norm_text(v).lower())
    out["home_key"] = out["home_team_code"].map(_norm_team)
    out["away_key"] = out["away_team_code"].map(_norm_team)
    out["player_team"] = out["team"].map(_norm_team)
    out["opponent_team"] = out["opponent"].map(_norm_team)
    keep = [
        "date",
        "player_id_key",
        "player_name_key",
        "prop_type_key",
        "home_key",
        "away_key",
        "player_team",
        "opponent_team",
        "game_id",
        "wide_source_file",
    ]
    out = out[keep].drop_duplicates(keep[:-1], keep="last")
    return out


def _attach_wide(wagers: pd.DataFrame, wide: pd.DataFrame) -> pd.DataFrame:
    if wagers.empty or wide.empty:
        return wagers.copy()
    keys = ["date", "player_id_key", "prop_type_key", "home_key", "away_key"]
    merged = wagers.merge(wide, on=keys, how="left", suffixes=("", "_wide"))
    missing = merged["player_team"].isna()
    if missing.any():
        fallback = wide.drop_duplicates(["date", "player_id_key", "prop_type_key"], keep="last")
        fallback_cols = ["date", "player_id_key", "prop_type_key", "player_team", "opponent_team", "game_id", "wide_source_file"]
        fallback = fallback[fallback_cols].rename(
            columns={
                "player_team": "player_team_fb",
                "opponent_team": "opponent_team_fb",
                "game_id": "game_id_fb",
                "wide_source_file": "wide_source_file_fb",
            }
        )
        merged = merged.merge(fallback, on=["date", "player_id_key", "prop_type_key"], how="left")
        for col in ["player_team", "opponent_team", "game_id", "wide_source_file"]:
            merged[col] = merged[col].fillna(merged[f"{col}_fb"])
            merged = merged.drop(columns=[f"{col}_fb"])
    return merged


def _environment_team(row: pd.Series) -> tuple[str, str]:
    prop = _norm_text(row.get("prop_type_key")).lower()
    if prop in BATTER_PROPS:
        return _norm_team(row.get("player_team")), "offense_environment_from_batter_team"
    if prop in PITCHER_PROPS:
        return _norm_team(row.get("opponent_team")), "offense_environment_from_pitcher_opponent"
    return _norm_team(row.get("player_team")), "offense_environment_from_player_team"


def _load_regimes(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df = df[df["role"].eq("offense_environment")].copy()
    df["environment_team"] = df["team"].map(_norm_team)
    return df.drop_duplicates("environment_team", keep="first")


def _alignment(side: str, regime: str) -> str:
    if not regime or regime == "unclassified":
        return "unclassified"
    if regime == "volatile_or_mixed":
        return "neutral_or_mixed"
    if side == "over" and regime == "recurring_overperformer":
        return "favorable_environment"
    if side == "under" and regime == "recurring_underperformer":
        return "favorable_environment"
    if side == "over" and regime == "recurring_underperformer":
        return "hostile_environment"
    if side == "under" and regime == "recurring_overperformer":
        return "hostile_environment"
    return "neutral_or_mixed"


def _enrich(wagers: pd.DataFrame, regimes: pd.DataFrame, wide: pd.DataFrame) -> pd.DataFrame:
    out = _attach_wide(wagers, wide)
    if out.empty:
        return out
    env_pairs = out.apply(_environment_team, axis=1, result_type="expand")
    out["environment_team"] = env_pairs[0]
    out["environment_join_role"] = env_pairs[1]
    out = out.merge(
        regimes[
            [
                "environment_team",
                "regime",
                "appearances",
                "persistence_duration_days",
                "same_side_rate",
                "residual_avg",
                "environment_volatility",
                "stability_score",
                "rolling_stability_rank",
            ]
        ],
        on="environment_team",
        how="left",
    )
    out["environment_regime"] = out["regime"].fillna("unclassified")
    out["environment_alignment"] = [
        _alignment(side, regime) for side, regime in zip(out["side_key"], out["environment_regime"], strict=False)
    ]
    out["resolved"] = out["result_key"].isin(["win", "loss", "push"])
    out["is_win"] = out["result_key"].eq("win")
    out["is_loss"] = out["result_key"].eq("loss")
    out["is_push"] = out["result_key"].eq("push")
    out["is_favorite"] = out["price_num"].lt(0)
    out["is_strong_favorite"] = out["price_num"].le(-150)
    out["favorite_loss"] = out["is_favorite"] & out["is_loss"]
    out["strong_favorite_loss"] = out["is_strong_favorite"] & out["is_loss"]
    return out


def _rate(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if denominator else np.nan


def _summarize_group(group: pd.DataFrame) -> pd.Series:
    resolved = group[group["resolved"]].copy()
    resolved_n = int(len(resolved))
    wins = int(resolved["is_win"].sum())
    losses = int(resolved["is_loss"].sum())
    pushes = int(resolved["is_push"].sum())
    units = float(resolved["units_num"].fillna(0).sum()) if resolved_n else 0.0
    favorites = group[group["is_favorite"]]
    favorite_losses = int((favorites["is_loss"] & favorites["resolved"]).sum())
    strong_favorites = group[group["is_strong_favorite"]]
    strong_favorite_losses = int((strong_favorites["is_loss"] & strong_favorites["resolved"]).sum())
    return pd.Series(
        {
            "rows": int(len(group)),
            "resolved": resolved_n,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": _rate(wins, wins + losses),
            "units": units,
            "roi": _rate(units, resolved_n),
            "avg_price": float(group["price_num"].mean()) if group["price_num"].notna().any() else np.nan,
            "favorite_rows": int(len(favorites)),
            "favorite_losses": favorite_losses,
            "favorite_loss_rate": _rate(favorite_losses, len(favorites)),
            "strong_favorite_rows": int(len(strong_favorites)),
            "strong_favorite_losses": strong_favorite_losses,
            "strong_favorite_loss_rate": _rate(strong_favorite_losses, len(strong_favorites)),
            "avg_stability_score": float(group["stability_score"].mean()) if group["stability_score"].notna().any() else np.nan,
            "avg_environment_volatility": (
                float(group["environment_volatility"].mean()) if group["environment_volatility"].notna().any() else np.nan
            ),
            "avg_regime_residual": float(group["residual_avg"].mean()) if group["residual_avg"].notna().any() else np.nan,
            "avg_persistence_duration_days": (
                float(group["persistence_duration_days"].mean())
                if group["persistence_duration_days"].notna().any()
                else np.nan
            ),
        }
    )


def _summary_table(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["row_type", "source_category", "side_key", "environment_regime", "environment_alignment"]
    if df.empty:
        return pd.DataFrame(columns=group_cols)
    out = df.groupby(group_cols, dropna=False).apply(_summarize_group, include_groups=False).reset_index()
    return out.sort_values(["row_type", "source_category", "side_key", "environment_regime", "environment_alignment"])


def _subset_metric(df: pd.DataFrame, mask: pd.Series) -> dict[str, Any]:
    part = df[mask].copy()
    s = _summarize_group(part) if not part.empty else _summarize_group(part)
    return {
        "rows": int(s["rows"]),
        "resolved": int(s["resolved"]),
        "wins": int(s["wins"]),
        "losses": int(s["losses"]),
        "pushes": int(s["pushes"]),
        "win_rate": float(s["win_rate"]) if pd.notna(s["win_rate"]) else None,
        "units": float(s["units"]),
        "roi": float(s["roi"]) if pd.notna(s["roi"]) else None,
    }


def _fmt_pct(value: float | None) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{value:.1%}"


def _fmt_num(value: float | None) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{value:.3f}"


def _roi_by_team_correlation(df: pd.DataFrame) -> tuple[float | None, int]:
    resolved = df[df["resolved"] & df["stability_score"].notna()].copy()
    if resolved.empty:
        return None, 0
    grouped = (
        resolved.groupby(["environment_team", "row_type", "source_category"], dropna=False)
        .agg(units=("units_num", "sum"), resolved=("resolved", "size"), stability_score=("stability_score", "mean"))
        .reset_index()
    )
    grouped["roi"] = grouped["units"] / grouped["resolved"].replace(0, np.nan)
    valid = grouped[["roi", "stability_score"]].dropna()
    if len(valid) < 3:
        return None, int(len(valid))
    return float(valid["stability_score"].corr(valid["roi"])), int(len(valid))


def _write_markdown(
    path: Path,
    detail: pd.DataFrame,
    summary: pd.DataFrame,
    inputs: dict[str, Any],
    freshness: dict[str, Any],
) -> None:
    actual = detail[detail["row_type"].eq("actual_wager")]
    uploads = detail[detail["row_type"].eq("upload_not_wagered")]

    q1_actual_over = _subset_metric(
        actual,
        actual["source_category"].eq("v2_ranking") & actual["environment_regime"].eq("recurring_overperformer"),
    )
    q1_actual_other = _subset_metric(
        actual,
        actual["source_category"].eq("v2_ranking") & actual["environment_regime"].ne("recurring_overperformer"),
    )
    q1_upload_over = _subset_metric(
        uploads,
        uploads["source_category"].eq("v2_ranking") & uploads["environment_regime"].eq("recurring_overperformer"),
    )
    q1_upload_other = _subset_metric(
        uploads,
        uploads["source_category"].eq("v2_ranking") & uploads["environment_regime"].ne("recurring_overperformer"),
    )

    q2_under_underperf = _subset_metric(detail, detail["side_key"].eq("under") & detail["environment_regime"].eq("recurring_underperformer"))
    q2_under_other = _subset_metric(detail, detail["side_key"].eq("under") & detail["environment_regime"].ne("recurring_underperformer"))

    corr, corr_n = _roi_by_team_correlation(detail)

    fav_losses = detail[detail["favorite_loss"] & detail["resolved"]]
    hostile_fav_losses = int(fav_losses["environment_alignment"].eq("hostile_environment").sum())
    fav_loss_total = int(len(fav_losses))
    hostile_fav_loss_share = _rate(hostile_fav_losses, fav_loss_total)
    hostile_all = detail[detail["environment_alignment"].eq("hostile_environment")]
    non_hostile_all = detail[detail["environment_alignment"].ne("hostile_environment")]
    hostile_fav = _summarize_group(hostile_all)
    non_hostile_fav = _summarize_group(non_hostile_all)

    unmatched_env = int(detail["environment_regime"].eq("unclassified").sum())
    resolved_n = int(detail["resolved"].sum())
    lines = [
        "# MLB v2 Environment Regime Interactions",
        "",
        "Descriptive interaction analysis only. No predictive model or optimization was built.",
        "",
        "## Inputs",
        f"- Regimes: `{inputs['regimes_csv']}`",
        f"- Reconcile root: `{inputs['reconcile_root']}`",
        f"- Odds history root: `{inputs['odds_history_root']}`",
        "",
        "## Coverage",
        f"- Reconcile rows analyzed: `{len(detail)}`",
        f"- Resolved rows: `{resolved_n}`",
        f"- Actual wager rows: `{len(actual)}`",
        f"- Upload-not-wagered result rows: `{len(uploads)}`",
        f"- Rows without an offense regime match: `{unmatched_env}`",
        "",
        "## Freshness",
        f"- Latest actual_wagers_by_source date found: `{freshness.get('latest_actual_wagers_by_source_date_found') or 'none'}`",
        f"- Latest interaction date included: `{freshness.get('latest_interaction_date_included') or 'none'}`",
        f"- Total rows loaded: `{freshness.get('total_rows_loaded', 0)}`",
        (
            f"- Warning: {freshness['warning']}"
            if freshness.get("warning")
            else "- Status: interaction analysis is current with available reconcile dates."
        ),
        "",
        "## Questions",
        "",
        "### 1. v2 ranking in recurring overperformer environments",
        f"- Actual wagers in recurring overperformers: `{q1_actual_over['resolved']}` resolved, ROI `{_fmt_num(q1_actual_over['roi'])}`, win rate `{_fmt_pct(q1_actual_over['win_rate'])}`, units `{q1_actual_over['units']:.3f}`",
        f"- Actual wagers elsewhere: `{q1_actual_other['resolved']}` resolved, ROI `{_fmt_num(q1_actual_other['roi'])}`, win rate `{_fmt_pct(q1_actual_other['win_rate'])}`, units `{q1_actual_other['units']:.3f}`",
        f"- Ranking upload rows in recurring overperformers: `{q1_upload_over['resolved']}` resolved, ROI `{_fmt_num(q1_upload_over['roi'])}`, win rate `{_fmt_pct(q1_upload_over['win_rate'])}`, units `{q1_upload_over['units']:.3f}`",
        f"- Ranking upload rows elsewhere: `{q1_upload_other['resolved']}` resolved, ROI `{_fmt_num(q1_upload_other['roi'])}`, win rate `{_fmt_pct(q1_upload_other['win_rate'])}`, units `{q1_upload_other['units']:.3f}`",
        "",
        "### 2. UNDER plays in recurring underperformer environments",
        f"- UNDER rows in recurring underperformers: `{q2_under_underperf['resolved']}` resolved, ROI `{_fmt_num(q2_under_underperf['roi'])}`, win rate `{_fmt_pct(q2_under_underperf['win_rate'])}`, units `{q2_under_underperf['units']:.3f}`",
        f"- UNDER rows elsewhere: `{q2_under_other['resolved']}` resolved, ROI `{_fmt_num(q2_under_other['roi'])}`, win rate `{_fmt_pct(q2_under_other['win_rate'])}`, units `{q2_under_other['units']:.3f}`",
        "",
        "### 3. Environment stability and ROI",
        f"- Team/source-level stability-score to ROI correlation: `{_fmt_num(corr)}` over `{corr_n}` grouped observations",
        "",
        "### 4. Favorite losses in hostile environments",
        f"- Favorite losses in hostile environments: `{hostile_fav_losses}` of `{fav_loss_total}` (`{_fmt_pct(hostile_fav_loss_share)}`)",
        f"- Hostile bucket favorite loss rate: `{_fmt_pct(float(hostile_fav['favorite_loss_rate']) if pd.notna(hostile_fav['favorite_loss_rate']) else None)}`",
        f"- Non-hostile bucket favorite loss rate: `{_fmt_pct(float(non_hostile_fav['favorite_loss_rate']) if pd.notna(non_hostile_fav['favorite_loss_rate']) else None)}`",
        "",
        "## Output",
        "- `v2_by_environment_regime.csv`",
        "- `v2_environment_interaction_rows.csv`",
        "",
        "## Top Buckets",
    ]
    if summary.empty:
        lines.append("- No grouped rows available.")
    else:
        ranked = summary[summary["resolved"].gt(0)].sort_values(["row_type", "source_category", "resolved"], ascending=[True, True, False]).head(12)
        for _, row in ranked.iterrows():
            lines.append(
                f"- `{row['row_type']}` `{row['source_category']}` `{row['side_key']}` "
                f"`{row['environment_regime']}` `{row['environment_alignment']}`: "
                f"resolved `{int(row['resolved'])}`, ROI `{_fmt_num(row['roi'])}`, "
                f"win rate `{_fmt_pct(row['win_rate'])}`, units `{row['units']:.3f}`"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--regimes-csv", default="artifacts/analysis/mlb/hits_environment_persistence/recurring_team_environment_regimes.csv")
    ap.add_argument("--reconcile-root", default="backend/mlb/exports/model_v2/reconcile")
    ap.add_argument("--odds-history-root", default="backend/mlb/exports/odds_history")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/v2_environment_interactions")
    args = ap.parse_args()

    regimes_csv = Path(args.regimes_csv)
    reconcile_root = Path(args.reconcile_root)
    odds_history_root = Path(args.odds_history_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    wagers = _load_reconcile_rows(reconcile_root)
    regimes = _load_regimes(regimes_csv)
    dates = {d for d in wagers.get("date", pd.Series(dtype=str)).dropna().astype(str).tolist() if d}
    wide = _load_wide_lookup(odds_history_root, dates)
    detail = _enrich(wagers, regimes, wide)
    summary = _summary_table(detail)
    freshness = _build_freshness(reconcile_root, detail, len(wagers))

    summary_csv = out_dir / "v2_by_environment_regime.csv"
    detail_csv = out_dir / "v2_environment_interaction_rows.csv"
    summary_md = out_dir / "summary.md"
    summary_json = out_dir / "summary.json"

    summary.to_csv(summary_csv, index=False)
    detail.to_csv(detail_csv, index=False)
    metadata = {
        "inputs": {
            "regimes_csv": str(regimes_csv),
            "reconcile_root": str(reconcile_root),
            "odds_history_root": str(odds_history_root),
        },
        "outputs": {
            "v2_by_environment_regime_csv": str(summary_csv),
            "v2_environment_interaction_rows_csv": str(detail_csv),
            "summary_md": str(summary_md),
        },
        "counts": {
            "reconcile_rows": int(len(wagers)),
            "wide_lookup_rows": int(len(wide)),
            "detail_rows": int(len(detail)),
            "summary_rows": int(len(summary)),
            "resolved_rows": int(detail["resolved"].sum()) if "resolved" in detail else 0,
        },
        "freshness": freshness,
    }
    summary_json.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    _write_markdown(summary_md, detail, summary, metadata["inputs"], freshness)
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
