#!/usr/bin/env python3
"""Describe v2 favorite performance by hits environment detail."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


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

MARKET_TO_PROP = {
    "batter_hits": "hits",
    "batter_runs": "runs_scored",
    "batter_rbis": "rbis",
    "batter_bases": "total_bases",
    "batter_total_bases": "total_bases",
    "batter_h+r+rbi": "hits_runs_rbis",
    "batter_hits_runs_rbis": "hits_runs_rbis",
    "batter_walks": "walks",
    "batter_strikeouts": "strikeouts_batting",
    "pitcher_hits": "hits_allowed",
    "pitcher_hits_allowed": "hits_allowed",
    "pitcher_strikeouts": "strikeouts_pitching",
    "pitcher_outs": "outs_recorded",
    "outs_recorded": "outs_recorded",
}


def _norm_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value).strip()


def _norm_team(value: Any) -> str:
    raw = _norm_text(value).upper()
    return TEAM_ALIASES.get(raw, raw)


def _norm_side(value: Any) -> str:
    raw = _norm_text(value).lower()
    if raw in {"o", "over"}:
        return "over"
    if raw in {"u", "under"}:
        return "under"
    return raw


def _norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _norm_text(value).lower()).strip()


def _date_key(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 8:
        dt = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(raw, errors="coerce")
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


def _line(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return round(float(parsed), 4) if pd.notna(parsed) else np.nan


def _price_bucket(price: Any) -> str:
    val = pd.to_numeric(pd.Series([price]), errors="coerce").iloc[0]
    if pd.isna(val):
        return "unknown"
    val = float(val)
    if -149 <= val < 0:
        return "-100_to_-149"
    if -199 <= val <= -150:
        return "-150_to_-199"
    if -249 <= val <= -200:
        return "-200_to_-249"
    if val <= -250:
        return "-250_or_worse"
    return "plus_money_or_even"


def _read_interaction_rows(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    df["date"] = df["date"].map(_date_key)
    df["player_id_key"] = df.get("player_id", pd.Series(index=df.index)).map(_id_text)
    df["player_name_key"] = df.get("player_name", pd.Series(index=df.index)).map(_norm_name)
    df["prop_type_key"] = df.get("prop_type", pd.Series(index=df.index)).map(lambda v: _norm_text(v).lower())
    df["side_key"] = df.get("side", pd.Series(index=df.index)).map(_norm_side)
    df["home_key"] = df.get("home", pd.Series(index=df.index)).map(_norm_team)
    df["away_key"] = df.get("away", pd.Series(index=df.index)).map(_norm_team)
    df["line_key"] = df.get("line", pd.Series(index=df.index)).map(_line)
    df["price_num"] = pd.to_numeric(df.get("price_num", df.get("price")), errors="coerce")
    df["units_num"] = pd.to_numeric(df.get("units_num", df.get("units")), errors="coerce")
    df["result_key"] = df.get("result_key", df.get("result", pd.Series(index=df.index))).map(lambda v: _norm_text(v).lower())
    df["resolved"] = df["result_key"].isin(["win", "loss", "push"])
    return df


def _load_regimes(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["team_key"] = df["team"].map(_norm_team)
    return df


def _attach_pitcher_staff_regime(df: pd.DataFrame, regimes: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    staff = regimes[regimes["role"].eq("pitcher_staff_environment")].copy()
    if staff.empty:
        for col in [
            "opponent_pitcher_staff_regime",
            "opponent_pitcher_staff_residual_avg",
            "opponent_pitcher_staff_stability_score",
            "opponent_pitcher_staff_volatility",
        ]:
            out[col] = np.nan
        return out
    staff = staff.rename(
        columns={
            "team_key": "opponent_team_key",
            "regime": "opponent_pitcher_staff_regime",
            "residual_avg": "opponent_pitcher_staff_residual_avg",
            "stability_score": "opponent_pitcher_staff_stability_score",
            "environment_volatility": "opponent_pitcher_staff_volatility",
            "rolling_stability_rank": "opponent_pitcher_staff_stability_rank",
        }
    )
    cols = [
        "opponent_team_key",
        "opponent_pitcher_staff_regime",
        "opponent_pitcher_staff_residual_avg",
        "opponent_pitcher_staff_stability_score",
        "opponent_pitcher_staff_volatility",
        "opponent_pitcher_staff_stability_rank",
    ]
    out["opponent_team_key"] = out.get("opponent_team", pd.Series(index=out.index)).map(_norm_team)
    out = out.merge(staff[cols], on="opponent_team_key", how="left")
    out["opponent_pitcher_staff_regime"] = out["opponent_pitcher_staff_regime"].fillna("unclassified")
    return out


def _load_reconcile_probs(root: Path, dates: set[str]) -> pd.DataFrame:
    frames = []
    for date in sorted(dates):
        path = root / date / "reconcile_rows.csv"
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty:
            continue
        df["date"] = df.get("game_date", df.get("slate_date", date)).map(_date_key)
        df["player_id_key"] = df.get("player_id", pd.Series(index=df.index)).map(_id_text)
        df["player_name_key"] = df.get("player_name", pd.Series(index=df.index)).map(_norm_name)
        df["prop_type_key"] = df.get("prop_type", pd.Series(index=df.index)).map(lambda v: _norm_text(v).lower())
        df["line_key"] = df.get("line", pd.Series(index=df.index)).map(_line)
        df["home_key"] = df.get("home_team_code", pd.Series(index=df.index)).map(_norm_team)
        df["away_key"] = df.get("away_team_code", pd.Series(index=df.index)).map(_norm_team)
        df["model_prob_over_num"] = pd.to_numeric(df.get("model_prob_over"), errors="coerce")
        df["model_prob_under_num"] = pd.to_numeric(df.get("model_prob_under"), errors="coerce")
        df["implied_over_num"] = pd.to_numeric(df.get("implied_over_novig", df.get("implied_over")), errors="coerce")
        df["implied_under_num"] = pd.to_numeric(df.get("implied_under_novig", df.get("implied_under")), errors="coerce")
        keep = [
            "date",
            "player_id_key",
            "prop_type_key",
            "line_key",
            "home_key",
            "away_key",
            "model_prob_over_num",
            "model_prob_under_num",
            "implied_over_num",
            "implied_under_num",
        ]
        frames.append(df[keep])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out.drop_duplicates(["date", "player_id_key", "prop_type_key", "line_key", "home_key", "away_key"], keep="last")


def _attach_reconcile_probs(df: pd.DataFrame, probs: pd.DataFrame) -> pd.DataFrame:
    if df.empty or probs.empty:
        out = df.copy()
        out["model_probability"] = np.nan
        out["market_implied_probability"] = np.nan
        return out
    keys = ["date", "player_id_key", "prop_type_key", "line_key", "home_key", "away_key"]
    out = df.merge(probs, on=keys, how="left")
    out["model_probability"] = np.where(
        out["side_key"].eq("over"),
        out["model_prob_over_num"],
        np.where(out["side_key"].eq("under"), out["model_prob_under_num"], np.nan),
    )
    out["market_implied_probability"] = np.where(
        out["side_key"].eq("over"),
        out["implied_over_num"],
        np.where(out["side_key"].eq("under"), out["implied_under_num"], np.nan),
    )
    return out


def _load_upload_probs(upload_root: Path, dates: set[str]) -> pd.DataFrame:
    frames = []
    for date in sorted(dates):
        day_root = upload_root / date
        if not day_root.exists():
            continue
        for path in sorted(day_root.glob("ranking_tool_upload_*.csv")):
            if "diagnostics" in path.name:
                continue
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            if df.empty or "WIN %" not in df:
                continue
            df["date"] = df.get("DATE", date).map(_date_key)
            df["player_id_key"] = df.get("SELECTOR", pd.Series(index=df.index)).map(_id_text)
            df["prop_type_key"] = df.get("MARKET", pd.Series(index=df.index)).map(
                lambda v: MARKET_TO_PROP.get(_norm_text(v).lower(), _norm_text(v).lower())
            )
            df["line_key"] = df.get("POINT", pd.Series(index=df.index)).map(_line)
            df["side_key"] = df.get("SIDE", pd.Series(index=df.index)).map(_norm_side)
            df["home_key"] = df.get("HOME", pd.Series(index=df.index)).map(_norm_team)
            df["away_key"] = df.get("AWAY", pd.Series(index=df.index)).map(_norm_team)
            df["upload_probability"] = pd.to_numeric(df.get("WIN %"), errors="coerce")
            df["upload_source_file"] = str(path)
            frames.append(
                df[
                    [
                        "date",
                        "player_id_key",
                        "prop_type_key",
                        "line_key",
                        "side_key",
                        "home_key",
                        "away_key",
                        "upload_probability",
                        "upload_source_file",
                    ]
                ]
            )
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out.drop_duplicates(["date", "player_id_key", "prop_type_key", "line_key", "side_key", "home_key", "away_key"], keep="last")


def _attach_upload_probs(df: pd.DataFrame, uploads: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if uploads.empty:
        out["upload_probability"] = np.nan
        return out
    keys = ["date", "player_id_key", "prop_type_key", "line_key", "side_key", "home_key", "away_key"]
    return out.merge(uploads, on=keys, how="left")


def _summarize(group: pd.DataFrame) -> pd.Series:
    bets = int(len(group))
    wins = int(group["result_key"].eq("win").sum())
    losses = int(group["result_key"].eq("loss").sum())
    pushes = int(group["result_key"].eq("push").sum())
    units = float(group["units_num"].fillna(0).sum())
    win_loss = wins + losses
    return pd.Series(
        {
            "bets": bets,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "win_rate": wins / win_loss if win_loss else np.nan,
            "roi": units / bets if bets else np.nan,
            "units": units,
            "avg_price": float(group["price_num"].mean()) if group["price_num"].notna().any() else np.nan,
            "avg_model_probability": (
                float(group["model_probability"].mean()) if group["model_probability"].notna().any() else np.nan
            ),
            "avg_upload_probability": (
                float(group["upload_probability"].mean()) if group["upload_probability"].notna().any() else np.nan
            ),
            "avg_market_implied_probability": (
                float(group["market_implied_probability"].mean())
                if group["market_implied_probability"].notna().any()
                else np.nan
            ),
            "avg_offense_stability_score": (
                float(group["stability_score"].mean()) if group["stability_score"].notna().any() else np.nan
            ),
            "avg_offense_residual": float(group["residual_avg"].mean()) if group["residual_avg"].notna().any() else np.nan,
            "avg_pitcher_staff_residual": (
                float(group["opponent_pitcher_staff_residual_avg"].mean())
                if group["opponent_pitcher_staff_residual_avg"].notna().any()
                else np.nan
            ),
        }
    )


def _breakdown(df: pd.DataFrame, name: str, cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.groupby(cols, dropna=False).apply(_summarize, include_groups=False).reset_index()
    out.insert(0, "breakdown", name)
    return out.sort_values(["breakdown", "roi", "bets"], ascending=[True, True, False])


def _fmt(value: float | None, digits: int = 3) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{value:.{digits}f}"


def _pct(value: float | None) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{value:.1%}"


def _metric(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"bets": 0, "wins": 0, "losses": 0, "pushes": 0, "win_rate": None, "roi": None, "units": 0.0}
    s = _summarize(df)
    return {
        "bets": int(s["bets"]),
        "wins": int(s["wins"]),
        "losses": int(s["losses"]),
        "pushes": int(s["pushes"]),
        "win_rate": float(s["win_rate"]) if pd.notna(s["win_rate"]) else None,
        "roi": float(s["roi"]) if pd.notna(s["roi"]) else None,
        "units": float(s["units"]),
    }


def _write_summary(path: Path, favorites: pd.DataFrame, breakdown: pd.DataFrame, metadata: dict[str, Any]) -> dict[str, Any]:
    actual = favorites[favorites["row_type"].eq("actual_wager")]
    all_hostile = favorites[favorites["environment_alignment"].eq("hostile_environment")]
    all_non_hostile = favorites[favorites["environment_alignment"].ne("hostile_environment")]
    actual_hostile = actual[actual["environment_alignment"].eq("hostile_environment")]
    actual_non_hostile = actual[actual["environment_alignment"].ne("hostile_environment")]

    recurring = breakdown[
        breakdown["breakdown"].eq("offense_environment_team")
        & breakdown["environment_regime"].isin(["recurring_overperformer", "recurring_underperformer"])
        & breakdown["bets"].ge(1)
    ].copy()
    worst = recurring.sort_values(["roi", "bets"], ascending=[True, False]).head(8)
    best = recurring.sort_values(["roi", "bets"], ascending=[False, False]).head(8)
    loss_teams = (
        favorites[favorites["result_key"].eq("loss")]
        .groupby(["environment_team", "environment_regime"], dropna=False)
        .size()
        .reset_index(name="favorite_losses")
        .sort_values(["favorite_losses", "environment_team"], ascending=[False, True])
        .head(10)
    )
    price_losses = (
        favorites.groupby(["price_bucket", "environment_alignment"], dropna=False)
        .apply(_summarize, include_groups=False)
        .reset_index()
        .sort_values(["losses", "bets"], ascending=[False, False])
    )

    payload = {
        **metadata,
        "focus_population": {
            "all_v2_favorites": _metric(favorites),
            "actual_wager_v2_favorites": _metric(actual),
            "all_hostile": _metric(all_hostile),
            "all_non_hostile": _metric(all_non_hostile),
            "actual_hostile": _metric(actual_hostile),
            "actual_non_hostile": _metric(actual_non_hostile),
        },
        "worst_recurring_environments_by_roi": worst.to_dict(orient="records"),
        "best_recurring_environments_by_roi": best.to_dict(orient="records"),
        "favorite_loss_teams": loss_teams.to_dict(orient="records"),
    }

    lines = [
        "# v2 Favorites Environment Breakdown",
        "",
        "Descriptive analysis only. No model logic, lane rules, upload generation, or production filters were changed.",
        "",
        "## Focus Population",
        f"- Rows: `{len(favorites)}` resolved v2 ranking favorites",
        f"- Actual wager rows: `{len(actual)}`",
        f"- Upload/result rows: `{len(favorites) - len(actual)}`",
        f"- Source rows: `{metadata['inputs']['interaction_rows_csv']}`",
        "",
        "## Hostile vs Non-Hostile",
        f"- All v2 favorites in hostile environments: `{payload['focus_population']['all_hostile']['bets']}` bets, ROI `{_fmt(payload['focus_population']['all_hostile']['roi'])}`, WR `{_pct(payload['focus_population']['all_hostile']['win_rate'])}`, units `{payload['focus_population']['all_hostile']['units']:.3f}`",
        f"- All v2 favorites in non-hostile environments: `{payload['focus_population']['all_non_hostile']['bets']}` bets, ROI `{_fmt(payload['focus_population']['all_non_hostile']['roi'])}`, WR `{_pct(payload['focus_population']['all_non_hostile']['win_rate'])}`, units `{payload['focus_population']['all_non_hostile']['units']:.3f}`",
        f"- Actual wagers hostile: `{payload['focus_population']['actual_hostile']['bets']}` bets, ROI `{_fmt(payload['focus_population']['actual_hostile']['roi'])}`, WR `{_pct(payload['focus_population']['actual_hostile']['win_rate'])}`, units `{payload['focus_population']['actual_hostile']['units']:.3f}`",
        f"- Actual wagers non-hostile: `{payload['focus_population']['actual_non_hostile']['bets']}` bets, ROI `{_fmt(payload['focus_population']['actual_non_hostile']['roi'])}`, WR `{_pct(payload['focus_population']['actual_non_hostile']['win_rate'])}`, units `{payload['focus_population']['actual_non_hostile']['units']:.3f}`",
        "",
        "## Worst Recurring Offense Environments",
    ]
    if worst.empty:
        lines.append("- No recurring offense environment rows found.")
    else:
        for _, row in worst.iterrows():
            lines.append(
                f"- `{row['environment_team']}` `{row['environment_regime']}`: `{int(row['bets'])}` bets, "
                f"ROI `{_fmt(row['roi'])}`, WR `{_pct(row['win_rate'])}`, units `{row['units']:.3f}`, avg price `{_fmt(row['avg_price'], 1)}`"
            )
    lines.extend(["", "## Best Recurring Offense Environments"])
    if best.empty:
        lines.append("- No recurring offense environment rows found.")
    else:
        for _, row in best.iterrows():
            lines.append(
                f"- `{row['environment_team']}` `{row['environment_regime']}`: `{int(row['bets'])}` bets, "
                f"ROI `{_fmt(row['roi'])}`, WR `{_pct(row['win_rate'])}`, units `{row['units']:.3f}`, avg price `{_fmt(row['avg_price'], 1)}`"
            )
    lines.extend(["", "## Teams Most Associated With Favorite Losses"])
    if loss_teams.empty:
        lines.append("- No favorite losses in the focus population.")
    else:
        for _, row in loss_teams.iterrows():
            lines.append(f"- `{row['environment_team']}` `{row['environment_regime']}`: `{int(row['favorite_losses'])}` losses")
    lines.extend(["", "## Loss Concentration By Price And Environment"])
    for _, row in price_losses.head(12).iterrows():
        lines.append(
            f"- `{row['price_bucket']}` `{row['environment_alignment']}`: `{int(row['losses'])}` losses, "
            f"`{int(row['bets'])}` bets, ROI `{_fmt(row['roi'])}`, WR `{_pct(row['win_rate'])}`"
        )
    lines.extend(
        [
            "",
            "## Interpretation Guardrail",
            "- This is a small-sample descriptive read. Treat strong-looking buckets as candidates for review, not production filters.",
            "- The hostile label is derived from current recurring offense regime alignment with the wager side, then checked against actual favorite outcomes.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--interaction-rows-csv",
        default="artifacts/analysis/mlb/v2_environment_interactions/v2_environment_interaction_rows.csv",
    )
    ap.add_argument("--regimes-csv", default="artifacts/analysis/mlb/hits_environment_persistence/recurring_team_environment_regimes.csv")
    ap.add_argument("--reconcile-rows-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--upload-root", default="backend/mlb/exports/model_v2/upload")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/hits_environment_persistence")
    args = ap.parse_args()

    interaction_path = Path(args.interaction_rows_csv)
    if not interaction_path.exists():
        fallback = Path("artifacts/analysis/mlb/hits_environment_persistence/v2_environment_interaction_rows.csv")
        if fallback.exists():
            interaction_path = fallback
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = _read_interaction_rows(interaction_path)
    regimes = _load_regimes(Path(args.regimes_csv))
    rows = _attach_pitcher_staff_regime(rows, regimes)
    dates = {d for d in rows["date"].dropna().astype(str).tolist() if d}
    rows = _attach_reconcile_probs(rows, _load_reconcile_probs(Path(args.reconcile_rows_root), dates))
    rows = _attach_upload_probs(rows, _load_upload_probs(Path(args.upload_root), dates))
    rows["price_bucket"] = rows["price_num"].map(_price_bucket)

    favorites = rows[
        rows["source_category"].eq("v2_ranking")
        & rows["resolved"]
        & rows["price_num"].lt(0)
    ].copy()

    breakdowns = [
        _breakdown(
            favorites,
            "offense_environment_team",
            ["row_type", "environment_team", "environment_regime", "environment_alignment"],
        ),
        _breakdown(
            favorites,
            "opponent_pitcher_staff_environment",
            ["row_type", "opponent_team_key", "opponent_pitcher_staff_regime", "environment_alignment"],
        ),
        _breakdown(favorites, "hostile_vs_non_hostile", ["row_type", "environment_alignment"]),
        _breakdown(favorites, "price_bucket", ["row_type", "price_bucket"]),
        _breakdown(favorites, "price_bucket_plus_offense_regime", ["row_type", "price_bucket", "environment_regime"]),
        _breakdown(favorites, "offense_regime", ["row_type", "environment_regime"]),
    ]
    breakdown = pd.concat([b for b in breakdowns if not b.empty], ignore_index=True, sort=False)

    out_csv = out_dir / "v2_favorites_environment_breakdown.csv"
    out_json = out_dir / "v2_favorites_environment_breakdown_summary.json"
    out_md = out_dir / "v2_favorites_environment_breakdown_summary.md"
    breakdown.to_csv(out_csv, index=False)

    metadata = {
        "inputs": {
            "interaction_rows_csv": str(interaction_path),
            "regimes_csv": args.regimes_csv,
            "reconcile_rows_root": args.reconcile_rows_root,
            "upload_root": args.upload_root,
        },
        "outputs": {
            "breakdown_csv": str(out_csv),
            "summary_json": str(out_json),
            "summary_md": str(out_md),
        },
        "counts": {
            "interaction_rows": int(len(rows)),
            "focus_rows": int(len(favorites)),
            "breakdown_rows": int(len(breakdown)),
        },
    }
    payload = _write_summary(out_md, favorites, breakdown, metadata)
    out_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
