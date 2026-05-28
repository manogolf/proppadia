#!/usr/bin/env python3
"""Descriptive persistence analysis for MLB hits environment artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _as_float(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(parsed) if pd.notna(parsed) else np.nan


def _as_int(value: Any) -> int:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return int(parsed) if pd.notna(parsed) else 0


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(json.loads(text))
    return rows


def _load_daily_history(path: Path) -> pd.DataFrame:
    rows = []
    for payload in _read_jsonl(path):
        league_env = payload.get("league_hits_environment") or {}
        league = league_env.get("today_vs_baseline") or {}
        baseline = league_env.get("baseline") or {}
        trend = league_env.get("recent_trend") or {}
        starter = payload.get("starter_hits_allowed_residual") or {}
        weighted = starter.get("weighted_baseline") or {}
        team_eval = payload.get("team_hits_allowed_matchup_evaluation") or {}
        rows.append(
            {
                "evaluation_date": payload.get("evaluation_date"),
                "requested_as_of_date": payload.get("requested_as_of_date"),
                "generated_at_utc": payload.get("generated_at_utc"),
                "status": payload.get("status"),
                "ok": bool(payload.get("ok")),
                "league_signal": league.get("signal"),
                "league_hits_per_game": _as_float(league.get("hits_per_game")),
                "league_zscore": _as_float(league.get("zscore")),
                "league_percentile": _as_float(league.get("percentile")),
                "baseline_rows": _as_int(baseline.get("rows")),
                "baseline_hits_per_game_mean": _as_float(baseline.get("mean_hits_per_game")),
                "baseline_hits_per_game_std": _as_float(baseline.get("std_hits_per_game")),
                "recent_mean_hits_per_game": _as_float(trend.get("recent_mean_hits_per_game")),
                "prior_recent_mean_hits_per_game": _as_float(trend.get("prior_recent_mean_hits_per_game")),
                "recent_delta_hits_per_game": _as_float(trend.get("delta_recent_minus_prior")),
                "starter_rows": _as_int(starter.get("rows")),
                "starter_actual_hits_allowed_avg": _as_float(starter.get("actual_hits_allowed_avg")),
                "starter_residual_vs_d7_avg": _as_float(starter.get("residual_vs_d7_avg")),
                "starter_residual_vs_weighted_avg": _as_float(weighted.get("residual_vs_weighted_avg")),
                "team_eval_context_as_of_date": team_eval.get("context_as_of_date"),
                "team_eval_rows_with_expected": _as_int(team_eval.get("rows_with_expected")),
                "team_eval_rows_with_actual": _as_int(team_eval.get("rows_with_actual")),
                "team_eval_coverage_pct": _as_float(team_eval.get("coverage_pct")),
                "team_eval_expected_avg": _as_float(team_eval.get("expected_team_hits_allowed_avg")),
                "team_eval_actual_avg": _as_float(team_eval.get("actual_offense_hits_avg")),
                "team_eval_residual_avg": _as_float(team_eval.get("residual_avg")),
                "team_eval_residual_total": _as_float(team_eval.get("residual_total")),
                "team_eval_mae": _as_float(team_eval.get("mae")),
                "team_eval_rmse": _as_float(team_eval.get("rmse")),
                "team_eval_starter_only_residual_avg": _as_float(team_eval.get("starter_only_residual_avg")),
                "team_eval_starter_only_residual_total": _as_float(team_eval.get("starter_only_residual_total")),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["evaluation_date"] = pd.to_datetime(df["evaluation_date"], errors="coerce")
    df = df.sort_values(["evaluation_date", "generated_at_utc"]).drop_duplicates("evaluation_date", keep="last")
    df["evaluation_date"] = df["evaluation_date"].dt.date.astype(str)
    df["environment_overperformance_score"] = df["team_eval_residual_avg"] / df["team_eval_rmse"].replace(0, np.nan)
    df["league_heat_score"] = df["league_zscore"]
    df["expected_actual_gap"] = df["team_eval_actual_avg"] - df["team_eval_expected_avg"]
    return df


def _load_tracker(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _rolling_stability(daily: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    if daily.empty:
        return daily
    df = daily.copy()
    df["evaluation_date_dt"] = pd.to_datetime(df["evaluation_date"], errors="coerce")
    df = df.sort_values("evaluation_date_dt")
    numeric_cols = [
        "league_hits_per_game",
        "league_zscore",
        "recent_delta_hits_per_game",
        "team_eval_residual_avg",
        "team_eval_mae",
        "team_eval_rmse",
        "environment_overperformance_score",
    ]
    out = pd.DataFrame({"evaluation_date": df["evaluation_date"]})
    for col in numeric_cols:
        if col not in df:
            continue
        roll = pd.to_numeric(df[col], errors="coerce").rolling(window=window, min_periods=3)
        out[f"{col}_roll7_mean"] = roll.mean()
        out[f"{col}_roll7_std"] = roll.std()
    residual = pd.to_numeric(df["team_eval_residual_avg"], errors="coerce")
    sign = np.sign(residual)
    out["team_eval_residual_sign"] = sign
    out["team_eval_residual_abs_roll7_mean"] = residual.abs().rolling(window=window, min_periods=3).mean()
    out["team_eval_residual_sign_flip_roll7"] = (
        pd.Series(sign).diff().ne(0).astype(float).rolling(window=window, min_periods=3).sum().values
    )
    out["league_signal"] = df["league_signal"].values
    out["league_signal_changes_roll7"] = (
        df["league_signal"].astype(str).ne(df["league_signal"].astype(str).shift()).astype(float).rolling(window=window, min_periods=3).sum()
    )
    return out


def _extract_matchup_residuals(history_path: Path) -> pd.DataFrame:
    rows = []
    for payload in _read_jsonl(history_path):
        eval_date = payload.get("evaluation_date")
        generated_at = payload.get("generated_at_utc")
        team_eval = payload.get("team_hits_allowed_matchup_evaluation") or {}
        groups = [
            ("persisted_top_over_slice", team_eval.get("top_over_expected_matchups") or []),
            ("persisted_top_under_slice", team_eval.get("top_under_expected_matchups") or []),
        ]
        for direction, items in groups:
            for item in items:
                row = dict(item)
                row["evaluation_date"] = eval_date
                row["generated_at_utc"] = generated_at
                row["persisted_slice"] = direction
                rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["evaluation_date", "generated_at_utc"]).drop_duplicates(
        ["evaluation_date", "game_id", "player_id"], keep="last"
    )
    df["residual"] = pd.to_numeric(df.get("residual_actual_minus_expected_team"), errors="coerce")
    df["abs_residual"] = df["residual"].abs()
    df["overperformance_score"] = df["residual"]
    df["direction"] = np.where(df["residual"].ge(0), "actual_over_expected", "actual_under_expected")
    return df


def _team_rankings(matchups: pd.DataFrame, latest_rows: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if not matchups.empty:
        for team_col, role in [("pitcher_team", "pitcher_team_allowed"), ("offense_team", "offense_actual")]:
            g = matchups.groupby(team_col, dropna=False)
            frames.append(
                g.agg(
                    team_top_residual_appearances=("residual", "size"),
                    residual_avg=("residual", "mean"),
                    residual_total=("residual", "sum"),
                    abs_residual_avg=("abs_residual", "mean"),
                    over_expected_appearances=("direction", lambda s: int((s == "actual_over_expected").sum())),
                    under_expected_appearances=("direction", lambda s: int((s == "actual_under_expected").sum())),
                )
                .reset_index()
                .rename(columns={team_col: "team"})
                .assign(role=role)
            )
    if not latest_rows.empty:
        latest_team = latest_rows.groupby("offense_team", dropna=False).agg(
            latest_slate_rows=("offense_team", "size"),
            latest_expected_team_hits_allowed_avg=("expected_team_hits_allowed_matchup", "mean"),
            latest_expected_hits_allowed_matchup_avg=("expected_hits_allowed_matchup", "mean"),
            latest_offense_factor_avg=("offense_factor_vs_league_clamped", "mean"),
            latest_line_minus_expected_avg=("line_minus_expected_hits_allowed_matchup", "mean"),
        ).reset_index().rename(columns={"offense_team": "team"})
        latest_team["role"] = "latest_slate_offense_context"
        frames.append(latest_team)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    if "residual_total" in out:
        out["environment_overperformance_score"] = pd.to_numeric(out["residual_total"], errors="coerce")
    out["environment_overperformance_score"] = out["environment_overperformance_score"].fillna(
        pd.to_numeric(out.get("latest_expected_team_hits_allowed_avg"), errors="coerce")
    ).fillna(0)
    role_order = {
        "offense_actual": 1,
        "pitcher_team_allowed": 2,
        "latest_slate_offense_context": 3,
    }
    out["role_order"] = out["role"].map(role_order).fillna(99)
    return out.sort_values(["role_order", "environment_overperformance_score"], ascending=[True, False]).drop(columns=["role_order"])


def _max_streak(signs: pd.Series, target: int) -> int:
    best = 0
    cur = 0
    for val in signs:
        if val == target:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _current_streak(signs: pd.Series) -> int:
    clean = [int(v) for v in signs if pd.notna(v) and int(v) != 0]
    if not clean:
        return 0
    target = clean[-1]
    count = 0
    for val in reversed(clean):
        if val == target:
            count += 1
        else:
            break
    return int(count * target)


def _recurring_team_environment(matchups: pd.DataFrame) -> pd.DataFrame:
    if matchups.empty:
        return pd.DataFrame()
    frames = []
    work = matchups.copy()
    work["evaluation_date_dt"] = pd.to_datetime(work["evaluation_date"], errors="coerce")
    work["residual_sign"] = np.sign(pd.to_numeric(work["residual"], errors="coerce")).fillna(0).astype(int)
    for team_col, role in [("offense_team", "offense_environment"), ("pitcher_team", "pitcher_staff_environment")]:
        rows = []
        for team, group in work.dropna(subset=[team_col]).groupby(team_col):
            g = group.sort_values("evaluation_date_dt").copy()
            residual = pd.to_numeric(g["residual"], errors="coerce")
            dates = g["evaluation_date_dt"].dropna()
            signs = g["residual_sign"]
            appearances = int(len(g))
            over_n = int((residual > 0).sum())
            under_n = int((residual < 0).sum())
            same_side_rate = max(over_n, under_n) / appearances if appearances else np.nan
            residual_avg = float(residual.mean()) if residual.notna().any() else np.nan
            residual_total = float(residual.sum()) if residual.notna().any() else np.nan
            residual_std = float(residual.std(ddof=0)) if residual.notna().sum() > 1 else 0.0
            abs_residual_avg = float(residual.abs().mean()) if residual.notna().any() else np.nan
            volatility_score = residual_std
            stability_score = (same_side_rate * np.sqrt(appearances)) / (1.0 + volatility_score) if appearances else np.nan
            rows.append(
                {
                    "team": team,
                    "role": role,
                    "appearances": appearances,
                    "first_seen": dates.min().date().isoformat() if not dates.empty else "",
                    "last_seen": dates.max().date().isoformat() if not dates.empty else "",
                    "persistence_duration_days": int((dates.max() - dates.min()).days) + 1 if len(dates) else 0,
                    "over_expected_count": over_n,
                    "under_expected_count": under_n,
                    "same_side_rate": same_side_rate,
                    "residual_avg": residual_avg,
                    "residual_total": residual_total,
                    "abs_residual_avg": abs_residual_avg,
                    "environment_volatility": volatility_score,
                    "max_over_streak": _max_streak(signs, 1),
                    "max_under_streak": _max_streak(signs, -1),
                    "current_signed_streak": _current_streak(signs),
                    "stability_score": stability_score,
                    "regime": (
                        "recurring_overperformer"
                        if residual_avg > 0 and same_side_rate >= 0.60
                        else "recurring_underperformer"
                        if residual_avg < 0 and same_side_rate >= 0.60
                        else "volatile_or_mixed"
                    ),
                }
            )
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame["rolling_stability_rank"] = frame["stability_score"].rank(ascending=False, method="dense").astype("Int64")
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out.sort_values(["role", "rolling_stability_rank", "abs_residual_avg"], ascending=[True, True, False])


def _latest_slate_rows(path: Path | None = None) -> pd.DataFrame:
    path = path or Path("tmp/analysis/mlb_hits_environment_hits_allowed_rows.csv")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in [
        "expected_team_hits_allowed_matchup",
        "expected_hits_allowed_matchup",
        "offense_factor_vs_league_clamped",
        "line_minus_expected_hits_allowed_matchup",
        "model_pick_prob",
    ]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _write_markdown(
    path: Path,
    daily: pd.DataFrame,
    rolling: pd.DataFrame,
    teams: pd.DataFrame,
    matchups: pd.DataFrame,
    recurring: pd.DataFrame,
) -> None:
    latest = daily.tail(1).iloc[0].to_dict() if not daily.empty else {}
    lines = [
        "# MLB Hits Environment Persistence",
        "",
        "Descriptive signal-quality pass only. No predictive model was built.",
        "",
        "## Latest Daily Read",
        f"- Evaluation date: `{latest.get('evaluation_date', 'n/a')}`",
        f"- League signal: `{latest.get('league_signal', 'n/a')}`; hits/game `{latest.get('league_hits_per_game', 'n/a')}`; z `{latest.get('league_zscore', 'n/a')}`",
        f"- Team expected avg `{latest.get('team_eval_expected_avg', 'n/a')}` vs actual avg `{latest.get('team_eval_actual_avg', 'n/a')}`",
        f"- Residual avg `{latest.get('team_eval_residual_avg', 'n/a')}`; MAE `{latest.get('team_eval_mae', 'n/a')}`; RMSE `{latest.get('team_eval_rmse', 'n/a')}`",
        "",
        "## Outputs",
        "- `daily_persistence_table.csv`",
        "- `rolling_7day_stability.csv`",
        "- `team_rankings.csv`",
        "- `environment_overperformance_scores.csv`",
        "- `hits_allowed_over_underperformance.csv`",
        "- `recurring_team_environment_regimes.csv`",
        "",
        "## Notes",
        f"- Daily rows: `{len(daily)}`",
        f"- Rolling rows: `{len(rolling)}`",
        f"- Team ranking rows: `{len(teams)}`",
        f"- Top matchup residual rows from history: `{len(matchups)}`",
        f"- Recurring team regime rows: `{len(recurring)}`",
        "- Historical team-level rankings use persisted top over/under expected matchup slices, not a full historical matchup table.",
    ]
    if not recurring.empty:
        over = recurring[recurring["regime"].eq("recurring_overperformer")].sort_values(
            ["stability_score", "residual_avg"], ascending=[False, False]
        ).head(8)
        under = recurring[recurring["regime"].eq("recurring_underperformer")].sort_values(
            ["stability_score", "residual_avg"], ascending=[False, True]
        ).head(8)
        lines.extend(["", "## Recurring Overperformers"])
        for _, row in over.iterrows():
            lines.append(
                f"- `{row['team']}` ({row['role']}): avg `{row['residual_avg']:.2f}`, "
                f"appearances `{int(row['appearances'])}`, duration `{int(row['persistence_duration_days'])}d`, "
                f"vol `{row['environment_volatility']:.2f}`, stability rank `{row['rolling_stability_rank']}`"
            )
        lines.extend(["", "## Recurring Underperformers"])
        for _, row in under.iterrows():
            lines.append(
                f"- `{row['team']}` ({row['role']}): avg `{row['residual_avg']:.2f}`, "
                f"appearances `{int(row['appearances'])}`, duration `{int(row['persistence_duration_days'])}d`, "
                f"vol `{row['environment_volatility']:.2f}`, stability rank `{row['rolling_stability_rank']}`"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--history-jsonl", default="artifacts/analysis/mlb/mlb_hits_environment_history.jsonl")
    ap.add_argument("--team-eval-tracker-csv", default="artifacts/analysis/mlb/mlb_hits_environment_team_eval_daily_tracker.csv")
    ap.add_argument("--hits-allowed-rows-csv", default="tmp/analysis/mlb_hits_environment_hits_allowed_rows.csv")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/hits_environment_persistence")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    daily = _load_daily_history(Path(args.history_jsonl))
    tracker = _load_tracker(Path(args.team_eval_tracker_csv))
    if not tracker.empty and not daily.empty:
        daily = daily.merge(
            tracker[["evaluation_date", "eval_snapshot_slate_csv", "eval_snapshot_wide_csv"]],
            on="evaluation_date",
            how="left",
        )
    rolling = _rolling_stability(daily)
    matchups = _extract_matchup_residuals(Path(args.history_jsonl))
    latest_rows = _latest_slate_rows(Path(args.hits_allowed_rows_csv))
    teams = _team_rankings(matchups, latest_rows)
    recurring = _recurring_team_environment(matchups)

    env_scores = daily[
        [
            "evaluation_date",
            "league_signal",
            "league_hits_per_game",
            "league_zscore",
            "recent_delta_hits_per_game",
            "team_eval_expected_avg",
            "team_eval_actual_avg",
            "team_eval_residual_avg",
            "team_eval_mae",
            "team_eval_rmse",
            "environment_overperformance_score",
        ]
    ].copy() if not daily.empty else pd.DataFrame()

    daily.to_csv(out_dir / "daily_persistence_table.csv", index=False)
    rolling.to_csv(out_dir / "rolling_7day_stability.csv", index=False)
    teams.to_csv(out_dir / "team_rankings.csv", index=False)
    env_scores.to_csv(out_dir / "environment_overperformance_scores.csv", index=False)
    matchups.to_csv(out_dir / "hits_allowed_over_underperformance.csv", index=False)
    recurring.to_csv(out_dir / "recurring_team_environment_regimes.csv", index=False)

    summary = {
        "inputs": {
            "history_jsonl": args.history_jsonl,
            "team_eval_tracker_csv": args.team_eval_tracker_csv,
            "hits_allowed_rows_csv": args.hits_allowed_rows_csv,
        },
        "outputs": {
            "daily_persistence_table_csv": str(out_dir / "daily_persistence_table.csv"),
            "rolling_7day_stability_csv": str(out_dir / "rolling_7day_stability.csv"),
            "team_rankings_csv": str(out_dir / "team_rankings.csv"),
            "environment_overperformance_scores_csv": str(out_dir / "environment_overperformance_scores.csv"),
            "hits_allowed_over_underperformance_csv": str(out_dir / "hits_allowed_over_underperformance.csv"),
            "recurring_team_environment_regimes_csv": str(out_dir / "recurring_team_environment_regimes.csv"),
            "summary_md": str(out_dir / "summary.md"),
        },
        "counts": {
            "daily_rows": int(len(daily)),
            "rolling_rows": int(len(rolling)),
            "team_ranking_rows": int(len(teams)),
            "matchup_residual_rows": int(len(matchups)),
            "recurring_team_regime_rows": int(len(recurring)),
        },
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    _write_markdown(out_dir / "summary.md", daily, rolling, teams, matchups, recurring)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
