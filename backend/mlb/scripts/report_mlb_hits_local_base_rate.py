#!/usr/bin/env python3
"""Diagnose local base-rate/intercept errors for hits line 0.5.

Holds rolling_result_avg_7 in mid-low buckets, then slices calibration by
available context dimensions. Diagnostics only; no ROI, no betting rules, no DB
writes, and no price explanation.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[2]))
try:
    from mlb.shared.team_name_map import getFullTeamAbbreviationFromID, normalizeTeamAbbreviation
except Exception:  # pragma: no cover - diagnostic fallback for unusual import contexts.
    getFullTeamAbbreviationFromID = None
    normalizeTeamAbbreviation = None


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_local_base_rate_surface.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_local_base_rate_summary.md")

RECENCY_BINS = [0.20, 0.30, 0.40, 0.50, 0.75]
RECENCY_LABELS = ["0.20-0.30", "0.30-0.40", "0.40-0.50", "0.50-0.75"]
HIT_BINS = [0.0, 0.25, 0.50, 0.75, 1.00, np.inf]
HIT_LABELS = ["0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]

OPTIONAL_MTP_COLUMNS = [
    "player_handedness",
    "player_hand",
    "batter_hand",
    "bats",
    "bat_side",
    "batting_order",
    "batting_order_spot",
    "lineup_spot",
    "starting_pitcher_hand",
    "opposing_pitcher_hand",
    "pitcher_hand",
    "pitcher_throws",
    "opposing_pitcher_throws",
    "starting_pitcher_throws",
    "starting_pitcher_id",
]
OPTIONAL_PDS_COLUMNS = [
    "player_handedness",
    "batter_hand",
    "bats",
    "bat_side",
    "batting_order",
    "batting_order_spot",
    "lineup_spot",
]
OPTIONAL_PFP_KEYS = OPTIONAL_MTP_COLUMNS + OPTIONAL_PDS_COLUMNS


def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise SystemExit("DATABASE_URL or SUPABASE_DB_URL must be set.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _discover_reconcile_files(root: Path, from_date: str, to_date: str) -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        if pd.isna(pd.to_datetime(date, errors="coerce")):
            continue
        if from_date and date < from_date:
            continue
        if to_date and date > to_date:
            continue
        files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_hits_reconcile(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    required = {
        "game_date",
        "game_id",
        "player_id",
        "prop_type",
        "line",
        "bookmaker_key",
        "home_team_code",
        "away_team_code",
        "model_prob_over",
        "model_prob_under",
        "actual_over_outcome",
        "actual_under_outcome",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-local-base-rate] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq("hits")].copy()
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible hits reconcile rows found.")
    return pd.concat(frames, ignore_index=True)


def _table_columns(engine, table: str) -> set[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'mlb'
          AND table_name = :table
        """
    )
    with engine.connect() as conn:
        return {str(r[0]) for r in conn.execute(sql, {"table": table}).fetchall()}


def _select_optional(alias: str, columns: Sequence[str], available: set[str]) -> list[str]:
    return [f"{alias}.{col} AS {alias}_{col}" for col in columns if col in available]


def _fetch_feature_context(engine, from_date: str, to_date: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    mt_cols = _table_columns(engine, "model_training_props")
    pds_cols = _table_columns(engine, "player_derived_stats")
    mt_optional = _select_optional("mt", OPTIONAL_MTP_COLUMNS, mt_cols)
    pds_optional = _select_optional("pds", OPTIONAL_PDS_COLUMNS, pds_cols)
    optional_sql = ""
    if mt_optional or pds_optional:
        optional_sql = ",\n          " + ",\n          ".join([*mt_optional, *pds_optional])

    sql = text(
        f"""
        SELECT
          mt.game_date,
          mt.game_id,
          mt.player_id,
          mt.player_name,
          mt.team,
          mt.opponent,
          mt.team_id,
          mt.opponent_team_id,
          mt.opponent_encoded,
          mt.is_home,
          mt.rolling_result_avg_7,
          pds.d15_hits,
          pds.d30_hits,
          pfp.features AS pfp_features,
          pfp.feature_set_tag,
          pfp.model_tag
          {optional_sql}
        FROM mlb.model_training_props mt
        LEFT JOIN mlb.player_derived_stats pds
          ON pds.player_id = mt.player_id
         AND pds.game_id = mt.game_id
         AND pds.game_date = mt.game_date
        LEFT JOIN mlb.prop_features_precomputed pfp
          ON pfp.player_id = mt.player_id
         AND pfp.game_id = mt.game_id
         AND pfp.game_date = mt.game_date
         AND pfp.prop_type = mt.prop_type
        WHERE mt.prop_type = 'hits'
          AND mt.game_date BETWEEN :from_date AND :to_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"from_date": from_date, "to_date": to_date})
    meta = {
        "model_training_props_optional_found": [c for c in OPTIONAL_MTP_COLUMNS if c in mt_cols],
        "player_derived_stats_optional_found": [c for c in OPTIONAL_PDS_COLUMNS if c in pds_cols],
        "optional_missing": sorted(set(OPTIONAL_MTP_COLUMNS + OPTIONAL_PDS_COLUMNS) - mt_cols - pds_cols),
    }
    return df, meta


def _parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _first_nonempty(row: pd.Series, candidates: Sequence[str]) -> str:
    for col in candidates:
        if col in row.index:
            val = _clean(row.get(col))
            if val:
                return val
    return ""


def _team_label(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    if raw.replace(".", "", 1).isdigit() and getFullTeamAbbreviationFromID is not None:
        try:
            abbr = getFullTeamAbbreviationFromID(int(float(raw)))
            if abbr:
                return str(abbr).upper()
        except Exception:
            pass
    if normalizeTeamAbbreviation is not None:
        try:
            return str(normalizeTeamAbbreviation(raw)).upper()
        except Exception:
            pass
    return raw.upper()


def _prep_context(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    parsed = out["pfp_features"].map(_parse_json_obj) if "pfp_features" in out.columns else pd.Series([{}] * len(out))
    for feature in ["rolling_result_avg_7", "d15_hits", "d30_hits"]:
        if feature not in out.columns:
            out[feature] = np.nan
        out[feature] = pd.to_numeric(out[feature], errors="coerce")
        mask = out[feature].isna()
        if mask.any():
            out.loc[mask, feature] = parsed[mask].map(lambda obj, key=feature: obj.get(key)).pipe(
                pd.to_numeric, errors="coerce"
            )

    for key in OPTIONAL_PFP_KEYS:
        pfp_col = f"pfp_{key}"
        out[pfp_col] = parsed.map(lambda obj, k=key: obj.get(k))

    out["d15_hits_bucket"] = pd.cut(out["d15_hits"], HIT_BINS, labels=HIT_LABELS, right=False, include_lowest=True)
    out["d30_hits_bucket"] = pd.cut(out["d30_hits"], HIT_BINS, labels=HIT_LABELS, right=False, include_lowest=True)
    out["team_context"] = out.apply(
        lambda r: _first_nonempty(r, ["team", "team_id", "mt_team_id", "pfp_team_id"]),
        axis=1,
    ).map(_team_label)
    out["opponent_context"] = out.apply(
        lambda r: _first_nonempty(r, ["opponent", "opponent_team_id", "opponent_encoded", "mt_opponent_team_id", "pfp_opponent_team_id"]),
        axis=1,
    ).map(_team_label)
    out["home_away"] = pd.to_numeric(out.get("is_home"), errors="coerce").map(
        lambda v: "home" if pd.notna(v) and bool(v) else ("away" if pd.notna(v) else "")
    )
    out["player_handedness"] = out.apply(
        lambda r: _first_nonempty(
            r,
            [
                "mt_player_handedness",
                "pds_player_handedness",
                "mt_batter_hand",
                "pds_batter_hand",
                "mt_bats",
                "pds_bats",
                "mt_bat_side",
                "pds_bat_side",
                "pfp_player_handedness",
                "pfp_batter_hand",
                "pfp_bats",
                "pfp_bat_side",
            ],
        ),
        axis=1,
    )
    out["batting_order_spot"] = out.apply(
        lambda r: _first_nonempty(
            r,
            [
                "mt_batting_order_spot",
                "pds_batting_order_spot",
                "mt_batting_order",
                "pds_batting_order",
                "mt_lineup_spot",
                "pds_lineup_spot",
                "pfp_batting_order_spot",
                "pfp_batting_order",
                "pfp_lineup_spot",
            ],
        ),
        axis=1,
    )
    out["starting_pitcher_handedness"] = out.apply(
        lambda r: _first_nonempty(
            r,
            [
                "mt_starting_pitcher_hand",
                "mt_opposing_pitcher_hand",
                "mt_pitcher_hand",
                "mt_pitcher_throws",
                "mt_opposing_pitcher_throws",
                "mt_starting_pitcher_throws",
                "pfp_starting_pitcher_hand",
                "pfp_opposing_pitcher_hand",
                "pfp_pitcher_hand",
                "pfp_pitcher_throws",
                "pfp_opposing_pitcher_throws",
                "pfp_starting_pitcher_throws",
            ],
        ),
        axis=1,
    )
    out["starting_pitcher_id"] = out.apply(
        lambda r: _first_nonempty(r, ["mt_starting_pitcher_id", "pfp_starting_pitcher_id"]),
        axis=1,
    )

    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out = out.sort_values(["date_key", "game_id_key", "player_id_key", "feature_set_tag", "model_tag"]).drop_duplicates(
        ["date_key", "game_id_key", "player_id_key"], keep="last"
    )
    keep = [
        "date_key",
        "game_id_key",
        "player_id_key",
        "rolling_result_avg_7",
        "d15_hits",
        "d30_hits",
        "d15_hits_bucket",
        "d30_hits_bucket",
        "team_context",
        "opponent_context",
        "home_away",
        "player_handedness",
        "batting_order_spot",
        "starting_pitcher_handedness",
        "starting_pitcher_id",
    ]
    return out[keep]


def _side_rows(reconcile: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    work = reconcile.copy()
    work["date_key"] = work["game_date"].map(_date_key)
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work["line_num"] = pd.to_numeric(work["line"], errors="coerce")
    work = work[work["line_num"].eq(0.5)].copy()
    work = work.merge(context, how="left", on=["date_key", "game_id_key", "player_id_key"])
    work["team_context"] = work["team_context"].where(work["team_context"].map(_clean).ne(""), work["home_team_code"].map(_team_label))
    work["opponent_context"] = work["opponent_context"].where(
        work["opponent_context"].map(_clean).ne(""), work["away_team_code"].map(_team_label)
    )
    work["recency_bucket"] = pd.cut(
        pd.to_numeric(work["rolling_result_avg_7"], errors="coerce"),
        RECENCY_BINS,
        labels=RECENCY_LABELS,
        right=False,
        include_lowest=True,
    )

    pieces = []
    for side in ("over", "under"):
        side_df = pd.DataFrame(
            {
                "side": side,
                "recency_bucket": work["recency_bucket"],
                "rolling_result_avg_7": pd.to_numeric(work["rolling_result_avg_7"], errors="coerce"),
                "d15_hits_bucket": work["d15_hits_bucket"].astype(str),
                "d30_hits_bucket": work["d30_hits_bucket"].astype(str),
                "team": work["team_context"].map(_clean),
                "opponent": work["opponent_context"].map(_clean),
                "home_away": work["home_away"].map(_clean),
                "player_handedness": work["player_handedness"].map(_clean),
                "batting_order_spot": work["batting_order_spot"].map(_clean),
                "starting_pitcher_handedness": work["starting_pitcher_handedness"].map(_clean),
                "starting_pitcher_id": work["starting_pitcher_id"].map(_clean),
                "bookmaker_key": work["bookmaker_key"].map(_clean),
                "model_prob": pd.to_numeric(work[f"model_prob_{side}"], errors="coerce"),
                "outcome": work[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
            }
        )
        pieces.append(side_df)
    rows = pd.concat(pieces, ignore_index=True)
    rows = rows[rows["recency_bucket"].notna() & rows["outcome"].isin({"win", "loss"}) & rows["model_prob"].notna()].copy()
    rows["win"] = rows["outcome"].eq("win").astype(float)
    return rows


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    model = float(group["model_prob"].mean()) if bets else np.nan
    actual = float(group["win"].mean()) if bets else np.nan
    return {
        "bets": bets,
        "avg_model_prob": model,
        "actual_win_rate": actual,
        "calibration_error": actual - model if bets else np.nan,
    }


def build_surface(rows: pd.DataFrame) -> pd.DataFrame:
    dimensions = [
        "side",
        "d30_hits_bucket",
        "d15_hits_bucket",
        "player_handedness",
        "batting_order_spot",
        "team",
        "opponent",
        "home_away",
        "starting_pitcher_handedness",
        "starting_pitcher_id",
        "bookmaker_key",
    ]
    records = []
    for recency_bucket, rb_df in rows.groupby("recency_bucket", observed=True, dropna=False):
        for dimension in dimensions:
            data = rb_df.copy()
            if dimension != "side":
                data = data[data[dimension].map(_clean).ne("")]
            if data.empty:
                continue
            if dimension == "side":
                for value, group in data.groupby("side", observed=True, dropna=False):
                    row = {
                        "recency_bucket": str(recency_bucket),
                        "dimension": "side",
                        "dimension_value": str(value),
                        "side": str(value),
                        "avg_rolling_result_avg_7": float(group["rolling_result_avg_7"].mean(skipna=True)),
                    }
                    row.update(_metrics(group))
                    records.append(row)
                continue
            group_cols = ["side"]
            for value, group in data.groupby(dimension, observed=True, dropna=False):
                row = {
                    "recency_bucket": str(recency_bucket),
                    "dimension": dimension,
                    "dimension_value": str(value),
                    "side": str(group["side"].iloc[0]) if len(group["side"].unique()) == 1 else "both",
                    "avg_rolling_result_avg_7": float(group["rolling_result_avg_7"].mean(skipna=True)),
                }
                row.update(_metrics(group))
                records.append(row)
            for keys, group in data.groupby([dimension, *group_cols], observed=True, dropna=False):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                value = keys[0]
                side = keys[1] if len(keys) > 1 else str(value)
                row = {
                    "recency_bucket": str(recency_bucket),
                    "dimension": f"{dimension}_x_side" if dimension != "side" else "side",
                    "dimension_value": str(value),
                    "side": str(side),
                    "avg_rolling_result_avg_7": float(group["rolling_result_avg_7"].mean(skipna=True)),
                }
                row.update(_metrics(group))
                records.append(row)
    out = pd.DataFrame(records)
    if out.empty:
        return out
    out["abs_calibration_error"] = pd.to_numeric(out["calibration_error"], errors="coerce").abs()
    out["sample_size_flag"] = np.select(
        [out["bets"].ge(75), out["bets"].ge(25)],
        ["strong_sample", "usable"],
        default="low_sample",
    )
    out["usable_sample"] = out["bets"].ge(25)
    return out.sort_values(["usable_sample", "abs_calibration_error", "bets"], ascending=[False, False, False])


def _fmt(value: Any, digits: int = 4) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value):.{digits}f}"


def _md_table(df: pd.DataFrame, cols: list[str], max_rows: int = 20) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].head(max_rows).fillna("")
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def write_summary(
    out_md: Path,
    surface: pd.DataFrame,
    rows: pd.DataFrame,
    meta: dict[str, Any],
    from_date: str,
    to_date: str,
    files: int,
) -> None:
    overview_rows = []
    for cols, label in [(["recency_bucket"], "recency"), (["recency_bucket", "side"], "recency_side")]:
        for keys, group in rows.groupby(cols, observed=True, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {"group": label, "recency_bucket": "", "side": "ALL"}
            row.update(dict(zip(cols, [str(k) for k in keys])))
            row.update(_metrics(group))
            overview_rows.append(row)
    overview = pd.DataFrame(overview_rows)
    for col in ["avg_model_prob", "actual_win_rate", "calibration_error"]:
        overview[col] = overview[col].map(_fmt)

    top = surface[surface["bets"].ge(25)].copy().head(40)
    for col in ["avg_rolling_result_avg_7", "avg_model_prob", "actual_win_rate", "calibration_error", "abs_calibration_error"]:
        top[col] = top[col].map(_fmt)

    coverage = []
    for dim in [
        "player_handedness",
        "batting_order_spot",
        "team",
        "opponent",
        "home_away",
        "starting_pitcher_handedness",
        "starting_pitcher_id",
        "bookmaker_key",
    ]:
        nonblank = rows[dim].map(_clean) if dim in rows.columns else pd.Series(dtype=str)
        coverage.append(
            {
                "dimension": dim,
                "nonblank_rows": int(nonblank.ne("").sum()) if dim in rows.columns else 0,
                "coverage": float(nonblank.ne("").mean()) if dim in rows.columns and len(rows) else 0.0,
                "unique_values": int(nonblank[nonblank.ne("")].nunique()) if dim in rows.columns else 0,
            }
        )
    coverage_df = pd.DataFrame(coverage)
    coverage_df["coverage"] = coverage_df["coverage"].map(_fmt)

    lines = [
        "# Hits Local Base-Rate Surface",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Scope: `prop_type = hits`, `line = 0.5`.",
        "",
        "Focus rolling_result_avg_7 buckets:",
        "- `0.20-0.30`",
        "- `0.30-0.40`",
        "- `0.40-0.50`",
        "- `0.50-0.75`",
        "",
        "No price is used as an explanation. `bookmaker_key` is included only as a market-source dimension because it is already present in reconcile rows.",
        "",
        f"Reconcile files: `{files}`",
        f"Evaluated side rows: `{len(rows)}`",
        f"Surface rows: `{len(surface)}`",
        "",
        "## Optional Context Availability",
        "",
        f"- model_training_props optional columns found: `{meta.get('model_training_props_optional_found')}`",
        f"- player_derived_stats optional columns found: `{meta.get('player_derived_stats_optional_found')}`",
        f"- optional columns missing from source tables: `{meta.get('optional_missing')}`",
        "",
        _md_table(coverage_df, ["dimension", "nonblank_rows", "coverage", "unique_values"], 20),
        "",
        "## Recency Bucket Overview",
        "",
        _md_table(overview, ["group", "recency_bucket", "side", "bets", "avg_model_prob", "actual_win_rate", "calibration_error"], 20),
        "",
        "## Largest Local Base-Rate Errors",
        "",
        _md_table(
            top,
            [
                "recency_bucket",
                "dimension",
                "dimension_value",
                "side",
                "bets",
                "avg_model_prob",
                "actual_win_rate",
                "calibration_error",
                "sample_size_flag",
            ],
            40,
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Diagnose hits local base-rate/intercept error.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_hits_reconcile(paths)
    engine = create_engine(_db_url(), pool_pre_ping=True)
    source, meta = _fetch_feature_context(engine, args.from_date, args.to_date)
    context = _prep_context(source)
    rows = _side_rows(reconcile, context)
    surface = build_surface(rows)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    surface.to_csv(out_csv, index=False)
    write_summary(out_md, surface, rows, meta, args.from_date, args.to_date, len(paths))

    print(
        "[hits-local-base-rate] "
        f"files={len(paths)} source_rows={len(source)} side_rows={len(rows)} "
        f"surface_rows={len(surface)} out_csv={out_csv} out_md={out_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
