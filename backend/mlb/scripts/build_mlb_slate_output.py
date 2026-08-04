#!/usr/bin/env python3
"""
Build canonical MLB slate output (model-only) from calibrated wide predictions.

Purpose:
- Create one normalized MLB slate artifact that downstream consumers can share:
  - MLB market board builder (future)
  - MLB book upload exporter (operator tooling)
- Keep predictions market-independent. This file contains model probabilities only.

Default input:
- backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv

Default output:
- backend/mlb/data/processed/mlb_slate_output.csv
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from backend.mlb.shared.market_audit_context import MARKET_AUDIT_CONTEXT_COLUMNS, add_market_audit_context
from backend.mlb.shared.hits05_production_replacement import (
    PROVENANCE_COLUMNS as HITS05_PROVENANCE_COLUMNS,
    apply_hits05_replacement,
)
from backend.mlb.shared.probability_calibration import calibrate_probability, load_calibrator
from backend.mlb.shared.time_utils_backend import get_time_of_day_bucket_et


BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
DEFAULT_PRED_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_predictions_wide_calibrated.csv"
DEFAULT_OUT_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_slate_output.csv"
DEFAULT_PREPARED_FEATURE_DEBUG_ROOT = (
    BASE_DIR / "mlb" / "exports" / "model_diagnostics" / "prepared_feature_vectors"
)

# External book-upload taxonomy (MARKET key) doubles as a stable cross-tool prop mapping.
DEFAULT_MARKET_BY_PROP: Dict[str, str] = {
    "hits": "batter_hits",
    "runs_scored": "batter_runs",
    "rbis": "batter_rbis",
    "runs_rbis": "batter_r+rbi",
    "total_bases": "batter_bases",
    "hits_runs_rbis": "batter_h+r+rbi",
    "walks": "batter_walks",
    "strikeouts_batting": "batter_strikeouts",
    "stolen_bases": "batter_stolen_bases",
    "singles": "batter_singles",
    "doubles": "batter_doubles",
    "triples": "batter_triples",
    "home_runs": "batter_home_runs",
    "hits_allowed": "pitcher_hits",
    "earned_runs": "pitcher_earned_runs",
    "outs_recorded": "pitcher_outs",
    "walks_allowed": "pitcher_walks",
    "strikeouts_pitching": "pitcher_strikeouts",
}

_PCOL_RE = re.compile(r"^p_over_(\d+)_([05])$")
PATCH_1A_CONTEXT_COLUMNS = [
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
    "is_home",
    "team",
    "team_id",
    "opponent",
    "opponent_id",
]
BVP_CONTEXT_COLUMNS = [
    "bvp_plate_appearances",
    "bvp_at_bats",
    "bvp_hits",
    "bvp_total_bases",
    "bvp_avg",
    "bvp_slg",
    "bvp_payload_present",
    "bvp_source",
]
ROLLING_CONTEXT_COLUMNS = [
    "rolling_result_avg_7",
    "d7_hits",
    "d15_hits",
    "d30_hits",
    "d7_total_bases",
    "d15_total_bases",
    "d30_total_bases",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "d7_strikeouts_batting",
    "d15_strikeouts_batting",
    "d30_strikeouts_batting",
    "d7_hits_allowed",
    "d15_hits_allowed",
    "d30_hits_allowed",
]
PASSIVE_CONTEXT_COLUMNS = [
    *PATCH_1A_CONTEXT_COLUMNS,
    *BVP_CONTEXT_COLUMNS,
    *ROLLING_CONTEXT_COLUMNS,
    *MARKET_AUDIT_CONTEXT_COLUMNS,
]


def _canonical_prop_type(value: object) -> str:
    return str(value or "").strip().lower()


def _clean_optional_str(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _game_day_of_week(value: object) -> Optional[str]:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return str(dt.day_name()).lower()


def _time_of_day_bucket(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    try:
        return get_time_of_day_bucket_et(value)
    except Exception:
        return None


def _truthy_rate(series: pd.Series) -> float:
    return float(
        series.map(lambda v: str(v).strip().lower() in {"1", "true", "yes", "on"} if pd.notna(v) else False).mean()
    )


def _context_field_diagnostics(df: pd.DataFrame, *, stage: str) -> Dict[str, object]:
    fields: Dict[str, object] = {}
    row_count = int(len(df))
    for col in PASSIVE_CONTEXT_COLUMNS:
        present = col in df.columns
        null_rate = None
        if present and row_count:
            null_rate = float(df[col].isna().mean())
        fields[col] = {
            "present": bool(present),
            "null_rate": null_rate,
            "payload_present_rate": (
                _truthy_rate(df[col])
                if col == "bvp_payload_present" and present and row_count
                else None
            ),
            "row_count": row_count,
            "missing_stage": "" if present else stage,
        }
    return {"stage": stage, "row_count": row_count, "fields": fields}


def _prepared_feature_dir(raw: str, slate_date: str) -> Optional[Path]:
    text = str(raw or "").strip()
    if text.lower() in {"0", "false", "no", "off", "none", "null"}:
        return None
    if text:
        return Path(text).expanduser()
    return DEFAULT_PREPARED_FEATURE_DEBUG_ROOT / str(slate_date)


def _load_prepared_feature_context(feature_dir: Optional[Path]) -> pd.DataFrame:
    market_source_columns = [
        "price_over_american",
        "price_under_american",
        "implied_over_novig",
        "implied_under_novig",
        "market_hold",
        "book_count_two_sided",
        "bookmaker_key",
        "odds_snapshot_file",
        "snapshot_time_utc",
        "snapshot_run_tag",
    ]
    context_columns = [
        *BVP_CONTEXT_COLUMNS,
        *ROLLING_CONTEXT_COLUMNS,
        *MARKET_AUDIT_CONTEXT_COLUMNS,
    ]
    columns = ["game_id", "player_id", "prop_type", "line", *context_columns]
    if feature_dir is None or not feature_dir.exists():
        return pd.DataFrame(columns=columns)

    frames: List[pd.DataFrame] = []
    for path in sorted(feature_dir.glob("*_features.csv")):
        try:
            frame = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        if frame.empty:
            continue
        for key in ("game_id", "player_id", "prop_type", "line"):
            if key not in frame.columns:
                frame[key] = pd.NA
        for col in [*BVP_CONTEXT_COLUMNS, *ROLLING_CONTEXT_COLUMNS, *market_source_columns, *MARKET_AUDIT_CONTEXT_COLUMNS]:
            if col not in frame.columns:
                frame[col] = pd.NA
        frame = add_market_audit_context(frame)
        frames.append(frame[columns].copy())
    if not frames:
        return pd.DataFrame(columns=columns)

    out = pd.concat(frames, ignore_index=True)
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["prop_type"] = out["prop_type"].map(_canonical_prop_type)
    out = out.dropna(subset=["game_id", "player_id", "line"])
    out = out[out["prop_type"].astype(str).str.len() > 0].copy()
    if out.empty:
        return pd.DataFrame(columns=columns)

    out["game_id"] = out["game_id"].astype(int)
    out["player_id"] = out["player_id"].astype(int)
    for col in [
        "bvp_plate_appearances",
        "bvp_at_bats",
        "bvp_hits",
        "bvp_total_bases",
        "bvp_avg",
        "bvp_slg",
        *ROLLING_CONTEXT_COLUMNS,
        "market_price_over",
        "market_price_under",
        "market_no_vig_implied_over",
        "market_no_vig_implied_under",
        "market_hold",
        "market_book_count_two_sided",
        "selected_side_price",
        "selected_side_no_vig_implied",
        "model_vs_market_gap",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if out["bvp_avg"].isna().any():
        ab = pd.to_numeric(out["bvp_at_bats"], errors="coerce")
        hits = pd.to_numeric(out["bvp_hits"], errors="coerce")
        derived = hits / ab.where(ab.gt(0))
        out["bvp_avg"] = out["bvp_avg"].where(out["bvp_avg"].notna(), derived)
    if out["bvp_slg"].isna().any():
        ab = pd.to_numeric(out["bvp_at_bats"], errors="coerce")
        tb = pd.to_numeric(out["bvp_total_bases"], errors="coerce")
        derived = tb / ab.where(ab.gt(0))
        out["bvp_slg"] = out["bvp_slg"].where(out["bvp_slg"].notna(), derived)
    if "bvp_payload_present" in out.columns:
        payload_from_counts = out[
            ["bvp_plate_appearances", "bvp_at_bats", "bvp_hits", "bvp_total_bases"]
        ].notna().any(axis=1)
        out["bvp_payload_present"] = out["bvp_payload_present"].map(
            lambda v: (
                True
                if str(v).strip().lower() in {"1", "true", "yes", "on"}
                else False
                if str(v).strip().lower() in {"0", "false", "no", "off"}
                else pd.NA
            )
        )
        out["bvp_payload_present"] = out["bvp_payload_present"].where(
            out["bvp_payload_present"].notna(),
            payload_from_counts,
        )
    if "bvp_source" in out.columns:
        out["bvp_source"] = out["bvp_source"].where(
            out["bvp_source"].notna(),
            out["bvp_payload_present"].map(lambda v: "prop_features_precomputed" if bool(v) else pd.NA),
        )
    out = out.drop_duplicates(subset=["game_id", "player_id", "prop_type", "line"], keep="first")
    return out[columns]


def _merge_prepared_feature_context(df_long: pd.DataFrame, feature_dir: Optional[Path]) -> pd.DataFrame:
    out = df_long.copy()
    before_rows = len(out)
    for col in [*BVP_CONTEXT_COLUMNS, *ROLLING_CONTEXT_COLUMNS, *MARKET_AUDIT_CONTEXT_COLUMNS]:
        if col not in out.columns:
            out[col] = pd.NA
    context = _load_prepared_feature_context(feature_dir)
    if context.empty:
        print(
            "[feature-lineage-prepared-context] "
            + json.dumps(
                {
                    "stage": "prepared_feature_context",
                    "feature_dir": str(feature_dir) if feature_dir else "",
                    "context_rows": 0,
                    "merged_rows": 0,
                    "row_count": before_rows,
                },
                sort_keys=True,
            )
        )
        return out

    key_cols = ["game_id", "player_id", "prop_type", "line"]
    prepared_context_columns = [*BVP_CONTEXT_COLUMNS, *ROLLING_CONTEXT_COLUMNS, *MARKET_AUDIT_CONTEXT_COLUMNS]
    merge_cols = key_cols + [f"{col}_prepared_ctx" for col in prepared_context_columns]
    context = context.rename(columns={col: f"{col}_prepared_ctx" for col in prepared_context_columns})
    out = out.merge(context[merge_cols], on=key_cols, how="left", sort=False, validate="many_to_one")
    for col in prepared_context_columns:
        src_col = f"{col}_prepared_ctx"
        out[col] = out[col].where(out[col].notna(), out[src_col])
        out = out.drop(columns=[src_col])

    merged_rows = int(
        out["bvp_payload_present"].map(
            lambda v: str(v).strip().lower() in {"1", "true", "yes", "on"} if pd.notna(v) else False
        ).sum()
    )
    rolling_rows = int(out[ROLLING_CONTEXT_COLUMNS].notna().any(axis=1).sum())
    if len(out) != before_rows:
        raise RuntimeError("prepared feature context merge changed slate row count")
    print(
        "[feature-lineage-prepared-context] "
        + json.dumps(
            {
                "stage": "prepared_feature_context",
                "feature_dir": str(feature_dir) if feature_dir else "",
                "context_rows": int(len(context)),
                "bvp_merged_rows": merged_rows,
                "rolling_merged_rows": rolling_rows,
                "row_count": before_rows,
            },
            sort_keys=True,
        )
    )
    return out


def _parse_lines_from_cols(cols: Iterable[str]) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for col in cols:
        m = _PCOL_RE.match(col)
        if not m:
            continue
        whole = int(m.group(1))
        half = int(m.group(2))
        line = float(whole) + (0.5 if half == 5 else 0.0)
        out.append((col, line))
    out.sort(key=lambda x: x[1])
    return out


def _prob_to_fair_american(prob: float) -> Optional[int]:
    if not (0.0 < prob < 1.0):
        return None
    if prob >= 0.5:
        return int(-round(100.0 * prob / (1.0 - prob)))
    return int(round(100.0 * (1.0 - prob) / prob))


def _load_market_map(arg_json: str, env_json: str) -> Dict[str, str]:
    out = dict(DEFAULT_MARKET_BY_PROP)
    raw = (arg_json or "").strip() or (env_json or "").strip()
    if not raw:
        return out
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("market map JSON must be an object")
    for key, value in payload.items():
        prop = _canonical_prop_type(key)
        val = str(value or "").strip()
        if prop and val:
            out[prop] = val
    return out


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _get_db_conn():
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("missing SUPABASE_DB_URL or DATABASE_URL")

    # Prefer psycopg2 when present, but support psycopg (v3) runtimes.
    try:
        import psycopg2  # type: ignore

        return psycopg2.connect(db_url)
    except Exception:
        try:
            import psycopg  # type: ignore

            return psycopg.connect(db_url)
        except Exception as exc:
            raise RuntimeError(f"database driver unavailable (need psycopg2 or psycopg): {exc}") from exc


def _table_columns(conn, *, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name::text
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        )
        return {str(r[0]) for r in (cur.fetchall() or [])}


def _fetch_games(conn, game_ids: List[int]) -> pd.DataFrame:
    if not game_ids:
        return pd.DataFrame(
            columns=["game_id", "game_date", "game_type", "home_team_code", "away_team_code"]
        )

    cols = _table_columns(conn, schema="mlb", table="game_info")
    game_type_expr = "game_type::text AS game_type" if "game_type" in cols else "NULL::text AS game_type"
    sql = f"""
    SELECT
      game_id::bigint AS game_id,
      game_date::date AS game_date,
      {game_type_expr},
      home_team_abbr::text AS home_team_code,
      away_team_abbr::text AS away_team_code
    FROM mlb.game_info
    WHERE game_id = ANY(%s::bigint[])
    """
    return pd.read_sql(sql, conn, params=(list(game_ids),))


def _fetch_players(conn, player_ids: List[int]) -> pd.DataFrame:
    if not player_ids:
        return pd.DataFrame(columns=["player_id", "player_name"])

    cols = _table_columns(conn, schema="mlb", table="player_ids")
    if "player_name" not in cols:
        return pd.DataFrame(columns=["player_id", "player_name"])
    sql = """
    SELECT
      player_id::bigint AS player_id,
      player_name::text AS player_name
    FROM mlb.player_ids
    WHERE player_id = ANY(%s::bigint[])
    """
    return pd.read_sql(sql, conn, params=(list(player_ids),))


def _load_predictions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing predictions file: {path}")
    print(f"[mlb-slate-output] reading wide predictions: {path}")
    return pd.read_csv(path)


def _melt_to_long(df_wide: pd.DataFrame, default_prop_type: Optional[str]) -> pd.DataFrame:
    for key in ("player_id", "game_id"):
        if key not in df_wide.columns:
            raise ValueError(f"predictions missing required column: {key}")

    col_lines = _parse_lines_from_cols(df_wide.columns)
    if not col_lines:
        raise ValueError("no p_over_* columns found in predictions input")
    prob_cols = [c for c, _ in col_lines]
    print(f"[mlb-slate-output] probability columns: {prob_cols}")

    use_prop_col = "prop_type" in df_wide.columns
    if not use_prop_col and not default_prop_type:
        raise ValueError("missing prop_type column and no --prop-type provided")

    id_cols = ["player_id", "game_id"] + (["prop_type"] if use_prop_col else [])
    for optional_col in (
        "player_name",
        "game_date",
        "game_type",
        "home_team_code",
        "away_team_code",
        "game_time",
        "team",
        "team_id",
        "opponent",
        "opponent_id",
        "is_home",
    ):
        if optional_col in df_wide.columns and optional_col not in id_cols:
            id_cols.append(optional_col)

    df_long = df_wide[id_cols + prob_cols].melt(
        id_vars=id_cols,
        value_vars=prob_cols,
        var_name="prob_col",
        value_name="prob_over",
    )
    line_map = {c: line for c, line in col_lines}
    df_long["line"] = df_long["prob_col"].map(line_map).astype(float)
    df_long = df_long.drop(columns=["prob_col"])

    if use_prop_col:
        df_long["prop_type"] = df_long["prop_type"].map(_canonical_prop_type)
    else:
        df_long["prop_type"] = _canonical_prop_type(default_prop_type)

    for c in ("player_id", "game_id", "prob_over", "line"):
        df_long[c] = pd.to_numeric(df_long[c], errors="coerce")

    if "player_name" in df_long.columns:
        df_long["player_name"] = df_long["player_name"].astype(str).str.strip()
        df_long.loc[df_long["player_name"].isin(["", "nan", "None"]), "player_name"] = None

    df_long = df_long.dropna(subset=["player_id", "game_id", "prob_over", "line"])
    df_long = df_long[df_long["prop_type"].astype(str).str.len() > 0]
    if df_long.empty:
        raise ValueError("no usable prediction rows after melt/cleanup")

    df_long["player_id"] = df_long["player_id"].astype(int)
    df_long["game_id"] = df_long["game_id"].astype(int)
    return df_long


def _enrich_with_db(df_long: pd.DataFrame) -> pd.DataFrame:
    unique_player_ids = sorted(df_long["player_id"].unique().tolist())
    need_game_cols = ["game_date", "home_team_code", "away_team_code"]
    embedded_complete = all(c in df_long.columns for c in need_game_cols) and not df_long[need_game_cols].isna().any(axis=None)

    # Preserve row order through DB merges. This prevents value re-alignment bugs
    # when pandas merge reindexes rows.
    merged = df_long.copy().reset_index(drop=True)
    merged["_row_idx"] = range(len(merged))

    if embedded_complete:
        print(
            f"[mlb-slate-output] using embedded game metadata for rows={len(df_long)} players={len(unique_player_ids)}"
        )
        # Prefer fully embedded rows (from build_mlb_predictions_wide) to avoid
        # unnecessary DB dependency during local/offline smoke runs.
        embedded_player_names = (
            "player_name" in merged.columns
            and merged["player_name"].map(_clean_optional_str).notna().any()
        )
        if embedded_player_names:
            players = pd.DataFrame(columns=["player_id", "player_name"])
            print("[mlb-slate-output] using embedded player names (DB player lookup skipped)")
        else:
            try:
                with _get_db_conn() as conn:
                    players = _fetch_players(conn, unique_player_ids)
            except Exception as exc:
                # When game metadata is already embedded, player-name lookup is optional.
                # Keep pipeline alive on lean/runtime images missing DB drivers.
                print(f"[mlb-slate-output] WARNING: player lookup skipped ({exc})")
                players = pd.DataFrame(columns=["player_id", "player_name"])
    else:
        unique_game_ids = sorted(df_long["game_id"].unique().tolist())
        print(
            f"[mlb-slate-output] fetching metadata for games={len(unique_game_ids)} players={len(unique_player_ids)}"
        )
        with _get_db_conn() as conn:
            games = _fetch_games(conn, unique_game_ids)
            players = _fetch_players(conn, unique_player_ids)

        if games.empty:
            raise RuntimeError("no matching rows in mlb.game_info for prediction game_ids")

        merged = merged.merge(games, on="game_id", how="left", suffixes=("", "_db"), sort=False)
        for c in ("game_date", "game_type", "home_team_code", "away_team_code"):
            db_col = f"{c}_db"
            if db_col in merged.columns:
                if c in merged.columns:
                    merged[c] = merged[c].where(merged[c].notna(), merged[db_col])
                else:
                    merged[c] = merged[db_col]
                merged = merged.drop(columns=[db_col])

    merged = merged.dropna(subset=["game_date", "home_team_code", "away_team_code"])

    # player_ids can contain duplicate player_id rows. De-duplicate before merge to
    # enforce a stable many-to-one join from prediction rows -> player dimension.
    if not players.empty:
        players = (
            players.dropna(subset=["player_id"])
            .sort_values(by=["player_id", "player_name"], kind="stable")
            .drop_duplicates(subset=["player_id"], keep="first")
            .reset_index(drop=True)
        )

    if "player_name" in merged.columns:
        merged = merged.merge(
            players.rename(columns={"player_name": "player_name_db"}),
            on="player_id",
            how="left",
            sort=False,
            validate="many_to_one",
        )
        pred_name = merged["player_name"].astype("object")
        pred_name_clean = pred_name.map(_clean_optional_str)
        merged["player_name"] = pred_name_clean.where(pred_name_clean.notna(), merged["player_name_db"])
        merged = merged.drop(columns=["player_name_db"])
    else:
        merged = merged.merge(
            players,
            on="player_id",
            how="left",
            sort=False,
            validate="many_to_one",
        )

    if merged.empty:
        raise RuntimeError("no rows remain after joining game metadata")
    merged = merged.sort_values("_row_idx", kind="stable").drop(columns=["_row_idx"]).reset_index(drop=True)
    return merged


def build_slate_output(
    *,
    df_long: pd.DataFrame,
    slate_date: str,
    strict: bool,
    drop_line_0_5: bool,
    market_map: Dict[str, str],
    pred_csv_path: Path,
    prepared_feature_dir: Optional[Path] = None,
    calibration_json: str = "",
    apply_upload_calibration: bool = False,
) -> pd.DataFrame:
    df_long = _merge_prepared_feature_context(df_long, prepared_feature_dir)
    df_long = apply_hits05_replacement(df_long, slate_date=slate_date)
    merged = _enrich_with_db(df_long)
    calibrator = load_calibrator(calibration_json) if apply_upload_calibration else None
    min_prop_samples = int((calibrator or {}).get("min_prop_samples") or 200)
    if calibrator:
        print(
            "[calibration] upload calibration enabled: "
            f"{calibrator.get('method')} path={calibration_json}"
        )
    else:
        if str(calibration_json or "").strip() and not apply_upload_calibration:
            print("[calibration] upload calibration disabled; using raw probabilities")
        elif not str(calibration_json or "").strip():
            print("[calibration] upload calibration disabled; using raw probabilities")

    merged["game_date"] = pd.to_datetime(merged["game_date"]).dt.date
    target_date = pd.to_datetime(slate_date).date()
    dates_present = sorted({d.isoformat() for d in merged["game_date"].dropna().tolist()})
    print(f"[mlb-slate-output] dates present after join: {dates_present}")

    before = len(merged)
    merged = merged[merged["game_date"] == target_date]
    after = len(merged)
    print(f"[mlb-slate-output] rows after slate filter ({slate_date}): {after}")
    if after == 0:
        raise RuntimeError(f"zero rows for slate_date={slate_date}; dates_present={dates_present}")
    if after < before:
        msg = f"filtered out {before - after} rows not on slate_date={slate_date}"
        if strict:
            raise RuntimeError(msg)
        print(f"[mlb-slate-output] WARNING: {msg}")

    if drop_line_0_5:
        before_map = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        merged = merged[merged["line"] != 0.5]
        after_map = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        print(f"[mlb-slate-output] dropped line 0.5: before={before_map} after={after_map}")

    rows: List[Dict[str, object]] = []
    generated_at_utc = datetime.now(timezone.utc).isoformat()

    present_props = sorted({_canonical_prop_type(x) for x in merged["prop_type"].tolist() if str(x or "").strip()})
    unmapped = [p for p in present_props if p not in market_map]
    if unmapped:
        raise RuntimeError(
            "unmapped prop_type(s): " + ", ".join(unmapped) +
            " (add mappings via --market-map-json / MLB_BOOK_UPLOAD_MARKET_MAP_JSON)"
        )

    for _, row in merged.iterrows():
        raw_p_over = float(row["prob_over"])
        p_over = raw_p_over
        if calibrator:
            calibrated = calibrate_probability(
                calibrator,
                prop_type=row.get("prop_type"),
                raw_prob=raw_p_over,
                min_prop_samples=min_prop_samples,
            )
            if calibrated is not None:
                p_over = float(calibrated)
        if not (0.0 < p_over < 1.0):
            continue
        p_under = 1.0 - p_over
        raw_p_under = 1.0 - raw_p_over
        odds_over = _prob_to_fair_american(p_over)
        odds_under = _prob_to_fair_american(p_under)
        if odds_over is None or odds_under is None:
            continue

        prop_type = _canonical_prop_type(row["prop_type"])
        pick_side = "over" if p_over >= 0.5 else "under"
        pick_prob = p_over if pick_side == "over" else p_under

        rows.append(
            {
                "league": "MLB",
                "slate_date": str(slate_date),
                "game_date": pd.to_datetime(row["game_date"]).strftime("%Y-%m-%d"),
                "game_id": int(row["game_id"]),
                "game_type": _clean_optional_str(row.get("game_type")),
                "game_time": _clean_optional_str(row.get("game_time")),
                "game_day_of_week": _game_day_of_week(row.get("game_date")),
                "time_of_day_bucket": _time_of_day_bucket(row.get("game_time")),
                "home_team_code": str(row["home_team_code"]).strip(),
                "away_team_code": str(row["away_team_code"]).strip(),
                "player_id": int(row["player_id"]),
                "player_name": _clean_optional_str(row.get("player_name")),
                "team": _clean_optional_str(row.get("team")),
                "team_id": row.get("team_id"),
                "opponent": _clean_optional_str(row.get("opponent")),
                "opponent_id": row.get("opponent_id"),
                "is_home": row.get("is_home"),
                "bvp_plate_appearances": row.get("bvp_plate_appearances"),
                "bvp_at_bats": row.get("bvp_at_bats"),
                "bvp_hits": row.get("bvp_hits"),
                "bvp_total_bases": row.get("bvp_total_bases"),
                "bvp_avg": row.get("bvp_avg"),
                "bvp_slg": row.get("bvp_slg"),
                "bvp_payload_present": row.get("bvp_payload_present"),
                "bvp_source": _clean_optional_str(row.get("bvp_source")),
                **{col: row.get(col) for col in ROLLING_CONTEXT_COLUMNS},
                "prop_type": prop_type,
                "market_key": market_map[prop_type],
                "line": float(row["line"]),
                "raw_prob_over": round(raw_p_over, 6),
                "raw_prob_under": round(raw_p_under, 6),
                "prob_over": round(p_over, 6),
                "prob_under": round(p_under, 6),
                "fair_odds_over_american": int(odds_over),
                "fair_odds_under_american": int(odds_under),
                "model_pick_side": pick_side,
                "model_pick_prob": round(float(pick_prob), 6),
                "market_price_over": row.get("market_price_over", row.get("price_over_american")),
                "market_price_under": row.get("market_price_under", row.get("price_under_american")),
                "market_no_vig_implied_over": row.get(
                    "market_no_vig_implied_over",
                    row.get("implied_over_novig"),
                ),
                "market_no_vig_implied_under": row.get(
                    "market_no_vig_implied_under",
                    row.get("implied_under_novig"),
                ),
                "market_hold": row.get("market_hold"),
                "market_book_count_two_sided": row.get(
                    "market_book_count_two_sided",
                    row.get("book_count_two_sided", row.get("books_two_sided")),
                ),
                "market_bookmaker_key": row.get("market_bookmaker_key", row.get("bookmaker_key")),
                "market_odds_snapshot_file": row.get("market_odds_snapshot_file", row.get("odds_snapshot_file")),
                "market_snapshot_time_utc": row.get("market_snapshot_time_utc", row.get("snapshot_time_utc")),
                "market_snapshot_run_tag": row.get("market_snapshot_run_tag", row.get("snapshot_run_tag")),
                **{col: row.get(col) for col in HITS05_PROVENANCE_COLUMNS},
                "calibration_method": str((calibrator or {}).get("method") or ""),
                "prediction_source_file": str(pred_csv_path),
                "generated_at_utc": generated_at_utc,
            }
        )

    if not rows:
        raise RuntimeError("no output rows generated")

    out = add_market_audit_context(
        pd.DataFrame(rows),
        side_col="model_pick_side",
        probability_col="model_pick_prob",
    )
    print("[feature-lineage-1a] " + json.dumps(_context_field_diagnostics(out, stage="slate_output"), sort_keys=True))
    out = out.sort_values(
        by=["game_date", "game_id", "player_name", "player_id", "prop_type", "line"],
        kind="stable",
    ).reset_index(drop=True)
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    from backend.mlb.shared.model_authority import assert_predictive_model_qualified

    assert_predictive_model_qualified("production_slate_generation")
    import argparse
    import pytz

    ap = argparse.ArgumentParser(description="Build canonical MLB slate output CSV from wide predictions.")
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--pred-csv", default=os.environ.get("MLB_PRED_CSV", str(DEFAULT_PRED_CSV)))
    ap.add_argument("--out-csv", default=os.environ.get("MLB_SLATE_OUTPUT_CSV", str(DEFAULT_OUT_CSV)))
    ap.add_argument(
        "--prepared-feature-dir",
        default=os.environ.get("MLB_PREPARED_FEATURE_DEBUG_OUT_DIR", ""),
        help=(
            "Directory of prepared prediction-time feature diagnostics used for passive context. "
            "Defaults to backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<slate-date>. "
            "Set to 0/false/off to disable."
        ),
    )
    ap.add_argument("--strict", action="store_true", help="Fail if source includes rows from non-slate dates.")
    ap.add_argument("--prop-type", default=os.environ.get("MLB_SLATE_PROP_TYPE", ""), help="Fallback prop_type when wide CSV omits prop_type column.")
    ap.add_argument("--market-map-json", default="", help="Optional JSON object prop_type->market_key overrides.")
    ap.add_argument("--drop-line-0-5", action="store_true", help="Drop line 0.5 rows (default keeps them).")
    ap.add_argument(
        "--calibration-json",
        default="",
        help=(
            "Optional isotonic probability calibration JSON to apply to prob_over. "
            "Ignored unless MLB_APPLY_PROBABILITY_CALIBRATION_TO_UPLOAD=1."
        ),
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    et = pytz.timezone("America/New_York")
    et_today = datetime.now(et).strftime("%Y-%m-%d")
    slate_date = (args.slate_date or os.environ.get("SLATE_DATE") or et_today).strip()
    pred_csv = Path(str(args.pred_csv)).expanduser()
    out_csv = Path(str(args.out_csv)).expanduser()
    prepared_feature_dir = _prepared_feature_dir(str(args.prepared_feature_dir or ""), slate_date)
    prop_type_arg = _canonical_prop_type(args.prop_type)
    apply_upload_calibration = _env_flag("MLB_APPLY_PROBABILITY_CALIBRATION_TO_UPLOAD", False)
    calibration_json = str(args.calibration_json or "").strip()
    if apply_upload_calibration and not calibration_json:
        calibration_json = str(os.environ.get("MLB_PROBABILITY_CALIBRATION_JSON", "") or "").strip()
    market_map = _load_market_map(
        arg_json=str(args.market_map_json),
        env_json=str(os.environ.get("MLB_BOOK_UPLOAD_MARKET_MAP_JSON", "")),
    )

    print(f"[mlb-slate-output] slate_date (ET) = {slate_date}")
    print(f"[mlb-slate-output] pred_csv = {pred_csv}")
    print(f"[mlb-slate-output] out_csv = {out_csv}")
    if prepared_feature_dir:
        print(f"[mlb-slate-output] prepared_feature_dir = {prepared_feature_dir}")
    else:
        print("[mlb-slate-output] prepared_feature_dir = <disabled>")

    try:
        df_wide = _load_predictions(pred_csv)
        df_long = _melt_to_long(df_wide, prop_type_arg)
        out = build_slate_output(
            df_long=df_long,
            slate_date=slate_date,
            strict=bool(args.strict),
            drop_line_0_5=bool(args.drop_line_0_5),
            market_map=market_map,
            pred_csv_path=pred_csv,
            prepared_feature_dir=prepared_feature_dir,
            calibration_json=calibration_json,
            apply_upload_calibration=apply_upload_calibration,
        )
    except Exception as exc:
        print(f"[mlb-slate-output] ERROR: {exc}", file=sys.stderr)
        return 1

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    print(f"[mlb-slate-output] wrote {len(out)} rows to {out_csv}")
    print(
        "[mlb-slate-output] prop counts:",
        out["prop_type"].value_counts(dropna=False).sort_index().to_dict(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
