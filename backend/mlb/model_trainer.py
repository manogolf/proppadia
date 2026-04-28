# backend/scripts/model_trainer.py
"""
Train and save per-prop models (LogReg + RandomForest) to local filesystem.

- Primary source: training_examples_v1 (if exists)
- Fallback: model_training_props + merge player_derived_stats for requested features
- Target: outcome ('win'→1, 'loss'→0)
- Saves models to: $MODELS_DIR/{latest,archive} (default /var/data/proppadia/models)
- Embeds exact feature lists used into joblib meta (features_num/features_cat)

Env:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY  (or SUPABASE_ANON_KEY for read-only)
  MODELS_DIR (optional, default /var/data/proppadia/models)
"""
from __future__ import annotations


import os, io, json
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import joblib

from supabase import create_client, Client
from backend.shared.db.pg import pg_fetchall

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score
from pandas.api.types import is_numeric_dtype

# ---- .env (optional) ---------------------------------------------------------
try:
    from dotenv import load_dotenv
    for p in (Path.cwd() / ".env", Path(__file__).resolve().parents[2] / ".env"):
        if p.exists():
            load_dotenv(p, override=False)
except Exception:
    pass

# ---- Config ------------------------------------------------------------------
DEFAULT_DAYS_BACK = 365
DEFAULT_ROW_LIMIT = 50_000

PROP_TYPES = [
    "doubles","earned_runs","hits","hits_allowed","hits_runs_rbis","home_runs",
    "outs_recorded","rbis","runs_rbis","runs_scored","singles","stolen_bases",
    "strikeouts_batting","strikeouts_pitching","total_bases","triples","walks",
    "walks_allowed",
]

# Props that are pitcher-centric (used to drop d7_* windows)
PITCHING_PROPS = {
    "hits_allowed",
    "earned_runs",
    "walks_allowed",
    "strikeouts_pitching",
    "outs_recorded",
}


MODELS_DIR  = Path(os.environ.get("MODELS_DIR", "/var/data/proppadia/models")).resolve()
LATEST_DIR  = MODELS_DIR / "latest"
ARCHIVE_DIR = MODELS_DIR / "archive"

# Feature spec JSON (same sources your registry uses)
FEATURE_JSON_CANDIDATES = [
    Path(os.environ["FEATURE_JSON"]) if os.getenv("FEATURE_JSON") else None,
    Path(__file__).resolve().parents[2] / "backend" / "mlb" / "modeling" / "feature_metadata.json",
    Path(__file__).resolve().parents[2] / "backend" / "mlb" / "modeling" / "feature_metadata_backup.json",
    Path(__file__).resolve().parents[2] / "backend" / "scripts" / "modeling" / "feature_metadata.json",
    Path(__file__).resolve().parents[2] / "backend" / "scripts" / "modeling" / "feature_metadata_backup.json",
]
FEATURE_JSON_CANDIDATES = [p for p in FEATURE_JSON_CANDIDATES if p]

# OneHotEncoder kw compat
try:
    _ = OneHotEncoder(sparse_output=True, handle_unknown="ignore")
    _ONEHOT_KW = dict(sparse_output=True, handle_unknown="ignore")
except TypeError:
    _ONEHOT_KW = dict(sparse=True, handle_unknown="ignore")

def _debug_feature_paths():
    print("Feature JSON search paths:")
    for p in FEATURE_JSON_CANDIDATES:
        print(" -", p, "✓" if p.exists() else "✗")

# before first use of load_feature_spec():
_debug_feature_paths()

# Single source of truth for the training view (public schema via PostgREST)
# Training data source selection:
# - reconcile_csv (default): market+outcome rows CSV + player_derived_stats join
# - base_merge: model_training_props + player_derived_stats join
# - view: explicit FEATURE_VIEW relation via Supabase table API
def _env_enabled(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


TRAIN_FEATURE_SOURCE = str(os.environ.get("TRAIN_FEATURE_SOURCE", "reconcile_csv")).strip().lower()
TRAIN_PROFILE = str(os.environ.get("MLB_TRAIN_PROFILE", "legacy")).strip().lower()
TRAIN_MARKET_ONLY = _env_enabled("MLB_TRAIN_MARKET_ONLY", False) or TRAIN_PROFILE in {
    "market_only",
    "bol_market_only",
}
FEATURE_VIEW = os.environ.get("FEATURE_VIEW", "").strip()
RECONCILE_ROWS_CSV = str(
    os.environ.get("MLB_TRAIN_RECONCILE_ROWS_CSV")
    or os.environ.get("MLB_RECONCILE_ROWS_OUT_CSV")
    or (Path(__file__).resolve().parents[2] / "tmp" / "mlb_base_vs_market_rows.csv")
)
RECONCILE_BOOKMAKER = str(os.environ.get("MLB_TRAIN_RECONCILE_BOOKMAKER", "") or "").strip().lower()
RECONCILE_REQUIRE_TWO_SIDED = str(os.environ.get("MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
RECONCILE_ALLOW_MISSING_PRICE_PROPS = {
    p.strip().lower()
    for p in str(os.environ.get("MLB_TRAIN_RECONCILE_ALLOW_MISSING_PRICE_PROPS", "runs_rbis")).split(",")
    if p.strip()
}
RECONCILE_FALLBACK_BASE_MERGE = str(
    os.environ.get("MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

BVP_FEATURE_SET_TAG = str(
    os.environ.get("MLB_TRAIN_BVP_FEATURE_SET_TAG")
    or os.environ.get("MLB_BVP_FEATURE_SET_TAG")
    or os.environ.get("MLB_PFP_OVERLAP_FEATURE_SET_TAG")
    or "v1"
).strip() or "v1"

ALWAYS_CATEGORICAL_FEATURES = {
    "streak_type",
    "time_of_day_bucket",
    "game_day_of_week",
    "home_team_code",
    "away_team_code",
}

# Brand-new market-native profile: use only OddsAPI/market-context features.
# This intentionally excludes d7/d15/d30 derived stats and BvP/PvB.
MARKET_ONLY_FEATURES: List[str] = [
    "line",
    "prop_value",
    "price_over_american",
    "price_under_american",
    "implied_over",
    "implied_under",
    "implied_over_novig",
    "implied_under_novig",
    "market_hold",
    "home_team_code",
    "away_team_code",
    "game_day_of_week",
]

_BVP_ALIAS_TO_CANONICAL = {
    "bvp_pa_prior": "bvp_plate_appearances",
    "bvp_ab_prior": "bvp_at_bats",
    "bvp_hits_prior": "bvp_hits",
    "bvp_hr_prior": "bvp_home_runs",
    "bvp_bb_prior": "bvp_walks",
    "bvp_so_prior": "bvp_strikeouts",
    "bvp_tb_prior": "bvp_total_bases",
}

# ---- Training thresholds (class balance) -------------------------------------
MIN_CLASS_COUNT = int(os.getenv("MIN_CLASS_COUNT", "100"))
MIN_MINORITY_PCT = float(os.getenv("MIN_MINORITY_PCT", "0.10"))
try:
    import json as _json
    MIN_CLASS_COUNT_BY_PROP = _json.loads(os.getenv("MIN_CLASS_COUNT_BY_PROP", "{}"))
    MIN_MINORITY_PCT_BY_PROP = _json.loads(os.getenv("MIN_MINORITY_PCT_BY_PROP", "{}"))
except Exception:
    MIN_CLASS_COUNT_BY_PROP = {}
    MIN_MINORITY_PCT_BY_PROP = {}

# Optional hits-only recency + line-regime weighting (off by default).
TRAIN_HITS_WEIGHTING = _env_enabled("MLB_TRAIN_HITS_WEIGHTING", False)
TRAIN_HITS_WEIGHTING_HALFLIFE_DAYS = max(
    float(os.getenv("MLB_TRAIN_HITS_WEIGHTING_HALFLIFE_DAYS", "90")),
    1.0,
)
TRAIN_HITS_WEIGHTING_LINE_GE_2_5 = float(os.getenv("MLB_TRAIN_HITS_WEIGHTING_LINE_GE_2_5", "4.0"))
TRAIN_HITS_WEIGHTING_LINE_GE_1_5 = float(os.getenv("MLB_TRAIN_HITS_WEIGHTING_LINE_GE_1_5", "1.5"))
TRAIN_HITS_WEIGHTING_LINE_LT_1_5 = float(os.getenv("MLB_TRAIN_HITS_WEIGHTING_LINE_LT_1_5", "1.0"))

TB_COMPONENT_FEATURES = [
    "d7_at_bats",
    "d15_at_bats",
    "d30_at_bats",
    "d7_extra_base_hits",
    "d15_extra_base_hits",
    "d30_extra_base_hits",
    "tb_per_ab_d7",
    "tb_per_ab_d15",
    "tb_per_ab_d30",
    "hits_per_ab_d7",
    "extra_base_per_ab_d7",
    "avg_ab_per_game_d7",
    "avg_ab_per_game_d15",
    "avg_ab_per_game_d30",
    "high_opportunity_games_last_10",
    "tb_opportunity_interaction",
    "power_opportunity_interaction",
    "tb_per_hit_d7",
    "tb_per_hit_d15",
    "tb_per_hit_d30",
    "extra_base_rate_d7",
    "d7_doubles",
    "d15_doubles",
    "d30_doubles",
    "high_tb_games_last_10",
    "rolling_tb_std_dev",
]


# ---- Utilities ---------------------------------------------------------------
def _atomic_write_bytes(path: Path, blob: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(blob)
    os.replace(tmp, path)

def _supabase_client() -> Optional[Client]:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        return None
    return create_client(url, key)

def _load_feature_spec() -> Dict[str, Any]:
    for p in FEATURE_JSON_CANDIDATES:
        try:
            if p and p.exists():
                return json.loads(p.read_text())
        except Exception:
            continue
    return {}

def _chunked(xs: List[Any], n: int) -> List[List[Any]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]

def _pg_data(resp):
    """Normalize PostgREST response to a list of rows across supabase-py versions."""
    # supabase-py v2 returns object with .data
    if hasattr(resp, "data"):
        return resp.data or []
    # some versions return dict
    if isinstance(resp, dict):
        return resp.get("data", []) or []
    # some return list directly
    if isinstance(resp, list):
        return resp
    return []


def _table_has_column(table_name: str, column_name: str) -> bool:
    rows = pg_fetchall(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'mlb'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (str(table_name), str(column_name)),
    )
    return bool(rows)


def _normalize_pfp_features_blob(features: Any) -> Dict[str, float]:
    payload: Dict[str, Any]
    if isinstance(features, dict):
        payload = features
    elif isinstance(features, str):
        try:
            parsed = json.loads(features)
            payload = parsed if isinstance(parsed, dict) else {}
        except Exception:
            payload = {}
    else:
        payload = {}

    out: Dict[str, float] = {}
    for k, v in payload.items():
        key = str(k).strip()
        if not key.startswith("bvp_"):
            continue
        try:
            out[key] = float(v)
        except Exception:
            continue

    # Normalize legacy aliases to canonical names.
    for alias, canonical in _BVP_ALIAS_TO_CANONICAL.items():
        if canonical not in out and alias in out:
            out[canonical] = out[alias]
    return out


def _fetch_pfp_feature_rows(
    sb: Optional[Client],
    *,
    game_ids: List[int],
    feature_set_tag: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for chunk in _chunked(game_ids, 1000):
        if sb is not None:
            r = (
                sb.schema("mlb").table("prop_features_precomputed")
                .select("prop_type,player_id,game_id,features")
                .eq("feature_set_tag", feature_set_tag)
                .in_("game_id", chunk)
                .execute()
            )
            part = _pg_data(r)
        else:
            placeholders = ",".join(["%s"] * len(chunk))
            sql = (
                "SELECT prop_type, player_id, game_id, features "
                "FROM mlb.prop_features_precomputed "
                f"WHERE feature_set_tag = %s AND game_id IN ({placeholders})"
            )
            part = pg_fetchall(sql, tuple([feature_set_tag, *chunk]))
        if part:
            rows.extend(part)
    return rows


def _merge_pfp_bvp_features(sb: Optional[Client], df: pd.DataFrame, feat_cols: List[str]) -> pd.DataFrame:
    """Merge BvP fields from prop_features_precomputed into the training frame."""
    bvp_cols = [c for c in feat_cols if str(c).startswith("bvp_")]
    if not bvp_cols:
        return df
    required = {"prop_type", "player_id", "game_id"}
    if not required.issubset(df.columns):
        return df

    out = df.copy()
    out["prop_type"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")

    keys_df = out[list(required)].dropna().drop_duplicates()
    if keys_df.empty:
        return out
    keys_df["player_id"] = keys_df["player_id"].astype(int)
    keys_df["game_id"] = keys_df["game_id"].astype(int)
    wanted_keys = {
        (str(r["prop_type"]).strip().lower(), int(r["player_id"]), int(r["game_id"]))
        for _, r in keys_df.iterrows()
    }
    game_ids = sorted({int(g) for g in keys_df["game_id"].tolist()})
    pfp_rows = _fetch_pfp_feature_rows(sb, game_ids=game_ids, feature_set_tag=BVP_FEATURE_SET_TAG)
    if not pfp_rows:
        return out

    merged_rows: List[Dict[str, Any]] = []
    for row in pfp_rows:
        try:
            key = (
                str(row.get("prop_type") or "").strip().lower(),
                int(row.get("player_id")),
                int(row.get("game_id")),
            )
        except Exception:
            continue
        if key not in wanted_keys:
            continue
        feats = _normalize_pfp_features_blob(row.get("features"))
        if not feats:
            continue
        entry: Dict[str, Any] = {
            "prop_type": key[0],
            "player_id": key[1],
            "game_id": key[2],
        }
        for c in bvp_cols:
            if c in feats:
                entry[c] = feats[c]
        if len(entry) > 3:
            merged_rows.append(entry)
    if not merged_rows:
        return out

    pfp_df = pd.DataFrame(merged_rows).drop_duplicates(
        subset=["prop_type", "player_id", "game_id"],
        keep="first",
    )
    out = out.merge(pfp_df, on=["prop_type", "player_id", "game_id"], how="left", suffixes=("", "_pfp"))
    for c in bvp_cols:
        pcol = f"{c}_pfp"
        if pcol not in out.columns:
            continue
        base = pd.to_numeric(out[c], errors="coerce") if c in out.columns else pd.Series(np.nan, index=out.index)
        fill = pd.to_numeric(out[pcol], errors="coerce")
        out[c] = base.where(base.notna(), fill)
        out.drop(columns=[pcol], inplace=True)
    return out


# ---- Data access -------------------------------------------------------------
def _fetch_from_view(sb: Optional[Client], prop_type: str, days_back: int, limit: int, cols: List[str]) -> Optional[pd.DataFrame]:
    """Use consolidated feature view/table (joined, de-duped, backfills applied)."""
    if not FEATURE_VIEW or sb is None:
        return None
    since_date = (datetime.utcnow() - timedelta(days=days_back)).date().isoformat()
    try:
        q = (
            sb.table(FEATURE_VIEW)
              .select("*")                      # tolerate per-prop columns
              .eq("prop_type", prop_type)
              .gte("game_date", since_date)
        )
        # prefer range for broader client compat
        if hasattr(q, "range") and isinstance(limit, int):
            q = q.range(0, max(0, limit - 1))
        else:
            q = q.limit(limit)
        resp = q.execute()                      # ← ensure we actually execute
        rows = _pg_data(resp)
        print(f"[trainer] source=view:{FEATURE_VIEW} prop={prop_type} rows={len(rows)}")
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[trainer] view fetch failed ({FEATURE_VIEW}) for {prop_type}: {e}")
        return None


def _fetch_base_rows_pg(prop_type: str, since_date: str, limit: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT *
    FROM mlb.model_training_props
    WHERE lower(trim(prop_type)) = lower(trim(%s))
      AND line IS NOT NULL
      AND prop_value IS NOT NULL
      AND game_date::date >= %s::date
      AND lower(trim(coalesce(outcome, ''))) IN ('win', 'loss')
    ORDER BY game_date DESC
    LIMIT %s
    """
    return pg_fetchall(sql, (prop_type, since_date, int(limit)))


def _fetch_base_and_merge(sb: Optional[Client], prop_type: str, days_back: int, limit: int, feat_cols: List[str]) -> pd.DataFrame:
    """Fallback: model_training_props + join derived features by (player_id, game_id)."""
    since_date = (datetime.utcnow() - timedelta(days=days_back)).date().isoformat()
    if sb is not None:
        resp = (
            sb.schema("mlb").table("model_training_props")
              .select("*")
              .eq("prop_type", prop_type)
              .not_.is_("line", "null")
              .not_.is_("prop_value", "null")
              .gte("game_date", since_date)
              .order("game_date", desc=True)
              .limit(limit)
              .execute()
        )
        rows = resp.data or []
        rows = [r for r in rows if r.get("outcome") in ("win", "loss")]
    else:
        rows = _fetch_base_rows_pg(prop_type, since_date, limit)

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"[trainer] source=fallback:base empty prop={prop_type}")
        return df

    df = _add_time_features(df)
    df = _merge_derived_features(sb, df, feat_cols)
    print(f"[trainer] source=fallback:base+merge prop={prop_type} rows={len(df)}")
    return df


def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "game_date" not in out.columns:
        return out
    try:
        dt = pd.to_datetime(out["game_date"])
    except Exception:
        dt = pd.to_datetime(out["game_date"], errors="coerce")
    hour = getattr(dt.dt, "hour", pd.Series([None] * len(out)))
    bucket = np.where(hour < 12, "morning", np.where(hour < 18, "afternoon", "night"))
    dow = dt.dt.day_name().str[:3]
    out["time_of_day_bucket"] = bucket
    out["game_day_of_week"] = dow
    return out


def _merge_derived_features(sb: Optional[Client], df: pd.DataFrame, feat_cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for k in ("player_id", "game_id"):
        if k in out.columns:
            out[k] = pd.to_numeric(out[k], errors="coerce")

    # merge in derived features for the exact games we have
    pairs = out[["player_id", "game_id"]].dropna().drop_duplicates() if {"player_id", "game_id"}.issubset(out.columns) else pd.DataFrame()
    game_ids = pairs["game_id"].astype(str).tolist()

    derived_frames: List[pd.DataFrame] = []
    for chunk in _chunked(game_ids, 1000):
        if sb is not None:
            r = (
                sb.schema("mlb").table("player_derived_stats")
                .select("*")
                .in_("game_id", chunk)
                .execute()
            )
            part = _pg_data(r)
        else:
            placeholders = ",".join(["%s"] * len(chunk))
            sql = f"SELECT * FROM mlb.player_derived_stats WHERE game_id IN ({placeholders})"
            part = pg_fetchall(sql, tuple(chunk))
        if part:
            derived_frames.append(pd.DataFrame(part))
    if derived_frames:
        derived = pd.concat(derived_frames, ignore_index=True)
    else:
        derived = pd.DataFrame(columns=["player_id","game_id"])

    # coerce keys on derived as well
    for k in ("player_id", "game_id"):
        if k in derived.columns:
            derived[k] = pd.to_numeric(derived[k], errors="coerce")

    if {"player_id", "game_id"}.issubset(out.columns):
        out = out.merge(derived, on=["player_id", "game_id"], how="left", suffixes=("", "_der"))

    out = _merge_pfp_bvp_features(sb, out, feat_cols)

    # ensure all requested features exist
    for f in feat_cols:
        if f not in out.columns:
            out[f] = np.nan
    return out


def _fetch_player_stats_tb_history(
    player_ids: List[int],
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    if not player_ids:
        return pd.DataFrame(columns=["player_id", "game_id", "game_date", "total_bases", "at_bats"])
    has_at_bats = _table_has_column("player_stats", "at_bats")
    at_bats_expr = "COALESCE(ps.at_bats, 0)::numeric AS at_bats" if has_at_bats else "NULL::numeric AS at_bats"
    frames: List[pd.DataFrame] = []
    unique_ids = sorted({int(pid) for pid in player_ids})
    for chunk in _chunked(unique_ids, 500):
        placeholders = ",".join(["%s"] * len(chunk))
        sql = f"""
            SELECT
                ps.player_id,
                ps.game_id,
                ps.game_date::date AS game_date,
                COALESCE(ps.total_bases, 0)::numeric AS total_bases,
                {at_bats_expr}
            FROM mlb.player_stats ps
            WHERE ps.player_id IN ({placeholders})
              AND ps.game_date >= %s::date
              AND ps.game_date <= %s::date
            ORDER BY ps.player_id, ps.game_date, ps.game_id
        """
        params = tuple(chunk) + (start_date, end_date)
        rows = pg_fetchall(sql, params)
        if rows:
            frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame(columns=["player_id", "game_id", "game_date", "total_bases", "at_bats"])
    out = pd.concat(frames, ignore_index=True)
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
    out["total_bases"] = pd.to_numeric(out["total_bases"], errors="coerce")
    out["at_bats"] = pd.to_numeric(out.get("at_bats"), errors="coerce")
    out = out.dropna(subset=["player_id", "game_id", "game_date"]).copy()
    return out


def _add_total_bases_component_features(
    df: pd.DataFrame,
    *,
    quiet: bool = True,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()

    for window in (7, 15, 30):
        ab_col = f"d{window}_at_bats"
        out[ab_col] = pd.to_numeric(out.get(ab_col), errors="coerce")
        out[f"avg_ab_per_game_d{window}"] = out[ab_col]

    # Core rolling component features from existing derived windows.
    for window in (7, 15, 30):
        tb_col = f"d{window}_total_bases"
        hits_col = f"d{window}_hits"
        ab_col = f"d{window}_at_bats"
        extra_col = f"d{window}_extra_base_hits"
        ratio_col = f"tb_per_hit_d{window}"
        tb_ab_col = f"tb_per_ab_d{window}"

        tb = pd.to_numeric(out.get(tb_col), errors="coerce")
        hits = pd.to_numeric(out.get(hits_col), errors="coerce")
        at_bats = pd.to_numeric(out.get(ab_col), errors="coerce")
        out[extra_col] = tb - hits
        out[ratio_col] = np.where(hits > 0, tb / hits, np.nan)
        out[tb_ab_col] = np.where(at_bats > 0, tb / at_bats, np.nan)

    d7_hits = pd.to_numeric(out.get("d7_hits"), errors="coerce")
    d7_extra = pd.to_numeric(out.get("d7_extra_base_hits"), errors="coerce")
    d7_at_bats = pd.to_numeric(out.get("d7_at_bats"), errors="coerce")
    out["extra_base_rate_d7"] = np.where(d7_hits > 0, d7_extra / d7_hits, np.nan)
    out["hits_per_ab_d7"] = np.where(d7_at_bats > 0, d7_hits / d7_at_bats, np.nan)
    out["extra_base_per_ab_d7"] = np.where(d7_at_bats > 0, d7_extra / d7_at_bats, np.nan)

    # Optional rolling doubles windows are direct pass-throughs when present.
    for col in ("d7_doubles", "d15_doubles", "d30_doubles"):
        out[col] = pd.to_numeric(out.get(col), errors="coerce")

    # Appearance-level distribution proxies from prior player game logs.
    out["high_tb_games_last_10"] = np.nan
    out["high_opportunity_games_last_10"] = np.nan
    out["rolling_tb_std_dev"] = np.nan
    if {"player_id", "game_id", "game_date"}.issubset(out.columns):
        keys = out[["player_id", "game_id", "game_date"]].copy()
        keys["player_id"] = pd.to_numeric(keys["player_id"], errors="coerce")
        keys["game_id"] = pd.to_numeric(keys["game_id"], errors="coerce")
        keys["game_date"] = pd.to_datetime(keys["game_date"], errors="coerce")
        keys = keys.dropna(subset=["player_id", "game_id", "game_date"]).drop_duplicates()

        if not keys.empty:
            min_hist = (keys["game_date"].min() - pd.Timedelta(days=90)).date().isoformat()
            max_hist = keys["game_date"].max().date().isoformat()
            try:
                hist = _fetch_player_stats_tb_history(
                    keys["player_id"].astype(int).tolist(),
                    start_date=min_hist,
                    end_date=max_hist,
                )
            except Exception as exc:
                if not quiet:
                    print(f"[features] TB component history fetch skipped: {exc}")
                hist = pd.DataFrame(
                    columns=["player_id", "game_id", "game_date", "total_bases"]
                )
            if not hist.empty:
                hist = hist.sort_values(["player_id", "game_date", "game_id"]).copy()
                hist["tb_ge_2"] = (hist["total_bases"] >= 2).astype(float)
                hist["ab_ge_4"] = (pd.to_numeric(hist["at_bats"], errors="coerce") >= 4).astype(float)
                by_player = hist.groupby("player_id", sort=False)
                hist["d7_at_bats"] = by_player["at_bats"].transform(
                    lambda s: s.shift(1).rolling(7, min_periods=1).mean()
                )
                hist["d15_at_bats"] = by_player["at_bats"].transform(
                    lambda s: s.shift(1).rolling(15, min_periods=1).mean()
                )
                hist["d30_at_bats"] = by_player["at_bats"].transform(
                    lambda s: s.shift(1).rolling(30, min_periods=1).mean()
                )
                hist["high_tb_games_last_10"] = by_player["tb_ge_2"].transform(
                    lambda s: s.shift(1).rolling(10, min_periods=1).sum()
                )
                hist["high_opportunity_games_last_10"] = by_player["ab_ge_4"].transform(
                    lambda s: s.shift(1).rolling(10, min_periods=1).sum()
                )
                hist["rolling_tb_std_dev"] = by_player["total_bases"].transform(
                    lambda s: s.shift(1).rolling(10, min_periods=2).std(ddof=0)
                )
                hist_features = hist[
                    [
                        "player_id",
                        "game_id",
                        "d7_at_bats",
                        "d15_at_bats",
                        "d30_at_bats",
                        "high_tb_games_last_10",
                        "high_opportunity_games_last_10",
                        "rolling_tb_std_dev",
                    ]
                ].drop_duplicates(subset=["player_id", "game_id"], keep="last")
                out = out.merge(
                    hist_features,
                    on=["player_id", "game_id"],
                    how="left",
                    suffixes=("", "_tbhist"),
                )
                for col in (
                    "d7_at_bats",
                    "d15_at_bats",
                    "d30_at_bats",
                    "high_tb_games_last_10",
                    "high_opportunity_games_last_10",
                    "rolling_tb_std_dev",
                ):
                    alias = f"{col}_tbhist"
                    if alias in out.columns:
                        base = pd.to_numeric(out[col], errors="coerce")
                        fill = pd.to_numeric(out[alias], errors="coerce")
                        out[col] = base.where(base.notna(), fill)
                        out.drop(columns=[alias], inplace=True)

    # Recompute AB-normalized features after history fallback filled at-bats.
    for window in (7, 15, 30):
        tb = pd.to_numeric(out.get(f"d{window}_total_bases"), errors="coerce")
        ab = pd.to_numeric(out.get(f"d{window}_at_bats"), errors="coerce")
        out[f"avg_ab_per_game_d{window}"] = ab
        out[f"tb_per_ab_d{window}"] = np.where(ab > 0, tb / ab, np.nan)

    d7_hits = pd.to_numeric(out.get("d7_hits"), errors="coerce")
    d7_extra = pd.to_numeric(out.get("d7_extra_base_hits"), errors="coerce")
    d7_at_bats = pd.to_numeric(out.get("d7_at_bats"), errors="coerce")
    out["hits_per_ab_d7"] = np.where(d7_at_bats > 0, d7_hits / d7_at_bats, np.nan)
    out["extra_base_per_ab_d7"] = np.where(d7_at_bats > 0, d7_extra / d7_at_bats, np.nan)
    out["tb_opportunity_interaction"] = pd.to_numeric(out.get("tb_per_ab_d7"), errors="coerce") * pd.to_numeric(
        out.get("avg_ab_per_game_d7"),
        errors="coerce",
    )
    out["power_opportunity_interaction"] = pd.to_numeric(
        out.get("extra_base_rate_d7"),
        errors="coerce",
    ) * pd.to_numeric(
        out.get("avg_ab_per_game_d7"),
        errors="coerce",
    )

    if not quiet:
        coverage_parts: List[str] = []
        for col in TB_COMPONENT_FEATURES:
            if col not in out.columns:
                coverage_parts.append(f"{col}=missing")
                continue
            cov = float(pd.to_numeric(out[col], errors="coerce").notna().mean())
            coverage_parts.append(f"{col}={cov:.1%}")
        print("[features] added TB at-bat/opportunity features: " + ", ".join(coverage_parts))
    return out


def _fetch_reconcile_and_merge(sb: Optional[Client], prop_type: str, days_back: int, limit: int, feat_cols: List[str]) -> pd.DataFrame:
    """
    Preferred source: row-level reconcile CSV (market lines + realized outcomes),
    then merge player_derived_stats for model features.
    """
    path = Path(RECONCILE_ROWS_CSV).expanduser()
    if not path.exists():
        print(f"[trainer] source=reconcile_csv missing file: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[trainer] source=reconcile_csv read failed ({path}): {e}")
        return pd.DataFrame()
    if df.empty:
        print(f"[trainer] source=reconcile_csv empty file: {path}")
        return df

    since_date = pd.Timestamp((datetime.utcnow() - timedelta(days=days_back)).date())
    out = df.copy()
    prop_series = out["prop_type"] if "prop_type" in out.columns else pd.Series([""] * len(out), index=out.index)
    out["prop_type"] = prop_series.astype(str).str.strip().str.lower()
    out = out[out["prop_type"] == str(prop_type).strip().lower()]
    if out.empty:
        print(f"[trainer] source=reconcile_csv no rows for prop={prop_type}")
        return out

    if RECONCILE_BOOKMAKER:
        book_col = out["bookmaker_key"] if "bookmaker_key" in out.columns else pd.Series([""] * len(out), index=out.index)
        out = out[book_col.astype(str).str.strip().str.lower().eq(RECONCILE_BOOKMAKER)]
        if out.empty:
            print(f"[trainer] source=reconcile_csv no rows for prop={prop_type} bookmaker={RECONCILE_BOOKMAKER}")
            return out

    if "game_date" in out.columns:
        out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
        out = out[out["game_date"] >= since_date]

    out["line"] = pd.to_numeric(out.get("line"), errors="coerce")
    out = out[out["line"].notna()]

    allow_missing_prices = (not TRAIN_MARKET_ONLY) and str(prop_type).strip().lower() in RECONCILE_ALLOW_MISSING_PRICE_PROPS
    if RECONCILE_REQUIRE_TWO_SIDED and not allow_missing_prices:
        px_o = pd.to_numeric(out.get("price_over_american"), errors="coerce")
        px_u = pd.to_numeric(out.get("price_under_american"), errors="coerce")
        out = out[px_o.notna() & px_u.notna()]

    over_outcome_series = (
        out["actual_over_outcome"]
        if "actual_over_outcome" in out.columns
        else pd.Series([""] * len(out), index=out.index)
    )
    over_outcome = over_outcome_series.astype(str).str.strip().str.lower()
    out = out[over_outcome.isin({"win", "loss"})].copy()
    if out.empty:
        print(f"[trainer] source=reconcile_csv no labeled rows for prop={prop_type}")
        return out
    out["y"] = (over_outcome == "win").astype("Int64")

    # Keep compatibility with legacy preprocessing fallbacks.
    out["prop_value"] = out["line"]

    # De-duplicate repeated records for same player/game/line.
    dedupe_cols = [c for c in ("player_id", "game_id", "line") if c in out.columns]
    if dedupe_cols:
        out = out.drop_duplicates(subset=dedupe_cols, keep="first")

    if "game_date" in out.columns:
        out = out.sort_values("game_date", ascending=False, na_position="last")
    if int(limit) > 0:
        out = out.head(int(limit))

    out = _add_time_features(out)
    if TRAIN_MARKET_ONLY:
        # Keep this profile clean-room: no player_derived_stats/BvP hydration.
        for f in feat_cols:
            if f not in out.columns:
                out[f] = np.nan
        print(f"[trainer] source=reconcile_csv market_only=1 prop={prop_type} rows={len(out)} file={path}")
        return out

    out = _merge_derived_features(sb, out, feat_cols)
    print(f"[trainer] source=reconcile_csv prop={prop_type} rows={len(out)} file={path}")
    return out


def fetch_training_rows(
    sb: Optional[Client],
    prop_type: str,
    days_back: int,
    limit: int,
    feat_cols: List[str],
    *,
    quiet: bool = True,
) -> pd.DataFrame:
    prop_key = str(prop_type).strip().lower()
    if TRAIN_FEATURE_SOURCE in {"reconcile", "reconcile_csv"}:
        df = _fetch_reconcile_and_merge(sb, prop_type, days_back, limit, feat_cols)
        if not df.empty and prop_key == "total_bases":
            df = _add_total_bases_component_features(df, quiet=quiet)
        if not df.empty:
            return df
        if not RECONCILE_FALLBACK_BASE_MERGE:
            print(f"[trainer] source=reconcile_csv strict mode; no fallback for prop={prop_type}")
            return df
        print(f"[trainer] source=reconcile_csv fallback->base_merge prop={prop_type}")
    if TRAIN_FEATURE_SOURCE == "view":
        df = _fetch_from_view(sb, prop_type, days_back, limit, feat_cols)
        if isinstance(df, pd.DataFrame) and not df.empty and prop_key == "total_bases":
            df = _add_total_bases_component_features(df, quiet=quiet)
        if df is not None:
            return df
    df = _fetch_base_and_merge(sb, prop_type, days_back, limit, feat_cols)
    if not df.empty and prop_key == "total_bases":
        df = _add_total_bases_component_features(df, quiet=quiet)
    return df


# ---- Preprocessing / pipelines ----------------------------------------------
def _hits_sample_weights(df: pd.DataFrame) -> np.ndarray:
    n = len(df)
    if n <= 0:
        return np.array([], dtype="float64")

    line = pd.to_numeric(df.get("line"), errors="coerce")
    line_w = np.where(
        line >= 2.5,
        TRAIN_HITS_WEIGHTING_LINE_GE_2_5,
        np.where(
            line >= 1.5,
            TRAIN_HITS_WEIGHTING_LINE_GE_1_5,
            TRAIN_HITS_WEIGHTING_LINE_LT_1_5,
        ),
    ).astype("float64")
    line_w = np.where(pd.isna(line), TRAIN_HITS_WEIGHTING_LINE_LT_1_5, line_w)

    age_days = np.zeros(n, dtype="float64")
    if "game_date" in df.columns:
        game_date = pd.to_datetime(df["game_date"], errors="coerce")
        max_date = game_date.max()
        if not pd.isna(max_date):
            raw_age = (max_date - game_date).dt.days.to_numpy(dtype="float64")
            age_days = np.where(np.isfinite(raw_age), np.maximum(raw_age, 0.0), 0.0)

    recency = np.exp(-age_days / float(TRAIN_HITS_WEIGHTING_HALFLIFE_DAYS))
    w = recency * line_w
    w = np.where(np.isfinite(w) & (w > 0.0), w, 1.0)
    return w.astype("float64")


def _prep_frame(df: pd.DataFrame, *, prop_type: str | None = None, quiet: bool = True) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    df = df.copy()

    # --- choose label source (strict) ---
    # Training target must be "actual over side" (1=over, 0=under), not generic win/loss.
    label_source = None
    if "y" in df.columns:
        y = pd.to_numeric(df["y"], errors="coerce")
        y = y.where(y.isin([0, 1]))
        df["y"] = y.astype("Int64")
        label_source = "provided(y)"
    elif {"over_under", "outcome"}.issubset(df.columns):
        ou = df["over_under"].astype(str).str.strip().str.lower()
        oc = df["outcome"].astype(str).str.strip().str.lower()
        y = pd.Series([pd.NA] * len(df), dtype="Int64")
        # If the pick was OVER and it won, actual side was over.
        y[(ou == "over") & (oc == "win")] = 1
        y[(ou == "over") & (oc == "loss")] = 0
        # If the pick was UNDER and it won, actual side was under.
        y[(ou == "under") & (oc == "win")] = 0
        y[(ou == "under") & (oc == "loss")] = 1
        df["y"] = y
        label_source = "derived(over_under+outcome)"
    elif {"result", "prop_value"}.issubset(df.columns):
        # derive: OVER wins if actual result > line
        r = pd.to_numeric(df["result"], errors="coerce")
        pv = pd.to_numeric(df["prop_value"], errors="coerce")
        df["y"] = (r > pv).astype("Int64")
        label_source = "derived(result>prop_value)"
    elif "status" in df.columns and df["status"].isin(["win", "loss"]).any():
        df["y"] = (df["status"] == "win").astype(int)
        label_source = "status"
    else:
        df["y"] = pd.Series([pd.NA] * len(df), dtype="Int64")
        label_source = "none"

    # drop unlabeled
    df = df[df["y"].notna()].copy()
    df["y"] = df["y"].astype(int)

    # log the label source early
    try:
        cnt = df["y"].value_counts().to_dict()
        print(f"[trainer] label_source={label_source} y_counts={cnt}")
    except Exception:
        pass

    # coerce binary flags
    for col in ("is_home","is_pitcher"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # sample weights: uniform unless hits-only weighting is explicitly enabled.
    w = np.ones(len(df), dtype="float64")
    prop_key = str(prop_type or "").strip().lower()
    if TRAIN_HITS_WEIGHTING and prop_key == "hits":
        w = _hits_sample_weights(df)
        if not quiet and len(w):
            print(
                "[trainer] hits_weighting=on "
                f"rows={len(w)} half_life_days={TRAIN_HITS_WEIGHTING_HALFLIFE_DAYS} "
                f"line_w={{lt1.5:{TRAIN_HITS_WEIGHTING_LINE_LT_1_5},"
                f"ge1.5:{TRAIN_HITS_WEIGHTING_LINE_GE_1_5},"
                f"ge2.5:{TRAIN_HITS_WEIGHTING_LINE_GE_2_5}}} "
                f"weight_mean={float(np.mean(w)):.4f} weight_min={float(np.min(w)):.4f} "
                f"weight_max={float(np.max(w)):.4f}"
            )
    df["sample_weight"] = w
    return df


def drop_all_null_fit_features(
    df_fit: pd.DataFrame,
    numeric_features: List[str],
    categorical_features: List[str],
    *,
    quiet: bool = True,
) -> tuple[pd.DataFrame, List[str], List[str], List[str]]:
    """
    Prune columns that are entirely missing in the actual fit frame.
    This runs immediately before any sklearn imputer fit.
    """
    out = df_fit.copy()
    dropped: List[str] = []
    keep_num: List[str] = []
    keep_cat: List[str] = []

    for col in numeric_features:
        if col not in out.columns:
            continue
        # Coerce at fit-time to match downstream median-imputer semantics.
        coerced = pd.to_numeric(out[col], errors="coerce")
        out[col] = coerced
        if coerced.isna().all():
            dropped.append(col)
            continue
        keep_num.append(col)

    for col in categorical_features:
        if col not in out.columns:
            continue
        vals = out[col]
        if vals.dtype == object:
            vals = vals.replace("", np.nan)
            out[col] = vals
        if vals.isna().all():
            dropped.append(col)
            continue
        keep_cat.append(col)

    if dropped and not quiet:
        print("[features] dropping all-null fit features: " + ", ".join(sorted(set(dropped))))

    return out, keep_num, keep_cat, dropped


def build_pipeline(num_cols: List[str], cat_cols: List[str]):
    num_transform = Pipeline([("imputer", SimpleImputer(strategy="median"))])
    cat_transform = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(**_ONEHOT_KW)),
    ])
    pre = ColumnTransformer(
        transformers=[
            ("num", num_transform, num_cols),
            ("cat", cat_transform, cat_cols),
        ],
        remainder="drop",
        sparse_threshold=0.3,
    )
    lr = LogisticRegression(max_iter=1000)
    rf = RandomForestClassifier(n_estimators=300, max_depth=None, n_jobs=-1, random_state=42, 
    class_weight="balanced"
    )
    lr_cal = CalibratedClassifierCV(lr, method="isotonic", cv=3)
    pipe_lr = Pipeline([("pre", pre), ("clf", lr_cal)])
    pipe_rf = Pipeline([("pre", pre), ("clf", rf)])
    return pipe_lr, pipe_rf


# ---- Trainer -----------------------------------------------------------------
def train_models_for_prop(prop_type: str, *, days_back=DEFAULT_DAYS_BACK, limit=DEFAULT_ROW_LIMIT, quiet=True):
    sb = _supabase_client()
    if sb is None and not quiet:
        print("[trainer] SUPABASE_URL/key not set; using DATABASE_URL/SUPABASE_DB_URL via direct Postgres fallback.")

    # 1) expected features
    if TRAIN_MARKET_ONLY:
        feat_list = list(MARKET_ONLY_FEATURES)
        if not quiet:
            print(f"[trainer] profile=market_only prop={prop_type} features={len(feat_list)}")
    else:
        # legacy universe from repo metadata
        spec_all = _load_feature_spec()
        spec = spec_all.get(prop_type) or {}
        feat_list = (
            spec.get("random_forest")
            or spec.get("rf")
            or spec.get("logistic_regression")
            or spec.get("lr")
            or spec.get("features")
            or []
        )
    if not feat_list:
        if not quiet:
            print(f"⏭️  {prop_type}: no feature list in feature_metadata.json; skipping.")
        return None

    # 2) fetch rows (view or fallback merge)
    df = fetch_training_rows(sb, prop_type, days_back, limit, feat_list, quiet=quiet)
    if df.empty:
        if not quiet:
            print(f"⏭️  {prop_type}: no training rows.")
        return None

    # 3) prep labels/weights
    df = _prep_frame(df, prop_type=prop_type, quiet=quiet)
    if df.empty or df["y"].nunique() < 2:
        if not quiet:
            print(f"⏭️  {prop_type}: target has a single class or no labeled rows; skipping.")
        return None

    # ⬇️ Insert the class-balance guard here
    threshold = int(MIN_CLASS_COUNT_BY_PROP.get(prop_type, MIN_CLASS_COUNT))
    minority_threshold = float(MIN_MINORITY_PCT_BY_PROP.get(prop_type, MIN_MINORITY_PCT))
    pos = int((df["y"] == 1).sum())
    neg = int((df["y"] == 0).sum())
    total = max(1, pos + neg)
    minority_rate = min(pos, neg) / float(total)
    if pos < threshold or neg < threshold:
        if not quiet:
            print(f"⏭️  {prop_type}: too few positives/negatives "
                f"(pos={pos}, neg={neg}, threshold={threshold}); skipping.")
        return None
    if minority_rate < minority_threshold:
        if not quiet:
            print(
                f"⏭️  {prop_type}: label imbalance too high "
                f"(pos={pos}, neg={neg}, minority_rate={minority_rate:.3f}, "
                f"min_minority_pct={minority_threshold:.3f}); skipping."
            )
        return None

    # --- Feature availability policy + pitching-specific trim ---
    # 1) Coerce candidate numeric features first, then drop true all-null numerics.
    numeric_candidates = [c for c in feat_list if c not in ALWAYS_CATEGORICAL_FEATURES and c in df.columns]
    for c in numeric_candidates:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    all_null_numeric = [c for c in numeric_candidates if df[c].isna().all()]
    if all_null_numeric and not quiet:
        print(f"[features] dropping all-null numeric features: {', '.join(sorted(all_null_numeric))}")
    feat_list = [c for c in feat_list if c not in all_null_numeric]

    # 2) For pitching props, drop d7* windows (rotation → weak coverage/signal)
    if prop_type in PITCHING_PROPS:
        drop_d7 = [c for c in feat_list if c.startswith("d7_") or c == "rolling_result_avg_7"]
        if drop_d7 and not quiet:
            print(f"ℹ️  {prop_type}: dropping pitcher d7-window features: {sorted(drop_d7)}")
        feat_list = [c for c in feat_list if c not in drop_d7]

    # (Informational) coverage log only — do NOT drop low-coverage columns
    if not quiet:
        cov = []
        for c in feat_list:
            if c in df.columns:
                cov.append((c, float(df[c].notna().mean())))
        cov.sort(key=lambda x: x[1])
        low = [f"{c}:{int(p*100)}%" for c,p in cov if p < 0.60]
        if low:
            print("ℹ️  {0}: low-coverage kept → {1}".format(prop_type, ", ".join(low[:12]) + (" ..." if len(low)>12 else "")))

    # 3) determine num/cat AFTER pruning

    num_used = [c for c in feat_list if c in df.columns and (is_numeric_dtype(df[c]) and c not in ALWAYS_CATEGORICAL_FEATURES)]
    cat_used = [c for c in feat_list if c in df.columns and (not is_numeric_dtype(df[c]) or c in ALWAYS_CATEGORICAL_FEATURES)]

    # 4) add missingness indicators for numeric features that have NaNs
    miss_inds = []
    for c in num_used:
        if df[c].isna().any():
            mcol = f"isna__{c}"
            df[mcol] = df[c].isna().astype(int)
            miss_inds.append(mcol)
    num_used = num_used + miss_inds

    cols_used = num_used + cat_used
    if not cols_used:
        if not quiet:
            print(f"⏭️  {prop_type}: no usable features after pruning; skipping.")
        return None

    # Coverage hint
    expected = set(feat_list)
    used = set(cols_used)
    if not quiet and expected:
        cov = len(used & expected) / max(1, len(expected))
        if cov < 0.6:
            print(f"⚠️  {prop_type}: feature coverage {cov:.0%} ({len(used & expected)}/{len(expected)})")

    # 5) stratified split (ensures both classes in val)
    # --- time-based holdout first, with stratified fallback if needed ---
    if "game_date" in df.columns:
        df = df.sort_values("game_date")
    else:
        df = df.sort_values("game_id")

    split = int(len(df) * 0.8)
    train_df, val_df = df.iloc[:split], df.iloc[split:]

    # ensure both classes exist in val; otherwise fallback to stratified
    if train_df["y"].nunique() < 2 or val_df["y"].nunique() < 2:
        from sklearn.model_selection import StratifiedShuffleSplit
        sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        (train_idx, val_idx), = sss.split(df[cols_used], df["y"])
        train_df, val_df = df.iloc[train_idx], df.iloc[val_idx]

    # 6) drop all-null features in the actual fit frame before any imputer fit.
    train_df, num_used, cat_used, _ = drop_all_null_fit_features(
        train_df,
        num_used,
        cat_used,
        quiet=quiet,
    )

    cols_used = num_used + cat_used
    if not cols_used:
        if not quiet:
            print(f"⏭️  {prop_type}: no usable features after train-split null pruning; skipping.")
        return None

    X_tr, y_tr, w_tr = train_df[cols_used], train_df["y"], train_df["sample_weight"]
    X_v,  y_v,  w_v  =  val_df[cols_used],  val_df["y"],  val_df["sample_weight"]

    # 7) build pipelines (this DEFINES pipe_lr / pipe_rf) and fit
    pipe_lr, pipe_rf = build_pipeline(num_used, cat_used)

    pipe_lr.fit(X_tr, y_tr, clf__sample_weight=w_tr)
    pipe_rf.fit(X_tr, y_tr, clf__sample_weight=w_tr)

    # 7) AUC — report both unweighted and weighted, then use weighted for selection/meta
    proba_lr = pipe_lr.predict_proba(X_v)[:, 1]
    proba_rf = pipe_rf.predict_proba(X_v)[:, 1]
    pos_rate = float(np.mean(y_v))

    def safe_auc(y, p, w=None):
        try:
            return roc_auc_score(y, p) if w is None else roc_auc_score(y, p, sample_weight=w)
        except Exception:
            return np.nan

    auc_lr_uw = safe_auc(y_v, proba_lr, w=None)
    auc_lr_w  = safe_auc(y_v, proba_lr, w_v)
    auc_rf_uw = safe_auc(y_v, proba_rf, w=None)
    auc_rf_w  = safe_auc(y_v, proba_rf, w_v)

    # keep weighted versions as the canonical ones for model selection & metadata
    auc_lr = auc_lr_w
    auc_rf = auc_rf_w

    def _w_from_auc(v: float) -> float:
        try:
            if np.isnan(v):
                return 0.0
            return max(float(v) - 0.5, 0.0)
        except Exception:
            return 0.0

    w_lr = _w_from_auc(auc_lr)
    w_rf = _w_from_auc(auc_rf)
    if (w_lr + w_rf) > 0:
        blend_val = ((proba_lr * w_lr) + (proba_rf * w_rf)) / (w_lr + w_rf)
    else:
        blend_val = (proba_lr + proba_rf) / 2.0

    # Tune per-prop decision threshold on validation to avoid one-size 0.5 cutoff.
    # Weighted accuracy is used to align with training-source weighting.
    yv = y_v.to_numpy(dtype=int)
    wv = np.asarray(w_v, dtype=float)
    if wv.size != len(yv):
        wv = np.ones(len(yv), dtype=float)
    thresholds = [round(x, 2) for x in np.arange(0.35, 0.66, 0.01)]
    best_thr = 0.5
    best_score = -1.0
    for thr in thresholds:
        pred = (blend_val >= thr).astype(int)
        denom = float(wv.sum()) if float(wv.sum()) > 0 else float(len(yv))
        score = float(((pred == yv).astype(float) * wv).sum()) / max(1e-9, denom)
        if score > best_score or (abs(score - best_score) < 1e-12 and abs(thr - 0.5) < abs(best_thr - 0.5)):
            best_score = score
            best_thr = thr

    if not quiet:
        fmt = lambda x: "NaN" if np.isnan(x) else f"{x:.3f}"
        print(
            f"📈 {prop_type}  AUC — "
            f"LR: {fmt(auc_lr_uw)} (uw) / {fmt(auc_lr_w)} (w);  "
            f"RF: {fmt(auc_rf_uw)} (uw) / {fmt(auc_rf_w)} (w);  "
            f"pos_rate={pos_rate:.3f}, n_val={len(y_v)}, decision_threshold={best_thr:.2f}, val_wacc={best_score:.3f}"
        )

    best_model = pipe_rf if (auc_rf >= (auc_lr if not np.isnan(auc_lr) else -1)) else pipe_lr

    # 6) serialize with exact lists we used
    payload = {
        "best": best_model,
        "lr": pipe_lr,
        "rf": pipe_rf,
        "meta": {
            "prop_type": prop_type,
            "trained_at": datetime.utcnow().isoformat(),
            "days_back": days_back,
            "limit": limit,
            "auc_lr": float(auc_lr) if not np.isnan(auc_lr) else None,
            "auc_rf": float(auc_rf) if not np.isnan(auc_rf) else None,
            "decision_threshold": float(best_thr),
            "val_weighted_accuracy": float(best_score),
            "input_columns": cols_used,
            "features_num": num_used,
            "features_cat": cat_used,
            "training_profile": "market_only" if TRAIN_MARKET_ONLY else "legacy",
            "reconcile_bookmaker": RECONCILE_BOOKMAKER or None,
        },
    }
    buf = io.BytesIO()
    joblib.dump(payload, buf, compress=3)
    model_bytes = buf.getvalue()

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    latest_path  = (LATEST_DIR / f"{prop_type}.joblib").resolve()
    archive_path = (ARCHIVE_DIR / prop_type / f"{prop_type}-{ts}.joblib").resolve()

    _atomic_write_bytes(latest_path, model_bytes)
    _atomic_write_bytes(archive_path, model_bytes)

    # 7) update MODEL_INDEX.json
    index_path = (LATEST_DIR / "MODEL_INDEX.json").resolve()
    index: Dict[str, Any] = {}
    if index_path.exists():
        try:
            index = json.loads(index_path.read_text())
            if not isinstance(index, dict):
                index = {}
        except Exception:
            index = {}
    index[prop_type] = {
        "prop_type": prop_type,
        "trained_at": datetime.utcnow().isoformat(),
        "file": latest_path.name,
        "auc_lr": None if np.isnan(auc_lr) else float(auc_lr),
        "auc_rf": None if np.isnan(auc_rf) else float(auc_rf),
        "decision_threshold": float(best_thr),
        "val_weighted_accuracy": float(best_score),
        "input_columns": cols_used,
        "rows": int(len(df)),
        "features_num": num_used,
        "features_cat": cat_used,
        "training_profile": "market_only" if TRAIN_MARKET_ONLY else "legacy",
        "reconcile_bookmaker": RECONCILE_BOOKMAKER or None,
    }
    _atomic_write_bytes(index_path, json.dumps(index, indent=2).encode("utf-8"))

    if not quiet:
        print(f"✅ {prop_type}: wrote latest → {latest_path}")
        print(f"📦 archived copy → {archive_path}")

    return {
        "prop_type": prop_type,
        "auc_lr": auc_lr,
        "auc_rf": auc_rf,
        "latest_path": str(latest_path),
        "archive_path": str(archive_path),
        "rows": int(len(df)),
    }


# ---- CLI ---------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--prop", help="Single prop type to train (default: all)", default=None)
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--limit", type=int, default=DEFAULT_ROW_LIMIT)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    props = [args.prop] if args.prop else PROP_TYPES
    results = []
    trained = skipped = 0

    for p in props:
        try:
            r = train_models_for_prop(p, days_back=args.days_back, limit=args.limit, quiet=args.quiet)
            if r:
                trained += 1
                results.append(r)
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            if not args.quiet:
                print(f"❌ {p}: {e}")

    print(json.dumps({"trained": trained, "skipped": skipped, "props": props, "results": results}, indent=2))
    sys.exit(0)
