#!/usr/bin/env python3
"""
Recommended entrypoint (daily chain):
  make mlb-daily-capture MLB_DATE=YYYY-MM-DD

Most common manual command (download ready upload CSV from remote run; no local rebuild):
  PROPPADIA_BACKEND_URL="https://baseball-streaks-sq44.onrender.com" \
  OPS_API_TOKEN="***" \
  .venv/bin/python backend/mlb/scripts/export_mlb_book_upload.py \
    --slate-date "$(date -u +%F)" \
    --remote-fetch-first \
    --remote-fetch-kind book_upload \
    --remote-fetch-required

Alternative (fetch remote slate first, then build upload CSV locally):
  PROPPADIA_BACKEND_URL="https://baseball-streaks-sq44.onrender.com" \
  OPS_API_TOKEN="***" \
  .venv/bin/python backend/mlb/scripts/export_mlb_book_upload.py \
    --slate-date "$(date -u +%F)" \
    --remote-fetch-first \
    --remote-fetch-kind slate_output \
    --remote-fetch-required

Direct script usage requires existing input artifacts:
  - wide predictions mode (default):
      python backend/mlb/scripts/export_mlb_book_upload.py \
        --slate-date YYYY-MM-DD
    and MLB_PRED_CSV must exist (default: backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv)

  - slate output mode (preferred for historical replay/smoke):
      python backend/mlb/scripts/export_mlb_book_upload.py \
        --slate-date YYYY-MM-DD \
        --use-slate-output \
        --slate-csv backend/mlb/exports/odds_history/YYYY-MM-DD/mlb_slate_output.csv \
        --policy-plan-csv backend/mlb/config/policy/all11_forward_plan_pass4.csv \
        --odds-snapshot-json backend/mlb/exports/odds_history/YYYY-MM-DD/odds_latest_compatible.json \
        --policy-allow-empty

MLB equivalent of NHL book-upload exporter.

Input modes:
- Preferred (new): canonical MLB slate output CSV (`mlb_slate_output.csv`)
- Back-compat: calibrated WIDE predictions with p_over_* columns
- Remote-prefetch mode: fetch prod12 artifact from ops API first
  (for example `--remote-fetch-first --remote-fetch-kind book_upload`).

Behavior:
- Filters to slate_date (ET)
- Writes BOTH over and under rows in external upload format

Input expectations:
- Required: player_id, game_id
- Prob columns: p_over_1_5, p_over_2_5, ... (regex: p_over_<int>_<0|5>)
- Prop type:
  - preferred column: prop_type
  - fallback: --prop-type / MLB_BOOK_UPLOAD_PROP_TYPE
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from backend.app.services.mlb import market_odds_service
from backend.mlb.scripts.build_mlb_reconcile_rows import (
    _build_market_index,
    _build_team_name_reverse,
    _line_key,
    _load_events,
    _norm_name,
)
from backend.mlb.shared.policy_plan import load_policy_plan, score_policy_plan_rows

BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
PRED_CSV = Path(
    os.environ.get(
        "MLB_PRED_CSV",
        os.environ.get(
            "PRED_CSV",
            str(BASE_DIR / "mlb" / "data" / "processed" / "mlb_predictions_wide_calibrated.csv"),
        ),
    )
)
SLATE_CSV = Path(
    os.environ.get(
        "MLB_SLATE_OUTPUT_CSV",
        str(BASE_DIR / "mlb" / "data" / "processed" / "mlb_slate_output.csv"),
    )
)
OUT_CSV = Path(
    os.environ.get(
        "MLB_BOOK_UPLOAD_OUT_CSV",
        os.environ.get(
            "OUT_CSV",
            str(BASE_DIR / "mlb" / "data" / "processed" / "mlb_book_upload.csv"),
        ),
    )
)

# External book-upload taxonomy (provided by operator).
# MARKET is the prop-type carrier in upload rows.
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
    # pitcher_win is yes/no (not over/under) and intentionally excluded here.
}

ALLOWED_UPLOAD_MARKETS = {
    "batter_hits",
    "batter_runs",
    "batter_rbis",
    "batter_bases",
    "batter_h+r+rbi",
    "batter_walks",
    "batter_strikeouts",
    "batter_stolen_bases",
    "batter_singles",
    "batter_doubles",
    "batter_triples",
    "batter_home_runs",
    "pitcher_hits",
    "pitcher_earned_runs",
    "pitcher_outs",
    "pitcher_walks",
    "pitcher_strikeouts",
}

UPLOAD_MARKET_ALIASES: Dict[str, str] = {
    "batter_hits_runs_rbis": "batter_h+r+rbi",
    "batter_total_bases": "batter_bases",
    "batter_runs_scored": "batter_runs",
    "pitcher_hits_allowed": "pitcher_hits",
}

_PCOL_RE = re.compile(r"^p_over_(\d+)_([05])$")


def _canonical_prop_type(value: object) -> str:
    return str(value or "").strip().lower()


def _clean_optional_str(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _normalize_upload_market(
    *,
    raw_market: object,
    prop_type: str,
    market_map: Dict[str, str],
) -> str:
    market = (_clean_optional_str(raw_market) or "").strip().lower()
    if not market:
        market = str(market_map.get(prop_type) or "").strip().lower()
    market = UPLOAD_MARKET_ALIASES.get(market, market)
    if market in ALLOWED_UPLOAD_MARKETS:
        return market
    raise ValueError(
        f"unsupported upload market '{market}' for prop_type='{prop_type}'. "
        f"Allowed markets: {sorted(ALLOWED_UPLOAD_MARKETS)}"
    )


def _parse_lines_from_cols(cols: Iterable[str]) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for col in cols:
        match = _PCOL_RE.match(col)
        if not match:
            continue
        whole = int(match.group(1))
        half = int(match.group(2))
        line = float(whole) + (0.5 if half == 5 else 0.0)
        out.append((col, line))
    out.sort(key=lambda x: x[1])
    return out


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name, "") or "").strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _fetch_remote_prod12_artifact(
    *,
    backend_url: str,
    ops_token: str,
    artifact_kind: str,
    mlb_date: str,
    out_path: Path,
    timeout_sec: float,
) -> Dict[str, object]:
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    base = str(backend_url or "").strip()
    token = str(ops_token or "").strip()
    kind = str(artifact_kind or "").strip().lower()
    date_text = str(mlb_date or "").strip()
    if not base:
        raise ValueError("missing remote backend URL (set --remote-backend-url or PROPPADIA_BACKEND_URL)")
    if not token:
        raise ValueError("missing ops token (set --remote-ops-token or OPS_API_TOKEN)")
    if kind not in {"book_upload", "predictions_wide", "slate_output", "archive_manifest"}:
        raise ValueError(
            "invalid remote artifact kind "
            f"'{kind}' (expected book_upload|predictions_wide|slate_output|archive_manifest)"
        )
    if not date_text:
        raise ValueError("missing mlb_date for remote fetch")

    timeout = max(1.0, float(timeout_sec))
    qs = urlencode({"kind": kind, "mlb_date": date_text})
    url = f"{base.rstrip('/')}/api/ops/mlb/prod12/artifact?{qs}"
    req = Request(url, headers={"X-Ops-Token": token})

    try:
        with urlopen(req, timeout=timeout) as resp:
            payload = resp.read()
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        raise RuntimeError(
            f"remote artifact fetch failed status={exc.code} kind={kind} mlb_date={date_text} "
            f"url={url} body={body[:300]}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f"remote artifact fetch failed kind={kind} mlb_date={date_text} url={url} reason={exc.reason}"
        ) from exc

    if not payload:
        raise RuntimeError(f"remote artifact fetch returned empty payload for kind={kind} mlb_date={date_text}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.parent / f".{out_path.name}.tmp.{os.getpid()}"
    tmp_path.write_bytes(payload)
    tmp_path.replace(out_path)
    return {"url": url, "bytes": len(payload), "path": str(out_path), "kind": kind, "mlb_date": date_text}


def _get_db_conn():
    import psycopg2  # type: ignore

    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("missing SUPABASE_DB_URL or DATABASE_URL")
    return psycopg2.connect(db_url)


def _fetch_games(conn, game_ids: List[int]) -> pd.DataFrame:
    if not game_ids:
        return pd.DataFrame(columns=["game_id", "game_date", "home_team_code", "away_team_code"])
    sql = """
    SELECT
      game_id::bigint AS game_id,
      game_date::date AS game_date,
      home_team_abbr::text AS home_team_code,
      away_team_abbr::text AS away_team_code
    FROM mlb.game_info
    WHERE game_id = ANY(%s::bigint[])
    """
    return pd.read_sql(sql, conn, params=(list(game_ids),))


def _load_predictions(path: Path) -> pd.DataFrame:
    print(f"[mlb-book-upload] reading predictions from: {path}")
    if not path.exists():
        raise FileNotFoundError(
            "missing predictions file: "
            f"{path}\n"
            "Run upstream capture first (make mlb-daily-capture) "
            "or pass --use-slate-output --slate-csv <path>."
        )
    return pd.read_csv(path)


def _load_slate_output(path: Path) -> pd.DataFrame:
    print(f"[mlb-book-upload] reading slate output from: {path}")
    if not path.exists():
        raise FileNotFoundError(f"missing slate output file: {path}")
    df = pd.read_csv(path)
    required = {
        "player_id",
        "game_id",
        "prop_type",
        "line",
        "prob_over",
        "game_date",
        "home_team_code",
        "away_team_code",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"slate output missing required columns: {missing}")
    return df


def _melt_to_long(df_wide: pd.DataFrame, default_prop_type: Optional[str]) -> pd.DataFrame:
    for key in ("player_id", "game_id"):
        if key not in df_wide.columns:
            raise ValueError(f"predictions missing required column: {key}")

    col_lines = _parse_lines_from_cols(df_wide.columns)
    if not col_lines:
        raise ValueError("no p_over_* columns found in predictions input")

    prob_cols = [col for col, _ in col_lines]
    print(f"[mlb-book-upload] found probability columns: {prob_cols}")

    use_prop_col = "prop_type" in df_wide.columns
    if not use_prop_col and not default_prop_type:
        raise ValueError("missing prop_type column and no --prop-type provided")

    id_cols = ["player_id", "game_id"] + (["prop_type"] if use_prop_col else [])
    df_long = df_wide[id_cols + prob_cols].melt(
        id_vars=id_cols,
        value_vars=prob_cols,
        var_name="prob_col",
        value_name="prob_over",
    )

    line_map = {col: line for col, line in col_lines}
    df_long["line"] = df_long["prob_col"].map(line_map).astype(float)
    df_long = df_long.drop(columns=["prob_col"])

    if use_prop_col:
        df_long["prop_type"] = df_long["prop_type"].map(_canonical_prop_type)
    else:
        df_long["prop_type"] = _canonical_prop_type(default_prop_type)

    df_long["player_id"] = pd.to_numeric(df_long["player_id"], errors="coerce")
    df_long["game_id"] = pd.to_numeric(df_long["game_id"], errors="coerce")
    df_long["prob_over"] = pd.to_numeric(df_long["prob_over"], errors="coerce")

    df_long = df_long.dropna(subset=["player_id", "game_id", "prob_over", "line"])
    df_long = df_long[df_long["prop_type"].astype(str).str.len() > 0]
    if df_long.empty:
        raise ValueError("no usable prediction rows after melt/cleanup")

    df_long["player_id"] = df_long["player_id"].astype(int)
    df_long["game_id"] = df_long["game_id"].astype(int)
    return df_long


def _normalize_slate_output(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["prop_type"] = out["prop_type"].map(_canonical_prop_type)
    for c in ("player_id", "game_id", "line", "prob_over"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    if "market_key" in out.columns:
        out["market_key"] = out["market_key"].map(_clean_optional_str)
    out = out.dropna(
        subset=["player_id", "game_id", "line", "prob_over", "game_date", "home_team_code", "away_team_code"]
    )
    out = out[out["prop_type"].astype(str).str.len() > 0]
    out["player_id"] = out["player_id"].astype(int)
    out["game_id"] = out["game_id"].astype(int)
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


def _market_candidates_for_prop(*, prop_type: str, base_market: Optional[str]) -> List[str]:
    out: List[str] = []

    fn = getattr(market_odds_service, "get_prop_market_candidates", None)
    if callable(fn):
        try:
            candidates = fn(prop_type=prop_type, include_aliases=True)
        except TypeError:
            candidates = fn(prop_type=prop_type)
        except Exception:
            candidates = []
        if isinstance(candidates, (list, tuple)):
            out.extend(str(x).strip() for x in candidates if str(x or "").strip())

    if not out:
        stable_map = getattr(market_odds_service, "PROP_TO_ODDS_MARKET", {}) or {}
        if isinstance(stable_map, dict):
            primary = str(stable_map.get(prop_type) or "").strip()
            if primary:
                out.append(primary)

        include_aliases = str(os.getenv("MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED", "0") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if include_aliases:
            aliases_map = getattr(market_odds_service, "PROP_TO_ODDS_MARKET_ALIASES", {}) or {}
            if isinstance(aliases_map, dict):
                for alias in aliases_map.get(prop_type) or ():
                    a = str(alias or "").strip()
                    if a:
                        out.append(a)

    if base_market:
        out.insert(0, str(base_market).strip())

    seen = set()
    uniq: List[str] = []
    for mk in out:
        key = str(mk).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(key)
    return uniq


def _pick_book_row(
    *,
    by_book: Dict[str, Dict[str, object]],
    bookmaker_key: str,
) -> Tuple[Optional[str], Optional[Dict[str, object]]]:
    target = str(bookmaker_key or "").strip().lower()
    if not target or not by_book:
        return None, None
    for key, payload in by_book.items():
        if str(key or "").strip().lower() == target:
            return str(key), payload
    return None, None


def _build_policy_candidate_rows(
    *,
    merged: pd.DataFrame,
    plan_df: pd.DataFrame,
    odds_snapshot_json: Path,
    market_map: Dict[str, str],
) -> pd.DataFrame:
    if not odds_snapshot_json.exists():
        raise FileNotFoundError(f"missing odds snapshot json for policy mode: {odds_snapshot_json}")

    events = _load_events(odds_snapshot_json)
    market_idx = _build_market_index(events=events, team_name_rev=_build_team_name_reverse())

    plan_by_prop = {str(r["prop_type"]).strip().lower(): r for _, r in plan_df.iterrows()}
    out_rows: List[Dict[str, object]] = []
    out_cols = [
        "league",
        "slate_date",
        "game_date",
        "game_id",
        "home_team_code",
        "away_team_code",
        "player_id",
        "player_name",
        "prop_type",
        "market_key",
        "line",
        "model_prob_over",
        "model_prob_under",
        "bookmaker_key",
        "price_over_american",
        "price_under_american",
    ]

    for _, row in merged.iterrows():
        prop_type = _canonical_prop_type(row.get("prop_type"))
        plan_row = plan_by_prop.get(prop_type)
        if plan_row is None:
            continue

        home = str(row.get("home_team_code") or "").strip().upper()
        away = str(row.get("away_team_code") or "").strip().upper()
        player_name = str(row.get("player_name") or "").strip()
        line = _line_key(row.get("line"))
        if not home or not away or not player_name or line is None:
            continue

        market_key = _clean_optional_str(row.get("market_key")) or market_map.get(prop_type)
        if not market_key:
            continue

        market_candidates = _market_candidates_for_prop(
            prop_type=prop_type,
            base_market=market_key,
        )
        candidate_keys = list(market_candidates)

        selected_market_key: Optional[str] = None
        selected_book_key: Optional[str] = None
        selected_book_row: Optional[Dict[str, object]] = None
        for market_key_try in candidate_keys:
            market_key_norm = str(market_key_try or "").strip()
            if not market_key_norm:
                continue
            k = (home, away, market_key_norm, _norm_name(player_name), float(line))
            by_book = market_idx.get(k, {})
            book_key, book_row = _pick_book_row(
                by_book=by_book,
                bookmaker_key=str(plan_row.get("bookmaker_key") or ""),
            )
            if book_key and book_row:
                selected_market_key = market_key_norm
                selected_book_key = book_key
                selected_book_row = book_row
                break

        if not selected_book_row:
            continue

        try:
            price_over = float(selected_book_row.get("over")) if selected_book_row.get("over") is not None else None
            price_under = (
                float(selected_book_row.get("under")) if selected_book_row.get("under") is not None else None
            )
        except Exception:
            price_over = None
            price_under = None

        out_rows.append(
            {
                "league": row.get("league"),
                "slate_date": row.get("slate_date"),
                "game_date": row.get("game_date"),
                "game_id": row.get("game_id"),
                "home_team_code": home,
                "away_team_code": away,
                "player_id": row.get("player_id"),
                "player_name": player_name,
                "prop_type": prop_type,
                "market_key": selected_market_key or market_key,
                "line": float(line),
                "model_prob_over": float(row.get("prob_over")),
                "model_prob_under": float(row.get("prob_under")),
                "bookmaker_key": str(selected_book_key or plan_row.get("bookmaker_key")),
                "price_over_american": price_over,
                "price_under_american": price_under,
            }
        )

    return pd.DataFrame(out_rows, columns=out_cols)


def main() -> None:
    import argparse
    from datetime import datetime
    import pytz

    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--strict", action="store_true", help="Fail if predictions contain non-slate rows.")
    ap.add_argument(
        "--use-slate-output",
        action="store_true",
        help="Use canonical MLB slate output CSV instead of wide predictions input.",
    )
    ap.add_argument(
        "--slate-csv",
        default="",
        help="Optional override path for canonical MLB slate output CSV.",
    )
    ap.add_argument("--prop-type", default=os.environ.get("MLB_BOOK_UPLOAD_PROP_TYPE", ""))
    ap.add_argument("--market", default="", help="Force one market key for all rows.")
    ap.add_argument("--market-map-json", default="", help="Optional JSON object prop_type->market_key overrides.")
    ap.add_argument("--league", default="MLB")
    ap.add_argument("--section", default="player_prop")
    ap.add_argument(
        "--drop-line-0-5",
        action="store_true",
        help="Drop 0.5 lines (default keeps them for MLB).",
    )
    ap.add_argument(
        "--policy-plan-csv",
        default=os.environ.get("MLB_POLICY_PLAN_CSV", ""),
        help="Optional per-prop policy plan CSV (book/side/thresholds). When set, emits selected-side rows only.",
    )
    ap.add_argument(
        "--odds-snapshot-json",
        default=os.environ.get("MLB_ODDS_SNAPSHOT_JSON", ""),
        help="Odds snapshot JSON used for policy plan filtering (required when --policy-plan-csv is set).",
    )
    ap.add_argument(
        "--policy-allow-one-sided",
        action="store_true",
        help="Allow one-sided rows in policy evaluation (default requires two-sided).",
    )
    ap.add_argument(
        "--policy-allow-empty",
        action="store_true",
        help="Allow policy mode to write an empty upload file when no rows pass.",
    )
    ap.add_argument(
        "--remote-fetch-first",
        action="store_true",
        help=(
            "Fetch a remote prod12 artifact before local processing "
            "(or set MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=1)."
        ),
    )
    ap.add_argument(
        "--remote-fetch-kind",
        choices=["book_upload", "predictions_wide", "slate_output", "archive_manifest"],
        default=os.environ.get("MLB_BOOK_UPLOAD_REMOTE_FETCH_KIND", "book_upload"),
        help="Artifact kind for --remote-fetch-first. Default: book_upload.",
    )
    ap.add_argument(
        "--remote-fetch-only",
        action="store_true",
        help=(
            "Exit after successful remote fetch "
            "(or set MLB_BOOK_UPLOAD_REMOTE_FETCH_ONLY=1)."
        ),
    )
    ap.add_argument(
        "--remote-fetch-required",
        action="store_true",
        help=(
            "Fail if remote fetch fails "
            "(or set MLB_BOOK_UPLOAD_REMOTE_FETCH_REQUIRED=1)."
        ),
    )
    ap.add_argument(
        "--remote-backend-url",
        default=os.environ.get("PROPPADIA_BACKEND_URL", ""),
        help="Backend URL for remote artifact fetch. Defaults to PROPPADIA_BACKEND_URL.",
    )
    ap.add_argument(
        "--remote-ops-token",
        default=os.environ.get("OPS_API_TOKEN", ""),
        help="Ops token for remote artifact fetch. Defaults to OPS_API_TOKEN.",
    )
    ap.add_argument(
        "--remote-fetch-timeout-sec",
        type=float,
        default=_float_env("MLB_BOOK_UPLOAD_REMOTE_FETCH_TIMEOUT_SEC", 90.0),
        help="Timeout (seconds) for remote artifact fetch.",
    )
    ap.add_argument(
        "--remote-fetch-mlb-date",
        default=os.environ.get("MLB_BOOK_UPLOAD_REMOTE_FETCH_MLB_DATE", ""),
        help="Optional mlb_date override for remote fetch. Defaults to --slate-date.",
    )
    args = ap.parse_args()

    et = pytz.timezone("America/New_York")
    et_today = datetime.now(et).strftime("%Y-%m-%d")
    slate_date = (args.slate_date or os.environ.get("SLATE_DATE") or et_today).strip()
    prop_type_arg = _canonical_prop_type(args.prop_type)
    market_map = _load_market_map(
        arg_json=str(args.market_map_json),
        env_json=str(os.environ.get("MLB_BOOK_UPLOAD_MARKET_MAP_JSON", "")),
    )
    print(f"[mlb-book-upload] slate_date (ET) = {slate_date}")

    slate_csv_arg = str(args.slate_csv or "").strip()
    use_slate_output = bool(args.use_slate_output or slate_csv_arg)
    remote_fetch_first = bool(args.remote_fetch_first or _env_truthy("MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST"))
    remote_fetch_only = bool(args.remote_fetch_only or _env_truthy("MLB_BOOK_UPLOAD_REMOTE_FETCH_ONLY"))
    remote_fetch_required = bool(args.remote_fetch_required or _env_truthy("MLB_BOOK_UPLOAD_REMOTE_FETCH_REQUIRED"))
    remote_kind = str(args.remote_fetch_kind or "").strip().lower() or "book_upload"
    remote_mlb_date = str(args.remote_fetch_mlb_date or "").strip() or slate_date

    if remote_fetch_first:
        if remote_kind == "book_upload":
            remote_out_path = OUT_CSV
        elif remote_kind == "slate_output":
            remote_out_path = Path(slate_csv_arg).expanduser() if slate_csv_arg else SLATE_CSV
        elif remote_kind == "predictions_wide":
            remote_out_path = PRED_CSV
        else:
            remote_out_path = BASE_DIR / "mlb" / "exports" / "odds_history" / remote_mlb_date / "manifest.json"

        try:
            fetched = _fetch_remote_prod12_artifact(
                backend_url=str(args.remote_backend_url or "").strip(),
                ops_token=str(args.remote_ops_token or "").strip(),
                artifact_kind=remote_kind,
                mlb_date=remote_mlb_date,
                out_path=remote_out_path,
                timeout_sec=float(args.remote_fetch_timeout_sec),
            )
            print(
                "[mlb-book-upload] remote fetch ok: "
                f"kind={fetched.get('kind')} mlb_date={fetched.get('mlb_date')} "
                f"bytes={fetched.get('bytes')} path={fetched.get('path')}"
            )
        except Exception as exc:
            if remote_fetch_required:
                print(f"ERROR: remote fetch failed and is required: {exc}", file=sys.stderr)
                sys.exit(1)
            print(f"[mlb-book-upload] WARNING: remote fetch failed; falling back to local build: {exc}")
        else:
            if remote_fetch_only or remote_kind == "book_upload":
                if remote_kind == "book_upload" and not remote_fetch_only:
                    print("[mlb-book-upload] fetched remote book_upload artifact; skipping local rebuild.")
                return
            if remote_kind == "slate_output":
                slate_csv_arg = str(remote_out_path)
                use_slate_output = True

    if use_slate_output:
        slate_path = Path(slate_csv_arg) if slate_csv_arg else SLATE_CSV
        print(f"[mlb-book-upload] using SLATE_CSV = {slate_path}")
        merged = _normalize_slate_output(_load_slate_output(slate_path))
    else:
        print(f"[mlb-book-upload] using PRED_CSV = {PRED_CSV}")
        df_wide = _load_predictions(PRED_CSV)
        df_long = _melt_to_long(df_wide, prop_type_arg)

        unique_game_ids = sorted(df_long["game_id"].unique().tolist())
        print(f"[mlb-book-upload] fetching game metadata for {len(unique_game_ids)} game_ids")

        with _get_db_conn() as conn:
            games = _fetch_games(conn, unique_game_ids)

        if games.empty:
            print("ERROR: no matching rows in mlb.game_info for game_ids in predictions", file=sys.stderr)
            sys.exit(1)

        merged = df_long.merge(games, on="game_id", how="left")
        merged = merged.dropna(subset=["game_date", "home_team_code", "away_team_code"])
        if merged.empty:
            print("ERROR: no rows after joining with mlb.game_info", file=sys.stderr)
            sys.exit(1)

    # Safety: fail fast when any prop_type in source lacks a market mapping.
    # (unless a single explicit --market override is provided).
    if not str(args.market).strip():
        present_props = sorted({_canonical_prop_type(x) for x in merged["prop_type"].tolist()})
        if "market_key" in merged.columns:
            # Rows with explicit market_key from slate output can bypass local map.
            missing_market_key_props = sorted(
                {
                    _canonical_prop_type(r.get("prop_type"))
                    for _, r in merged.iterrows()
                    if _canonical_prop_type(r.get("prop_type"))
                    and not _clean_optional_str(r.get("market_key"))
                    and _canonical_prop_type(r.get("prop_type")) not in market_map
                }
            )
            unmapped = missing_market_key_props
        else:
            unmapped = [p for p in present_props if p and p not in market_map]
        if unmapped:
            print(
                "[mlb-book-upload] ERROR: unmapped prop_type(s) found in source input: "
                + ", ".join(unmapped),
                file=sys.stderr,
            )
            print(
                "[mlb-book-upload] Add mapping via --market-map-json / MLB_BOOK_UPLOAD_MARKET_MAP_JSON "
                "or provide market_key in MLB slate output.",
                file=sys.stderr,
            )
            sys.exit(1)

    merged["game_date"] = pd.to_datetime(merged["game_date"]).dt.date
    target_date = pd.to_datetime(slate_date).date()
    dates_present = sorted({d.isoformat() for d in merged["game_date"].dropna().tolist()})

    before = len(merged)
    merged = merged[merged["game_date"] == target_date]
    after = len(merged)
    print(f"[mlb-book-upload] dates present after join: {dates_present}")
    print(f"[mlb-book-upload] merged rows after date filter: {after}")

    if after == 0:
        print(
            f"ERROR: zero rows for slate_date={slate_date}. dates_present={dates_present}",
            file=sys.stderr,
        )
        sys.exit(1)

    if after < before:
        msg = f"filtered out {before - after} rows not on slate_date={slate_date}"
        if args.strict:
            print(f"ERROR: {msg}", file=sys.stderr)
            sys.exit(1)
        print(f"[mlb-book-upload] WARNING: {msg}")

    if args.drop_line_0_5:
        lines_before = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        merged = merged[merged["line"] != 0.5]
        lines_after = merged["line"].value_counts(dropna=False).sort_index().to_dict()
        print(f"[mlb-book-upload] dropped line 0.5: before={lines_before} after={lines_after}")

    rows: List[Dict[str, object]] = []
    policy_plan_csv = Path(str(args.policy_plan_csv or "").strip()).expanduser() if str(args.policy_plan_csv or "").strip() else None
    if policy_plan_csv is not None:
        odds_snapshot_raw = str(args.odds_snapshot_json or "").strip()
        if not odds_snapshot_raw:
            raise RuntimeError("--odds-snapshot-json is required when --policy-plan-csv is set")
        odds_snapshot_json = Path(odds_snapshot_raw).expanduser()
        plan_df = load_policy_plan(policy_plan_csv, include_actions=("enable",))
        candidate_rows = _build_policy_candidate_rows(
            merged=merged,
            plan_df=plan_df,
            odds_snapshot_json=odds_snapshot_json,
            market_map=market_map,
        )
        scored = score_policy_plan_rows(
            candidate_rows,
            plan_df,
            require_two_sided=not bool(args.policy_allow_one_sided),
        )
        selected = scored[scored["pass_policy"]].copy() if not scored.empty else scored
        print(
            "[mlb-book-upload] policy mode: candidates=",
            len(candidate_rows),
            "scored=",
            len(scored),
            "selected=",
            len(selected),
        )

        for _, row in selected.iterrows():
            side = str(row.get("plan_side") or "").strip().lower()
            if side not in {"over", "under"}:
                continue
            side_prob = float(row.get("side_model_prob"))
            if not (0.0 < side_prob < 1.0):
                continue
            win_pct = _prob_to_fair_american(side_prob)
            if win_pct is None:
                continue

            prop_type = _canonical_prop_type(row.get("prop_type"))
            market = str(args.market).strip()
            if not market:
                market = _normalize_upload_market(
                    raw_market=row.get("market_key"),
                    prop_type=prop_type,
                    market_map=market_map,
                )
            else:
                market = _normalize_upload_market(
                    raw_market=market,
                    prop_type=prop_type,
                    market_map=market_map,
                )

            date_str = pd.to_datetime(row["game_date"]).strftime("%Y%m%d")
            rows.append(
                {
                    "LEAGUE": str(args.league).strip() or "MLB",
                    "DATE": date_str,
                    "HOME": str(row["home_team_code"]).strip(),
                    "AWAY": str(row["away_team_code"]).strip(),
                    "DOUBLEHEADER": "",
                    "SECTION": str(args.section).strip() or "player_prop",
                    "MARKET": market,
                    "SELECTOR": int(row["player_id"]),
                    "POINT": float(row["line"]),
                    "SIDE": side,
                    "WIN %": int(win_pct),
                }
            )
    else:
        for _, row in merged.iterrows():
            p_over = float(row["prob_over"])
            if not (0.0 < p_over < 1.0):
                continue
            p_under = 1.0 - p_over

            odds_over = _prob_to_fair_american(p_over)
            odds_under = _prob_to_fair_american(p_under)
            if odds_over is None or odds_under is None:
                continue

            prop_type = _canonical_prop_type(row["prop_type"])
            market = str(args.market).strip()
            if not market:
                raw_market = _clean_optional_str(row.get("market_key")) if "market_key" in merged.columns else None
                market = _normalize_upload_market(
                    raw_market=raw_market,
                    prop_type=prop_type,
                    market_map=market_map,
                )
            else:
                market = _normalize_upload_market(
                    raw_market=market,
                    prop_type=prop_type,
                    market_map=market_map,
                )
            date_str = pd.to_datetime(row["game_date"]).strftime("%Y%m%d")

            base = {
                "LEAGUE": str(args.league).strip() or "MLB",
                "DATE": date_str,
                "HOME": str(row["home_team_code"]).strip(),
                "AWAY": str(row["away_team_code"]).strip(),
                "DOUBLEHEADER": "",
                "SECTION": str(args.section).strip() or "player_prop",
                "MARKET": market,
                "SELECTOR": int(row["player_id"]),
                "POINT": float(row["line"]),
            }
            rows.append({**base, "SIDE": "over", "WIN %": int(odds_over)})
            rows.append({**base, "SIDE": "under", "WIN %": int(odds_under)})

    if not rows:
        if policy_plan_csv is not None and args.policy_allow_empty:
            out_df = pd.DataFrame(
                columns=[
                    "LEAGUE",
                    "DATE",
                    "HOME",
                    "AWAY",
                    "DOUBLEHEADER",
                    "SECTION",
                    "MARKET",
                    "SELECTOR",
                    "POINT",
                    "SIDE",
                    "WIN %",
                ]
            )
            OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
            out_df.to_csv(OUT_CSV, index=False)
            print(f"[mlb-book-upload] wrote empty policy output to {OUT_CSV}")
            return
        print("ERROR: no output rows generated", file=sys.stderr)
        sys.exit(1)

    out_df = pd.DataFrame(rows)
    if policy_plan_csv is None:
        expected = 2 * len(merged)
        if len(out_df) != expected:
            raise AssertionError(f"unexpected row count: wrote {len(out_df)} expected {expected}")
    bad_sides = sorted(set(out_df["SIDE"].dropna().unique()) - {"over", "under"})
    if bad_sides:
        raise AssertionError(f"invalid SIDE values: {bad_sides}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_CSV, index=False)
    print(f"[mlb-book-upload] wrote {len(out_df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
