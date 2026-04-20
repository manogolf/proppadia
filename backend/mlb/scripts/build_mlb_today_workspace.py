#!/usr/bin/env python3
"""
Load today's MLB file-based artifacts into staging tables for /mlb/today workspace.

Stages loaded:
- mlb.today_odds_book_rows
- mlb.today_slate_rows
- mlb.today_wide_rows

Notes:
- Uses existing archived artifacts under backend/mlb/exports/odds_history/<SLATE_DATE>/.
- Resolves odds rows to stable player_id/game_id via slate matching before insert.
- No picks/recommendations logic; descriptive market + player context backend only.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.services.mlb.market_odds_service import get_market_to_prop_map
from backend.mlb.scripts.build_mlb_reconcile_rows import (
    _build_team_name_reverse,
    _line_key,
    _load_events,
    _norm_name,
)
from backend.shared.db.pg import pg_connect


ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

SNAPSHOT_TS_RE = re.compile(r".*_(\d{8}T\d{6})(?:Z|_\d+)?\.json$")


@dataclass(frozen=True)
class SnapshotRef:
    path: Path
    ts: datetime


def _et_today() -> str:
    return datetime.now(ET).date().isoformat()


def _clean_str(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _canonical_prop_type(value: object) -> str:
    return str(value or "").strip().lower()


def _sanitize_american_odds_frame(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if df.empty:
        return df, {
            "invalid_odds_values_nullified": 0,
            "rows_dropped_no_valid_odds": 0,
        }

    out = df.copy()
    invalid_count = 0
    for col in ("price_over_american", "price_under_american"):
        vals = pd.to_numeric(out[col], errors="coerce")
        invalid_mask = vals.notna() & (vals.abs() < 100)
        invalid_count += int(invalid_mask.sum())
        vals = vals.mask(invalid_mask)
        out[col] = vals

    before = len(out)
    out = out[out["price_over_american"].notna() | out["price_under_american"].notna()].copy()
    dropped = before - len(out)
    return out, {
        "invalid_odds_values_nullified": int(invalid_count),
        "rows_dropped_no_valid_odds": int(dropped),
    }


def _resolve_snapshot_ts(path: Path) -> datetime:
    m = SNAPSHOT_TS_RE.match(path.name)
    if m:
        return datetime.strptime(m.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=UTC)

    # Fallback to payload timestamp when filename is not tagged.
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        captured = _clean_str(raw.get("captured_at_utc")) if isinstance(raw, dict) else None
        if captured:
            return datetime.fromisoformat(captured.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        pass

    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _find_snapshot_files(day_dir: Path, explicit_snapshot: Optional[Path]) -> List[SnapshotRef]:
    if explicit_snapshot is not None:
        if not explicit_snapshot.exists():
            raise FileNotFoundError(f"snapshot file not found: {explicit_snapshot}")
        return [SnapshotRef(path=explicit_snapshot, ts=_resolve_snapshot_ts(explicit_snapshot))]

    files = sorted(
        set(day_dir.glob("odds_mlb_playerprops_*.json")) | set(day_dir.glob("odds_mlb_playerprops__*.json"))
    )

    # Keep only timestamp-tagged paths; drop aliases like *_earliest.json.
    tagged = [p for p in files if SNAPSHOT_TS_RE.match(p.name)]
    if not tagged:
        latest = day_dir / "odds_mlb_playerprops.json"
        if not latest.exists():
            raise FileNotFoundError(f"no odds snapshots found under: {day_dir}")
        tagged = [latest]

    refs = [SnapshotRef(path=p, ts=_resolve_snapshot_ts(p)) for p in tagged]
    refs.sort(key=lambda r: (r.ts, r.path.name))
    return refs


def _load_slate_rows(slate_csv: Path, slate_date: str) -> pd.DataFrame:
    if not slate_csv.exists():
        raise FileNotFoundError(f"slate csv not found: {slate_csv}")
    df = pd.read_csv(slate_csv)
    required = {
        "slate_date",
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "home_team_code",
        "away_team_code",
        "prop_type",
        "line",
        "prob_over",
        "prob_under",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"slate csv missing columns: {missing}")

    out = df.copy()
    out["slate_date"] = pd.to_datetime(out["slate_date"], errors="coerce").dt.date
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce").dt.date
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    out["line"] = pd.to_numeric(out["line"], errors="coerce").round(3)
    out["prob_over"] = pd.to_numeric(out["prob_over"], errors="coerce")
    out["prob_under"] = pd.to_numeric(out["prob_under"], errors="coerce")
    out["prop_type"] = out["prop_type"].map(_canonical_prop_type)
    out["player_name"] = out["player_name"].map(lambda v: _clean_str(v) or "")
    out["market_key"] = out["market_key"].map(_clean_str) if "market_key" in out.columns else None

    target_date = pd.to_datetime(slate_date).date()
    out = out[out["slate_date"] == target_date].copy()
    out = out.dropna(subset=["game_date", "game_id", "player_id", "line", "prob_over", "prob_under"])
    out = out[out["prop_type"].astype(str).str.len() > 0]
    out = out[out["home_team_code"].astype(str).str.len() > 0]
    out = out[out["away_team_code"].astype(str).str.len() > 0]
    out["player_name_norm"] = out["player_name"].map(_norm_name)

    out["player_id"] = out["player_id"].astype("int64")
    out["game_id"] = out["game_id"].astype("int64")
    out = out.sort_values(["game_id", "player_id", "prop_type", "line"], kind="stable")
    out = out.drop_duplicates(
        subset=["slate_date", "game_id", "player_id", "prop_type", "line"],
        keep="first",
    )

    cols = [
        "slate_date",
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "player_name_norm",
        "home_team_code",
        "away_team_code",
        "prop_type",
        "market_key",
        "line",
        "prob_over",
        "prob_under",
    ]
    return out[cols].reset_index(drop=True)


def _load_wide_rows(wide_csv: Path, slate_date: str) -> pd.DataFrame:
    if not wide_csv.exists():
        raise FileNotFoundError(f"wide csv not found: {wide_csv}")
    df = pd.read_csv(wide_csv)
    required = {
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "prop_type",
        "team",
        "opponent",
        "is_home",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"wide csv missing columns: {missing}")

    out = df.copy()
    out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce").dt.date
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["prop_type"] = out["prop_type"].map(_canonical_prop_type)
    out["player_name"] = out["player_name"].map(lambda v: _clean_str(v) or "")
    out["team"] = out["team"].map(_clean_str)
    out["opponent"] = out["opponent"].map(_clean_str)
    out["is_home"] = out["is_home"].astype("boolean")

    target_date = pd.to_datetime(slate_date).date()
    out = out[out["game_date"] == target_date].copy()
    out = out.dropna(subset=["game_date", "game_id", "player_id"])
    out = out[out["prop_type"].astype(str).str.len() > 0]
    out["slate_date"] = target_date
    out["player_id"] = out["player_id"].astype("int64")
    out["game_id"] = out["game_id"].astype("int64")
    out["player_name_norm"] = out["player_name"].map(_norm_name)

    out = out.sort_values(["game_id", "player_id", "prop_type"], kind="stable")
    out = out.drop_duplicates(
        subset=["slate_date", "game_id", "player_id", "prop_type"],
        keep="first",
    )
    cols = [
        "slate_date",
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "player_name_norm",
        "prop_type",
        "team",
        "opponent",
        "is_home",
    ]
    return out[cols].reset_index(drop=True)


def _build_slate_lookup(slate_rows: pd.DataFrame) -> Dict[Tuple[str, str, str, float, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str, str, float, str], Dict[str, Any]] = {}
    for _, r in slate_rows.iterrows():
        key = (
            str(r["home_team_code"]).strip().upper(),
            str(r["away_team_code"]).strip().upper(),
            str(r["prop_type"]).strip().lower(),
            float(r["line"]),
            str(r["player_name_norm"]).strip(),
        )
        out[key] = {
            "slate_date": r["slate_date"],
            "game_date": r["game_date"],
            "game_id": int(r["game_id"]),
            "player_id": int(r["player_id"]),
            "player_name": r["player_name"],
            "home_team_code": str(r["home_team_code"]).strip().upper(),
            "away_team_code": str(r["away_team_code"]).strip().upper(),
            "prop_type": str(r["prop_type"]).strip().lower(),
            "line": float(r["line"]),
        }
    return out


def _build_wide_lookup(wide_rows: pd.DataFrame) -> Dict[Tuple[int, int, str], Dict[str, Any]]:
    out: Dict[Tuple[int, int, str], Dict[str, Any]] = {}
    for _, r in wide_rows.iterrows():
        key = (
            int(r["game_id"]),
            int(r["player_id"]),
            str(r["prop_type"]).strip().lower(),
        )
        out[key] = {
            "team": _clean_str(r.get("team")),
            "opponent": _clean_str(r.get("opponent")),
            "is_home": (None if pd.isna(r.get("is_home")) else bool(r.get("is_home"))),
        }
    return out


def _parse_snapshot_rows(
    *,
    snapshot_ref: SnapshotRef,
    market_to_prop: Dict[str, str],
    slate_lookup: Dict[Tuple[str, str, str, float, str], Dict[str, Any]],
    wide_lookup: Dict[Tuple[int, int, str], Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    team_rev = _build_team_name_reverse()
    rows: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {
        "events": 0,
        "raw_outcomes": 0,
        "unsupported_market": 0,
        "missing_team_map": 0,
        "missing_player_or_line": 0,
        "unmatched_to_slate": 0,
        "matched_rows": 0,
    }

    events = _load_events(snapshot_ref.path)
    counts["events"] = len(events)
    grouped: Dict[Tuple[str, str, str, str, str, float, str], Dict[str, Any]] = {}

    for ev in events:
        home_name = _clean_str(ev.get("home_team"))
        away_name = _clean_str(ev.get("away_team"))
        event_id = _clean_str(ev.get("id"))
        if not home_name or not away_name:
            counts["missing_team_map"] += 1
            continue
        home_code = team_rev.get(_norm_name(home_name))
        away_code = team_rev.get(_norm_name(away_name))
        if not home_code or not away_code:
            counts["missing_team_map"] += 1
            continue

        for book in ev.get("bookmakers") or []:
            bookmaker_key = _clean_str(book.get("key")) or _clean_str(book.get("title"))
            if not bookmaker_key:
                continue
            bookmaker_key = bookmaker_key.strip().lower()

            for market in book.get("markets") or []:
                market_key = _clean_str(market.get("key"))
                if not market_key:
                    continue
                prop_type = _canonical_prop_type(market_to_prop.get(str(market_key).strip()))
                if not prop_type:
                    counts["unsupported_market"] += 1
                    continue

                for outcome in market.get("outcomes") or []:
                    side = str(outcome.get("name") or "").strip().lower()
                    if side not in {"over", "under"}:
                        continue
                    player_name = _clean_str(outcome.get("description"))
                    line = _line_key(outcome.get("point"))
                    if not player_name or line is None:
                        counts["missing_player_or_line"] += 1
                        continue
                    try:
                        price = float(outcome.get("price"))
                    except Exception:
                        continue

                    counts["raw_outcomes"] += 1
                    key = (
                        str(home_code).upper(),
                        str(away_code).upper(),
                        prop_type,
                        _norm_name(player_name),
                        str(market_key).strip(),
                        float(line),
                        bookmaker_key,
                    )
                    rec = grouped.setdefault(
                        key,
                        {
                            "snapshot_ts": snapshot_ref.ts,
                            "snapshot_file": str(snapshot_ref.path),
                            "event_id": event_id,
                            "home_team_code": str(home_code).upper(),
                            "away_team_code": str(away_code).upper(),
                            "prop_type": prop_type,
                            "market_key": str(market_key).strip(),
                            "line": float(line),
                            "bookmaker_key": bookmaker_key,
                            "player_name": player_name,
                            "player_name_norm": _norm_name(player_name),
                            "price_over_american": None,
                            "price_under_american": None,
                        },
                    )
                    rec[f"price_{side}_american"] = float(price)

    for _, rec in grouped.items():
        lookup_key = (
            rec["home_team_code"],
            rec["away_team_code"],
            rec["prop_type"],
            rec["line"],
            rec["player_name_norm"],
        )
        matched = slate_lookup.get(lookup_key)
        if matched is None:
            counts["unmatched_to_slate"] += 1
            continue

        wide = wide_lookup.get(
            (
                int(matched["game_id"]),
                int(matched["player_id"]),
                str(matched["prop_type"]).strip().lower(),
            ),
            {},
        )
        rows.append(
            {
                "slate_date": matched["slate_date"],
                "game_date": matched["game_date"],
                "snapshot_ts": rec["snapshot_ts"],
                "snapshot_file": rec["snapshot_file"],
                "event_id": rec["event_id"],
                "game_id": int(matched["game_id"]),
                "player_id": int(matched["player_id"]),
                "player_name": matched["player_name"] or rec["player_name"],
                "player_name_norm": rec["player_name_norm"],
                "home_team_code": rec["home_team_code"],
                "away_team_code": rec["away_team_code"],
                "team": wide.get("team"),
                "opponent": wide.get("opponent"),
                "is_home": wide.get("is_home"),
                "prop_type": rec["prop_type"],
                "market_key": rec["market_key"],
                "line": float(rec["line"]),
                "bookmaker_key": rec["bookmaker_key"],
                "price_over_american": rec["price_over_american"],
                "price_under_american": rec["price_under_american"],
            }
        )
        counts["matched_rows"] += 1

    return rows, counts


def _ensure_stage_tables() -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS mlb;

    CREATE TABLE IF NOT EXISTS mlb.today_odds_book_rows (
      slate_date date NOT NULL,
      game_date date NOT NULL,
      snapshot_ts timestamptz NOT NULL,
      snapshot_file text NOT NULL,
      event_id text,
      game_id bigint NOT NULL,
      player_id bigint NOT NULL,
      player_name text NOT NULL,
      player_name_norm text NOT NULL,
      home_team_code text NOT NULL,
      away_team_code text NOT NULL,
      team text,
      opponent text,
      is_home boolean,
      prop_type text NOT NULL,
      market_key text NOT NULL,
      line numeric(8,3) NOT NULL,
      bookmaker_key text NOT NULL,
      price_over_american numeric,
      price_under_american numeric
    );

    CREATE TABLE IF NOT EXISTS mlb.today_slate_rows (
      slate_date date NOT NULL,
      game_date date NOT NULL,
      game_id bigint NOT NULL,
      player_id bigint NOT NULL,
      player_name text NOT NULL,
      player_name_norm text NOT NULL,
      home_team_code text NOT NULL,
      away_team_code text NOT NULL,
      prop_type text NOT NULL,
      market_key text,
      line numeric(8,3) NOT NULL,
      prob_over numeric,
      prob_under numeric
    );

    CREATE TABLE IF NOT EXISTS mlb.today_wide_rows (
      slate_date date NOT NULL,
      game_date date NOT NULL,
      game_id bigint NOT NULL,
      player_id bigint NOT NULL,
      player_name text NOT NULL,
      player_name_norm text NOT NULL,
      prop_type text NOT NULL,
      team text,
      opponent text,
      is_home boolean
    );

    CREATE INDEX IF NOT EXISTS idx_today_odds_key
      ON mlb.today_odds_book_rows (slate_date, game_id, player_id, prop_type, line, snapshot_ts);
    CREATE INDEX IF NOT EXISTS idx_today_odds_book
      ON mlb.today_odds_book_rows (slate_date, bookmaker_key);
    CREATE INDEX IF NOT EXISTS idx_today_slate_key
      ON mlb.today_slate_rows (slate_date, game_id, player_id, prop_type, line);
    CREATE INDEX IF NOT EXISTS idx_today_wide_key
      ON mlb.today_wide_rows (slate_date, game_id, player_id, prop_type);
    """
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()


def _replace_date_rows(
    *,
    slate_date: str,
    slate_rows: pd.DataFrame,
    wide_rows: pd.DataFrame,
    odds_rows: pd.DataFrame,
) -> Dict[str, int]:
    def _py(value: Any) -> Any:
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        return value

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM mlb.today_odds_book_rows WHERE slate_date = %s::date", (slate_date,))
        cur.execute("DELETE FROM mlb.today_slate_rows WHERE slate_date = %s::date", (slate_date,))
        cur.execute("DELETE FROM mlb.today_wide_rows WHERE slate_date = %s::date", (slate_date,))

        inserted: Dict[str, int] = {
            "today_slate_rows": 0,
            "today_wide_rows": 0,
            "today_odds_book_rows": 0,
        }

        if not slate_rows.empty:
            cols = [
                "slate_date",
                "game_date",
                "game_id",
                "player_id",
                "player_name",
                "player_name_norm",
                "home_team_code",
                "away_team_code",
                "prop_type",
                "market_key",
                "line",
                "prob_over",
                "prob_under",
            ]
            sql = f"""
            INSERT INTO mlb.today_slate_rows ({",".join(cols)})
            VALUES ({",".join(["%s"] * len(cols))})
            """
            cur.executemany(sql, [tuple(_py(r[c]) for c in cols) for _, r in slate_rows.iterrows()])
            inserted["today_slate_rows"] = len(slate_rows)

        if not wide_rows.empty:
            cols = [
                "slate_date",
                "game_date",
                "game_id",
                "player_id",
                "player_name",
                "player_name_norm",
                "prop_type",
                "team",
                "opponent",
                "is_home",
            ]
            sql = f"""
            INSERT INTO mlb.today_wide_rows ({",".join(cols)})
            VALUES ({",".join(["%s"] * len(cols))})
            """
            cur.executemany(sql, [tuple(_py(r[c]) for c in cols) for _, r in wide_rows.iterrows()])
            inserted["today_wide_rows"] = len(wide_rows)

        if not odds_rows.empty:
            cols = [
                "slate_date",
                "game_date",
                "snapshot_ts",
                "snapshot_file",
                "event_id",
                "game_id",
                "player_id",
                "player_name",
                "player_name_norm",
                "home_team_code",
                "away_team_code",
                "team",
                "opponent",
                "is_home",
                "prop_type",
                "market_key",
                "line",
                "bookmaker_key",
                "price_over_american",
                "price_under_american",
            ]
            sql = f"""
            INSERT INTO mlb.today_odds_book_rows ({",".join(cols)})
            VALUES ({",".join(["%s"] * len(cols))})
            """
            cur.executemany(sql, [tuple(_py(r[c]) for c in cols) for _, r in odds_rows.iterrows()])
            inserted["today_odds_book_rows"] = len(odds_rows)

        conn.commit()
    return inserted


def main(argv: Optional[Iterable[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Load MLB /today workspace staging tables from current file artifacts.")
    ap.add_argument("--slate-date", default=_et_today(), help="Target slate date (ET) in YYYY-MM-DD.")
    ap.add_argument("--odds-root", default="backend/mlb/exports/odds_history", help="Odds history root path.")
    ap.add_argument("--slate-csv", default="", help="Optional explicit path to mlb_slate_output.csv.")
    ap.add_argument("--wide-csv", default="", help="Optional explicit path to mlb_predictions_wide_calibrated.csv.")
    ap.add_argument("--snapshot-json", default="", help="Optional explicit odds snapshot json to load.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    slate_date = str(args.slate_date).strip()
    day_dir = Path(str(args.odds_root)).expanduser() / slate_date
    if not day_dir.exists():
        raise FileNotFoundError(f"odds day folder not found: {day_dir}")

    slate_csv = Path(str(args.slate_csv)).expanduser() if str(args.slate_csv).strip() else (day_dir / "mlb_slate_output.csv")
    wide_csv = Path(str(args.wide_csv)).expanduser() if str(args.wide_csv).strip() else (
        day_dir / "mlb_predictions_wide_calibrated.csv"
    )
    snapshot_json = Path(str(args.snapshot_json)).expanduser() if str(args.snapshot_json).strip() else None

    print(f"[mlb-today-workspace] slate_date={slate_date}")
    print(f"[mlb-today-workspace] day_dir={day_dir}")
    print(f"[mlb-today-workspace] slate_csv={slate_csv}")
    print(f"[mlb-today-workspace] wide_csv={wide_csv}")

    slate_rows = _load_slate_rows(slate_csv, slate_date)
    wide_rows = _load_wide_rows(wide_csv, slate_date)
    if slate_rows.empty:
        raise RuntimeError(f"no slate rows for date={slate_date}")

    slate_lookup = _build_slate_lookup(slate_rows)
    wide_lookup = _build_wide_lookup(wide_rows)

    market_to_prop = {
        str(k).strip(): _canonical_prop_type(v)
        for k, v in get_market_to_prop_map(include_aliases=True).items()
        if _clean_str(k) and _clean_str(v)
    }
    snapshots = _find_snapshot_files(day_dir, snapshot_json)
    print(f"[mlb-today-workspace] snapshots_found={len(snapshots)}")

    odds_out: List[Dict[str, Any]] = []
    summary_counts: Dict[str, int] = {
        "events": 0,
        "raw_outcomes": 0,
        "unsupported_market": 0,
        "missing_team_map": 0,
        "missing_player_or_line": 0,
        "unmatched_to_slate": 0,
        "matched_rows": 0,
    }
    sanitize_stats: Dict[str, int] = {
        "invalid_odds_values_nullified": 0,
        "rows_dropped_no_valid_odds": 0,
    }
    for ref in snapshots:
        rows, counts = _parse_snapshot_rows(
            snapshot_ref=ref,
            market_to_prop=market_to_prop,
            slate_lookup=slate_lookup,
            wide_lookup=wide_lookup,
        )
        odds_out.extend(rows)
        for k, v in counts.items():
            summary_counts[k] = int(summary_counts.get(k, 0) + int(v))

    odds_rows = pd.DataFrame(odds_out)
    if not odds_rows.empty:
        odds_rows["line"] = pd.to_numeric(odds_rows["line"], errors="coerce").round(3)
        odds_rows = odds_rows.dropna(subset=["game_id", "player_id", "prop_type", "line", "bookmaker_key"])
        odds_rows["game_id"] = odds_rows["game_id"].astype("int64")
        odds_rows["player_id"] = odds_rows["player_id"].astype("int64")
        odds_rows, sanitize_stats = _sanitize_american_odds_frame(odds_rows)
        odds_rows = odds_rows.sort_values(
            ["snapshot_ts", "game_id", "player_id", "prop_type", "line", "bookmaker_key"],
            kind="stable",
        )
        odds_rows = odds_rows.drop_duplicates(
            subset=[
                "slate_date",
                "snapshot_file",
                "event_id",
                "game_id",
                "player_id",
                "prop_type",
                "line",
                "bookmaker_key",
            ],
            keep="first",
        ).reset_index(drop=True)
    else:
        odds_rows = pd.DataFrame(
            columns=[
                "slate_date",
                "game_date",
                "snapshot_ts",
                "snapshot_file",
                "event_id",
                "game_id",
                "player_id",
                "player_name",
                "player_name_norm",
                "home_team_code",
                "away_team_code",
                "team",
                "opponent",
                "is_home",
                "prop_type",
                "market_key",
                "line",
                "bookmaker_key",
                "price_over_american",
                "price_under_american",
            ]
        )

    # Required verification: ensure stable player_id resolution is actually achieved.
    resolved_odds = int(len(odds_rows))
    unresolved = int(summary_counts.get("unmatched_to_slate", 0))
    if resolved_odds == 0:
        raise RuntimeError(
            "no odds rows resolved to stable player_id/game_id keys. "
            "Check slate-date alignment and snapshot availability."
        )

    _ensure_stage_tables()
    inserted = _replace_date_rows(
        slate_date=slate_date,
        slate_rows=slate_rows,
        wide_rows=wide_rows,
        odds_rows=odds_rows,
    )

    print("[mlb-today-workspace] load summary")
    print(f"  today_slate_rows={inserted['today_slate_rows']}")
    print(f"  today_wide_rows={inserted['today_wide_rows']}")
    print(f"  today_odds_book_rows={inserted['today_odds_book_rows']}")
    print("[mlb-today-workspace] odds parse stats")
    print(f"  events={summary_counts['events']}")
    print(f"  raw_outcomes={summary_counts['raw_outcomes']}")
    print(f"  unsupported_market={summary_counts['unsupported_market']}")
    print(f"  missing_team_map={summary_counts['missing_team_map']}")
    print(f"  missing_player_or_line={summary_counts['missing_player_or_line']}")
    print(f"  unmatched_to_slate={unresolved}")
    print(f"  matched_rows={summary_counts['matched_rows']}")
    print("[mlb-today-workspace] odds sanitization")
    print(f"  invalid_odds_values_nullified={sanitize_stats['invalid_odds_values_nullified']}")
    print(f"  rows_dropped_no_valid_odds={sanitize_stats['rows_dropped_no_valid_odds']}")
    print("[mlb-today-workspace] player_id resolution")
    print(f"  resolved_odds_rows={resolved_odds}")
    print(f"  unresolved_odds_rows={unresolved}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
