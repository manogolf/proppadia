# backend/app/routes/admin.py
import os, re, hmac, shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
import requests
from typing import Any
from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import FileResponse, JSONResponse

# prefer psycopg v3; fall back to psycopg2
try:
    import psycopg  # type: ignore
    _PSYCOPG_IS_V3 = True
except Exception:  # pragma: no cover
    import psycopg2 as psycopg  # type: ignore
    _PSYCOPG_IS_V3 = False

router = APIRouter()


# ----------------------------- helpers ----------------------------------------

def _safe_eq(a: str | None, b: str | None) -> bool:
    a = (a or "").strip()
    b = (b or "").strip()
    return bool(a and b and hmac.compare_digest(a, b))

def _require_auth(token: str | None):
    if not _safe_eq(token, os.getenv("EXPORT_TOKEN")):
        raise HTTPException(status_code=401, detail="unauthorized")

def _root() -> Path:
    return Path(os.getenv("EXPORT_ROOT", "/var/data/proppadia"))

def _exports_dir(day: str | None = None) -> Path:
    d = (day or date.today().isoformat()).strip()
    return _root() / "nhl" / "exports" / d

def _get_db_url() -> str:
    """
    Resolve Postgres URL from envs and normalize for psycopg.
    Prefer DATABASE_URL; fall back to POSTGRES_URL / PGDATABASE_URL.
    """
    for name in ("DATABASE_URL", "POSTGRES_URL", "PGDATABASE_URL"):
        raw = os.getenv(name)
        if not raw:
            continue
        url = raw.strip()
        # normalize sqlalchemy-style driver suffix
        if url.startswith("postgresql+"):
            url = re.sub(r"^postgresql\+[^:]+://", "postgresql://", url, count=1)
        elif url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        return url
    raise HTTPException(status_code=500,
                        detail="DB URL not found (expected DATABASE_URL / POSTGRES_URL / PGDATABASE_URL)")

def _copy_to_csv(cur, sql: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # generous server-side timeouts for COPY
    cur.execute("SET statement_timeout = '10min';")
    cur.execute("SET lock_timeout = '30s';")
    cur.execute("SET idle_in_transaction_session_timeout = '5min';")

    if _PSYCOPG_IS_V3:
        with open(out_path, "wb") as f:
            with cur.copy(sql) as cp:
                while True:
                    chunk = cp.read()
                    if not chunk:
                        break
                    f.write(chunk)
    else:
        with open(out_path, "wb") as f:
            cur.copy_expert(sql, f)

def _exec_sqls(cur, statements: Iterable[str]) -> None:
    """Run a few DDL statements with safe timeouts."""
    cur.execute("SET statement_timeout = '10min';")
    cur.execute("SET lock_timeout = '30s';")
    cur.execute("SET idle_in_transaction_session_timeout = '5min';")
    for s in statements:
        cur.execute(s)


# ----------------------------- endpoints --------------------------------------

@router.get("/health")
def health():
    return {"status": "ok", "ts": datetime.utcnow().isoformat() + "Z"}

@router.post("/db-ping")
def db_ping(token: str | None = Query(None), token_body: dict | None = Body(None)):
    _require_auth(token or (isinstance(token_body, dict) and token_body.get("token")))
    url = _get_db_url()
    if _PSYCOPG_IS_V3:
        with psycopg.connect(url) as conn, conn.cursor() as cur:
            cur.execute("select now()")
            now = cur.fetchone()[0]
    else:
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("select now()")
                now = cur.fetchone()[0]
    return {"ok": True, "now": str(now)}


def _exec_many(cur, sql: str, rows: list[tuple[Any, ...]]):
    if not rows: return
    cur.executemany(sql, rows)

@router.post("/ingest-schedule")
def ingest_schedule(
    token: str,
    date_str: str = Query(..., description="YYYY-MM-DD"),
    provider: str = Query("nhl"),
    raw: dict | None = Body(None),   # << NEW: optional pre-fetched JSON
):
    _require_auth(token)
    url = _get_db_url()

    # 1) get schedule data
    if isinstance(raw, dict) and "dates" in raw:
        data = raw
    else:
        resp = requests.get(
            "https://statsapi.web.nhl.com/api/v1/schedule",
            params={"date": date_str, "expand": "schedule.teams,schedule.linescore"},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

    games_api = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            game_pk = str(g.get("gamePk"))
            status = (g.get("status", {}) or {}).get("abstractGameState", "scheduled").lower()
            home = (g.get("teams", {}) or {}).get("home", {}) or {}
            away = (g.get("teams", {}) or {}).get("away", {}) or {}
            ht = home.get("team", {}) or {}
            at = away.get("team", {}) or {}
            games_api.append({
                "game_pk": game_pk,
                "game_date": date_str,
                "status": "final" if status == "final" else ("live" if status == "live" else "scheduled"),
                "home_id": int(ht.get("id")),
                "home_abbr": ht.get("abbreviation") or ht.get("triCode") or ht.get("name"),
                "home_name": ht.get("name"),
                "away_id": int(at.get("id")),
                "away_abbr": at.get("abbreviation") or at.get("triCode") or at.get("name"),
                "away_name": at.get("name"),
            })

    if not games_api:
        return {"ok": True, "date": date_str, "found": 0, "inserted_or_updated": 0, "mapped": 0}

    # 2) upsert teams + games + external ids
    if _PSYCOPG_IS_V3:
        with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
            # teams
            team_rows = []
            for g in games_api:
                team_rows.append((g["home_id"], g["home_abbr"], g["home_name"]))
                team_rows.append((g["away_id"], g["away_abbr"], g["away_name"]))
            _exec_many(cur, """
                INSERT INTO nhl.teams (team_id, abbr, name, active)
                VALUES (%s, %s, %s, true)
                ON CONFLICT (team_id) DO UPDATE
                  SET abbr = EXCLUDED.abbr,
                      name = EXCLUDED.name;
            """, team_rows)

            # games (use NHL gamePk as our game_id if your schema allows bigints matching provider ids;
            # if not, you can generate your own game_id here. Your schema uses bigint keys already.)
            game_rows = []
            for g in games_api:
                game_rows.append((
                    int(g["game_pk"]),
                    date_str,
                    g["home_id"],
                    g["away_id"],
                    g["status"],
                ))
            _exec_many(cur, """
                INSERT INTO nhl.games (game_id, game_date, home_team_id, away_team_id, status)
                VALUES (%s, %s::date, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE
                  SET game_date = EXCLUDED.game_date,
                      home_team_id = EXCLUDED.home_team_id,
                      away_team_id = EXCLUDED.away_team_id,
                      status = EXCLUDED.status;
            """, game_rows)

            # external id mapping
            map_rows = []
            for g in games_api:
                map_rows.append((int(g["game_pk"]), provider, g["game_pk"]))
            _exec_many(cur, """
                INSERT INTO nhl.game_external_ids (game_id, provider, provider_game_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (game_id, provider) DO UPDATE
                  SET provider_game_id = EXCLUDED.provider_game_id;
            """, map_rows)
    else:
        with psycopg.connect(url) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                team_rows = []
                for g in games_api:
                    team_rows.append((g["home_id"], g["home_abbr"], g["home_name"]))
                    team_rows.append((g["away_id"], g["away_abbr"], g["away_name"]))
                _exec_many(cur, """
                    INSERT INTO nhl.teams (team_id, abbr, name, active)
                    VALUES (%s, %s, %s, true)
                    ON CONFLICT (team_id) DO UPDATE
                      SET abbr = EXCLUDED.abbr,
                          name = EXCLUDED.name;
                """, team_rows)

                game_rows = []
                for g in games_api:
                    game_rows.append((
                        int(g["game_pk"]),
                        date_str,
                        g["home_id"],
                        g["away_id"],
                        g["status"],
                    ))
                _exec_many(cur, """
                    INSERT INTO nhl.games (game_id, game_date, home_team_id, away_team_id, status)
                    VALUES (%s, %s::date, %s, %s, %s)
                    ON CONFLICT (game_id) DO UPDATE
                      SET game_date = EXCLUDED.game_date,
                          home_team_id = EXCLUDED.home_team_id,
                          away_team_id = EXCLUDED.away_team_id,
                          status = EXCLUDED.status;
                """, game_rows)

                map_rows = []
                for g in games_api:
                    map_rows.append((int(g["game_pk"]), provider, g["game_pk"]))
                _exec_many(cur, """
                    INSERT INTO nhl.game_external_ids (game_id, provider, provider_game_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (game_id, provider) DO UPDATE
                      SET provider_game_id = EXCLUDED.provider_game_id;
                """, map_rows)

    return {
        "ok": True,
        "date": date_str,
        "found": len(games_api),
        "inserted_or_updated": len(games_api),
        "mapped": len(games_api),
    }

@router.post("/ingest-logs")
def ingest_logs(
    token: str = Query(...),
    provider: str = Query("nhl"),
    raw: dict | None = Body(None),
):
    _require_auth(token)
    url = _get_db_url()

    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail="JSON body required")

    # Expect: {"gamePk": 2025010018, "skaters":[...], "goalies":[...]}
    try:
        game_pk = int(raw["gamePk"])
    except Exception:
        raise HTTPException(status_code=400, detail="missing/invalid gamePk")

    skaters = raw.get("skaters", []) or []
    goalies = raw.get("goalies", []) or []

    # minimal expected fields per row:
    # skater: {player_id, team_id, opponent_id, is_home, shots_on_goal, shot_attempts, toi_minutes, game_date}
    # goalie: {player_id, team_id, opponent_id, is_home, saves, shots_faced, goals_allowed, toi_minutes, game_date}

    if _PSYCOPG_IS_V3:
        with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
            # insert skaters
            if skaters:
                cur.executemany("""
                    INSERT INTO nhl.skater_game_logs_raw
                      (player_id, game_id, team_id, opponent_id, is_home,
                       shots_on_goal, shot_attempts, toi_minutes, game_date)
                    VALUES
                      (%(player_id)s, %(game_id)s, %(team_id)s, %(opponent_id)s, %(is_home)s,
                       %(shots_on_goal)s, %(shot_attempts)s, %(toi_minutes)s, %(game_date)s::date)
                    ON CONFLICT (player_id, game_id) DO UPDATE
                      SET team_id=EXCLUDED.team_id,
                          opponent_id=EXCLUDED.opponent_id,
                          is_home=EXCLUDED.is_home,
                          shots_on_goal=EXCLUDED.shots_on_goal,
                          shot_attempts=EXCLUDED.shot_attempts,
                          toi_minutes=EXCLUDED.toi_minutes,
                          game_date=EXCLUDED.game_date;
                """, [{**r, "game_id": game_pk} for r in skaters])

            # insert goalies
            if goalies:
                cur.executemany("""
                    INSERT INTO nhl.goalie_game_logs_raw
                      (player_id, game_id, team_id, opponent_id, is_home,
                       saves, shots_faced, goals_allowed, toi_minutes, game_date)
                    VALUES
                      (%(player_id)s, %(game_id)s, %(team_id)s, %(opponent_id)s, %(is_home)s,
                       %(saves)s, %(shots_faced)s, %(goals_allowed)s, %(toi_minutes)s, %(game_date)s::date)
                    ON CONFLICT (player_id, game_id) DO UPDATE
                      SET team_id=EXCLUDED.team_id,
                          opponent_id=EXCLUDED.opponent_id,
                          is_home=EXCLUDED.is_home,
                          saves=EXCLUDED.saves,
                          shots_faced=EXCLUDED.shots_faced,
                          goals_allowed=EXCLUDED.goals_allowed,
                          toi_minutes=EXCLUDED.toi_minutes,
                          game_date=EXCLUDED.game_date;
                """, [{**r, "game_id": game_pk} for r in goalies])

            # mark game final if we got any logs
            if skaters or goalies:
                cur.execute("""
                    UPDATE nhl.games SET status='final'
                    WHERE game_id = %s
                """, (game_pk,))

    else:
        with psycopg.connect(url) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                if skaters:
                    cur.executemany("""
                        INSERT INTO nhl.skater_game_logs_raw
                          (player_id, game_id, team_id, opponent_id, is_home,
                           shots_on_goal, shot_attempts, toi_minutes, game_date)
                        VALUES
                          (%(player_id)s, %(game_id)s, %(team_id)s, %(opponent_id)s, %(is_home)s,
                           %(shots_on_goal)s, %(shot_attempts)s, %(toi_minutes)s, %(game_date)s::date)
                        ON CONFLICT (player_id, game_id) DO UPDATE
                          SET team_id=EXCLUDED.team_id,
                              opponent_id=EXCLUDED.opponent_id,
                              is_home=EXCLUDED.is_home,
                              shots_on_goal=EXCLUDED.shots_on_goal,
                              shot_attempts=EXCLUDED.shot_attempts,
                              toi_minutes=EXCLUDED.toi_minutes,
                              game_date=EXCLUDED.game_date;
                    """, [{**r, "game_id": game_pk} for r in skaters])

                if goalies:
                    cur.executemany("""
                        INSERT INTO nhl.goalie_game_logs_raw
                          (player_id, game_id, team_id, opponent_id, is_home,
                           saves, shots_faced, goals_allowed, toi_minutes, game_date)
                        VALUES
                          (%(player_id)s, %(game_id)s, %(team_id)s, %(opponent_id)s, %(is_home)s,
                           %(saves)s, %(shots_faced)s, %(goals_allowed)s, %(toi_minutes)s, %(game_date)s::date)
                        ON CONFLICT (player_id, game_id) DO UPDATE
                          SET team_id=EXCLUDED.team_id,
                              opponent_id=EXCLUDED.opponent_id,
                              is_home=EXCLUDED.is_home,
                              saves=EXCLUDED.saves,
                              shots_faced=EXCLUDED.shots_faced,
                              goals_allowed=EXCLUDED.goals_allowed,
                              toi_minutes=EXCLUDED.toi_minutes,
                              game_date=EXCLUDED.game_date;
                    """, [{**r, "game_id": game_pk} for r in goalies])

                if skaters or goalies:
                    cur.execute("UPDATE nhl.games SET status='final' WHERE game_id=%s", (game_pk,))

    return {"ok": True, "game_id": game_pk, "skaters": len(skaters), "goalies": len(goalies)}

@router.get("/version")
def version():
    """
    Minimal deploy sanity:
      - build commit (if Render provides it)
      - server time
      - important env flags present
      - disk mount and free space
    """
    # Render sets these sometimes; fallbacks are fine
    commit = os.getenv("RENDER_GIT_COMMIT") or os.getenv("RENDER_GIT_BRANCH") or "unknown"
    mount = "/var/data"
    total, used, free = shutil.disk_usage(mount)
    return {
        "ok": True,
        "ts": datetime.utcnow().isoformat() + "Z",
        "commit": commit,
        "env": {
            "EXPORT_TOKEN_set": bool(os.getenv("EXPORT_TOKEN")),
            "SUPABASE_URL_set": bool(os.getenv("SUPABASE_URL")),
            "SUPABASE_DB_URL_set": bool(os.getenv("SUPABASE_DB_URL")),
        },
        "disk": {
            "mount": mount,
            "total_gb": round(total / 1e9, 2),
            "free_gb": round(free / 1e9, 2),
        },
    }

@router.post("/refresh-ready")
def refresh_ready(token: str | None = Query(None), token_body: dict | None = Body(None)):
    """
    Refresh the two materialized views used for exports.
    Runs CONCURRENTLY when possible; ignores if objects missing.
    Returns row counts after refresh.
    """
    _require_auth(token or (isinstance(token_body, dict) and token_body.get("token")))
    url = _get_db_url()

    mv_sog = "nhl.training_features_nhl_sog_v2_ready"
    mv_gsv = "nhl.training_features_goalie_saves_v2_ready"

    counts = {}
    if _PSYCOPG_IS_V3:
        with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
            # best-effort concurrent refresh
            for mv in (mv_sog, mv_gsv):
                try:
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv};")
                except Exception:
                    try:
                        cur.execute(f"REFRESH MATERIALIZED VIEW {mv};")
                    except Exception:
                        pass
            # counts
            for mv in (mv_sog, mv_gsv):
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {mv}")
                    counts[mv] = int(cur.fetchone()[0])
                except Exception:
                    counts[mv] = None
    else:
        with psycopg.connect(url) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for mv in (mv_sog, mv_gsv):
                    try:
                        cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv};")
                    except Exception:
                        try:
                            cur.execute(f"REFRESH MATERIALIZED VIEW {mv};")
                        except Exception:
                            pass
                for mv in (mv_sog, mv_gsv):
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {mv}")
                        counts[mv] = int(cur.fetchone()[0])
                    except Exception:
                        counts[mv] = None

    return {"ok": True, "counts": counts}

@router.post("/refresh-export")
def refresh_export(
    token: str | None = Query(None),                 # auth via query param
    token_body: dict | None = Body(None),           # or JSON: {"token":"..."}
):
    _require_auth(token or (isinstance(token_body, dict) and token_body.get("token")))

    out_dir = _exports_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    url = _get_db_url()
    sog_path = out_dir / "train_nhl_sog_v2.csv"
    gsv_path = out_dir / "train_goalie_saves_v2.csv"

    try:
        if _PSYCOPG_IS_V3:
            with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
                _copy_to_csv(cur,
                    "COPY (SELECT * FROM nhl.export_training_nhl_sog_v2 ORDER BY game_date, player_id) "
                    "TO STDOUT WITH CSV HEADER", sog_path)
                _copy_to_csv(cur,
                    "COPY (SELECT * FROM nhl.export_training_goalie_saves_v2 ORDER BY game_date, player_id) "
                    "TO STDOUT WITH CSV HEADER", gsv_path)
                # optional sanity counts
                cur.execute("SELECT COUNT(*) FROM nhl.export_training_nhl_sog_v2")
                sog_rows = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM nhl.export_training_goalie_saves_v2")
                gsv_rows = int(cur.fetchone()[0])
        else:
            with psycopg.connect(url) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    _copy_to_csv(cur,
                        "COPY (SELECT * FROM nhl.export_training_nhl_sog_v2 ORDER BY game_date, player_id) "
                        "TO STDOUT WITH CSV HEADER", sog_path)
                    _copy_to_csv(cur,
                        "COPY (SELECT * FROM nhl.export_training_goalie_saves_v2 ORDER BY game_date, player_id) "
                        "TO STDOUT WITH CSV HEADER", gsv_path)
                    cur.execute("SELECT COUNT(*) FROM nhl.export_training_nhl_sog_v2")
                    sog_rows = int(cur.fetchone()[0])
                    cur.execute("SELECT COUNT(*) FROM nhl.export_training_goalie_saves_v2")
                    gsv_rows = int(cur.fetchone()[0])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"export failed: {type(e).__name__}: {e}")

    return {
        "ok": True,
        "out_dir": str(out_dir),
        "files": [
            {"name": sog_path.name, "bytes": sog_path.stat().st_size, "rows": sog_rows},
            {"name": gsv_path.name, "bytes": gsv_path.stat().st_size, "rows": gsv_rows},
        ],
    }

@router.get("/download-export")
def download_export(
    token: str,
    which: str,                   # "sog" or "goalie"
    date_str: str | None = None,  # defaults to today
):
    _require_auth(token)
    fname = "train_nhl_sog_v2.csv" if which == "sog" else "train_goalie_saves_v2.csv"
    fpath = _exports_dir(date_str) / fname
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"not found: {fpath}")
    return FileResponse(str(fpath), media_type="text/csv", filename=fname)

@router.get("/list-exports")
def list_exports(token: str, limit: int = Query(60, ge=1, le=365)):
    _require_auth(token)
    base = _root() / "nhl" / "exports"
    if not base.exists():
        return {"ok": True, "dates": []}
    entries = []
    for child in sorted(base.iterdir(), reverse=True):
        if not child.is_dir():
            continue
        try:
            # expect YYYY-MM-DD
            datetime.strptime(child.name, "%Y-%m-%d")
        except Exception:
            continue
        info = {"date": child.name, "files": []}
        for fname in ("train_nhl_sog_v2.csv", "train_goalie_saves_v2.csv"):
            p = child / fname
            info["files"].append({"name": fname, "exists": p.exists(), "bytes": p.stat().st_size if p.exists() else 0})
        entries.append(info)
        if len(entries) >= limit:
            break
    return {"ok": True, "dates": entries}

@router.post("/cleanup-exports")
def cleanup_exports(token: str, keep_days: int = Query(30, ge=7, le=365)):
    """
    Delete export folders older than `keep_days`.
    Safety floor = 7 days.
    """
    _require_auth(token)
    base = _root() / "nhl" / "exports"
    if not base.exists():
        return {"ok": True, "deleted": 0, "kept": 0}

    cutoff = date.today() - timedelta(days=keep_days)
    deleted = 0
    kept = 0
    for child in base.iterdir():
        if not child.is_dir():
            continue
        try:
            d = datetime.strptime(child.name, "%Y-%m-%d").date()
        except Exception:
            continue
        if d < cutoff:
            try:
                shutil.rmtree(child)
                deleted += 1
            except Exception:
                pass
        else:
            kept += 1
    return {"ok": True, "deleted": deleted, "kept": kept, "cutoff": str(cutoff)}
