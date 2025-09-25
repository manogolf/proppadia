# ml/export_tb_training.py

import os
import re
import socket
from pathlib import Path
from urllib.parse import quote_plus

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

def _clean(v: str | None) -> str:
    v = (v or "").strip()
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        v = v[1:-1].strip()
    return v

def _resolve_addrs(host: str, port: int = 5432):
    """Return (ipv4, ipv6) string literals if resolvable, else (None, None)."""
    ipv4 = None
    ipv6 = None
    try:
        for fam in (socket.AF_INET, socket.AF_INET6):
            try:
                infos = socket.getaddrinfo(host, port, family=fam, type=socket.SOCK_STREAM)
                if infos:
                    addr = infos[0][4][0]
                    if fam == socket.AF_INET and not ipv4:
                        ipv4 = addr
                    if fam == socket.AF_INET6 and not ipv6:
                        ipv6 = addr
            except Exception:
                pass
    except Exception:
        pass
    return ipv4, ipv6

def _conninfo() -> str:
    """
    Prefer DATABASE_URL; otherwise build a libpq-style DSN using PG* vars.
    Adds sslmode=require, and includes hostaddr= to bypass DNS at connect time.
    """
    url = _clean(os.getenv("DATABASE_URL"))
    if url and not re.search(r"YOUR[_-]?PASSWORD", url, re.I):
        if "sslmode=" not in url:
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return url

    required = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    vals = {k: _clean(os.getenv(k)) for k in required}
    missing = [k for k, v in vals.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing required env vars: {', '.join(missing)}. "
            "Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD in your .env"
        )

    host = vals["PGHOST"]
    db   = vals["PGDATABASE"]
    user = vals["PGUSER"]
    pwd  = vals["PGPASSWORD"]
    port = _clean(os.getenv("PGPORT")) or "5432"
    sslm = _clean(os.getenv("PGSSLMODE")) or "require"

    ipv4, ipv6 = _resolve_addrs(host, int(port))
    # Prefer IPv4 if available; libpq accepts hostaddr for numeric IPs
    parts = []
    if ipv4:
        parts.append(f"hostaddr={ipv4}")
    elif ipv6:
        parts.append(f"hostaddr={ipv6}")
    # Always include hostname too (useful for TLS SNI / certs)
    parts += [
        f"host={host}",
        f"port={port}",
        f"dbname={db}",
        f"user={quote_plus(user)}",
        f"password={quote_plus(pwd)}",
        f"sslmode={sslm}",
    ]
    return " ".join(parts)

# ---------- Output & SQL ----------
OUT = Path("ml/train_batter_total_bases.csv")
SQL = """
COPY (
WITH base AS (
  SELECT
    ps.game_id::bigint                   AS game_id,
    ps.player_id::bigint                 AS player_id,
    ps.game_date                         AS game_date,
    ps.team                              AS team,
    ps.opponent                          AS opponent,
    COALESCE(ps.total_bases, 0)::numeric AS y_tb
  FROM player_stats ps
  WHERE ps.game_date IS NOT NULL
),
prb AS (
  SELECT
    p.player_id::bigint AS player_id,
    p.game_id::bigint   AS game_id,
    p.game_date,
    p.d7_hits,  p.d15_hits,  p.d30_hits,
    p.d7_total_bases, p.d15_total_bases, p.d30_total_bases,
    p.d7_home_runs,  p.d15_home_runs,  p.d30_home_runs,
    p.d7_rbis,       p.d15_rbis,       p.d30_rbis,
    p.d7_walks,      p.d15_walks,      p.d30_walks,
    p.d7_strikeouts_batting, p.d15_strikeouts_batting, p.d30_strikeouts_batting
  FROM player_rolling_batting_agg p
),
os AS (
  SELECT
    os.game_id::bigint            AS game_id,
    os.team                       AS team,
    os.starter_pitcher_id::bigint AS starter_pitcher_id
  FROM opp_starter_per_game os
),
pr9 AS (
  SELECT
    pr.player_id::bigint AS pitcher_id,
    pr.game_id::bigint   AS game_id,
    pr.d15_k_per9, pr.d30_k_per9,
    pr.d15_bb_per9, pr.d30_bb_per9,
    pr.d15_era,    pr.d30_era
  FROM pitcher_rolling_per9 pr
),
bvp AS (
  SELECT
    b.batter_id::bigint  AS batter_id,
    b.pitcher_id::bigint AS pitcher_id,
    b.game_id::bigint    AS game_id,
    b.pa_prior::numeric,
    b.ab_prior::numeric,
    b.hits_prior::numeric,
    b.hr_prior::numeric,
    b.bb_prior::numeric,
    b.so_prior::numeric,
    b.tb_prior::numeric
  FROM bvp_rollup_prior b
)
SELECT
  b.game_id, b.player_id, b.game_date, b.team, b.opponent,
  b.y_tb,

  prb.d7_hits,  prb.d15_hits,  prb.d30_hits,
  prb.d7_total_bases, prb.d15_total_bases, prb.d30_total_bases,
  prb.d7_home_runs,  prb.d15_home_runs,  prb.d30_home_runs,
  prb.d7_rbis,       prb.d15_rbis,       prb.d30_rbis,
  prb.d7_walks,      prb.d15_walks,      prb.d30_walks,
  prb.d7_strikeouts_batting, prb.d15_strikeouts_batting, prb.d30_strikeouts_batting,

  pr9.d15_k_per9, pr9.d30_k_per9,
  pr9.d15_bb_per9, pr9.d30_bb_per9,
  pr9.d15_era,    pr9.d30_era,

  bvp.pa_prior    AS bvp_pa_prior,
  bvp.ab_prior    AS bvp_ab_prior,
  bvp.hits_prior  AS bvp_hits_prior,
  bvp.tb_prior    AS bvp_tb_prior,
  bvp.hr_prior    AS bvp_hr_prior,
  bvp.bb_prior    AS bvp_bb_prior,
  bvp.so_prior    AS bvp_so_prior,

  CASE WHEN bvp.ab_prior IS NOT NULL
       THEN (bvp.hits_prior + 1.0) / (bvp.ab_prior + 2.0)
  END AS bvp_avg_prior_sm,

  CASE WHEN bvp.ab_prior IS NOT NULL
       THEN (bvp.tb_prior   + 1.5) / (bvp.ab_prior + 3.0)
  END AS bvp_tb_per_ab_prior_sm,

  CASE WHEN bvp.pa_prior IS NOT NULL
       THEN (bvp.bb_prior   + 0.5) / (bvp.pa_prior + 2.0)
  END AS bvp_bb_rate_prior_sm,

  CASE WHEN bvp.pa_prior IS NOT NULL
       THEN (bvp.so_prior   + 0.5) / (bvp.pa_prior + 2.0)
  END AS bvp_so_rate_prior_sm

FROM base b
LEFT JOIN prb
  ON prb.player_id = b.player_id
 AND prb.game_id   = b.game_id
LEFT JOIN os
  ON os.game_id = b.game_id
 AND os.team    = b.opponent
LEFT JOIN pr9
  ON pr9.pitcher_id = os.starter_pitcher_id
 AND pr9.game_id    = b.game_id
LEFT JOIN bvp
  ON bvp.batter_id  = b.player_id
 AND bvp.pitcher_id = os.starter_pitcher_id
 AND bvp.game_id    = b.game_id
WHERE b.y_tb IS NOT NULL
ORDER BY b.game_date, b.game_id, b.player_id
) TO STDOUT WITH CSV HEADER
"""

# ---------- Run ----------
OUT.parent.mkdir(parents=True, exist_ok=True)

# Show what we’re using
pg_host = _clean(os.getenv("PGHOST"))
pg_port = _clean(os.getenv("PGPORT")) or "5432"
print("Connecting via PG* vars:")
print(f"  PGHOST = {repr(pg_host)}")
print(f"  PGPORT = {repr(pg_port)}")
print(f"  PGDATABASE = {repr(_clean(os.getenv('PGDATABASE')))}")
print(f"  PGUSER = {repr(_clean(os.getenv('PGUSER')))}")
print(f"  PGSSLMODE = {repr(_clean(os.getenv('PGSSLMODE') or 'require'))}")

ipv4, ipv6 = _resolve_addrs(pg_host or "", int(pg_port))
print(f"Resolved addresses → IPv4: {ipv4 or 'none'}  |  IPv6: {ipv6 or 'none'}")

dsn = _conninfo()
# Mask password in any print
print("DSN preview:", re.sub(r"(password=)[^ ]+", r"\1******", dsn))

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    with cur.copy(SQL) as copy, open(OUT, "wb") as f:
        while data := copy.read():
            f.write(data)

print(f"Wrote {OUT.resolve()}")
