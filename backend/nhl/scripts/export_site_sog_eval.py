#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2


def require_db_url() -> str:
    db = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL).")
    return db


def main() -> None:
    db = require_db_url()
    out_path = Path("nhl/site/data/sog_eval.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sql = "SELECT * FROM nhl.v_site_sog_eval_publish;"

    with psycopg2.connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d.name for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    payload = {
        "kind": "sog_eval",
        "rows": rows,
    }

    out_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    print(f"✅ wrote {out_path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
