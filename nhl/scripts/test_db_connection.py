#!/usr/bin/env python3
"""
scripts/test_db_connection.py

What it checks (in order):
1) DNS → can we resolve the host to an IP?
2) TCP → can we open a socket to host:port?
3) AUTH → can we authenticate to Postgres with your password?

Usage:
  python scripts/test_db_connection.py --uri "postgresql://postgres:YOUR_PASSWORD@db.<ref>.supabase.co:6543/postgres?sslmode=require"

Notes:
- Use the **Pooler** URI (port 6543) or the Direct (5432) one — either works; the test will try both DNS and a DNS-bypass connect.
- If DNS fails but AUTH later succeeds via hostaddr, your password is fine; the problem is purely DNS on your machine.
"""

import argparse, socket, subprocess, sys, time
from urllib.parse import urlparse, unquote

def resolve_host(host: str) -> str | None:
    # 1) Try system resolver
    try:
        return socket.getaddrinfo(host, 0)[0][4][0]
    except Exception:
        pass
    # 2) Try Cloudflare (dig) if available
    try:
        out = subprocess.run(["dig", "+short", "@1.1.1.1", host], capture_output=True, text=True, timeout=5)
        ip = out.stdout.strip().splitlines()[0] if out.stdout.strip() else ""
        return ip or None
    except Exception:
        return None

def tcp_check(ip: str, port: int, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True, help="Postgres URI (pooler or direct).")
    args = ap.parse_args()

    p = urlparse(args.uri)
    host = p.hostname or ""
    port = p.port or 5432
    db   = (p.path or "/postgres").lstrip("/") or "postgres"
    user = p.username or "postgres"
    pwd  = unquote(p.password or "")

    if not host or not pwd:
        print("❌ URI missing host or password. Re-copy the connection string and include your actual password.")
        sys.exit(2)

    print(f"Host: {host}")
    print(f"Port: {port}  (6543 = pooler, 5432 = direct)")
    print(f"DB:   {db}")
    print(f"User: {user}")

    # 1) DNS
    ip = resolve_host(host)
    if ip:
        print(f"✅ DNS OK → {host} -> {ip}")
    else:
        print(f"❌ DNS FAILED for {host} (system + Cloudflare). We will still try a hostaddr connect if you provide an IP.")
        # If DNS failed, we cannot proceed to TCP unless you supply an IP manually
        # but we'll keep ip=None and skip the TCP direct step.

    # 2) TCP
    if ip:
        if tcp_check(ip, port):
            print(f"✅ TCP OK → {ip}:{port}")
        else:
            print(f"❌ TCP FAILED to {ip}:{port} (network/firewall?).")
            # We can still try libpq auth in case TCP was flaky, but it will likely fail.

    # 3) AUTH (psycopg2)
    try:
        import psycopg2
    except Exception:
        print("ℹ️ Installing psycopg2-binary…")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-qU", "psycopg2-binary"])
        import psycopg2  # type: ignore

    # Prefer a DSN that includes host (for TLS/SNI) + hostaddr (to bypass DNS) when we have an IP.
    if ip:
        dsn = f"host={host} hostaddr={ip} port={port} dbname={db} user={user} password={pwd} sslmode=require connect_timeout=5"
        label = "AUTH via hostaddr (DNS-bypass)"
    else:
        # Fall back to the raw URI (will fail if DNS is the problem).
        dsn = args.uri
        label = "AUTH via URI (DNS required)"

    print(f"→ Attempting {label} …")
    try:
        t0 = time.time()
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute("select current_user, inet_client_addr(), inet_server_addr()")
        row = cur.fetchone()
        dt = time.time() - t0
        print(f"✅ AUTH OK in {dt:.2f}s  current_user={row[0]}  client={row[1]}  server={row[2]}")
        cur.close()
        conn.close()
        sys.exit(0)
    except psycopg2.OperationalError as e:
        msg = str(e)
        if "password authentication failed" in msg.lower():
            print("❌ AUTH FAILED: password rejected by server.")
            print("   → Your password is wrong (or the user does not exist).")
        elif "timeout" in msg.lower() or "could not translate host name" in msg.lower():
            print("❌ CONNECTION FAILED at network/DNS stage (not password).")
            print("   → DNS or reachability issue. Try the pooler URI (6543) and/or hostaddr with a resolved IP.")
        else:
            print("❌ OperationalError:", msg.strip())
        sys.exit(1)
    except Exception as e:
        print("❌ Unexpected error:", repr(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
