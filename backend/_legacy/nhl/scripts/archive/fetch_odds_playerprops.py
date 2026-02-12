#!/usr/bin/env python3
import os, sys, json, time, argparse, pathlib, concurrent.futures, urllib.request, urllib.error

# ----------------------------
# Minimal .env loader (no deps)
# ----------------------------
def load_env_file(path: pathlib.Path):
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip("'").strip('"')
        if k and v:
            os.environ.setdefault(k, v)

# ----------------------------
# HTTP helpers (urllib, retries)
# ----------------------------
def http_get(url: str, timeout: int = 30, retries: int = 2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                code = resp.getcode()
                data = resp.read()
                if code != 200:
                    raise urllib.error.HTTPError(url, code, f"HTTP {code}", hdrs=None, fp=None)
                return data
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
            else:
                raise last_err

def json_get(url: str, timeout: int = 30, retries: int = 2):
    raw = http_get(url, timeout=timeout, retries=retries)
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON from {url}: {e}")

# ----------------------------
# Main
# ----------------------------
def main():
    # Resolve paths relative to this file
    script_dir = pathlib.Path(__file__).resolve().parent
    backend_dir = script_dir.parent.parent
    repo_root = backend_dir.parent

    # Load env (backend/.env then backend/.env.local)
    load_env_file(backend_dir / ".env")
    load_env_file(backend_dir / ".env.local")

    parser = argparse.ArgumentParser(description="Fetch NHL player prop odds (SOG, Saves, Points) from The Odds API")
    parser.add_argument("days_from", nargs="?", default="1",
                        help="daysFrom for events (default: 1)")
    parser.add_argument("--markets", default=os.environ.get("MARKETS", "player_shots_on_goal,player_total_saves,player_points"),
                        help="Comma-separated markets (default: player_shots_on_goal,player_total_saves,player_points)")
    parser.add_argument("--regions", default=os.environ.get("REGIONS", "us"),
                        help="Odds API regions (default: us)")
    parser.add_argument("--format", default=os.environ.get("FORMAT", "american"),
                        help="Odds format (default: american)")
    parser.add_argument("--outdir", default=str(repo_root / "nhl" / "site" / "data"),
                        help="Output directory for JSON files")
    parser.add_argument("--concurrency", type=int, default=int(os.environ.get("CONCURRENCY", "6")),
                        help="Max concurrent event requests (default: 6)")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds (default: 30)")
    args = parser.parse_args()

    api_key = os.environ.get("ODDS_API_KEY", "").strip()
    if not api_key:
        print("⚠️  ODDS_API_KEY not set (in env or backend/.env). Aborting.", file=sys.stderr)
        sys.exit(1)

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) Fetch events
    events_url = (
        "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events"
        f"?dateFormat=iso&daysFrom={args.days_from}&apiKey={api_key}"
    )
    print(f"→ Fetching events (daysFrom={args.days_from})…")
    try:
        events = json_get(events_url, timeout=args.timeout)
    except Exception as e:
        print(f"❌ Events fetch failed: {e}", file=sys.stderr)
        sys.exit(1)

    events_path = outdir / "events_today.json"
    events_path.write_text(json.dumps(events, indent=2))
    print(f"   events_today.json → {len(events)} events")

    if not events:
        # still write empty array for downstream sanity
        odds_path = outdir / "odds_nhl_playerprops_today.json"
        odds_path.write_text("[]")
        print(f"✅ Wrote {odds_path} (empty)")
        return

    # 2) Fetch per-event odds concurrently
    print(f"→ Fetching player props (markets={args.markets}, regions={args.regions})…")

    base_odds = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events/{eid}/odds"
    urls = []
    event_ids = []
    for ev in events:
        eid = ev.get("id")
        if not eid:
            continue
        url = (
            base_odds.format(eid=eid)
            + f"?regions={args.regions}&markets={args.markets}"
            + f"&oddsFormat={args.format}&apiKey={api_key}"
        )
        urls.append(url)
        event_ids.append(eid)

    results = []
    def fetch(idx_url):
        idx, url = idx_url
        try:
            return json_get(url, timeout=args.timeout)
        except Exception as e:
            # Write "{}" equivalent as fallback (to mirror bash behavior)
            return {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        for obj in ex.map(fetch, enumerate(urls, start=1)):
            results.append(obj)

    # 3) Write merged odds JSON
    odds_path = outdir / "odds_nhl_playerprops_today.json"
    # results is a list of objects; make it a JSON array
    odds_path.write_text(json.dumps(results, indent=2))
    size = odds_path.stat().st_size
    print(f"✅ Wrote {odds_path}")
    print(f"   size: {size} bytes | events: {len(results)}")

    # Sanity note for player_points presence (not a hard fail)
    try:
        keys = []
        def walk(o):
            if isinstance(o, dict):
                if "key" in o:
                    keys.append(o["key"])
                for v in o.values(): walk(v)
            elif isinstance(o, list):
                for v in o: walk(v)
        walk(results)
        pts_count = sum(1 for k in keys if k == "player_points")
        print(f"   player_points market occurrences: {pts_count}")
    except Exception:
        pass

if __name__ == "__main__":
    main()
