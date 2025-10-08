# backend/scripts/retrain_all_models.py

from __future__ import annotations
import os, sys, json, argparse, pathlib

# load .env for Python
from pathlib import Path
try:
    from dotenv import load_dotenv
    for p in (Path.cwd() / ".env", Path(__file__).resolve().parents[2] / ".env"):
        if p.exists():
            load_dotenv(p, override=False)
except Exception:
    pass  # keep going if python-dotenv isn't installed


# --- robust import of model_trainer ---
try:
    # when run as a module: python -m backend.scripts.retrain_all_models
    from .model_trainer import (
        train_models_for_prop, PROP_TYPES,
        DEFAULT_DAYS_BACK, DEFAULT_ROW_LIMIT
    )
except Exception:
    try:
        # absolute import if package name is available
        from backend.scripts.model_trainer import (
            train_models_for_prop, PROP_TYPES,
            DEFAULT_DAYS_BACK, DEFAULT_ROW_LIMIT
        )
    except Exception:
        # direct script run fallback
        ROOT = pathlib.Path(__file__).resolve().parents[2]  # repo root
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        from backend.scripts.model_trainer import (
            train_models_for_prop, PROP_TYPES,
            DEFAULT_DAYS_BACK, DEFAULT_ROW_LIMIT
        )

def main():
    p = argparse.ArgumentParser(description="Retrain models per prop")
    p.add_argument("--prop", action="append",
                   help="Prop type to train (repeatable). If omitted, train all.")
    p.add_argument("--days-back", type=int,
                   default=int(os.getenv("DAYS_BACK", DEFAULT_DAYS_BACK)),
                   help=f"Lookback window (default {DEFAULT_DAYS_BACK})")
    p.add_argument("--limit", type=int,
                   default=int(os.getenv("ROW_LIMIT", DEFAULT_ROW_LIMIT)),
                   help=f"Row cap per prop (default {DEFAULT_ROW_LIMIT})")
    p.add_argument("--quiet", action="store_true",
                   help="Reduce logging")
    args = p.parse_args()

    props = args.prop or PROP_TYPES
    results = []
    ok = 0
    skipped = 0

    for prop in props:
        try:
            r = train_models_for_prop(
                prop,
                days_back=args.days_back,
                limit=args.limit,
                quiet=args.quiet,
            )
            if r is None:
                skipped += 1
            else:
                ok += 1
                results.append(r)
        except Exception as e:
            skipped += 1
            if not args.quiet:
                print(f"❌ {prop}: {e}")

    summary = {
        "trained": ok,
        "skipped": skipped,
        "props": props,
        "results": results,
    }
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
