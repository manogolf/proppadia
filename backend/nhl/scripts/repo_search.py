#!/usr/bin/env python3
"""
repo_search_small.py — find the REAL writer + REAL reader for NHL SOG stage, without terminal spam.

Prints:
- unique file paths containing needles
- up to 2 matches per file (line number + the matched line)
- no context blocks unless you set CONTEXT>0

Usage:
  python repo_search_small.py

Env:
  ROOT=/path/to/repo   (default: cwd)
  CONTEXT=0            (default: 0; set to 2 if you want a couple context lines)
"""
from __future__ import annotations

import os, re, sys
from pathlib import Path
from collections import defaultdict

ROOT = Path(os.environ.get("ROOT", Path.cwd())).resolve()
CONTEXT = int(os.environ.get("CONTEXT", "0"))

GLOBS = ["**/*.py", "**/*.js", "**/*.mjs", "**/*.sql", "**/*.sh", "**/*.jsx"]
SKIP_DIRS = {".git", "node_modules", ".venv", "venv", "__pycache__", "dist", "build", ".next"}

# Minimal needles that actually resolve your current ambiguity:
NEEDLES = [
    # the stage table + long view (writer/reader)
    "predictions_sog_stage",
    "v_predictions_sog_stage_long",

    # the summary block labels (where counters are computed)
    "games_today",
    "roster_rows_today",
    "sog_stage",
    "saves_stage",
    "Daily pipeline complete",

    # API endpoints used by frontend
    "/api/nhl/sog_stage",
    "/api/nhl/saves_stage",
]

def iter_files(root: Path):
    for g in GLOBS:
        for p in root.glob(g):
            if not p.is_file():
                continue
            if set(p.parts) & SKIP_DIRS:
                continue
            yield p

def main():
    pats = [re.compile(re.escape(n)) for n in NEEDLES]
    hits = defaultdict(list)

    for p in iter_files(ROOT):
        try:
            lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue
        for i, line in enumerate(lines, start=1):
            if any(pat.search(line) for pat in pats):
                hits[str(p)].append((i, line.strip()))
                # keep it quiet: only keep first 2 hits per file
                if len(hits[str(p)]) >= 2:
                    continue

    if not hits:
        print("No matches.")
        return

    print(f"ROOT={ROOT}")
    print(f"files_matched={len(hits)}")
    for fp in sorted(hits.keys()):
        print("\n" + fp)
        for ln, text in hits[fp][:2]:
            print(f"  {ln}: {text}")

        if CONTEXT > 0:
            # optional: print tiny context around the first match
            ln0 = hits[fp][0][0]
            try:
                lines = Path(fp).read_text(encoding="utf-8", errors="ignore").splitlines()
                lo = max(1, ln0 - CONTEXT)
                hi = min(len(lines), ln0 + CONTEXT)
                for k in range(lo, hi + 1):
                    mark = ">>" if k == ln0 else "  "
                    print(f"  {mark} {k}: {lines[k-1]}")
            except Exception:
                pass

if __name__ == "__main__":
    main()
