#!/usr/bin/env python3
"""Validate that make targets referenced in docs exist in Makefile."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set


TARGET_DEF_RE = re.compile(r"^([A-Za-z0-9_.-]+)\s*:(?:\s.*)?$")
MAKE_WORD_RE = re.compile(r"\bmake\b")


@dataclass
class Reference:
    file: str
    line: int
    target: str
    text: str


def load_make_targets(makefile: Path) -> Set[str]:
    out: Set[str] = set()
    for raw in makefile.read_text(encoding="utf-8").splitlines():
        if not raw or raw.startswith("\t") or raw.startswith("#"):
            continue
        m = TARGET_DEF_RE.match(raw.strip())
        if m:
            out.add(m.group(1))
    return out


def _extract_target_from_line(line: str) -> str | None:
    if "make" not in line:
        return None
    # Reduce false positives from prose by requiring command-ish shape.
    s = line.strip()
    if not (s.startswith("make ") or s.startswith("- make ") or s.startswith("* make ") or "`make " in s):
        return None
    if not MAKE_WORD_RE.search(s):
        return None

    cleaned = s.replace("`", " ")
    parts = re.split(r"\s+", cleaned)
    try:
        i = parts.index("make")
    except ValueError:
        # handle '/usr/bin/make'
        i = next((idx for idx, p in enumerate(parts) if p.endswith("/make")), -1)
        if i < 0:
            return None
    tokens = parts[i + 1 :]
    for tok in tokens:
        token = tok.strip().strip(".,;:()[]{}")
        if not token:
            continue
        if token.startswith("-"):
            continue
        if "=" in token:
            continue
        if any(x in token for x in ("*", "?", "$(", ")")):
            return None
        return token
    return None


def extract_doc_references(docs_root: Path) -> List[Reference]:
    refs: List[Reference] = []
    for path in sorted(docs_root.rglob("*.md")):
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            target = _extract_target_from_line(line)
            if not target:
                continue
            refs.append(Reference(file=str(path), line=i, target=target, text=line.strip()))
    return refs


def find_unknown_targets(make_targets: Set[str], references: Iterable[Reference]) -> List[Reference]:
    return [r for r in references if r.target not in make_targets]


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Check docs make target references against Makefile.")
    ap.add_argument("--makefile", default="Makefile")
    ap.add_argument("--docs-root", default="docs")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    makefile = Path(args.makefile)
    docs_root = Path(args.docs_root)
    if not makefile.exists():
        print(json.dumps({"ok": False, "status": "fail", "error": f"missing makefile: {makefile}"}, indent=2))
        return 2
    if not docs_root.exists():
        print(json.dumps({"ok": False, "status": "fail", "error": f"missing docs root: {docs_root}"}, indent=2))
        return 2

    make_targets = load_make_targets(makefile)
    refs = extract_doc_references(docs_root)
    unknown = find_unknown_targets(make_targets, refs)

    by_target: Dict[str, List[Dict[str, object]]] = {}
    for ref in unknown:
        by_target.setdefault(ref.target, []).append({"file": ref.file, "line": ref.line, "text": ref.text})

    payload = {
        "ok": len(unknown) == 0,
        "status": "pass" if len(unknown) == 0 else "fail",
        "summary": {
            "make_targets": len(make_targets),
            "doc_refs": len(refs),
            "unknown_refs": len(unknown),
            "unknown_targets": len(by_target),
        },
        "unknown": by_target,
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print("Docs make target audit:", payload["status"])
        print(
            "summary:",
            f"make_targets={payload['summary']['make_targets']}",
            f"doc_refs={payload['summary']['doc_refs']}",
            f"unknown_refs={payload['summary']['unknown_refs']}",
        )
        for target, rows in sorted(by_target.items()):
            for row in rows:
                print(f"- {target}: {row['file']}:{row['line']} | {row['text']}")
    return 0 if len(unknown) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
