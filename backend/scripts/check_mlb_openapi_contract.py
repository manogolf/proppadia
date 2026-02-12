#!/usr/bin/env python3
"""
Check MLB OpenAPI contract drift against snapshot.

Compares:
- MLB path set
- Per-method request/response schema refs for MLB paths
- Referenced component schemas (deep, via $ref traversal)

Exit codes:
- 0: no drift
- 1: drift detected or snapshot missing
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


SNAPSHOT_DEFAULT = Path("docs/openapi/openapi.snapshot.json")


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def _mlb_path(path: str) -> bool:
    return (
        path.startswith("/api/mlb")
        or path.startswith("/api/players")
        or path.startswith("/api/player-profile")
        or path.startswith("/api/games/context")
        or path.startswith("/api/prepareProp")
        or path.startswith("/api/predict")
        or path.startswith("/api/props/")
        or path.startswith("/api/model")
        or path.startswith("/api/user-vs-model")
    )


def _mlb_paths(spec: Dict[str, Any]) -> Dict[str, Any]:
    return {p: v for p, v in (spec.get("paths") or {}).items() if _mlb_path(p)}


def _method_schemas(op: Dict[str, Any]) -> Dict[str, Any]:
    out = {"request": None, "response_200": None}
    req = op.get("requestBody", {}).get("content", {}).get("application/json", {}).get("schema")
    out["request"] = req
    rsp = (
        op.get("responses", {})
        .get("200", {})
        .get("content", {})
        .get("application/json", {})
        .get("schema")
    )
    out["response_200"] = rsp
    return out


def _collect_refs(node: Any, refs: Set[str]) -> None:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            refs.add(ref)
        for v in node.values():
            _collect_refs(v, refs)
    elif isinstance(node, list):
        for v in node:
            _collect_refs(v, refs)


def _component_name_from_ref(ref: str) -> str | None:
    prefix = "#/components/schemas/"
    if ref.startswith(prefix):
        return ref[len(prefix) :]
    return None


def _expand_component_refs(spec: Dict[str, Any], seed_refs: Set[str]) -> Set[str]:
    schemas = (spec.get("components") or {}).get("schemas") or {}
    out: Set[str] = set()
    queue = list(seed_refs)
    while queue:
        ref = queue.pop()
        if ref in out:
            continue
        out.add(ref)
        name = _component_name_from_ref(ref)
        if not name:
            continue
        schema = schemas.get(name)
        if not schema:
            continue
        nested: Set[str] = set()
        _collect_refs(schema, nested)
        for n in nested:
            if n not in out:
                queue.append(n)
    return out


def _normalize(obj: Any) -> Any:
    return json.loads(json.dumps(obj, sort_keys=True))


def diff_contract(snapshot: Dict[str, Any], current: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    notes: List[str] = []

    s_paths = _mlb_paths(snapshot)
    c_paths = _mlb_paths(current)
    s_set = set(s_paths.keys())
    c_set = set(c_paths.keys())

    missing_paths = sorted(s_set - c_set)
    new_paths = sorted(c_set - s_set)
    if missing_paths:
        errors.append(f"Missing MLB paths: {missing_paths}")
    if new_paths:
        errors.append(f"New MLB paths not in snapshot: {new_paths}")

    common_paths = sorted(s_set & c_set)
    seed_s_refs: Set[str] = set()
    seed_c_refs: Set[str] = set()

    for p in common_paths:
        s_item = s_paths[p]
        c_item = c_paths[p]
        for m in ("get", "post", "put", "patch", "delete"):
            s_op = s_item.get(m)
            c_op = c_item.get(m)
            if bool(s_op) != bool(c_op):
                errors.append(f"Method mismatch for {m.upper()} {p}")
                continue
            if not s_op:
                continue
            s_schema = _method_schemas(s_op)
            c_schema = _method_schemas(c_op)
            if _normalize(s_schema) != _normalize(c_schema):
                errors.append(f"Schema binding drift for {m.upper()} {p}")
            _collect_refs(s_schema, seed_s_refs)
            _collect_refs(c_schema, seed_c_refs)

    s_refs = _expand_component_refs(snapshot, seed_s_refs)
    c_refs = _expand_component_refs(current, seed_c_refs)

    s_ref_names = sorted(r for r in s_refs if _component_name_from_ref(r))
    c_ref_names = sorted(r for r in c_refs if _component_name_from_ref(r))
    if s_ref_names != c_ref_names:
        errors.append("Referenced component set drift")
        notes.append(f"snapshot refs={s_ref_names}")
        notes.append(f"current  refs={c_ref_names}")

    s_schemas = (snapshot.get("components") or {}).get("schemas") or {}
    c_schemas = (current.get("components") or {}).get("schemas") or {}
    for ref in sorted(set(s_ref_names) & set(c_ref_names)):
        name = _component_name_from_ref(ref)
        if not name:
            continue
        if _normalize(s_schemas.get(name)) != _normalize(c_schemas.get(name)):
            errors.append(f"Component schema drift: {name}")

    return errors, notes


def main() -> int:
    ap = argparse.ArgumentParser(description="Check MLB OpenAPI contract drift")
    ap.add_argument("--snapshot", default=str(SNAPSHOT_DEFAULT))
    args = ap.parse_args()

    snapshot_path = Path(args.snapshot)
    if not snapshot_path.exists():
        print(f"FAIL snapshot not found: {snapshot_path}")
        print("Hint: generate it first (see docs/MLB OpenAPI Review.md).")
        return 1

    from backend.app.api_server import app

    current = app.openapi()
    snapshot = _load_json(snapshot_path)
    errors, notes = diff_contract(snapshot, current)
    if errors:
        print("FAIL MLB OpenAPI contract drift detected:")
        for e in errors:
            print(f"- {e}")
        for n in notes:
            print(f"  {n}")
        return 1

    print("PASS MLB OpenAPI contract matches snapshot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

