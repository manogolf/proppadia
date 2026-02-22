from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Set, Tuple


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def filtered_paths(spec: Dict[str, Any], include_path: Callable[[str], bool]) -> Dict[str, Any]:
    return {p: v for p, v in (spec.get("paths") or {}).items() if include_path(p)}


def method_schemas(op: Dict[str, Any]) -> Dict[str, Any]:
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


def collect_refs(node: Any, refs: Set[str]) -> None:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            refs.add(ref)
        for v in node.values():
            collect_refs(v, refs)
    elif isinstance(node, list):
        for v in node:
            collect_refs(v, refs)


def component_name_from_ref(ref: str) -> str | None:
    prefix = "#/components/schemas/"
    if ref.startswith(prefix):
        return ref[len(prefix) :]
    return None


def expand_component_refs(spec: Dict[str, Any], seed_refs: Set[str]) -> Set[str]:
    schemas = (spec.get("components") or {}).get("schemas") or {}
    out: Set[str] = set()
    queue = list(seed_refs)
    while queue:
        ref = queue.pop()
        if ref in out:
            continue
        out.add(ref)
        name = component_name_from_ref(ref)
        if not name:
            continue
        schema = schemas.get(name)
        if not schema:
            continue
        nested: Set[str] = set()
        collect_refs(schema, nested)
        for n in nested:
            if n not in out:
                queue.append(n)
    return out


def normalize(obj: Any) -> Any:
    return json.loads(json.dumps(obj, sort_keys=True))


def diff_contract(
    *,
    snapshot: Dict[str, Any],
    current: Dict[str, Any],
    include_path: Callable[[str], bool],
    label: str,
) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    notes: List[str] = []

    s_paths = filtered_paths(snapshot, include_path)
    c_paths = filtered_paths(current, include_path)
    s_set = set(s_paths.keys())
    c_set = set(c_paths.keys())

    missing_paths = sorted(s_set - c_set)
    new_paths = sorted(c_set - s_set)
    if missing_paths:
        errors.append(f"Missing {label} paths: {missing_paths}")
    if new_paths:
        errors.append(f"New {label} paths not in snapshot: {new_paths}")

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
            s_schema = method_schemas(s_op)
            c_schema = method_schemas(c_op)
            if normalize(s_schema) != normalize(c_schema):
                errors.append(f"Schema binding drift for {m.upper()} {p}")
            collect_refs(s_schema, seed_s_refs)
            collect_refs(c_schema, seed_c_refs)

    s_refs = expand_component_refs(snapshot, seed_s_refs)
    c_refs = expand_component_refs(current, seed_c_refs)

    s_ref_names = sorted(r for r in s_refs if component_name_from_ref(r))
    c_ref_names = sorted(r for r in c_refs if component_name_from_ref(r))
    if s_ref_names != c_ref_names:
        errors.append("Referenced component set drift")
        notes.append(f"snapshot refs={s_ref_names}")
        notes.append(f"current  refs={c_ref_names}")

    s_schemas = (snapshot.get("components") or {}).get("schemas") or {}
    c_schemas = (current.get("components") or {}).get("schemas") or {}
    for ref in sorted(set(s_ref_names) & set(c_ref_names)):
        name = component_name_from_ref(ref)
        if not name:
            continue
        if normalize(s_schemas.get(name)) != normalize(c_schemas.get(name)):
            errors.append(f"Component schema drift: {name}")

    return errors, notes
