"""Package model artifacts and upload to Supabase Storage.

Primary upload target is controlled by BUNDLE_OBJECT.
For prod12 bundles, this script can also keep a stable alias key up to date:
mlb/prod12/latest.tgz.
"""

from __future__ import annotations

import io
import json
import os
import tarfile
import time
from pathlib import Path

from supabase import create_client

# load .env if present (repo root or two-levels up from this file)
try:
    from dotenv import load_dotenv
    for p in (Path.cwd() / ".env", Path(__file__).resolve().parents[2] / ".env"):
        if p.exists():
            load_dotenv(p, override=False)
except Exception:
    pass

MODELS_DIR = Path(os.getenv("MODELS_DIR", "./models_out")).resolve()
BUCKET = os.getenv("MODELS_BUCKET", "models")
OBJECT_PATH = os.getenv("BUNDLE_OBJECT", "models_bundle.tgz")
MANIFEST_OBJECT = os.getenv("MODELS_MANIFEST_OBJECT", "manifest.json")
BUNDLE_LATEST_OBJECT = os.getenv("BUNDLE_LATEST_OBJECT", "")
BUNDLE_ALIAS_OBJECTS = os.getenv("BUNDLE_ALIAS_OBJECTS", "")
BUNDLE_AUTO_PROD12_LATEST_ALIAS = os.getenv("BUNDLE_AUTO_PROD12_LATEST_ALIAS", "1")


def _env_first(*names: str) -> str:
    for name in names:
        value = str(os.getenv(name) or "").strip()
        if value:
            return value
    return ""

def _tar_gz_dir(root: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for p in sorted(root.rglob("*")):
            tar.add(p, arcname=p.relative_to(root).as_posix())
    return buf.getvalue()


def _csv_items(raw: str) -> list[str]:
    out: list[str] = []
    for item in (raw or "").split(","):
        s = item.strip()
        if s:
            out.append(s)
    return out


def _default_prod12_alias(primary_object: str) -> str | None:
    head, _, tail = primary_object.rpartition("/")
    if head != "mlb/prod12":
        return None
    if not tail.startswith("mlb_latest_") or not tail.endswith(".tgz"):
        return None
    return "mlb/prod12/latest.tgz"


def _resolve_upload_paths(primary_object: str) -> list[str]:
    ordered: list[str] = []

    def add(path: str) -> None:
        p = str(path or "").strip()
        if not p:
            return
        if p not in ordered:
            ordered.append(p)

    add(primary_object)
    add(BUNDLE_LATEST_OBJECT)
    for alias in _csv_items(BUNDLE_ALIAS_OBJECTS):
        add(alias)
    if BUNDLE_AUTO_PROD12_LATEST_ALIAS == "1":
        auto_alias = _default_prod12_alias(primary_object)
        if auto_alias:
            add(auto_alias)
    return ordered

def main() -> int:
    url = os.environ["SUPABASE_URL"]
    key = _env_first("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SECRET_KEY")
    if not key:
        raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SECRET_KEY")
    sb = create_client(url, key)

    assert MODELS_DIR.exists(), f"{MODELS_DIR} not found"
    assert (MODELS_DIR / "latest").exists(),  "latest/ missing in models dir"
    assert (MODELS_DIR / "archive").exists(), "archive/ missing in models dir"

    blob = _tar_gz_dir(MODELS_DIR)
    print(f"📦 Built tarball: {len(blob)} bytes")
    upload_paths = _resolve_upload_paths(OBJECT_PATH)
    print(f"⬆️  Upload targets: {', '.join(f'{BUCKET}/{p}' for p in upload_paths)}")

    for target in upload_paths:
        # IMPORTANT: use HTTP header names with string values
        sb.storage.from_(BUCKET).upload(
            target,
            blob,
            {"content-type": "application/gzip", "x-upsert": "true"},
        )
        print(f"✅ uploaded {BUCKET}/{target}")

    stamp = {
        "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "size_bytes": len(blob),
        "bucket": BUCKET,
        "uploaded_objects": upload_paths,
    }
    sb.storage.from_(BUCKET).upload(
        MANIFEST_OBJECT,
        json.dumps(stamp).encode("utf-8"),
        {"content-type": "application/json", "x-upsert": "true"},
    )
    print("✅ upload complete")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
