# backend/scripts/modeling/package_and_upload.py
import os, io, tarfile, time, json
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
BUCKET      = os.getenv("MODELS_BUCKET", "models")
OBJECT_PATH = os.getenv("BUNDLE_OBJECT", "models_bundle.tgz")

def _tar_gz_dir(root: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for p in sorted(root.rglob("*")):
            tar.add(p, arcname=p.relative_to(root).as_posix())
    return buf.getvalue()

def main() -> int:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    sb  = create_client(url, key)

    assert MODELS_DIR.exists(), f"{MODELS_DIR} not found"
    assert (MODELS_DIR / "latest").exists(),  "latest/ missing in models dir"
    assert (MODELS_DIR / "archive").exists(), "archive/ missing in models dir"

    blob = _tar_gz_dir(MODELS_DIR)
    print(f"📦 Built tarball: {len(blob)} bytes")
    print(f"⬆️  Uploading to {BUCKET}/{OBJECT_PATH}")

    # IMPORTANT: use HTTP header names with string values
    sb.storage.from_(BUCKET).upload(
        OBJECT_PATH,
        blob,
        {"content-type": "application/gzip", "x-upsert": "true"},
    )

    stamp = {
        "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "size_bytes": len(blob),
    }
    sb.storage.from_(BUCKET).upload(
        "manifest.json",
        json.dumps(stamp).encode("utf-8"),
        {"content-type": "application/json", "x-upsert": "true"},
    )
    print("✅ upload complete")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
