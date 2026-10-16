"""Execute one separately authorized, field-scoped NHL raw-source UPDATE."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

from backend.nhl.source_correction_access import (
    SourceCorrectionError,
    execute_authorized_correction,
    load_admin_dsn,
    validate_correction_sql,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql-file", required=True, help="Reviewed single UPDATE statement")
    parser.add_argument("--sql-sha256", required=True, help="Expected digest of the reviewed SQL file")
    parser.add_argument("--authorization-reference", required=True, help="Owner approval or change reference")
    parser.add_argument(
        "--execute-authorized-correction", action="store_true",
        help="Required explicit execution switch; use only after separate owner approval",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.execute_authorized_correction:
        print("BLOCKED: EXPLICIT_EXECUTION_SWITCH_REQUIRED", file=sys.stderr)
        return 2
    if not re.fullmatch(r"[0-9a-f]{64}", args.sql_sha256 or ""):
        print("BLOCKED: CORRECTION_SQL_DIGEST_INVALID", file=sys.stderr)
        return 2
    try:
        sql_bytes = Path(args.sql_file).read_bytes()
        sql = sql_bytes.decode("utf-8")
        validate_correction_sql(sql)
        digest = hashlib.sha256(sql_bytes).hexdigest()
        if digest != args.sql_sha256:
            raise SourceCorrectionError("CORRECTION_SQL_DIGEST_MISMATCH")
        dsn = load_admin_dsn()
        result = execute_authorized_correction(
            dsn=dsn,
            sql=sql,
            sql_sha256=args.sql_sha256,
            authorization_reference=args.authorization_reference,
        )
    except SourceCorrectionError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    except Exception:
        # Do not render provider exceptions, file paths, DSNs, or SQL values.
        print("BLOCKED: CORRECTION_EXECUTION_FAILED", file=sys.stderr)
        return 2

    print(json.dumps({
        "status": "CORRECTION_COMMITTED",
        "affected_rows": result.affected_rows,
        "sql_sha256": result.sql_sha256,
        "authorization_reference": result.authorization_reference,
        "role_membership_revoked_and_independently_verified": True,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
