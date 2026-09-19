"""Durable, bounded BvP acquisition authority; no network at import time.

This local operational ledger stores claims and receipts, never baseball data.
The canonical PostgreSQL schema, acquisition functions and identity contract
are unchanged. All ledger evidence is append-only; an OS lock spans acquisition.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import uuid
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
CONTRACT = "MLB_BVP_DAILY_INLINE_ACQUISITION_V1"
EFFECTIVE_DATE = "2026-09-19"
STATE_PATH = ROOT / "artifacts/ops/bvp_inline_v1/acquisition.sqlite3"
WINDOWS = ("0530", "0830", "1100", "1300", "1630")
PT = ZoneInfo("America/Los_Angeles")
SUCCESS = {"BVP_INLINE_PRIMARY_SUCCESS", "BVP_INLINE_RECOVERY_SUCCESS"}


class BVPInlineUnavailable(RuntimeError):
    pass


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def now_utc():
    return datetime.now(timezone.utc)


def stamp(value):
    return value.astimezone(timezone.utc).isoformat()


def connect(path=STATE_PATH):
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink():
        raise BVPInlineUnavailable("STATE_SYMLINK_REJECTED")
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    os.close(fd)
    os.chmod(path, 0o600)
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS attempts (
        attempt_id TEXT PRIMARY KEY, slate_date TEXT NOT NULL,
        attempt_type TEXT NOT NULL CHECK(attempt_type IN ('PRIMARY','RECOVERY','MANUAL_AUTHORIZED_RECOVERY')),
        window TEXT NOT NULL, automatic INTEGER NOT NULL CHECK(automatic IN (0,1)),
        authorization_id TEXT, claim_json TEXT NOT NULL, claim_sha256 TEXT NOT NULL);
      CREATE UNIQUE INDEX IF NOT EXISTS one_auto_window ON attempts(slate_date,window) WHERE automatic=1;
      CREATE UNIQUE INDEX IF NOT EXISTS one_manual_authorization ON attempts(slate_date,authorization_id) WHERE automatic=0;
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
        event_json TEXT NOT NULL, event_sha256 TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS results (
        attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
        result_json TEXT NOT NULL, result_sha256 TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS successes (
        slate_date TEXT PRIMARY KEY, attempt_id TEXT UNIQUE NOT NULL REFERENCES results(attempt_id),
        receipt_json TEXT NOT NULL, receipt_sha256 TEXT NOT NULL);
    """)
    for table in ("attempts", "events", "results", "successes"):
        for operation in ("UPDATE", "DELETE"):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS bvp_inline_{table}_{operation.lower()} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_BVP_INLINE_EVIDENCE'); END")
    db.commit()
    return db


def valid_receipt(receipt):
    """Reject writes alone, incomplete identity validation and volatile receipts."""
    try:
        v = receipt["validation"]
        counts = receipt["counts"]
        rows = receipt["admitted_rows"]
        start = datetime.fromisoformat(receipt["acquisition_started_at_utc"])
        end = datetime.fromisoformat(receipt["acquisition_ended_at_utc"])
        claimed = datetime.fromisoformat(receipt["claim_timestamp_utc"])
        completed = datetime.fromisoformat(receipt["completion_timestamp_utc"])
        if (receipt["contract"] != CONTRACT or receipt["status"] not in SUCCESS
            or not v["canonical_identity_valid"] or not v["committed_request_journal_valid"]
            or v["identity_contract"] != "BVP_CANONICAL_SLATE_IDENTITY_V1"
            or v["durable_database_rows_verified"] != len(rows)
            or counts["rows_prepared"] != counts["rows_written"] or counts["rows_written"] != len(rows)
            or not re.fullmatch(r"[0-9a-f]{64}", v["journal_sha256"])
            or any(t.tzinfo is None for t in (start,end,claimed,completed))
            or not claimed <= start <= end <= completed
            or start.astimezone(PT).date().isoformat()!=receipt["slate_date"]
            or receipt["actual_collection_timestamp_utc"]!=receipt["acquisition_started_at_utc"]):
            return False
        if any(counts.get(k, 0) for k in ("roster_fetch_errors", "bvp_fetch_errors", "off_date_rejected_games",
            "canonical_identity_unresolved_games", "off_date_rejected_rows", "canonical_identity_unresolved_rows")):
            return False
        games = set(v["canonical_game_ids"])
        if counts["games"] != len(games):
            return False
        keys = set()
        for row in rows:
            key = (row["prop_type"],row["player_id"],row["game_id"],row["feature_set_tag"])
            if key in keys or row["game_date"] != receipt["slate_date"] or row["game_id"] not in games:
                return False
            keys.add(key)
            if not re.fullmatch(r"[0-9a-f]{64}",row["bvp_features_sha256"]):
                return False
        return True
    except (KeyError, TypeError, ValueError):
        return False


def certified_success(slate_date, path=STATE_PATH):
    """Read-only admission never creates or repairs state."""
    path = Path(path)
    if not path.exists() or path.is_symlink():
        return None
    try:
        with sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True) as db:
            row = db.execute("SELECT receipt_json,receipt_sha256 FROM successes WHERE slate_date=?",(slate_date,)).fetchone()
            if row and hashlib.sha256(row[0].encode()).hexdigest()==row[1]:
                receipt=json.loads(row[0])
                if receipt["slate_date"]==slate_date and valid_receipt(receipt):
                    return receipt
    except (sqlite3.Error, ValueError, KeyError, OSError):
        pass
    return None


def require_certified_date(slate_date, *, dependency=True, path=STATE_PATH):
    if dependency:
        try:
            datetime.strptime(str(slate_date),"%Y-%m-%d")
        except (ValueError,TypeError):
            raise BVPInlineUnavailable("BVP_DEPENDENT_LANE_BLOCKED_MISSING_OR_INVALID_SLATE_DATE") from None
    if dependency and str(slate_date) >= EFFECTIVE_DATE:
        try:
            if Path(path).is_symlink():
                raise OSError("STATE_SYMLINK_REJECTED")
            info=Path(path).stat()
            certified=_date_certified(str(Path(path).resolve()),str(slate_date),info.st_mtime_ns,info.st_size,info.st_ino)
        except OSError:
            certified=False
        if not certified:
            raise BVPInlineUnavailable("BVP_DEPENDENT_LANE_BLOCKED_NO_CERTIFIED_SAME_DATE_ACQUISITION")


@lru_cache(maxsize=8)
def _date_certified(path, day, modified_ns, size, inode):
    # A boolean only; no mutable receipt shared with scorers. Metadata is cache
    # invalidation, not authority. Hash/identity validation occurs on each miss.
    return certified_success(day,path) is not None


def row_admitted(row, path=STATE_PATH):
    day=str(row.get("game_date"))[:10]
    if day < EFFECTIVE_DATE:
        return True  # Legacy contracts retain their own game/pitcher limitations.
    path=Path(path)
    if path.is_symlink():
        return False
    try:
        info=path.stat()
        admitted=_admission_index(str(path.resolve()),day,info.st_mtime_ns,info.st_size,info.st_ino)
    except OSError:
        return False
    if not admitted:
        return False
    feature_hash=digest({k:v for k,v in (row.get("features") or {}).items() if str(k).startswith("bvp_")})
    key=tuple(row.get(k) for k in ("prop_type","player_id","game_id","feature_set_tag","model_tag"))
    return admitted.get(key)==feature_hash


@lru_cache(maxsize=8)
def _admission_index(path, day, modified_ns, size, inode):
    # File metadata invalidates the cache ONLY; receipt hashes/identity remain
    # the authority. Avoid parsing a full immutable receipt for every row.
    receipt=certified_success(day,path)
    return {tuple(r[k] for k in ("prop_type","player_id","game_id","feature_set_tag","model_tag")):
            r["bvp_features_sha256"] for r in receipt["admitted_rows"]} if receipt else {}


@contextmanager
def acquisition_lock(path):
    lock=Path(str(path)+".acquisition.lock")
    lock.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(lock,os.O_CREAT|os.O_RDWR|getattr(os,"O_NOFOLLOW",0),0o600)
    try:
        try:
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(fd)  # OS release on every exit or crash; lock inode is never removed.


def append_event(db, attempt_id, event):
    db.execute("INSERT INTO events VALUES (?,?,?,?)",(uuid.uuid4().hex,attempt_id,encoded(event),digest(event)))
    db.commit()


def result(status, reason, **fields):
    return {"contract":CONTRACT,"status":status,"reason":reason,"certified":False,**fields}


def run_inline(slate_date, run_tag, window, acquire, *, path=STATE_PATH,
               trigger="automatic", authorization_id=None, clock=now_utc):
    """One primary + one strictly later natural-window recovery. No backdating.

    The injected acquisition adapter performs the canonical collector and
    durable-write/request-journal validation. Tests supply only offline fixtures.
    Manual recovery uses this same authority and a once-only authorization ID.
    """
    now=clock()
    if (not re.fullmatch(r"\d{4}-\d{2}-\d{2}",slate_date) or slate_date < EFFECTIVE_DATE
        or slate_date!=now.astimezone(PT).date().isoformat()):
        return result("BVP_INLINE_NOT_DUE","OUTSIDE_CURRENT_EFFECTIVE_SLATE_DATE")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}",run_tag):
        raise BVPInlineUnavailable("INVALID_RUN_IDENTITY")
    if trigger not in {"automatic","manual"}:
        raise BVPInlineUnavailable("INVALID_TRIGGER")
    if trigger=="manual" and not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}",authorization_id or ""):
        return result("BVP_INLINE_NOT_DUE","EXPLICIT_MANUAL_AUTHORIZATION_REQUIRED")
    if trigger=="automatic" and (window not in WINDOWS or window>now.astimezone(PT).strftime("%H%M")):
        return result("BVP_INLINE_NOT_DUE","NATURAL_WINDOW_NOT_DUE")
    with acquisition_lock(path) as owned:
        if not owned:
            return result("BVP_INLINE_NOT_DUE","CONCURRENT_ACQUISITION_OWNS_LOCK")
        db=connect(path)
        try:
            success=certified_success(slate_date,path)
            if success:
                return result("BVP_INLINE_SUCCESS_ALREADY_EXISTS","CERTIFIED_DATE_ALREADY_ACQUIRED",certified=True,
                              receipt_sha256=digest(success),original_run_tag=success["run_tag"])
            if db.execute("SELECT 1 FROM successes WHERE slate_date=?",(slate_date,)).fetchone():
                return result("BVP_INLINE_IDENTITY_VALIDATION_FAILED","EXISTING_RECEIPT_INVALID_FAIL_CLOSED")
            db.execute("BEGIN IMMEDIATE")
            previous=list(db.execute("SELECT * FROM attempts WHERE slate_date=? ORDER BY rowid",(slate_date,)))
            auto=[r for r in previous if r["automatic"]]
            if trigger=="automatic" and (len(auto)>=2 or (auto and window<=auto[-1]["window"])):
                db.rollback()
                return result("BVP_INLINE_NOT_DUE","AUTOMATIC_ATTEMPT_BUDGET_EXHAUSTED" if len(auto)>=2 else "RECOVERY_REQUIRES_LATER_NATURAL_WINDOW")
            if trigger=="manual" and any(r["authorization_id"]==authorization_id for r in previous):
                db.rollback()
                return result("BVP_INLINE_NOT_DUE","MANUAL_AUTHORIZATION_ALREADY_CLAIMED")
            for prior in previous:
                if not db.execute("SELECT 1 FROM results WHERE attempt_id=?",(prior["attempt_id"],)).fetchone():
                    abandoned=result("BVP_INLINE_PRIMARY_FAILED" if prior["attempt_type"]=="PRIMARY" else "BVP_INLINE_RECOVERY_FAILED",
                        "PROCESS_ENDED_WITHOUT_DURABLE_COMPLETION_PARTIAL_WRITES_POSSIBLE",verification_timestamp_utc=stamp(now))
                    db.execute("INSERT INTO results VALUES (?,?,?)",(prior["attempt_id"],encoded(abandoned),digest(abandoned)))
            attempt_type="MANUAL_AUTHORIZED_RECOVERY" if trigger=="manual" else "PRIMARY" if not auto else "RECOVERY"
            claim={"contract":CONTRACT,"attempt_id":uuid.uuid4().hex,"slate_date":slate_date,
                   "attempt_type":attempt_type,"window":window,"run_tag":run_tag,"claim_timestamp_utc":stamp(now),
                   "automatic":trigger=="automatic","authorization_id":authorization_id,
                   "expected_primary_window":"0530","actual_collection_date_pt":now.astimezone(PT).date().isoformat()}
            db.execute("INSERT INTO attempts VALUES (?,?,?,?,?,?,?,?)",(claim["attempt_id"],slate_date,attempt_type,window,
                int(trigger=="automatic"),authorization_id,encoded(claim),digest(claim)))
            db.commit()  # Fail closed: acquisition is NEVER called before durable claim.
            append_event(db,claim["attempt_id"],{"event":"ACQUISITION_STARTED","timestamp_utc":stamp(clock())})
            status="BVP_INLINE_PRIMARY_SUCCESS" if attempt_type=="PRIMARY" else "BVP_INLINE_RECOVERY_SUCCESS"
            try:
                payload=acquire(claim)
                receipt={**payload,**claim,"status":status,"completion_timestamp_utc":stamp(clock())}
                if not valid_receipt(receipt):
                    completed=result("BVP_INLINE_IDENTITY_VALIDATION_FAILED","DURABLE_WRITES_OR_IDENTITY_RECEIPT_NOT_CERTIFIED",**claim,
                                     acquisition_evidence=payload,completion_timestamp_utc=stamp(clock()))
                else:
                    completed={**receipt,"certified":True,"receipt_sha256":digest(receipt)}
            except Exception as error:
                failure_reason=getattr(error,"audit_evidence",{}).get("reason","")
                failure_status=("BVP_INLINE_IDENTITY_VALIDATION_FAILED" if any(t in failure_reason for t in ("IDENTITY","JOURNAL","ROW_COUNT_MISMATCH"))
                    else "BVP_INLINE_PRIMARY_FAILED" if attempt_type=="PRIMARY" else "BVP_INLINE_RECOVERY_FAILED")
                completed=result(failure_status,
                    "ACQUISITION_TECHNICAL_FAILURE",**claim,error_class=type(error).__name__,
                    completion_timestamp_utc=stamp(clock()),acquisition_evidence=getattr(error,"audit_evidence",{}))
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO results VALUES (?,?,?)",(claim["attempt_id"],encoded(completed),digest(completed)))
            if completed["certified"]:
                db.execute("INSERT INTO successes VALUES (?,?,?,?)",(slate_date,claim["attempt_id"],encoded(receipt),digest(receipt)))
            db.commit()  # Receipt publication follows request journal + database verification.
            return completed
        finally:
            db.close()


def read_status(slate_date, path=STATE_PATH):
    success=certified_success(slate_date,path)
    if success:
        return {"contract":CONTRACT,"status":success["status"],"certified":True,
                "run_tag":success["run_tag"],"counts":success["counts"],"receipt_sha256":digest(success)}
    if not Path(path).exists():
        return result("BVP_INLINE_NOT_DUE","NO_DURABLE_ATTEMPT_EXISTS")
    try:
        with sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True) as db:
            if db.execute("SELECT 1 FROM successes WHERE slate_date=?",(slate_date,)).fetchone():
                return result("BVP_INLINE_IDENTITY_VALIDATION_FAILED","EXISTING_RECEIPT_INVALID_FAIL_CLOSED")
            r=db.execute("SELECT a.attempt_type,a.claim_json,a.claim_sha256,r.result_json,r.result_sha256 FROM attempts a LEFT JOIN results r USING(attempt_id) WHERE a.slate_date=? ORDER BY a.rowid DESC LIMIT 1",(slate_date,)).fetchone()
            if not r:
                return result("BVP_INLINE_NOT_DUE","NO_DURABLE_ATTEMPT_EXISTS")
            if hashlib.sha256(r[1].encode()).hexdigest()!=r[2]:
                return result("BVP_INLINE_IDENTITY_VALIDATION_FAILED","CLAIM_HASH_INVALID")
            if r[3] is None:
                return result("BVP_INLINE_PRIMARY_FAILED" if r[0]=="PRIMARY" else "BVP_INLINE_RECOVERY_FAILED",
                              "DURABLE_CLAIM_WITHOUT_COMPLETION_PARTIAL_WRITES_POSSIBLE")
            if hashlib.sha256(r[3].encode()).hexdigest()!=r[4]:
                return result("BVP_INLINE_IDENTITY_VALIDATION_FAILED","RESULT_HASH_INVALID")
            current=json.loads(r[3])
            if current.get("certified"):
                return result("BVP_INLINE_IDENTITY_VALIDATION_FAILED","SUCCESS_RECEIPT_MISSING")
            return current
    except (sqlite3.Error,ValueError,OSError):
        return result("BVP_INLINE_IDENTITY_VALIDATION_FAILED","STATE_UNREADABLE")
