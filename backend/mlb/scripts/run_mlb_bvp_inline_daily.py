"""Inline acquisition adapter. Invoke only at a natural window or with approval.

Implementation/tests import this module without fetching or connecting. The
collector's wake retry, feature formulas, source journal and idempotent upsert
functions are reused verbatim. The adapter verifies committed rows read-only.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re

from backend.mlb.shared import bvp_inline as state


class AcquisitionCertificationFailure(RuntimeError):
    def __init__(self, reason, evidence):
        super().__init__(reason)
        self.audit_evidence={"reason":reason,**evidence}


class SanitizedCollectorLog:
    """Never persist raw exception messages, provider URLs, DSNs or proxy IDs."""
    def __init__(self, output):
        self.output=output
        self.pending=""

    def write(self, text):
        self.pending+=text
        while "\n" in self.pending:
            line,self.pending=self.pending.split("\n",1)
            safe="BVP_COLLECTOR_DIAGNOSTIC_REDACTED"
            if line.startswith("[bvp-refresh] {"):
                try:
                    value=json.loads(line.split(" ",1)[1])
                    fields={"contract","stage","status","attempt","attempts_used","classification","next_wait_sec",
                        "http_response_received","timestamp_utc","total_retry_delay_sec","first_failure_timestamp_utc",
                        "successful_attempt_timestamp_utc"}
                    safe="[bvp-refresh] "+state.encoded({k:v for k,v in value.items() if k in fields})
                except ValueError:
                    pass
            elif line.startswith("BVP_INLINE_REQUEST_STARTED "):
                try:
                    value=json.loads(line.split(" ",1)[1])
                    observed=datetime.fromisoformat(value["timestamp_utc"])
                    if isinstance(value["attempt"],int) and not isinstance(value["attempt"],bool) and observed.tzinfo:
                        safe="BVP_INLINE_REQUEST_STARTED "+state.encoded({"attempt":value["attempt"],"timestamp_utc":observed.isoformat()})
                except (ValueError,KeyError,TypeError):
                    pass
            elif "statsapi fetch retry" in line:
                match=re.search(r"attempt=(\d+/\d+).*sleep_sec=([0-9.]+).*error=([A-Za-z]+)",line)
                if match:
                    safe=f"BVP_REQUEST_RETRY_METADATA attempt={match[1]} sleep_sec={match[2]} error_class={match[3]}"
            self.output.write(safe+"\n")
        return len(text)

    def flush(self):
        if self.pending:
            self.write("\n")
        self.output.flush()
        try: os.fsync(self.output.fileno())
        except (OSError,ValueError): pass


@contextmanager
def request_start_observer(collector):
    # Instrumentation only: same header function, URL, timeout and retry policy.
    original=collector._request_schedule_headers
    attempt=0
    def observed(url,timeout_sec):
        nonlocal attempt
        attempt+=1
        print("BVP_INLINE_REQUEST_STARTED "+state.encoded({"attempt":attempt,"timestamp_utc":state.stamp(state.now_utc())}),flush=True)
        return original(url,timeout_sec)
    collector._request_schedule_headers=observed
    try:
        yield
    finally:
        collector._request_schedule_headers=original


def file_sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_database_rows(prepared, journals):
    from backend.shared.db.pg import pg_connect
    from backend.mlb.shared import bvp_identity as identity
    expected={(r[0],r[1],r[2],r[5]):r for r in prepared}
    found=[]
    with pg_connect() as conn,conn.cursor() as q:
        q.execute("SET TRANSACTION READ ONLY")
        q.execute("SET LOCAL statement_timeout='60s'")
        keys=list(expected)
        for offset in range(0,len(keys),500):
            batch=keys[offset:offset+500]
            placeholders=",".join("(%s,%s,%s,%s)" for _ in batch)
            q.execute("SELECT prop_type,player_id,game_id,game_date,features,feature_set_tag,model_tag,computed_at FROM mlb.prop_features_precomputed WHERE (prop_type,player_id,game_id,feature_set_tag) IN ("+placeholders+")",
                      tuple(v for key in batch for v in key))
            found.extend(q.fetchall())
    if len(found)!=len(expected):
        raise AcquisitionCertificationFailure("COMMITTED_ROW_COUNT_MISMATCH",{"expected":len(expected),"verified":len(found)})
    admitted=[]
    for row in found:
        original=expected[(row["prop_type"],row["player_id"],row["game_id"],row["feature_set_tag"])]
        bvp={k:v for k,v in row["features"].items() if str(k).startswith("bvp_")}
        if (str(row["game_date"])[:10]!=original[3] or row["model_tag"]!=original[6]
            or identity.stable_hash(bvp)!=identity.stable_hash(original[4])
            or not identity.forward_source_identity_valid(row,[journals])):
            raise AcquisitionCertificationFailure("COMMITTED_ROW_IDENTITY_OR_SOURCE_RECEIPT_MISMATCH",{"verified":len(admitted)})
        admitted.append({"prop_type":row["prop_type"],"player_id":row["player_id"],"game_id":row["game_id"],
            "game_date":str(row["game_date"])[:10],"feature_set_tag":row["feature_set_tag"],"model_tag":row["model_tag"],
            "computed_at_utc":str(row["computed_at"]),"bvp_features_sha256":identity.stable_hash(bvp)})
    return sorted(admitted,key=state.encoded)


def canonical_acquire(claim):
    # Lazy imports keep every status/readiness/fixture operation network-free.
    from backend.mlb.scripts import refresh_mlb_bvp_pvb as collector
    from backend.mlb.shared import bvp_identity as identity
    source=Path(collector.__file__)
    governed=json.loads((state.ROOT/"backend/mlb/config/bvp_inline_acquisition_v1.json").read_text())
    if file_sha(source)!=governed["canonical_collector_sha256"]:
        raise AcquisitionCertificationFailure("UNREVIEWED_COLLECTOR_SOURCE_HASH",{})
    folder=state.STATE_PATH.parent/"runs"/claim["slate_date"]
    folder.mkdir(mode=0o700,parents=True,exist_ok=True)
    log=folder/(claim["run_tag"]+"_"+claim["attempt_id"]+".log")
    fd=os.open(log,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    started=state.stamp(state.now_utc())
    evidence={"acquisition_started_at_utc":started,"log_path":str(log.relative_to(state.ROOT)),
              "source_code_hashes":{"collector":file_sha(source),"identity":file_sha(Path(identity.__file__)),
                                    "inline_state":file_sha(Path(state.__file__)),"adapter":file_sha(Path(__file__))}}
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as output:
          sanitized=SanitizedCollectorLog(output)
          with redirect_stdout(sanitized),redirect_stderr(sanitized),request_start_observer(collector):
            rows,counters=collector._build_rows_for_date(claim["slate_date"],feature_set_tag="v1",model_tag="bvp_pvb_refresh_v1",
                timeout_sec=int(os.getenv("MLB_BVP_REQUEST_TIMEOUT_SEC","20")),retries=int(os.getenv("MLB_BVP_REQUEST_RETRIES","3")))
            evidence["counts"]={**dict(counters),"rows_prepared":len(rows),"rows_written":0}
            if any(counters.get(k,0) for k in ("off_date_rejected_games","canonical_identity_unresolved_games",
                                              "off_date_rejected_rows","canonical_identity_unresolved_rows")):
                raise AcquisitionCertificationFailure("BVP_INLINE_IDENTITY_VALIDATION_FAILED",evidence)
            if counters.get("roster_fetch_errors",0) or counters.get("bvp_fetch_errors",0):
                raise AcquisitionCertificationFailure("BVP_REQUEST_OR_ROSTER_ACQUISITION_FAILED",evidence)
            path=getattr(counters,"audit_path",None)
            if path is None:
                if rows or counters.get("games",0):
                    raise AcquisitionCertificationFailure("REQUEST_JOURNAL_MISSING",evidence)
                with collector.IdentityAudit(claim["slate_date"]) as audit:
                    audit.record("VALID_EMPTY_SLATE",source_branch="STATSAPI_CANONICAL_SCHEDULE")
                    audit.record("PRE_WRITE_IDENTITY_VALIDATION_COMPLETE",rows_prepared=0,
                        feature_set_tag="v1",model_tag="bvp_pvb_refresh_v1",prepared_row_stream_sha256=identity.stable_hash([]))
                    path=audit.path
            written=collector._upsert_rows(rows,batch_size=int(os.getenv("MLB_BVP_BATCH_SIZE","1000")))
            evidence["counts"]["rows_written"]=written
            if written!=len(rows):
                raise AcquisitionCertificationFailure("PARTIAL_DATABASE_WRITE",evidence)
            commit={"contract":identity.CONTRACT,"reason_code":"DATABASE_WRITE_COMMITTED",
                    "acquisition_timestamp_utc":state.stamp(state.now_utc()),"rows_written_total":written}
            with Path(path).open("a",encoding="utf-8") as journal:
                journal.write(state.encoded(commit)+"\n"); journal.flush(); os.fsync(journal.fileno())
            journals=[json.loads(line) for line in Path(path).read_text().splitlines()]
            if (not any(j.get("reason_code")=="PRE_WRITE_IDENTITY_VALIDATION_COMPLETE" for j in journals)
                or any(j.get("reason_code") in {"CANONICAL_IDENTITY_UNRESOLVED","BVP_REQUEST_FAILED","ROSTER_FETCH_FAILED",
                    "EMPTY_BVP_RESPONSE_UNVERIFIABLE","OFF_DATE_GAME_REJECTED"} for j in journals)):
                raise AcquisitionCertificationFailure("REQUEST_JOURNAL_NOT_CERTIFIABLE",evidence)
            admitted=verify_database_rows(rows,journals)
            evidence["admitted_rows"]=admitted
            evidence["validation"]={"canonical_identity_valid":True,"committed_request_journal_valid":True,
                "identity_contract":identity.CONTRACT,"durable_database_rows_verified":len(admitted),
                "journal_path":str(Path(path).relative_to(state.ROOT)),"journal_sha256":file_sha(path),
                "canonical_game_ids":sorted({j["game_id"] for j in journals if j.get("reason_code")=="CANONICAL_SLATE_IDENTITY_VALID"}),
                "prepared_row_stream_sha256":identity.stable_hash(rows)}
            evidence["identity_exclusions"]=[j for j in journals if "REJECTED" in j.get("reason_code","") or "UNRESOLVED" in j.get("reason_code","") and j.get("reason_code")!="OPPOSING_STARTER_UNRESOLVED"]
            evidence["starter_exclusions"]=[j for j in journals if j.get("reason_code")=="OPPOSING_STARTER_UNRESOLVED"]
            evidence["empty_responses"]=[j for j in journals if j.get("reason_code")=="EMPTY_BVP_RESPONSE"]
            sanitized.flush(); os.fsync(output.fileno())
    except Exception as error:
        evidence["acquisition_ended_at_utc"]=state.stamp(state.now_utc())
        evidence["retry_telemetry"]=retry_telemetry(log)
        starts=[json.loads(line.split(" ",1)[1]) for line in log.read_text().splitlines() if line.startswith("BVP_INLINE_REQUEST_STARTED ")]
        evidence["request_attempt_start_timestamps_utc"]=[s["timestamp_utc"] for s in starts]
        evidence["request_started_at_utc"]=starts[0]["timestamp_utc"] if starts else None
        if isinstance(error,AcquisitionCertificationFailure):
            error.audit_evidence.update(evidence)
            raise
        raise AcquisitionCertificationFailure(type(error).__name__,evidence) from None
    evidence["acquisition_ended_at_utc"]=state.stamp(state.now_utc())
    evidence["actual_collection_timestamp_utc"]=evidence["acquisition_started_at_utc"]
    evidence["retry_telemetry"]=retry_telemetry(log)
    starts=[json.loads(line.split(" ",1)[1]) for line in log.read_text().splitlines() if line.startswith("BVP_INLINE_REQUEST_STARTED ")]
    evidence["request_attempt_start_timestamps_utc"]=[s["timestamp_utc"] for s in starts]
    evidence["request_started_at_utc"]=starts[0]["timestamp_utc"] if starts else None
    evidence["log_sha256"]=file_sha(log)
    return evidence


def retry_telemetry(log):
    records=[]
    for line in Path(log).read_text(errors="replace").splitlines():
        if line.startswith("[bvp-refresh] {"):
            event=json.loads(line.split(" ",1)[1])
            if event.get("contract")=="BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1":
                records.append(event)
        elif line.startswith("BVP_REQUEST_RETRY_METADATA "):
            match=re.fullmatch(r"BVP_REQUEST_RETRY_METADATA attempt=(\d+)/(\d+) sleep_sec=([0-9.]+) error_class=([A-Za-z]+)",line)
            if match:
                records.append({"contract":"BVP_REGULAR_STATSAPI_RETRY_METADATA_V1","attempt":int(match[1]),
                    "attempts_configured":int(match[2]),"sleep_sec":float(match[3]),"error_class":match[4]})
    return records


def natural_window(started_at):
    local=datetime.fromisoformat(started_at.replace("Z","+00:00")).astimezone(state.PT)
    return next((w for w in reversed(state.WINDOWS) if w<=local.strftime("%H%M")),"NOT_DUE")


AUTOMATIC_AUTHORITY = "AUTOMATIC_DAILY_WRAPPER"
MANUAL_AUTHORITY = "AUTHORIZED_MANUAL_RECOVERY"


def dispatch_for_authority(slate_date, run_tag, started_at, authority,
                           manual_authorization_id=None, acquire=canonical_acquire):
    """Translate an explicit caller declaration into the durable state contract.

    XPC_SERVICE_NAME and every other ambient launchd variable are deliberately
    irrelevant. Unknown, conflicting, or incomplete declarations fail before a
    durable attempt can be claimed.
    """
    if authority == AUTOMATIC_AUTHORITY and not manual_authorization_id:
        trigger = "automatic"
    elif authority == MANUAL_AUTHORITY and manual_authorization_id:
        trigger = "manual"
    elif authority == MANUAL_AUTHORITY:
        return state.result("BVP_INLINE_NOT_DUE", "EXPLICIT_MANUAL_AUTHORIZATION_REQUIRED")
    elif authority == AUTOMATIC_AUTHORITY:
        return state.result("BVP_INLINE_NOT_DUE", "AUTHORITY_ARGUMENT_CONFLICT")
    else:
        return state.result("BVP_INLINE_NOT_DUE", "UNRECOGNIZED_INVOCATION_AUTHORITY")
    return state.run_inline(
        slate_date, run_tag, natural_window(started_at), acquire,
        trigger=trigger, authorization_id=manual_authorization_id,
    )


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--date",required=True); p.add_argument("--run-tag",required=True)
    p.add_argument("--started-at",required=True); p.add_argument("--output",required=True,type=Path)
    p.add_argument("--invocation-authority",required=True)
    p.add_argument("--manual-authorization-id")
    args=p.parse_args()
    # The shell hook verifies inherited shared lock and owns the BvP-specific lock.
    if os.getenv("MLB_BVP_INLINE_LOCK_CONTEXT")!="SHARED_PIPELINE_HELD_AND_BVP_SPECIFIC_HELD":
        print(state.encoded(state.result("BVP_INLINE_NOT_DUE","GOVERNED_LOCK_CONTEXT_REQUIRED")))
        return 2
    try:
        record=dispatch_for_authority(
            args.date,args.run_tag,args.started_at,args.invocation_authority,
            args.manual_authorization_id,
        )
    except Exception as error:
        record=state.result("BVP_INLINE_PRIMARY_FAILED","DURABLE_CLAIM_OR_STATE_WRITE_FAILED",error_class=type(error).__name__)
    args.output.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(args.output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,"w") as output:
        output.write(state.encoded(record)+"\n"); output.flush(); os.fsync(output.fileno())
    # Console metadata excludes payloads, addresses, identities and exception text.
    print(state.encoded({k:record[k] for k in ("contract","status","reason","certified") if k in record}))
    return 0 if record["status"] in state.SUCCESS|{"BVP_INLINE_SUCCESS_ALREADY_EXISTS","BVP_INLINE_NOT_DUE"} else 2


if __name__=="__main__":
    raise SystemExit(main())
