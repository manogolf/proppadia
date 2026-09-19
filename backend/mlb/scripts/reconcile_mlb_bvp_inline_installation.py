"""Offline preparation/validation and separately approved local installation.

Never runs the wrapper, collector, Makefile target or network probe. Installed
scripts remain installed-owned. Full .txt files are non-executable deployment/
rollback evidence, not a competing tracked executable wrapper or template.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import sqlite3
import subprocess
import tempfile
import time

from backend.mlb.shared import bvp_inline as b

ROOT=b.ROOT
OUT=ROOT/"artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_inline_consolidation_v1"
DAILY=Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
PREWARM=Path("/Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh")
LABEL="com.proppadia.mlb.bvp.prewarm.daily"
PLIST=Path("/Users/jerrystrain/Library/LaunchAgents")/(LABEL+".plist")
DAILY_PLIST=PLIST.parent/"com.proppadia.mlb.refresh.daily.plist"
HITS_MANIFEST=ROOT/"artifacts/analysis/model_development/mlb_hits05_sportsbook_independent_full_board_shadow_stream_v1/2026-08-23/sha256_manifest.json"
SOURCES=("backend/mlb/shared/bvp_inline.py","backend/mlb/scripts/run_mlb_bvp_inline_daily.py",
    "backend/mlb/scripts/reconcile_mlb_bvp_inline_installation.py","backend/mlb/shared/bvp_identity.py",
    "backend/domains/mlb/prop_workflow.py","backend/mlb/prediction/make_prediction.py",
    "backend/mlb/scripts/build_mlb_predictions_wide.py","backend/mlb/scripts/report_mlb_daily_ops_brief.py",
    "backend/mlb/config/bvp_inline_acquisition_v1.json","backend/mlb/tests/test_bvp_inline_daily.py",
    "backend/mlb/tests/test_mlb_bvp_prewarm_exit_semantics.py",
    "bin/mlb_bvp_inline_daily_hook.sh","bin/mlb_bvp_inline_manual_recovery.sh","bin/mlb_predictions_wide_guarded.sh")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def put(name,data):
    (OUT/name).write_text(json.dumps(data,sort_keys=True,indent=2)+"\n")


def command(*args):
    return subprocess.run(args,capture_output=True,text=True,check=False)


def metadata(path):
    s=path.stat()
    return {"path":str(path),"sha256":sha(path),"mode":oct(s.st_mode&0o777),"uid":s.st_uid,"gid":s.st_gid,"bytes":s.st_size}


def repeating_power():
    p=command("pmset","-g","sched")
    assert p.returncode==0
    return p.stdout.split("Scheduled power events:")[0].strip()


def loaded_labels():
    p=command("launchctl","list")
    assert p.returncode==0
    return sorted(line.split()[-1] for line in p.stdout.splitlines()
                  if line.split() and "proppadia" in line.split()[-1].lower())


def topology(plist_data,daily_data):
    old=plistlib.loads(plist_data); daily=plistlib.loads(daily_data)
    assert old["Label"]==LABEL and old["StartCalendarInterval"]=={"Hour":3,"Minute":30}
    assert daily["StartCalendarInterval"]==[{"Hour":h,"Minute":m} for h,m in ((5,30),(8,30),(11,0),(13,0),(16,30))]
    return {"dedicated_label":old["Label"],"dedicated_schedule":old["StartCalendarInterval"],
            "daily_label":daily["Label"],"daily_schedule":daily["StartCalendarInterval"]}


def runtime():
    text=(ROOT/"artifacts/ops/mlb_bvp_prewarm_daily.out.log").read_text()
    starts=list(re.finditer(r"\[([^\]]+)\] START local MLB BvP prewarm \(MLB_DATE_ET=(\d{4}-\d{2}-\d{2})",text))
    rows=[]
    for day in range(11,18):
        date=f"2026-09-{day:02d}"
        selected=[(n,s) for n,s in enumerate(starts) if s[2]==date]
        if not selected: continue
        n,match=selected[-1]
        chunk=text[match.start():starts[n+1].start() if n+1<len(starts) else len(text)]
        acquired=re.search(r"\[([^\]]+)\] INFO BVP_ACQUISITION_STATUS=SUCCESS",chunk)
        done=re.search(r"\[([^\]]+)\] DONE local MLB BvP prewarm",chunk)
        summary=re.search(r"\[bvp-refresh\] summary (.+)",chunk)
        if not acquired or not done or not summary: continue
        times=[datetime.fromisoformat(s.replace("Z","+00:00")) for s in (match[1],acquired[1],done[1])]
        rows.append({"slate_date":date,"wrapper_start_utc":match[1],"acquisition_success_utc":acquired[1],"done_utc":done[1],
            "acquisition_including_make_startup_seconds":(times[1]-times[0]).total_seconds(),
            "downstream_seconds":(times[2]-times[1]).total_seconds(),"total_seconds":(times[2]-times[0]).total_seconds(),
            "summary":dict((k,int(v)) for k,v in re.findall(r"(\w+)=(\d+)",summary[1])),
            "retained_segment_sha256":hashlib.sha256(chunk.encode()).hexdigest()})
    assert len(rows)==7 and max(r["acquisition_including_make_startup_seconds"] for r in rows)<3600
    manual=ROOT/"artifacts/ops/manual_bvp_20260918T171242Z_29198.log"
    return {"recent_successful_runs":rows,"september18":{"log_path":str(manual.relative_to(ROOT)),"sha256":sha(manual),
        "start_utc":"2026-09-18T17:12:42Z","end_utc":"2026-09-18T17:13:44Z","duration_seconds":62,
        "bvp_fetches":392,"errors":0,"rows_written":1937,"dry_run":False,"cache_only":False,
        "qualification":"Retained request-success and upsert evidence; in-run pair cache preserved, not a cache-only acquisition. Legacy identity/pitcher certification is NOT repaired."}}


def prepare():
    assert sha(DAILY)==sha(OUT/"daily_wrapper.prechange.rollback-source.txt")
    assert sha(PREWARM)==sha(OUT/"bvp_prewarm.prechange.rollback-source.txt")
    assert PLIST.read_bytes()==(OUT/"bvp_prewarm.prechange.rollback-plist.txt").read_bytes()
    governance=json.loads(HITS_MANIFEST.read_text())
    bound=next(r for r in governance["files"] if r["path"]==str(DAILY))
    assert sha(DAILY)==bound["sha256"]=="96b371f3b8d52cd73c9950b031829e7e7c3d49edf8d90857a81e9afab261038b"
    assert sha(PREWARM)=="23016b56dfc85eddf9f11eab12010388ddb833fa73bc994a367bac3a632fefdb"
    plan={"contract":b.CONTRACT,"effective_date":b.EFFECTIVE_DATE,"captured_at_utc":b.stamp(b.now_utc()),
        "installed_before":{"daily":metadata(DAILY),"prewarm":metadata(PREWARM),"plist":metadata(PLIST),"daily_plist":metadata(DAILY_PLIST)},
        "topology_before":topology(PLIST.read_bytes(),DAILY_PLIST.read_bytes()),"repeating_power_before":repeating_power(),
        "ownership":"Daily and prewarm wrappers remain installed-owned; .txt files are non-executable deployment/rollback evidence, not tracked executable owners",
        "state":"New local operational claims/receipts are necessary: existing identity journals cannot provide transactional date claims. No PostgreSQL schema change.",
        "historical_manifest_policy":"Historical audit/implementation snapshot manifests remain immutable. This reconciliation supersedes their current-installed-state assertions; only active Hits package governance binding is updated.",
        "external_requests":0,"production_acquisition_runs":0}
    assert "5:27AM every day" in plan["repeating_power_before"]
    put("reconciliation_manifest.json",plan); put("retained_runtime_evidence.json",runtime())
    print(b.encoded({"prepare":"PASS","pre_daily_sha256":sha(DAILY),"recent_acquisition_max_seconds":85,"september18_acquisition_seconds":62}))


def preflight():
    for label in (LABEL,"com.proppadia.mlb.refresh.daily"):
        r=command("launchctl","print",f"gui/{os.getuid()}/{label}")
        if r.returncode==0 and ("state = running" in r.stdout or re.search(r"\bpid = [1-9]",r.stdout)):
            raise RuntimeError("ACTIVE_OPERATION_BLOCKS_INSTALL")
    busy=[p.name for p in (ROOT/"artifacts/ops/locks").glob("*.lock") if p.name in {"mlb-bvp-prewarm.lock","mlb-pipeline.lock","mlb-daily-refresh.lock"}]
    if busy: raise RuntimeError("EXISTING_GOVERNED_LOCK_BLOCKS_INSTALL")
    processes=command("ps","-axo","pid=,command=")
    assert processes.returncode==0
    for line in processes.stdout.splitlines():
        if re.search(r"(?:/Users/jerrystrain/bin/proppadia_mlb_(?:refresh_daily|bvp_prewarm)\.sh|python\S*\s+(?:-m\s+backend\.mlb\.scripts\.)?refresh_mlb_bvp_pvb)",line):
            raise RuntimeError("ACTIVE_PROCESS_BLOCKS_INSTALL")
    local=b.now_utc().astimezone(b.PT)
    for w in b.WINDOWS:
        due=local.replace(hour=int(w[:2]),minute=int(w[2:]),second=0,microsecond=0)
        if abs((due-local).total_seconds())<300: raise RuntimeError("INSTALL_TOO_CLOSE_TO_NATURAL_DISPATCH")


def replace_installed(path,content,expected,mode):
    assert sha(path)==expected
    assert command("zsh","-n",str(content)).returncode==0
    fd,temp=tempfile.mkstemp(prefix=".bvp-inline-authorized-",dir=path.parent)
    try:
        with os.fdopen(fd,"wb") as f:
            f.write(content.read_bytes()); f.flush(); os.fsync(f.fileno())
        os.chmod(temp,mode)
        os.replace(temp,path)  # Atomic publication, never a truncated executable.
    finally:
        if Path(temp).exists(): Path(temp).unlink()


def wait_for_idle():
    deadline=time.monotonic()+3600
    while True:
        try:
            preflight()
            return
        except RuntimeError as error:
            if str(error) not in {"ACTIVE_OPERATION_BLOCKS_INSTALL","EXISTING_GOVERNED_LOCK_BLOCKS_INSTALL",
                "ACTIVE_PROCESS_BLOCKS_INSTALL","INSTALL_TOO_CLOSE_TO_NATURAL_DISPATCH"}:
                raise
            if time.monotonic()>=deadline:
                raise RuntimeError("BOUNDED_IDLE_WAIT_EXPIRED_NO_INSTALL") from None
            print(b.encoded({"install_wait":"READ_ONLY","reason":str(error),"timestamp_utc":b.stamp(b.now_utc())}),flush=True)
            time.sleep(30)


def install():
    plan=json.loads((OUT/"reconciliation_manifest.json").read_text())
    preflight()
    labels_before=loaded_labels()
    plists_before={str(p):sha(p) for p in PLIST.parent.glob("*proppadia*.plist")}
    assert PLIST.read_bytes()==(OUT/"bvp_prewarm.prechange.rollback-plist.txt").read_bytes()
    assert sha(DAILY)==plan["installed_before"]["daily"]["sha256"]
    assert sha(PREWARM)==plan["installed_before"]["prewarm"]["sha256"]
    for key,path in (("daily",DAILY),("prewarm",PREWARM)):
        current=metadata(path)
        assert all(current[field]==plan["installed_before"][key][field] for field in ("uid","gid","mode"))
    bound=next(r for r in json.loads(HITS_MANIFEST.read_text())["files"] if r["path"]==str(DAILY))
    target_sha=sha(OUT/"daily_wrapper.authorized-postchange.installation-evidence.txt")
    assert target_sha=="7df06cfc8084653f2ab4f4e18fc8a6f6fc19188686a92a0016cb6097fa08d43a"
    assert sha(OUT/"bvp_prewarm.retired.installation-evidence.txt")=="91d43cb2393fc9e0a57bea0a309abeb2632b03100cb92210847f0f0b3f169149"
    assert bound["sha256"] in {plan["installed_before"]["daily"]["sha256"],target_sha}
    assert repeating_power()==plan["repeating_power_before"]
    domain=f"gui/{os.getuid()}"
    assert command("launchctl","disable",domain+"/"+LABEL).returncode==0
    unloaded=command("launchctl","bootout",domain+"/"+LABEL)
    assert unloaded.returncode==0 or command("launchctl","print",domain+"/"+LABEL).returncode!=0
    # Protect against any unobservable obsolete shortcut/alias direct invocation.
    replace_installed(PREWARM,OUT/"bvp_prewarm.retired.installation-evidence.txt",plan["installed_before"]["prewarm"]["sha256"],int(plan["installed_before"]["prewarm"]["mode"],8))
    replace_installed(DAILY,OUT/"daily_wrapper.authorized-postchange.installation-evidence.txt",plan["installed_before"]["daily"]["sha256"],int(plan["installed_before"]["daily"]["mode"],8))
    # Mechanical binding update only after idle deployment; keep the baseline
    # hash valid throughout any preceding natural run / bounded idle wait.
    governance=json.loads(HITS_MANIFEST.read_text())
    target=next(r for r in governance["files"] if r["path"]==str(DAILY))
    target["sha256"]=target_sha
    HITS_MANIFEST.write_text(json.dumps(governance,indent=2)+"\n")
    db=b.connect(); db.close()  # Empty operational schema only, NEVER acquire.
    disabled=command("launchctl","print-disabled",domain)
    assert command("launchctl","print",domain+"/"+LABEL).returncode!=0
    assert re.search(r'"'+re.escape(LABEL)+r'" => disabled',disabled.stdout)
    assert sha(DAILY_PLIST)==plan["installed_before"]["daily_plist"]["sha256"]
    assert sha(PLIST)==plan["installed_before"]["plist"]["sha256"]
    assert repeating_power()==plan["repeating_power_before"]
    for key,path in (("daily",DAILY),("prewarm",PREWARM)):
        current=metadata(path)
        assert all(current[field]==plan["installed_before"][key][field] for field in ("uid","gid","mode"))
    labels_after=loaded_labels()
    assert set(labels_after)==set(labels_before)-{LABEL}
    assert all(sha(Path(p))==value for p,value in plists_before.items())
    plan.update({"installed_after":{"daily":metadata(DAILY),"prewarm":metadata(PREWARM),"plist":metadata(PLIST),"daily_plist":metadata(DAILY_PLIST)},
        "loaded_proppadia_labels_before":labels_before,"loaded_proppadia_labels_after":labels_after,
        "all_proppadia_plists_unchanged":plists_before,
        "installed_at_utc":b.stamp(b.now_utc()),"retirement":{"label":LABEL,"loaded":False,"disabled":True,"plist_bytes_unchanged":True,
            "original_wrapper_body_preserved_under_retirement_guard":True,"direct_obsolete_invocation":"BLOCKED_EXIT_78_BEFORE_ANY_LOCK_ENV_OR_REQUEST"},
        "repeating_power_after":repeating_power(),"acquisition_invocations":0})
    put("reconciliation_manifest.json",plan)
    print(b.encoded({"install":"PASS","legacy_agent":"UNLOADED_DISABLED_AND_DIRECT_INVOCATION_GUARDED","post_daily_sha256":sha(DAILY),"acquisitions":0,"power_changed":False}))


def manifest():
    paths=[p for p in OUT.iterdir() if p.is_file() and p.name!="sha256_manifest.json"]
    paths.extend(ROOT/p for p in SOURCES)
    paths.extend((DAILY,PREWARM,PLIST,DAILY_PLIST,HITS_MANIFEST,ROOT/"backend/mlb/scripts/refresh_mlb_bvp_pvb.py"))
    paths.extend((ROOT/"docs/MLB BvP Daily Inline Acquisition Contract V1.md",ROOT/"docs/MLB BvP Daily Inline Operator Runbook V1.md",ROOT/"docs/Prod12 Automation Runbook.md"))
    put("sha256_manifest.json",{"contract":b.CONTRACT,"files":{str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p):sha(p) for p in sorted(set(paths))},"self_excluded":True,"operational_ledger_excluded":"MUTABLE_APPEND_ONLY_AUTHORITY_VALIDATED_STRUCTURALLY_NOT_FROZEN_FILE_HASH"})


def validate():
    entries=json.loads((OUT/"sha256_manifest.json").read_text())["files"]
    assert all(sha(ROOT/path)==value for path,value in entries.items())
    plan=json.loads((OUT/"reconciliation_manifest.json").read_text())
    assert plan["retirement"]["disabled"] and not plan["retirement"]["loaded"]
    assert repeating_power()==plan["repeating_power_before"]==plan["repeating_power_after"]
    assert sha(DAILY)==sha(OUT/"daily_wrapper.authorized-postchange.installation-evidence.txt")
    assert sha(PREWARM)==sha(OUT/"bvp_prewarm.retired.installation-evidence.txt")
    with sqlite3.connect(b.STATE_PATH.resolve().as_uri()+"?mode=ro",uri=True) as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
        assert not db.execute("PRAGMA foreign_key_check").fetchall()
        counts={t:db.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("attempts","events","results","successes")}
        assert all(v==0 for v in counts.values()), "IMPLEMENTATION_MUST_NOT_ACQUIRE"
    print(b.encoded({"validation":"PASS","sha256_entries":len(entries),"ledger_counts":counts,"live_api_requests":0}))


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("operation",choices=("prepare","install","manifest","validate"))
    p.add_argument("--wait-for-idle",action="store_true",help="Installation only: bounded read-only idle wait, at most one hour")
    args=p.parse_args()
    try:
        if args.wait_for_idle:
            assert args.operation=="install"
            wait_for_idle()
        globals()[args.operation]()
    except Exception as error:
        print(b.encoded({"reconciliation":"FAIL","error_class":type(error).__name__,"diagnostic":"STOP_NO_FURTHER_OPERATIONAL_MUTATION"}))
        raise SystemExit(1) from None
