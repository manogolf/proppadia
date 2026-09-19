"""Offline contracts, concurrency, crash recovery and isolated shell fixtures."""
from datetime import datetime, timezone
import ast
from concurrent.futures import ThreadPoolExecutor
import json
import io
import multiprocessing
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import threading
from unittest.mock import Mock

import pytest

from backend.mlb.shared import bvp_inline as b
from backend.mlb.scripts import run_mlb_bvp_inline_daily as adapter

DAY="2026-09-19"
ROOT=b.ROOT
EVIDENCE=ROOT/"artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_inline_consolidation_v1"


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def fail(*args,**kwargs):
        raise AssertionError("LIVE_NETWORK_FORBIDDEN")
    monkeypatch.setattr(socket.socket,"connect",fail)
    monkeypatch.setattr(socket,"getaddrinfo",fail)
    monkeypatch.setattr(socket,"create_connection",fail)


def at(window):
    return datetime.fromisoformat(f"{DAY}T{window[:2]}:{window[2:]}:00-07:00").astimezone(timezone.utc)


def receipt(claim):
    return {"contract":b.CONTRACT,"acquisition_started_at_utc":claim["claim_timestamp_utc"],
        "acquisition_ended_at_utc":claim["claim_timestamp_utc"],"actual_collection_timestamp_utc":claim["claim_timestamp_utc"],
        "counts":{"games":1,"rows_prepared":1,"rows_written":1,"empty_bvp_rows":0,"skip_no_opp_sp":0},
        "retry_telemetry":[],"source_code_hashes":{"fixture":"0"*64},
        "admitted_rows":[{"prop_type":"hits","player_id":1,"game_id":2,"game_date":DAY,
            "feature_set_tag":"v1","model_tag":"bvp_pvb_refresh_v1","bvp_features_sha256":b.digest({"bvp_hits":1})}],
        "validation":{"canonical_identity_valid":True,"committed_request_journal_valid":True,
            "identity_contract":"BVP_CANONICAL_SLATE_IDENTITY_V1","durable_database_rows_verified":1,
            "journal_sha256":"0"*64,"canonical_game_ids":[2]}}


def run(path, window="0530", acquire=receipt, **kwargs):
    return b.run_inline(DAY,"fixture_"+window,window,acquire,path=path,clock=lambda:at(window),**kwargs)


def test_primary_success_and_every_later_window_skips(tmp_path):
    path=tmp_path/"state.db"; acquire=Mock(side_effect=receipt)
    assert run(path,acquire=acquire)["status"]=="BVP_INLINE_PRIMARY_SUCCESS"
    for window in b.WINDOWS[1:]:
        assert run(path,window,acquire)["status"]=="BVP_INLINE_SUCCESS_ALREADY_EXISTS"
    assert acquire.call_count==1 and b.certified_success(DAY,path)


def failed(claim):
    raise TimeoutError("FIXTURE_ONLY")


def test_one_later_window_recovery_and_actual_timestamp(tmp_path):
    path=tmp_path/"state.db"
    assert run(path,acquire=failed)["status"]=="BVP_INLINE_PRIMARY_FAILED"
    assert run(path,acquire=receipt)["reason"]=="RECOVERY_REQUIRES_LATER_NATURAL_WINDOW"
    recovered=run(path,"0830")
    assert recovered["status"]=="BVP_INLINE_RECOVERY_SUCCESS"
    assert recovered["actual_collection_timestamp_utc"]==b.stamp(at("0830"))
    assert recovered["attempt_type"]=="RECOVERY"


def test_two_failures_prohibit_every_third_automatic_attempt(tmp_path):
    path=tmp_path/"state.db"
    assert run(path,acquire=failed)["status"]=="BVP_INLINE_PRIMARY_FAILED"
    assert run(path,"0830",failed)["status"]=="BVP_INLINE_RECOVERY_FAILED"
    acquire=Mock(side_effect=receipt)
    for w in b.WINDOWS[2:]:
        assert run(path,w,acquire)["reason"]=="AUTOMATIC_ATTEMPT_BUDGET_EXHAUSTED"
    acquire.assert_not_called()


def test_deterministic_concurrent_entries_acquire_exactly_once(tmp_path):
    path=tmp_path/"state.db"; entered=threading.Event(); release=threading.Event(); calls=[]
    def acquire(claim):
        calls.append(claim["attempt_id"]); entered.set()
        assert release.wait(5)
        return receipt(claim)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first=pool.submit(run,path,"0530",acquire)
        assert entered.wait(5)
        second=pool.submit(run,path,"0530",acquire)
        assert second.result(timeout=5)["reason"]=="CONCURRENT_ACQUISITION_OWNS_LOCK"
        release.set(); assert first.result(timeout=5)["certified"]
    assert len(calls)==1


def _crash(path,partial):
    def acquire(claim):
        if partial:
            with sqlite3.connect(str(path)+".partial_fixture") as db:
                db.execute("CREATE TABLE rows (identity TEXT PRIMARY KEY,value INTEGER)")
                db.execute("INSERT INTO rows VALUES ('same_player_game',1)")
        os._exit(91)
    run(path,acquire=acquire)


@pytest.mark.parametrize("partial",[False,True])
def test_real_process_crash_releases_os_lock_and_retains_attempt(tmp_path,partial):
    path=tmp_path/"state.db"
    process=multiprocessing.get_context("fork").Process(target=_crash,args=(path,partial))
    process.start(); process.join(5)
    assert process.exitcode==91 and not b.certified_success(DAY,path)
    assert b.read_status(DAY,path)["status"]=="BVP_INLINE_PRIMARY_FAILED"
    assert "WITHOUT_COMPLETION" in b.read_status(DAY,path)["reason"]
    assert run(path)["reason"]=="RECOVERY_REQUIRES_LATER_NATURAL_WINDOW"
    def recover(claim):
        if partial:
            with sqlite3.connect(str(path)+".partial_fixture") as db:
                db.execute("INSERT INTO rows VALUES ('same_player_game',1) ON CONFLICT(identity) DO UPDATE SET value=excluded.value")
                assert db.execute("SELECT count(*) FROM rows").fetchone()[0]==1
        return receipt(claim)
    assert run(path,"0830",recover)["status"]=="BVP_INLINE_RECOVERY_SUCCESS"
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM attempts").fetchone()[0]==2
        assert db.execute("SELECT count(*) FROM successes").fetchone()[0]==1
        assert "PARTIAL_WRITES_POSSIBLE" in db.execute("SELECT result_json FROM results ORDER BY rowid LIMIT 1").fetchone()[0]


@pytest.mark.parametrize("mutation",["identity", "journal", "write_count", "wrong_date", "wrong_game", "duplicate"])
def test_valid_writes_without_valid_identity_receipt_never_succeed(tmp_path,mutation):
    def invalid(claim):
        p=receipt(claim)
        if mutation=="identity": p["validation"]["canonical_identity_valid"]=False
        elif mutation=="journal": p["validation"]["committed_request_journal_valid"]=False
        elif mutation=="write_count": p["counts"]["rows_written"]=0
        elif mutation=="wrong_date": p["admitted_rows"][0]["game_date"]="2026-09-18"
        elif mutation=="wrong_game": p["admitted_rows"][0]["game_id"]=3
        else: p["admitted_rows"]*=2
        return p
    path=tmp_path/"state.db"
    assert run(path,acquire=invalid)["status"]=="BVP_INLINE_IDENTITY_VALIDATION_FAILED"
    assert not b.certified_success(DAY,path)


def test_claim_write_failure_never_acquires(tmp_path,monkeypatch):
    acquire=Mock()
    monkeypatch.setattr(b,"connect",Mock(side_effect=PermissionError("fixture blocked")))
    with pytest.raises(PermissionError): run(tmp_path/"state.db",acquire=acquire)
    acquire.assert_not_called()


def test_manual_recovery_uses_same_state_once_only_authorization(tmp_path):
    path=tmp_path/"state.db"
    run(path,acquire=failed); run(path,"0830",failed)
    assert run(path,"1100",trigger="manual")["reason"]=="EXPLICIT_MANUAL_AUTHORIZATION_REQUIRED"
    assert run(path,"1100",trigger="manual",authorization_id="USER_APPROVAL_1")["status"]=="BVP_INLINE_RECOVERY_SUCCESS"
    assert run(path,"1300")["status"]=="BVP_INLINE_SUCCESS_ALREADY_EXISTS"


def test_ledger_is_append_only_and_same_date_dependency_gate(tmp_path):
    path=tmp_path/"state.db"; run(path)
    with b.connect(path) as db:
        for table in ("attempts","results","events","successes"):
            with pytest.raises(sqlite3.IntegrityError): db.execute(f"DELETE FROM {table}")
        assert db.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
        assert not db.execute("PRAGMA foreign_key_check").fetchall()
    b.require_certified_date(DAY,path=path)
    b.require_certified_date("2026-09-20",dependency=False,path=path)
    with pytest.raises(b.BVPInlineUnavailable): b.require_certified_date("2026-09-20",path=path)
    with pytest.raises(b.BVPInlineUnavailable): b.require_certified_date("",path=path)
    assert not path.stat().st_mode & 0o077


def test_legacy_date_unchanged_and_cutover_not_backdated(tmp_path):
    b.require_certified_date("2026-09-18",path=tmp_path/"absent")
    assert b.row_admitted({"game_date":"2026-09-18"},path=tmp_path/"absent")
    acquire=Mock()
    assert b.run_inline("2026-09-18","fixture","1630",acquire,path=tmp_path/"state",clock=lambda:at("1630"))["status"]=="BVP_INLINE_NOT_DUE"
    acquire.assert_not_called()


def test_exact_source_row_gate_after_success(tmp_path):
    path=tmp_path/"state.db"; run(path)
    row={"prop_type":"hits","player_id":1,"game_id":2,"game_date":DAY,
         "feature_set_tag":"v1","model_tag":"bvp_pvb_refresh_v1","features":{"bvp_hits":1}}
    assert b.row_admitted(row,path)
    assert not b.row_admitted({**row,"game_id":3},path)
    assert not b.row_admitted({**row,"features":{"bvp_hits":2}},path)


def test_delayed_dispatch_retains_actual_window_and_collection_time():
    assert adapter.natural_window("2026-09-19T12:42:00Z")=="0530"
    assert adapter.natural_window("2026-09-19T15:40:00Z")=="0830"


def test_wake_retry_source_and_feature_formulas_unchanged():
    path="backend/mlb/scripts/refresh_mlb_bvp_pvb.py"
    previous=subprocess.run(["git","show","1c36620785837e057f89b263d3ab88f28c723469:"+path],capture_output=True,text=True,check=True).stdout
    assert (ROOT/path).read_text()==previous


def test_bvp_block_is_not_swallowed_by_heuristic_fallback():
    source=(ROOT/"backend/domains/mlb/prop_workflow.py").read_text()
    tree=ast.parse(source)
    function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=="predict_prop")
    block=next(n for n in ast.walk(function) if isinstance(n,ast.Try))
    assert isinstance(block.handlers[0].type,ast.Name) and block.handlers[0].type.id=="BVPInlineUnavailable"
    assert isinstance(block.handlers[0].body[0],ast.Raise)


def test_current_ops_brief_excludes_historical_stderr(monkeypatch,tmp_path):
    from backend.mlb.scripts import report_mlb_daily_ops_brief as brief
    monkeypatch.setattr(b,"read_status",lambda day:b.result("BVP_INLINE_PRIMARY_FAILED","CURRENT_FIXTURE_TIMEOUT"))
    historical=tmp_path/"historical.stderr"; historical.write_text("old DNS failure secret=DO_NOT_INCLUDE")
    note=brief._bvp_prewarm_failure_note(current_slate_date=DAY,completed_slate_date="2026-09-18",out_log=historical,err_log=historical)
    assert "BVP_INLINE_PRIMARY_FAILED" in note and "old DNS" not in note and "secret" not in note


@pytest.mark.parametrize("changed",[False,True])
def test_dependency_only_wide_block_preserves_market_and_independent_continuity(tmp_path,changed):
    output=tmp_path/"wide.csv"; output.write_text("original\n")
    command=tmp_path/"fixture.zsh"
    command.write_text("#!/bin/zsh\n"+(f"print changed > '{output}'\n" if changed else "")+
        f"print 'BVP_DEPENDENT_WIDE_NO_WORK_CERTIFIED slate_date={DAY} blocked_rows=4 market_snapshot_preserved=1' >&2\nexit 76\n")
    completed=subprocess.run(["zsh",str(ROOT/"bin/mlb_predictions_wide_guarded.sh"),"--slate-date",DAY,"--output",str(output),"--","zsh",str(command)],capture_output=True,text=True)
    assert completed.returncode==(76 if changed else 0)


def test_generic_failure_never_becomes_dependency_skip(tmp_path):
    completed=subprocess.run(["zsh",str(ROOT/"bin/mlb_predictions_wide_guarded.sh"),"--slate-date",DAY,"--output",str(tmp_path/"absent"),"--","zsh","-c","exit 2"],capture_output=True,text=True)
    assert completed.returncode==2


def test_private_exception_and_provider_url_never_enter_inline_log():
    output=io.StringIO(); log=adapter.SanitizedCollectorLog(output)
    log.write("[bvp-refresh] statsapi fetch retry attempt=2/3 sleep_sec=1.5 url=https://example/?apiKey=SECRET error=ConnectionError: proxy 10.1.2.3 user:SECRET\n")
    log.write('BVP_INLINE_REQUEST_STARTED {"attempt":1,"timestamp_utc":"2026-09-19T12:30:00+00:00","credential":"SECRET"}\n')
    log.write("database password=SECRET host=192.168.1.2\n"); log.flush()
    retained=output.getvalue()
    assert "ConnectionError" in retained
    assert '"credential"' not in retained and "2026-09-19T12:30:00+00:00" in retained
    assert all(s not in retained for s in ("SECRET","apiKey","10.1.2.3","192.168.1.2","https://"))


def test_original_header_function_arguments_and_restore(monkeypatch,capsys):
    from backend.mlb.scripts import refresh_mlb_bvp_pvb as collector
    original=Mock(return_value="fixture")
    monkeypatch.setattr(collector,"_request_schedule_headers",original)
    with adapter.request_start_observer(collector):
        assert collector._request_schedule_headers("PUBLIC_FIXTURE_URL",5)=="fixture"
    assert collector._request_schedule_headers is original
    original.assert_called_once_with("PUBLIC_FIXTURE_URL",5)
    assert "BVP_INLINE_REQUEST_STARTED" in capsys.readouterr().out


def test_regular_retry_telemetry_is_structured_without_url_or_exception_text(tmp_path):
    log=tmp_path/"private.log"
    log.write_text("BVP_REQUEST_RETRY_METADATA attempt=2/3 sleep_sec=1.5 error_class=ConnectionError\n")
    assert adapter.retry_telemetry(log)==[{"contract":"BVP_REGULAR_STATSAPI_RETRY_METADATA_V1","attempt":2,
        "attempts_configured":3,"sleep_sec":1.5,"error_class":"ConnectionError"}]


@pytest.mark.parametrize("mode",["valid","missing_starter","empty","off_date","partial","missing_journal"])
def test_canonical_adapter_offline_commit_certification_and_journals(tmp_path,monkeypatch,mode):
    from backend.mlb.scripts import refresh_mlb_bvp_pvb as collector
    from backend.mlb.shared import bvp_identity as identity
    from backend.shared.db import pg
    monkeypatch.setattr(b,"ROOT",tmp_path); monkeypatch.setattr(b,"STATE_PATH",tmp_path/"artifacts/ops/bvp_inline_v1/state.db")
    monkeypatch.setattr(identity,"ROOT",tmp_path); monkeypatch.setattr(b,"now_utc",lambda:at("0530"))
    config=tmp_path/"backend/mlb/config"; config.mkdir(parents=True)
    (config/"bvp_inline_acquisition_v1.json").write_text((ROOT/"backend/mlb/config/bvp_inline_acquisition_v1.json").read_text())
    game=collector.GameRow(2,DAY,108,142,111,None if mode=="missing_starter" else 222,
        official_game_id=2,official_date="2026-09-18" if mode=="off_date" else DAY,
        scheduled_start_utc="2026-09-20T01:38:00Z",schedule_date=DAY,game_state="Scheduled",
        source_observed_at_utc=b.stamp(at("0530")))
    monkeypatch.setattr(collector,"_fetch_schedule_games",lambda *a,**k:[game])
    def map_optional(games,date):
        counters=collector.IdentityCounters(int); counters["local_game_id_unmapped"]=1; counters.unmapped_game_ids=[2]
        return games,counters
    monkeypatch.setattr(collector,"_map_games_to_local_game_ids",map_optional)
    monkeypatch.setattr(collector,"_augment_games_with_db_starters",lambda games,date:(games,{}))
    monkeypatch.setattr(collector,"_active_hitters",lambda team,*a,**k:[team])
    def stats(*a,evidence,**k):
        evidence.update({"successful_response":True,"successful_empty_response_verified":mode=="empty",
            "response_observed_at_utc":b.stamp(at("0530")),"raw_response_sha256":"0"*64,"empty_response_payload":{"stats":[]}})
        return {} if mode=="empty" else {"bvp_hits":1.0}
    monkeypatch.setattr(collector,"_bvp_stats",stats)
    writes=[]
    def write(rows,**kwargs):
        writes.extend(rows)
        if mode=="missing_journal":
            for p in (tmp_path/"artifacts/ops/bvp_identity_v1"/DAY).glob("*.jsonl"): p.write_text("")
        return len(rows)-1 if mode=="partial" else len(rows)
    monkeypatch.setattr(collector,"_upsert_rows",write)
    q=Mock(); q.__enter__=Mock(return_value=q); q.__exit__=Mock(return_value=False)
    def fetched():
        return [{"prop_type":r[0],"player_id":r[1],"game_id":r[2],"game_date":r[3],"features":r[4],
            "feature_set_tag":r[5],"model_tag":r[6],"computed_at":b.stamp(at("0530"))} for r in writes]
    q.fetchall.side_effect=fetched
    connection=Mock(); connection.__enter__=Mock(return_value=connection); connection.__exit__=Mock(return_value=False); connection.cursor.return_value=q
    monkeypatch.setattr(pg,"pg_connect",lambda:connection)
    record=run(b.STATE_PATH,acquire=adapter.canonical_acquire)
    if mode in {"off_date","partial","missing_journal"}:
        assert not record["certified"] and not b.certified_success(DAY,b.STATE_PATH)
        if mode=="off_date": assert writes==[]
    else:
        assert record["certified"] and record["validation"]["committed_request_journal_valid"]
        assert record["counts"]["local_game_id_unmapped"]==1  # Optional enrichment does not substitute ID.
        if mode=="missing_starter": assert record["starter_exclusions"][0]["team_id"]==108
        if mode=="empty": assert len(record["empty_responses"])==2 and writes==[]
        if writes: assert any(call.args[0]=="SET TRANSACTION READ ONLY" for call in q.execute.call_args_list)


def test_retired_wrapper_never_reaches_original_body():
    retired=EVIDENCE/"bvp_prewarm.retired.installation-evidence.txt"
    p=subprocess.run(["zsh",str(retired)],capture_output=True,text=True)
    assert p.returncode==78 and "RETIRED_BVP_PREWARM" in p.stderr
    assert "acquired lock" not in p.stdout
    pre=(EVIDENCE/"bvp_prewarm.prechange.rollback-source.txt").read_text()
    prefix=retired.read_text().split("set -euo pipefail",1)[0]
    assert retired.read_text()[len(prefix):]==pre.split("\n",1)[1]


def test_rollback_topology_and_unrelated_daily_bytes():
    from backend.mlb.scripts import reconcile_mlb_bvp_inline_installation as installation
    pre=(EVIDENCE/"daily_wrapper.prechange.rollback-source.txt").read_text()
    post=(EVIDENCE/"daily_wrapper.authorized-postchange.installation-evidence.txt").read_text()
    start=post.index("# Governed BvP stage after roster refresh")
    end=post.index("MLB_STAT_DAYS_AGO=2",start)
    old_start=pre.index("# BvP/PvB is expected to run in the prewarm job.")
    old_end=pre.index("MLB_STAT_DAYS_AGO=2",old_start)
    normalized=post[:start]+pre[old_start:old_end]+post[end:]
    normalized=normalized.replace("# MLB_DAILY_BVP_FALLBACK_ENABLED is superseded; it cannot bypass date authority.",
        'MLB_DAILY_BVP_FALLBACK_ENABLED="${MLB_DAILY_BVP_FALLBACK_ENABLED:-0}"')
    normalized=normalized.replace('MLB_BVP_INLINE_RESULT="artifacts/ops/bvp_inline_v1/results/${MLB_DATE_ET}/${MLB_RUN_TAG}.json"\nMLB_BVP_INLINE_RC=0\n',"")
    normalized=normalized.replace('  MLB_BVP_INLINE_RESULT="$MLB_BVP_INLINE_RESULT" \\\n  MLB_BVP_INLINE_RC="$MLB_BVP_INLINE_RC" \\\n',"")
    new_phase_start=normalized.index('    phase(\n        "governed inline BvP acquisition"')
    new_phase_end=normalized.index('    phase(\n        "date ownership"',new_phase_start)
    normalized=normalized[:new_phase_start]+normalized[new_phase_end:]
    assert normalized==pre  # COMPLETE diff, not merely blessing the current hash.
    topo=installation.topology((EVIDENCE/"bvp_prewarm.prechange.rollback-plist.txt").read_bytes(),installation.DAILY_PLIST.read_bytes())
    assert topo["dedicated_schedule"]=={"Hour":3,"Minute":30} and len(topo["daily_schedule"])==5


@pytest.mark.parametrize("failed_inline",[False,True])
def test_actual_daily_roster_inline_fragment_continues_independent_work(tmp_path,failed_inline):
    post=(EVIDENCE/"daily_wrapper.authorized-postchange.installation-evidence.txt").read_text()
    fragment=post[post.index("# 1) Keep player/derived data fresh locally."):post.index("MLB_STAT_DAYS_AGO=2")]
    (tmp_path/"bin").mkdir(); fakebin=tmp_path/"fakebin"; fakebin.mkdir()
    make=fakebin/"make"; make.write_text("#!/bin/zsh\nprint ROSTER >> trace\nexit 0\n"); make.chmod(0o755)
    hook=tmp_path/"bin/mlb_bvp_inline_daily_hook.sh"
    hook.write_text("#!/bin/zsh\nprint BVP >> trace\nexit "+("2" if failed_inline else "0")+"\n"); hook.chmod(0o755)
    script=tmp_path/"fixture.zsh"
    script.write_text('set -euo pipefail\nMLB_DATE_ET=2026-09-19\nMLB_RUN_TAG=fixture\nMLB_WRAPPER_RUN_STARTED_AT_UTC=2026-09-19T12:30:00Z\nMLB_BVP_INLINE_RESULT=fixture.json\nMLB_ROSTER_REFRESH_RETRY_ATTEMPTS=1\nMLB_ROSTER_REFRESH_RETRY_SLEEP_SEC=0\n'+fragment+
        '\nprint INDEPENDENT_TOTALS_AND_MARKETS >> trace\nprint SKIPPED_NO_QUALIFIED_MODEL >> trace\nexit 0\n')
    completed=subprocess.run(["zsh",str(script)],cwd=tmp_path,env={**os.environ,"PATH":str(fakebin)+":"+os.environ["PATH"]},capture_output=True,text=True)
    assert completed.returncode==0
    assert (tmp_path/"trace").read_text().splitlines()==["ROSTER","BVP","INDEPENDENT_TOTALS_AND_MARKETS","SKIPPED_NO_QUALIFIED_MODEL"]


def test_actual_hook_owns_only_specific_lock_and_preserves_parent_shared_lock(tmp_path):
    scripts=tmp_path/"backend/mlb/scripts"; scripts.mkdir(parents=True)
    (scripts/"launchagent_lock.zsh").write_bytes((ROOT/"backend/mlb/scripts/launchagent_lock.zsh").read_bytes())
    venv=tmp_path/".venv/bin"; venv.mkdir(parents=True)
    python=venv/"python"
    python.write_text('#!/bin/zsh\nset -eu\n[[ -d artifacts/ops/locks/mlb-pipeline.lock ]]\n[[ -d artifacts/ops/locks/mlb-bvp-prewarm.lock ]]\n[[ "$MLB_BVP_INLINE_LOCK_CONTEXT" == SHARED_PIPELINE_HELD_AND_BVP_SPECIFIC_HELD ]]\nprint BOTH_LOCKS_HELD\nexit 0\n')
    python.chmod(0o755)
    code='source backend/mlb/scripts/launchagent_lock.zsh\ntrap release_launchagent_locks EXIT\nacquire_launchagent_lock mlb-pipeline 0 14400\n'+str(ROOT/"bin/mlb_bvp_inline_daily_hook.sh")+' 2026-09-19 fixture 2026-09-19T12:30:00Z unused.json\n[[ -d artifacts/ops/locks/mlb-pipeline.lock ]]\n[[ ! -d artifacts/ops/locks/mlb-bvp-prewarm.lock ]]\nprint PARENT_LOCK_PRESERVED\n'
    p=subprocess.run(["zsh","-c",code],cwd=tmp_path,capture_output=True,text=True)
    assert p.returncode==0 and "BOTH_LOCKS_HELD" in p.stdout and "PARENT_LOCK_PRESERVED" in p.stdout
    assert not list((tmp_path/"artifacts/ops/locks").glob("*.lock"))


def test_future_reader_cannot_fall_back_to_legacy_or_wrong_game_bvp(monkeypatch):
    from backend.domains.mlb import prop_workflow as workflow
    rows=[{"game_date":"2026-09-18","game_id":2,"features":{"bvp_hits":9}},
          {"game_date":DAY,"game_id":3,"features":{"bvp_hits":8}}]
    monkeypatch.setattr(workflow,"pg_fetchall",lambda *a,**k:rows)
    monkeypatch.setattr(workflow,"certified_rows",lambda rows:rows)
    args={"prop_type":"hits","player_id":1,"game_id":2,"game_date":DAY,"feature_set_tag":"v1"}
    assert workflow._load_latest_pfp_features(**args)=={}
    rows.append({"game_date":DAY,"game_id":2,"features":{"bvp_hits":1}})
    assert workflow._load_latest_pfp_features(**args)=={"bvp_hits":1}
    assert workflow._load_latest_pfp_features(**{**args,"game_id":None})=={}
    assert workflow._load_latest_pfp_features(**{**args,"game_date":"2026-09-18"})=={"bvp_hits":9}
