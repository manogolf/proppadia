#!/usr/bin/env python3
import copy, csv, hashlib, json, plistlib, tempfile, unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import joblib

from backend.mlb.scripts.dh_forward_automation_common import ROOT, append_unique_atomic, exclusive_lock, load_config, read_csv, validate_scorer
from backend.mlb.scripts.run_mlb_dh_forward_capture import FIELDS as PRED_FIELDS, score_feed
from backend.mlb.scripts.run_mlb_dh_forward_grade import FIELDS as OUT_FIELDS, classify, retained_source


class DHForwardAutomationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.base=Path(self.tmp.name)
    def tearDown(self): self.tmp.cleanup()
    def test_scorer_hash_mismatch(self):
        cfg=load_config(); cfg["scorer_sha256"]="0"*64; artifact=joblib.load(cfg["scorer_path"])
        with self.assertRaisesRegex(RuntimeError,"SCORER_HASH"): validate_scorer(cfg,artifact)
    def test_feature_schema_mismatch(self):
        cfg=load_config(); artifact=joblib.load(cfg["scorer_path"]); artifact=dict(artifact); artifact["feature_columns"]=["bad"]
        with self.assertRaisesRegex(RuntimeError,"FEATURE_SCHEMA"): validate_scorer(cfg,artifact)
    def test_duplicate_and_repeated_idempotent_capture(self):
        path=self.base/"p.csv"; rows=[{"canonical_identity":"x","v":"1"}]
        self.assertEqual(append_unique_atomic(path,["canonical_identity","v"],rows,"canonical_identity",self.base/"b"),(1,0))
        before=path.read_bytes(); self.assertEqual(append_unique_atomic(path,["canonical_identity","v"],rows,"canonical_identity",self.base/"b"),(0,1)); self.assertEqual(before,path.read_bytes())
    def test_atomic_append_failure_recovery(self):
        path=self.base/"p.csv"; append_unique_atomic(path,["canonical_identity"],[{"canonical_identity":"x"}],"canonical_identity",self.base/"b"); before=path.read_bytes()
        with self.assertRaisesRegex(RuntimeError,"SIMULATED"): append_unique_atomic(path,["canonical_identity"],[{"canonical_identity":"y"}],"canonical_identity",self.base/"b",True)
        self.assertEqual(before,path.read_bytes())
    def test_process_lock(self):
        lock=self.base/"x.lock"
        with exclusive_lock(lock):
            with self.assertRaisesRegex(RuntimeError,"PROCESS_LOCKED"): 
                with exclusive_lock(lock): pass
        self.assertFalse(lock.exists())
    def _feed(self, start_delta=timedelta(hours=1), order=True, starter=True):
        start=(datetime.now(timezone.utc)+start_delta).isoformat().replace("+00:00","Z"); away_order=list(range(1,10)) if order else []
        players={f"ID{i}":{"batSide":{"code":"R"},"pitchHand":{"code":"R"},"primaryPosition":{"abbreviation":"1B"}} for i in range(1,40)}
        tplayers={f"ID{i}":{"allPositions":[{"abbreviation":"DH" if i==1 else "1B"}],"position":{"abbreviation":"DH" if i==1 else "1B"}} for i in range(1,10)}
        return {"gamePk":1,"gameData":{"datetime":{"dateTime":start},"teams":{"away":{"id":10},"home":{"id":20}},"players":players,"status":{"abstractGameState":"Preview"}},"liveData":{"boxscore":{"teams":{"away":{"battingOrder":away_order,"players":tplayers,"bench":[11,12,13],"bullpen":[21]},"home":{"battingOrder":list(range(21,30)),"players":{},"bench":[31],"bullpen":[30,31,32],"pitchers":[30] if starter else []}}}}}
    def _hist(self):
        d={k:defaultdict(int) for k in ("pstarts","pph","pdh","prem4","teamdh","teamph")}; d["role"]=defaultdict(lambda:defaultdict(int)); d["pitching"]=defaultdict(list)
        for p in (11,12,13): d["role"][p]["games"]=20
        for p in (31,32): d["pitching"][p]=[("2026-07-20",10,3),("2026-07-22",10,3),("2026-07-24",10,3)]
        return d
    def test_pending_lineup(self):
        art=joblib.load(load_config()["scorer_path"]); _,a=score_feed(self._feed(order=False),"h",datetime.now(timezone.utc),"2026-08-04",art,"s",self._hist(),set()); self.assertIn("PENDING_OFFICIAL_LINEUP",{x["status"] for x in a})
    def test_pending_starter(self):
        art=joblib.load(load_config()["scorer_path"]); _,a=score_feed(self._feed(starter=False),"h",datetime.now(timezone.utc),"2026-08-04",art,"s",self._hist(),set()); self.assertIn("PENDING_CONFIRMED_STARTER",{x["status"] for x in a})
    def test_started_game(self):
        art=joblib.load(load_config()["scorer_path"]); _,a=score_feed(self._feed(start_delta=timedelta(hours=-1)),"h",datetime.now(timezone.utc),"2026-08-04",art,"s",self._hist(),set()); self.assertIn("BLOCKED_GAME_ALREADY_STARTED",{x["status"] for x in a})
    def test_certified_successful_capture(self):
        art=joblib.load(load_config()["scorer_path"]); rows,a=score_feed(self._feed(),"h",datetime.now(timezone.utc),"2026-08-04",art,"s",self._hist(),set()); self.assertEqual(len(rows),1); self.assertIn("CAPTURED_SCORER_CERTIFIED",{x["status"] for x in a})
    def test_successful_and_idempotent_grading(self):
        feed=self._feed(); feed["gameData"]["status"]={"abstractGameState":"Final"}; feed["liveData"]["boxscore"]["teams"]["away"]["players"]["ID1"]["stats"]={"batting":{"plateAppearances":4,"hits":2}}
        status,values=classify(feed,{"game_pk":"1","player_mlb_id":"1","team_mlb_id":"10"}); self.assertEqual(status,"RESOLVED_COMPLETED"); self.assertEqual(values["hits_o15_outcome"],"WIN")
        path=self.base/"o.csv"; outcome={"canonical_identity":"x","grading_status":status}
        self.assertEqual(append_unique_atomic(path,["canonical_identity","grading_status"],[outcome],"canonical_identity",self.base/"b"),(1,0))
        before=path.read_bytes(); self.assertEqual(append_unique_atomic(path,["canonical_identity","grading_status"],[outcome],"canonical_identity",self.base/"b"),(0,1)); self.assertEqual(before,path.read_bytes())
    def test_outcome_prediction_separation(self):
        self.assertNotIn("original_dh_hits",PRED_FIELDS); self.assertNotIn("cumulative_score",OUT_FIELDS)
    def test_grade_source_existing_cache_and_new_fetch(self):
        cfg=load_config(); cfg=dict(cfg); cfg["prior_feed_cache"]=self.base/"cache"; cfg["immutable_grade_sources"]=self.base/"sources"; cfg["prior_feed_cache"].mkdir()
        final=self._feed(); final["gameData"]["status"]={"abstractGameState":"Final"}; raw=json.dumps(final,sort_keys=True).encode(); cache=cfg["prior_feed_cache"]/"1.json"; cache.write_bytes(raw)
        got,used,path,state=retained_source(cfg,1,lambda u: self.fail("network used"),"r1")
        self.assertEqual(used,raw); self.assertEqual(path,cache); self.assertEqual(state,"CERTIFIED_EXISTING_CACHE")
        cache.unlink(); calls=[]
        got,used,path,state=retained_source(cfg,1,lambda u:(calls.append(u) or (final,raw)),"r2")
        self.assertEqual(len(calls),1); self.assertEqual(path.read_bytes(),raw); self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),hashlib.sha256(used).hexdigest()); self.assertEqual(state,"IMMUTABLE_LIVE_RESPONSE_RETAINED")
    def test_launchd_wrapper_invocation(self):
        for name,module in (("com.proppadia.mlb.dh-forward-capture.plist","backend.mlb.scripts.run_mlb_dh_forward_capture"),("com.proppadia.mlb.dh-forward-grade.plist","backend.mlb.scripts.run_mlb_dh_forward_grade")):
            payload=plistlib.loads((ROOT/"backend/mlb/launchagents"/name).read_bytes()); self.assertEqual(payload["ProgramArguments"][2],module); self.assertEqual(payload["WorkingDirectory"],str(ROOT))

if __name__=="__main__": unittest.main(verbosity=2)
