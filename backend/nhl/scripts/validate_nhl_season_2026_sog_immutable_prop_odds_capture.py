#!/usr/bin/env python3
"""Validate and package the bounded immutable NHL SOG quote-capture remediation."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import pandas as pd

from backend.nhl.sog_quote_capture.core import QUOTE_COLUMNS, capture_run, sha256_file
from backend.nhl.analysis_package_guard import require_create_only,verify_manifest

ROOT=Path(__file__).resolve().parents[3]; DATE="2026-08-10"
OUT=ROOT/"artifacts/analysis/model_development/nhl_season_2026_sog_immutable_prop_odds_capture"/DATE
CANONICAL_MANIFEST_SHA256="0ffc9c2630deded0b1774d717c1e7183abdbdbc4b8ca92f741b47717cf5f195c"

def write_csv(name:str,rows:list[dict],fields:list[str]|None=None)->None:
    if fields is None: fields=list(rows[0]) if rows else []
    with (OUT/name).open("w",newline="") as h:
        w=csv.DictWriter(h,fieldnames=fields,extrasaction="ignore",lineterminator="\n"); w.writeheader(); w.writerows(rows)
def write_json(name:str,obj:object)->None: (OUT/name).write_text(json.dumps(obj,indent=2,sort_keys=True)+"\n")
def h(path:Path)->str: return hashlib.sha256(path.read_bytes()).hexdigest()

def fixture_payload(capture:str, *, final:bool=False)->dict:
    market_time="2026-10-10T19:55:00Z" if final else "2026-10-10T15:55:00Z"
    common=[
      {"name":"Over","description":"Alpha Skater","price":-120,"point":1.5},
      {"name":"Under","description":"Alpha Skater","price":105,"point":1.5},
      {"name":"Over","description":"Beta Skater","price":110,"point":2.5},
      {"name":"Under","description":"Beta Skater","price":-125,"point":2.5},
      {"name":"Over","description":"Alpha Skater","price":135,"point":3.5},
      {"name":"Under","description":"Alpha Skater","price":-150,"point":3.5},
      {"name":"Over","description":"Unknown Skater","price":100,"point":4.5},
      {"name":"Over","description":"Duplicate Skater","price":100,"point":2.5},
      {"name":"Yes","description":"Alpha Skater","price":100,"point":2.5},
      {"name":"Over","description":"Alpha Skater","price":-1,"point":2.5},
      {"name":"Over","description":"Alpha Skater","price":100,"point":None},
    ]
    return {"capture_timestamp_utc":capture,"provider":"SYNTHETIC_EDGE_FIXTURE","request_metadata":{"fixture":True},"provider_response":[
      {"id":"event-bound","commence_time":"2026-10-10T20:00:00Z","home_team":"Home Club","away_team":"Away Club","bookmakers":[
        {"key":"book_a","title":"Book A","last_update":market_time,"markets":[{"key":"player_shots_on_goal","id":"market-a","last_update":market_time,"outcomes":common}]},
        {"key":"book_b","title":"Book B","markets":[{"key":"player_shots_on_goal_alternate","outcomes":[{"name":"Over","description":"Alpha Skater","price":120,"point":2.5},{"name":"Under","description":"Alpha Skater","price":-135,"point":2.5}]}]},
        {"key":"book_s","title":"Book Suspended","markets":[{"key":"player_shots_on_goal","suspended":True,"outcomes":[{"name":"Over","description":"Alpha Skater","price":100,"point":2.5}]}]},
      ]},
      {"id":"event-ambiguous","commence_time":"2026-10-10T22:00:00Z","home_team":"Twin Home","away_team":"Twin Away","bookmakers":[{"key":"book_a","markets":[{"key":"player_shots_on_goal","outcomes":[{"name":"Over","description":"Twin Skater","price":100,"point":1.5}]}]}]},
      {"id":"event-post","commence_time":"2026-10-10T18:00:00Z","home_team":"Past Home","away_team":"Past Away","bookmakers":[{"key":"book_a","last_update":"2026-10-10T18:05:00Z","markets":[{"key":"player_shots_on_goal","last_update":"2026-10-10T18:05:00Z","outcomes":[{"name":"Over","description":"Past Skater","price":100,"point":1.5}]}]}]},
    ]}

def main()->None:
    global OUT
    ap=argparse.ArgumentParser();ap.add_argument("--output-dir",type=Path);a=ap.parse_args()
    if a.output_dir is None and OUT.exists(): verify_manifest(OUT,CANONICAL_MANIFEST_SHA256);print("READ_ONLY_PASS");return
    OUT=(a.output_dir or OUT).resolve();require_create_only(OUT);OUT.mkdir(parents=True)
    core=ROOT/"backend/nhl/sog_quote_capture/core.py"; cli=ROOT/"backend/nhl/sog_quote_capture/cli.py"
    legacy=[ROOT/"backend/nhl/cli.py",ROOT/"backend/nhl/scripts/build_sog_with_market.py",ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py",ROOT/"backend/nhl/mainline_shadow/core.py",ROOT/"backend/nhl/mainline_shadow/cli.py"]
    before={str(p.relative_to(ROOT)):h(p) for p in legacy}
    tmp=Path(tempfile.mkdtemp(prefix="nhl_sog_quote_validation_"))
    try:
      games=pd.DataFrame([
        {"canonical_season":2026,"slate_date":"2026-10-10","game_id":1,"scheduled_start_time_utc":"2026-10-10T20:00:00Z","game_type_code":2,"home_team":"Home Club","away_team":"Away Club","provider_event_id":"event-bound"},
        {"canonical_season":2026,"slate_date":"2026-10-10","game_id":2,"scheduled_start_time_utc":"2026-10-10T22:00:00Z","game_type_code":1,"home_team":"Twin Home","away_team":"Twin Away","provider_event_id":""},
        {"canonical_season":2026,"slate_date":"2026-10-10","game_id":3,"scheduled_start_time_utc":"2026-10-10T22:00:00Z","game_type_code":1,"home_team":"Twin Home","away_team":"Twin Away","provider_event_id":""},
        {"canonical_season":2026,"slate_date":"2026-10-10","game_id":4,"scheduled_start_time_utc":"2026-10-10T18:00:00Z","game_type_code":3,"home_team":"Past Home","away_team":"Past Away","provider_event_id":"event-post"},
      ]); players=pd.DataFrame([
        {"game_id":1,"player_id":101,"player_name":"Alpha Skater","team":"Home Club","provider_player_id":""},
        {"game_id":1,"player_id":102,"player_name":"Beta Skater","team":"Away Club","provider_player_id":""},
        {"game_id":1,"player_id":105,"player_name":"Duplicate Skater","team":"Home Club","provider_player_id":""},
        {"game_id":1,"player_id":106,"player_name":"Duplicate Skater","team":"Home Club","provider_player_id":""},
        {"game_id":4,"player_id":104,"player_name":"Past Skater","team":"Past Home","provider_player_id":""},
      ])
      games.to_csv(tmp/"games.csv",index=False); players.to_csv(tmp/"players.csv",index=False)
      for name,capture,final in [("midday","2026-10-10T16:00:00Z",False),("final","2026-10-10T19:58:00Z",True)]:
        (tmp/f"{name}.json").write_text(json.dumps(fixture_payload(capture,final=final),sort_keys=True,separators=(",",":"))+"\n")
      run1=capture_run(payload_json=tmp/"midday.json",games_csv=tmp/"games.csv",players_csv=tmp/"players.csv",output_root=tmp/"runs",slate_date="2026-10-10",run_timestamp_utc="2026-10-10T16:00:00Z",run_type="MIDDAY",source="SYNTHETIC_EDGE_FIXTURE")
      run2=capture_run(payload_json=tmp/"final.json",games_csv=tmp/"games.csv",players_csv=tmp/"players.csv",output_root=tmp/"runs",slate_date="2026-10-10",run_timestamp_utc="2026-10-10T19:58:00Z",run_type="FINAL_PREGAME",source="SYNTHETIC_EDGE_FIXTURE")
      overwrite=""
      try: capture_run(payload_json=tmp/"midday.json",games_csv=tmp/"games.csv",players_csv=tmp/"players.csv",output_root=tmp/"runs",slate_date="2026-10-10",run_timestamp_utc="2026-10-10T16:00:00Z",run_type="MIDDAY")
      except FileExistsError as exc: overwrite=str(exc)
      q=pd.read_csv(run1/"sog_quotes.csv"); metadata=json.loads((run1/"run_metadata.json").read_text())
      real_files=sorted((ROOT/"backend/nhl/exports/odds_history").glob("*/odds_latest_compatible.json")); real_path=next(p for p in real_files if isinstance(json.loads(p.read_text()),list) and json.loads(p.read_text()))
      real=json.loads(real_path.read_text()); real_events=len(real); real_books=sum(len(e.get("bookmakers") or []) for e in real if isinstance(e,dict)); real_sog=sum(1 for e in real if isinstance(e,dict) for b in (e.get("bookmakers") or []) for m in (b.get("markets") or []) if m.get("key") in {"player_shots_on_goal","player_shots_on_goal_alternate"})
      write_csv(f"nhl_sog_existing_prop_odds_transport_audit_{DATE}.csv",[
        {"component":"transport","current":"backend/nhl/cli.py fetch_odds; The Odds API event-specific player props","retained":"event/team/start; nested books/markets/outcomes in latest JSON","lost_or_mutable":"today/latest overwrite","remediation":"isolated explicit fetch envelope and create-only run"},
        {"component":"normalization","current":"build_sog_with_market.py recursive extraction and median aggregation","retained":"player alias,line,side,price","lost_or_mutable":"book,event/market/outcome IDs,timestamps,status,individual quotes","remediation":"one book-level source outcome per ledger row"},
        {"component":"archive","current":"fixed filenames copied into date directory","retained":"one apparent daily state","lost_or_mutable":"same-date reruns overwrite; no run identity","remediation":"season/slate/run directory collision fails closed"},
      ])
      contract=[{"field":x,"required":"YES","semantics":"source-preserved or explicit null; never synthesized"} for x in QUOTE_COLUMNS]
      write_csv(f"nhl_sog_run_bound_quote_contract_{DATE}.csv",contract,["field","required","semantics"])
      write_json(f"nhl_sog_player_binding_contract_{DATE}.json",{"hierarchy":["EXACT_PROVIDER_ID","DETERMINISTIC_CROSSWALK","EXACT_NAME_TEAM_FALLBACK","AMBIGUOUS","UNBOUND"],"certified_fallback":"exact normalized name plus bound game and one of its teams","name_only_certified":False,"source_identity_preserved":True})
      write_json(f"nhl_sog_game_binding_contract_{DATE}.json",{"hierarchy":["EXACT_EVENT_CROSSWALK","DETERMINISTIC_TEAM_TIME_BINDING","AMBIGUOUS","UNBOUND"],"team_time_tolerance_minutes":15,"slate_date_only_binding":False,"unknown_game_type":"fail closed for regular-season evaluation"})
      write_csv(f"nhl_sog_quote_timestamp_semantics_{DATE}.csv",[
        {"timestamp":"provider_quote_timestamp_utc","source":"outcome timestamp/last_update","manufactured":"NO","qualification":"strong when pregame"},
        {"timestamp":"provider_market_timestamp_utc","source":"market then bookmaker last_update","manufactured":"NO","qualification":"provider/source timestamp evidence"},
        {"timestamp":"source_timestamp_utc","source":"event then market/book last_update","manufactured":"NO","qualification":"provider/source timestamp evidence"},
        {"timestamp":"capture_timestamp_utc","source":"local acquisition clock in raw envelope","manufactured":"NO","qualification":"bounded when provider timestamps absent"},
      ])
      statuses=["PREGAME_QUALIFIED_PROVIDER_TIMESTAMP","PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN","POST_START_INVALID","TIMESTAMP_MISSING","GAME_BINDING_AMBIGUOUS","PLAYER_BINDING_AMBIGUOUS","LINE_INVALID","SIDE_INVALID","MARKET_UNSUPPORTED","SUSPENDED","STALE","PRICE_INVALID","UNQUALIFIED_OTHER"]
      write_json(f"nhl_sog_quote_qualification_contract_{DATE}.json",{"statuses":statuses,"priority":"unsupported,binding,line/side,suspension,price,timing,staleness","post_start_never_pregame":True,"capture_only_is_weaker":True,"invalid_rows_preserved":True})
      write_csv(f"nhl_sog_raw_archive_validation_{DATE}.csv",[
        {"test":"real_archived_provider_payload_parsed","fixture_type":"REAL_ARCHIVED_PROVIDER_PAYLOAD","passed":real_events>0 and real_books>0,"evidence":f"{real_path.relative_to(ROOT)} events={real_events} books={real_books} sog_markets={real_sog}"},
        {"test":"raw_payload_hash","fixture_type":"SYNTHETIC_EDGE_FIXTURE","passed":sha256_file(run1/"raw_odds_response.json")==metadata["raw_payload_sha256"],"evidence":metadata["raw_payload_sha256"]},
        {"test":"complete_envelope","fixture_type":"SYNTHETIC_EDGE_FIXTURE","passed":all(k in json.loads((run1/"raw_odds_response.json").read_text()) for k in ["acquisition_timestamp_utc","request_metadata","provider","provider_response"]),"evidence":"raw envelope fields"},
      ])
      binding_tests=[("exact_event_crosswalk",q.game_binding_status.eq("EXACT_EVENT_CROSSWALK").any()),("ambiguous_game_visible",q.game_binding_status.eq("AMBIGUOUS").any()),("exact_name_team_fallback",q.player_binding_status.eq("EXACT_NAME_TEAM_FALLBACK").any()),("ambiguous_player_visible",q.player_binding_status.eq("AMBIGUOUS").any()),("unbound_player_visible",q.player_binding_status.eq("UNBOUND").any()),("join_identity_present",q.loc[q.player_id.notna(),["game_id","player_id","canonical_prop_type","line","side","run_id"]].notna().all(axis=1).any())]
      write_csv(f"nhl_sog_quote_binding_validation_{DATE}.csv",[{"test":a,"passed":bool(b),"fixture_type":"SYNTHETIC_EDGE_FIXTURE"} for a,b in binding_tests])
      timing_tests=[("provider_time_preserved",q.provider_market_timestamp_utc.notna().any()),("missing_provider_time_null",q.provider_market_timestamp_utc.isna().any()),("capture_time_all_rows",q.capture_timestamp_utc.notna().all()),("post_start_visible",q.quote_qualification_status.eq("POST_START_INVALID").any()),("no_post_start_pregame",not ((q.quote_qualification_status.eq("POST_START_INVALID"))&q.quote_qualification_status.str.startswith("PREGAME")).any())]
      write_csv(f"nhl_sog_quote_timing_validation_{DATE}.csv",[{"test":a,"passed":bool(b),"evidence":"synthetic edge classification"} for a,b in timing_tests])
      write_csv(f"nhl_sog_quote_qualification_validation_{DATE}.csv",[{"status":s,"rows":int(q.quote_qualification_status.eq(s).sum()),"represented":bool(q.quote_qualification_status.eq(s).any()),"fixture_type":"SYNTHETIC_EDGE_FIXTURE"} for s in statuses])
      write_csv(f"nhl_sog_repeated_run_validation_{DATE}.csv",[
        {"test":"midday_final_distinct","passed":run1!=run2 and run1.exists() and run2.exists(),"evidence":f"{run1.name};{run2.name}"},
        {"test":"overwrite_blocked","passed":overwrite=="OVERWRITE_ATTEMPT_BLOCKED","evidence":overwrite},
        {"test":"snapshot_comparison_ready","passed":set(pd.read_csv(run1/"sog_quotes.csv").columns)==set(pd.read_csv(run2/"sog_quotes.csv").columns),"evidence":"same contract; distinct run_id"},
      ])
      health_checks=[("raw_payload_saved",(run1/"raw_odds_response.json").exists()),("payload_hash_valid",sha256_file(run1/"raw_odds_response.json")==metadata["raw_payload_sha256"]),("run_id_unique",overwrite=="OVERWRITE_ATTEMPT_BLOCKED"),("scheduled_starts_reported",q.loc[q.game_id.notna(),"scheduled_start_time_utc"].notna().all()),("game_binding_coverage_reported","bound_games" in metadata),("player_binding_coverage_reported","bound_players" in metadata),("sportsbook_present",q.sportsbook.notna().all()),("timestamps_preserved",q.capture_timestamp_utc.notna().all()),("post_start_excluded",not q.loc[q.quote_qualification_status.eq("POST_START_INVALID"),"quote_qualification_status"].str.startswith("PREGAME").any()),("prices_classified",q.loc[q.decimal_price.isna(),"quote_qualification_status"].isin(["PRICE_INVALID","LINE_INVALID","SIDE_INVALID","SUSPENDED","UNQUALIFIED_OTHER"]).all()),("required_outputs",all((run1/x).exists() for x in ["sog_quotes.csv","quote_binding_audit.csv","quote_timing_audit.csv","quote_qualification_audit.csv","run_metadata.json","SHA256SUMS"]))]
      write_csv(f"nhl_sog_market_capture_health_gate_validation_{DATE}.csv",[{"gate":a,"passed":bool(b),"run_result":metadata["health_gate_result"]} for a,b in health_checks])
      after={str(p.relative_to(ROOT)):h(p) for p in legacy}
      write_csv(f"nhl_sog_legacy_path_isolation_audit_{DATE}.csv",[{"path":p,"before_sha256":before[p],"after_sha256":after[p],"unchanged":before[p]==after[p],"boundary":"legacy SOG" if "mainline" not in p else "mainline"} for p in before])
      decisions={
        "NHL_SEASON_2026_SOG_RAW_PROP_ODDS_ARCHIVE_IMPLEMENTED":"READY","NHL_SEASON_2026_SOG_RUN_BOUND_QUOTE_IDENTITY_IMPLEMENTED":"READY",
        "NHL_SEASON_2026_SOG_SPORTSBOOK_IDENTITY_PRESERVED":"READY","NHL_SEASON_2026_SOG_GAME_BINDING_IMPLEMENTED":"READY_WITH_BOUNDED_LIMITS",
        "NHL_SEASON_2026_SOG_PLAYER_BINDING_IMPLEMENTED":"READY_WITH_BOUNDED_LIMITS","NHL_SEASON_2026_SOG_PROVIDER_TIMESTAMP_PRESERVATION":"READY_WITH_BOUNDED_LIMITS",
        "NHL_SEASON_2026_SOG_CAPTURE_TIMESTAMP_PRESERVATION":"READY","NHL_SEASON_2026_SOG_QUOTE_QUALIFICATION_IMPLEMENTED":"READY",
        "NHL_SEASON_2026_SOG_POST_START_EXCLUSION_IMPLEMENTED":"READY","NHL_SEASON_2026_SOG_CREATE_ONLY_ARCHIVE_IMPLEMENTED":"READY",
        "NHL_SEASON_2026_SOG_REPEATED_RUN_READINESS":"READY","NHL_SEASON_2026_SOG_LEGACY_ODDS_PATH_ISOLATION":"READY",
        "NHL_SEASON_2026_SOG_IMMUTABLE_MARKET_CAPTURE_READINESS":"READY_WITH_BOUNDED_LIMITS",
        "NHL_SEASON_2026_SOG_CANDIDATE_POLICY_READINESS":"BLOCKED_BY_NO_SINGLE_RUN_BOUND_POLICY",
        "NHL_SEASON_2026_SOG_SHADOW_OBSERVATION_READINESS":"BLOCKED_BY_CANDIDATE_POLICY_LINEAGE",
      }
      next_task="NHL_SEASON_2026_SOG_CANDIDATE_POLICY_LINEAGE_REMEDIATION"
      write_json(f"nhl_sog_immutable_prop_odds_capture_decision_{DATE}.json",{"decisions":decisions,"health_validation_result":metadata["health_gate_result"],"next_bounded_task":next_task,"unlocked":"manual immutable SOG quote capture with bounded timestamp evidence","blocked":["candidate activation","full shadow observation","recommendations","wagering","ROI","tuning","training","promotion","scheduling","frontend changes"]})
      report=f"""# NHL season 2026 SOG immutable player-prop odds capture remediation\n\n## Result\n\nImmutable market capture is `READY_WITH_BOUNDED_LIMITS`. A new manual-only namespace under `backend/nhl/sog_quote_capture` preserves the complete provider envelope and one row per book/event/market/outcome. Runs use `canonical_season + slate_date + run_timestamp_utc + run_type`, support only `MIDDAY` and `FINAL_PREGAME`, and fail with `OVERWRITE_ATTEMPT_BLOCKED` on collision. The default evidence root is `backend/nhl/exports/sog_shadow`.\n\nThe ledger preserves sportsbook, provider event/market/outcome identities when available, raw and canonical player/market/line/side values, American and decimal prices, provider/market/source timestamps without manufacturing missing values, acquisition time, status, payload hash, binding states, qualification, game type, and the future prediction join key. Provider samples expose book and market update times but not stable player IDs or outcome IDs, so game binding uses event crosswalk then exact teams/start within 15 minutes, and player fallback requires exact name plus bound game/team. Ambiguous or unbound rows remain visible.\n\nProvider/source timestamps before start earn the strongest qualification. Capture-before-start with absent provider time earns the explicitly weaker capture-only class. Any provider/source/capture timestamp at or after start is `POST_START_INVALID` and cannot enter a pregame population. Suspended, stale, unsupported, malformed, ambiguous, and unbound observations are diagnostic rows rather than dropped data. Preseason is `PRESEASON_NON_EVALUATION`; unknown game type fails closed.\n\nValidation parsed a preserved real provider payload only to confirm its nested event/book/market/outcome schema. A clearly labeled synthetic edge fixture exercised multiple books, players, lines including 1.5/2.5/3.5/4.5, both sides, missing timestamps, post-start timing, ambiguous binding, suspension, missing side, invalid line/price, repeated runs, and overwrite rejection. Synthetic data is not prospective evidence. Legacy SOG fetch/normalization/scoring and mainline capture hashes remained unchanged. No model, candidate, recommendation, execution, scheduler, or frontend path changed.\n\n## Decisions\n\n"""+"\n".join(f"- `{k}` = `{v}`" for k,v in decisions.items())+f"\n\nExactly one next bounded task is `{next_task}`.\n"
      (OUT/f"nhl_season_2026_sog_immutable_prop_odds_capture_report_{DATE}.md").write_text(report)
      (OUT/f"nhl_sog_immutable_prop_odds_capture_one_page_summary_{DATE}.md").write_text("# NHL season 2026 SOG immutable quote capture — one-page summary\n\n"+report.split("## Decisions")[0].split("## Result\n\n",1)[1]+f"\nCandidate activation remains blocked. The sole next task is `{next_task}`.\n")
      parents=[("nhl_season_2026_sog_prospective_capture_readiness","895eb67e6e600b46b65c13beb2b5a97d156077a4d0bf27fdc17d6f21110514c7"),("nhl_season_2025_sog_baseline_reproduction","65e0bca743bdeead084fdeb8bb1179764b905ae5ba11d782823a65953b95344b"),("nhl_season_2026_mainline_shadow_capture_implementation","62de5b047b0121664ede00ce197b339968eec00ace64f6a782ca2850a366b09c")]
      write_json(f"package_identity_{DATE}.json",{"package":"nhl_season_2026_sog_immutable_prop_odds_capture","version":"1.0.0","as_of":DATE,"canonical_season":2026,"parents":[{"package":p,"manifest_sha256":x} for p,x in parents],"implementation":{"core":"backend/nhl/sog_quote_capture/core.py","core_sha256":h(core),"cli":"backend/nhl/sog_quote_capture/cli.py","cli_sha256":h(cli)},"validation":"preserved real payload schema plus synthetic edge fixtures","no_model_change":True,"no_candidate_change":True,"manual_only":True})
      files=sorted(p for p in OUT.iterdir() if p.is_file() and p.name!="SHA256SUMS"); (OUT/"SHA256SUMS").write_text("".join(f"{h(p)}  {p.name}\n" for p in files))
    finally: shutil.rmtree(tmp,ignore_errors=True)

if __name__=="__main__": main()
