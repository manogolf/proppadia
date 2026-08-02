#!/usr/bin/env python3
"""Schedule-relative unattended TB Under 1.5 cohort lifecycle V2."""
from __future__ import annotations
import argparse,csv,json,os,shutil,tempfile
from datetime import date,datetime,time,timedelta,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import requests
from backend.mlb.scripts.cleanroom_v1 import fixed_cohort_lifecycle as v1

ROOT=v1.ROOT; EXPORT_ROOT=v1.EXPORT_ROOT; COHORT_ROOT=EXPORT_ROOT/'schedule_cohorts'; PT=ZoneInfo('America/Los_Angeles')
CONTRACT='MLB_CLEANROOM_BOL_TB15_SCHEDULE_RELATIVE_COHORT_V2';BLOCK='MLB_CLEANROOM_H1_SCHEDULE_RELATIVE_BLOCK_V2'
WRAPPERS=(time(5,30),time(9,30),time(11,0),time(13,0),time(16,30));MIN_GAME=30;MAX_GAME=240
LABELS={'FIXED_COHORT_BASELINE':'SCHEDULE_COHORT_BASELINE','FIXED_COHORT_REJECTED_TOP_ORDER':'SCHEDULE_COHORT_REJECTED_TOP_ORDER','FIXED_COHORT_RETAINED_LOWER_ORDER':'SCHEDULE_COHORT_RETAINED_LOWER_ORDER'}
FILES={'fixed_cohort_baseline.csv':'schedule_cohort_baseline.csv','fixed_cohort_rejected_top_order.csv':'schedule_cohort_rejected_top_order.csv','fixed_cohort_retained_lower_order.csv':'schedule_cohort_retained_lower_order.csv','fixed_cohort_exclusions.csv':'schedule_cohort_exclusions.csv','fixed_cohort_manifest.json':'schedule_cohort_manifest.json','fixed_cohort_capture_report.md':'schedule_cohort_capture_report.md'}
def schedule_games(payload,slate):return [g for d in payload.get('dates',[]) for g in d.get('games',[]) if g.get('officialDate')==slate]
def select_wrapper(earliest_pt:datetime):
 candidates=[]
 for t in WRAPPERS:
  at=datetime.combine(earliest_pt.date(),t,tzinfo=PT);minutes=(earliest_pt-at).total_seconds()/60
  if 45<=minutes<=240:candidates.append(at)
 return max(candidates) if candidates else None
def matches(now,target):return target<=now<=target+timedelta(minutes=20)
def current_date_guard(slate,now):
 if date.fromisoformat(slate)!=now.date():raise RuntimeError(f'CAPTURE_DATE_MISMATCH requested_date={slate} current_local_date={now.date()}')
def fetch_schedule(slate):
 r=requests.get('https://statsapi.mlb.com/api/v1/schedule',params={'sportId':1,'date':slate,'hydrate':'team'},timeout=45);r.raise_for_status();return r.json()
def csv_rows(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def transform_package(temp_root:Path,final:Path,target,emergency):
 root=temp_root/final.name;m=json.loads((root/'fixed_cohort_manifest.json').read_text())
 for old,new in list(FILES.items())[:3]:
  rr=csv_rows(root/old)
  for r in rr:r['population_membership']=LABELS[r['population_membership']]
  (root/new).write_bytes(v1.csv_data(v1.FIELDS,rr));(root/old).unlink()
 for old,new in list(FILES.items())[3:]:os.replace(root/old,root/new)
 mp=root/'schedule_cohort_manifest.json';execution_mode='EMERGENCY_CURRENT_VALID_WINDOW' if emergency else 'SELECTED_SCHEDULE_RELATIVE_WRAPPER';m.update({'contract_name':CONTRACT,'contract_version':2,'research_block':BLOCK,'selected_wrapper_time_pacific':target.strftime('%H:%M') if target else None,'execution_window_decision':execution_mode,'execution_mode':execution_mode,'attempt_designation':'POST_HARDENING_SCHEDULE_RELATIVE_ATTEMPT_001','v1_august1_included':False})
 m['status']='EMPTY_ELIGIBLE_COHORT' if not m['baseline_identities'] else 'SCHEDULE_COHORT_FROZEN';m['output_hashes']={name:v1.sha(root/name) for name in FILES.values() if name.endswith('.csv')};mp.write_text(json.dumps(m,indent=2)+'\n');(root/'schedule_cohort_capture_report.md').write_text(f"# Schedule-relative cohort — {final.name}\n\nStatus: `{m['status']}`\nSelected wrapper: `{m['selected_wrapper_time_pacific']}` PT\nBaseline: {m['baseline_identities']}; rejected: {m['top_order_rejected_identities']}; retained: {m['lower_order_retained_identities']}.\n")
 os.replace(root,final);temp_root.rmdir();return m
def capture(slate,emergency=False,at=None):
 final=COHORT_ROOT/slate;mp=final/'schedule_cohort_manifest.json'
 if mp.exists():return {**json.loads(mp.read_text()),'status':'SCHEDULE_COHORT_ALREADY_FROZEN'}
 now=(at or datetime.now(timezone.utc)).astimezone(PT);current_date_guard(slate,now);payload=fetch_schedule(slate);games=schedule_games(payload,slate)
 if not games:raise RuntimeError('NO_OFFICIAL_GAMES')
 earliest=min(v1.parse(g['gameDate']).astimezone(PT) for g in games);target=select_wrapper(earliest)
 if not target and not emergency:return {'status':'NO_VALID_SCHEDULE_RELATIVE_WRAPPER','earliest_first_pitch_pt':earliest.isoformat()}
 if not emergency and not matches(now,target):return {'status':'NOT_SELECTED_SCHEDULE_RELATIVE_WRAPPER','selected_wrapper_time_pacific':target.strftime('%H:%M'),'current_timestamp_pt':now.isoformat()}
 if emergency:
  future=[(v1.parse(g['gameDate'])-now.astimezone(timezone.utc)).total_seconds()/60 for g in games]
  if not any(MIN_GAME<=x<=MAX_GAME for x in future) or any(0<x<MIN_GAME for x in future):raise RuntimeError('EMERGENCY_CURRENT_VALID_WINDOW_CLOSED')
 temp=Path(tempfile.mkdtemp(dir=COHORT_ROOT.parent,prefix='.schedule_v2_'));old_root,old_min,old_max,old_guard=v1.COHORT_ROOT,v1.MIN_GAME,v1.MAX_GAME,v1.guard
 try:
  v1.COHORT_ROOT=temp;v1.MIN_GAME=MIN_GAME;v1.MAX_GAME=MAX_GAME;v1.guard=lambda slate,at=None:now
  v1.capture(slate,now)
 finally:v1.COHORT_ROOT, v1.MIN_GAME,v1.MAX_GAME,v1.guard=old_root,old_min,old_max,old_guard
 COHORT_ROOT.mkdir(parents=True,exist_ok=True);return transform_package(temp,final,target,emergency)
def aliases(root):
 for sched,fixed,label in [('schedule_cohort_baseline.csv','fixed_cohort_baseline.csv','FIXED_COHORT_BASELINE'),('schedule_cohort_rejected_top_order.csv','fixed_cohort_rejected_top_order.csv','FIXED_COHORT_REJECTED_TOP_ORDER'),('schedule_cohort_retained_lower_order.csv','fixed_cohort_retained_lower_order.csv','FIXED_COHORT_RETAINED_LOWER_ORDER')]:
  rr=csv_rows(root/sched)
  for r in rr:r['population_membership']=label
  (root/fixed).write_bytes(v1.csv_data(v1.FIELDS,rr))
 shutil.copy2(root/'schedule_cohort_manifest.json',root/'fixed_cohort_manifest.json')
def closeout(slate):
 root=COHORT_ROOT/slate
 if not (root/'schedule_cohort_manifest.json').exists():raise RuntimeError('SCHEDULE_COHORT_FREEZE_REQUIRED')
 aliases(root);old=v1.COHORT_ROOT
 try:v1.COHORT_ROOT=COHORT_ROOT;return v1.closeout(slate)
 finally:
  v1.COHORT_ROOT=old
  for p in ('fixed_cohort_baseline.csv','fixed_cohort_rejected_top_order.csv','fixed_cohort_retained_lower_order.csv','fixed_cohort_manifest.json'):(root/p).unlink(missing_ok=True)
def status(slate):
 root=COHORT_ROOT/slate;m=json.loads((root/'schedule_cohort_manifest.json').read_text()) if (root/'schedule_cohort_manifest.json').exists() else {};c=json.loads((root/'fixed_cohort_closeout_manifest.json').read_text()) if (root/'fixed_cohort_closeout_manifest.json').exists() else {};base=next((x for x in c.get('summaries',[]) if x['population']=='FIXED_COHORT_BASELINE'),{})
 return {'attempt_status':'ATTEMPTED' if m else 'NOT_ATTEMPTED','freeze_status':m.get('status','NOT_ATTEMPTED'),'selected_wrapper_time_pacific':m.get('selected_wrapper_time_pacific'),'capture_timestamp':m.get('capture_timestamp_utc'),'games_inside_window':m.get('games_inside_fixed_window',0),'baseline_rows':m.get('baseline_identities',0),'rejected_top_order_rows':m.get('top_order_rejected_identities',0),'retained_lower_order_rows':m.get('lower_order_retained_identities',0),'exclusion_counts':m.get('exclusions_by_reason',{}),'closeout_revision':c.get('revision',0),'wins':base.get('wins',0),'losses':base.get('losses',0),'no_action':base.get('no_action',0),'pending':base.get('pending',0),'technical_unresolved':base.get('technical_unresolved',0),'net':base.get('net_dollars'),'roi':base.get('roi'),'terminal_date_status':c.get('status') or m.get('status','NOT_ATTEMPTED')}
def block_status():
 dates=[]
 for root in sorted(COHORT_ROOT.glob('????-??-??')):
  m=root/'schedule_cohort_manifest.json'
  if not m.exists():continue
  c=json.loads((root/'fixed_cohort_closeout_manifest.json').read_text()) if (root/'fixed_cohort_closeout_manifest.json').exists() else {};base=next((x for x in c.get('summaries',[]) if x['population']=='FIXED_COHORT_BASELINE'),{});rej=next((x for x in c.get('summaries',[]) if x['population']=='FIXED_COHORT_REJECTED_TOP_ORDER'),{});dates.append({'date':root.name,'status':c.get('status',json.loads(m.read_text())['status']),'baseline_actionable':base.get('actionable_wagers',0),'rejected_actionable':rej.get('actionable_wagers',0)})
 attempts=len(dates);b=sum(x['baseline_actionable'] for x in dates);r=sum(x['rejected_actionable'] for x in dates);complete=(b>=100 and r>=30) or attempts>=5
 return {'contract':BLOCK,'attempted_dates':attempts,'actionable_baseline_wagers':b,'actionable_rejected_wagers':r,'stop_rule_met':complete,'collection_state':'BLOCK_COMPLETE' if complete else 'COLLECTING','dates':dates}
def main():
 p=argparse.ArgumentParser();p.add_argument('--date');p.add_argument('--mode',required=True,choices=('capture','closeout','status','block-status'));p.add_argument('--execute-current-valid-window',action='store_true');a=p.parse_args()
 if a.mode!='block-status' and not a.date:p.error('--date required')
 result=capture(a.date,a.execute_current_valid_window) if a.mode=='capture' else closeout(a.date) if a.mode=='closeout' else status(a.date) if a.mode=='status' else block_status();print(json.dumps(result,indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
