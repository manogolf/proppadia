#!/usr/bin/env python3
"""Package the preserved July 30 neutral population without changing membership."""
import csv, hashlib, io, json, shutil
from pathlib import Path
from backend.mlb.scripts.cleanroom_v1.closeout_cleanroom_bol_tb15 import EXPORT_ROOT, baseline

DATE="2026-07-30"
def content(fields, rows):
    s=io.StringIO(); w=csv.DictWriter(s,fieldnames=fields,lineterminator="\n"); w.writeheader(); w.writerows(rows); return s.getvalue().encode()
def main():
    root=EXPORT_ROOT/DATE; source=root/'under_hypotheses'; out=root/'neutral_closeout'
    population=list(csv.DictReader((source/'baseline_all_under.csv').open()))
    settled=list(csv.DictReader((source/'under_closeout_rows.csv').open()))
    key=lambda r:(r['slate_date'],r['game_pk'],r['player_mlb_id'],r['prop_type'],r['line'])
    if {key(r) for r in population}!={key(r) for r in settled}: raise SystemExit('immutable membership mismatch')
    rows=[]
    for r in sorted(settled,key=key):
        under=r['under_outcome']; over='OVER_WIN' if under=='UNDER_LOSS' else 'OVER_LOSS' if under=='UNDER_WIN' else 'NO_ACTION'
        rows.append({'slate_date':DATE,'game_pk':r['game_pk'],'player_mlb_id':r['player_mlb_id'],'player':r['player'],'prop_type':'Total Bases','line':'1.5','governing_run_tag':r['governing_run_tag'],'market_timestamp_utc':r['market_timestamp_utc'],'final_pregame_over_odds':r['final_over_odds'],'final_pregame_under_odds':r['final_under_odds'],'plate_appearances':r['plate_appearances'],'total_bases':r['total_bases'],'outcome':over,'settlement_status':r['settlement_status'],'outcome_source':r['outcome_source'],'source_sha256':r['outcome_sha256']})
    fields=list(rows[0]); data=content(fields,rows); digest=hashlib.sha256(data).hexdigest()
    manifest_path=out/'neutral_closeout_manifest.json'
    if manifest_path.exists():
        prior=json.loads(manifest_path.read_text())
        if prior['content_sha256']!=digest: raise SystemExit('historical neutral package collision')
        print(json.dumps({'status':'ALREADY_PACKAGED_IDENTICAL','revision':prior['revision'],'content_sha256':digest},indent=2)); return 0
    out.mkdir(parents=True,exist_ok=False)
    csv_path=out/f'bol_tb15_neutral_closeout_{DATE}.csv'; csv_path.write_bytes(data)
    over=baseline(rows,'Over')
    under_rows=[{**r,'outcome':'UNDER_WIN' if r['outcome']=='OVER_LOSS' else 'UNDER_LOSS' if r['outcome']=='OVER_WIN' else r['outcome']} for r in rows]
    under=baseline(under_rows,'Under')
    source_rows=sorted({(r['outcome_source'],r['source_sha256']) for r in rows})
    source_manifest=out/'outcome_source_manifest.csv'
    source_manifest.write_bytes(content(['raw_payload_path','sha256'],[{'raw_payload_path':p,'sha256':s} for p,s in source_rows]))
    report=out/f'bol_tb15_neutral_closeout_{DATE}.md'
    report.write_text(f"# July 30 neutral TB 1.5 closeout\n\n129 frozen; 126 actionable; 3 officially supported NO_ACTION; 0 pending; 0 unresolved.\n\nOver: {over['wins']}-{over['losses']}, net ${over['net_dollars']:.6f}, ROI {over['roi']:.8%}.\n\nUnder: {under['wins']}-{under['losses']}, net ${under['net_dollars']:.6f}, ROI {under['roi']:.8%}.\n")
    pop_manifest=source/'under_hypothesis_manifest.json'
    manifest={'slate_date':DATE,'revision':1,'parent_revision':None,'status':'FINAL','membership_unchanged':True,'frozen_identities':129,'actionable':126,'no_action':3,'pending':0,'technical_unresolved':0,'population_path':str((source/'baseline_all_under.csv').relative_to(EXPORT_ROOT.parents[4])),'population_manifest_path':str(pop_manifest.relative_to(EXPORT_ROOT.parents[4])),'population_manifest_sha256':hashlib.sha256(pop_manifest.read_bytes()).hexdigest(),'outcome_source_manifest_sha256':hashlib.sha256(source_manifest.read_bytes()).hexdigest(),'closeout_csv_sha256':digest,'closeout_report_sha256':hashlib.sha256(report.read_bytes()).hexdigest(),'content_sha256':digest,'over_baseline':over,'under_baseline':under}
    manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
    rev=out/'revisions/revision_001'; rev.mkdir(parents=True)
    for p in (csv_path,report,source_manifest,manifest_path): shutil.copy2(p,rev/p.name)
    print(json.dumps({'status':'PACKAGED','revision':1,'content_sha256':digest},indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
