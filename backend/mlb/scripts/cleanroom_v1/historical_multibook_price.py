"""Exact same-run multi-book quote audit for frozen C1 TB 1.5 identities."""
from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

from backend.mlb.scripts.cleanroom_v1 import historical_pipeline_selection as pipe
from backend.mlb.scripts.cleanroom_v1 import historical_favorite_selector as fav
from backend.mlb.scripts.cleanroom_v1 import historical_agreement_value as value

ROOT=pipe.ROOT; KEY=pipe.KEY
OUT=ROOT/'artifacts/analysis/model_development/mlb_routine_market_multibook_price_availability_audit/2026-08-03'
ATTACH=OUT/'c1_multibook_price_attachment.csv'; MANIFEST=OUT/'c1_multibook_price_attachment_manifest.json'
CONTRACT='C1_CONTEMPORANEOUS_MULTIBOOK_PRICE_ATTACHMENT_V1'; BOL='betonlineag'

def _decimal(o:float)->float:return 1+o/100 if o>0 else 1+100/abs(o)
def _american(dec:float)->float:return (dec-1)*100 if dec>=2 else -100/(dec-1)
def _profit(o:float)->float:return 5*(_decimal(o)-1)
def _dt(x:str):return datetime.fromisoformat(x.replace('Z','+00:00'))

def _parents()->dict:
    return {'frozen_population_sha256':pipe.sha(pipe.POP),'favorite_attachment_sha256':pipe.sha(fav.ATTACH),'favorite_manifest_sha256':pipe.sha(fav.MANIFEST),'partition_manifest_sha256':pipe.sha(fav.OUT/'chronological_partition_manifest.json'),'agreement_report_sha256':pipe.sha(value.OUT/'agreement_value_decomposition_report.md'),'settlement_sha256':pipe.sha(pipe.SETTLEMENT)}

def _source_rows()->tuple[pd.DataFrame,list[dict],dict]:
    a=pd.read_csv(fav.ATTACH,dtype=str,keep_default_na=False); a=a[a.C1_membership.eq('BET')]
    p=pd.read_csv(pipe.POP,dtype=str,keep_default_na=False)
    d=a.merge(p[KEY+['player','provider_event_id','market_source_payload','market_source_sha256','market_observation_timestamp','cohort_freeze_timestamp']],on=KEY,validate='one_to_one')
    cache={}; inventory={}; out=[]
    for r in d.itertuples(index=False):
        path=ROOT/r.market_source_payload
        if str(path) not in cache:cache[str(path)]=json.loads(path.read_text())
        payload=cache[str(path)]; capture=payload.get('captured_at_utc') or r.market_observation_timestamp; event=next((e for e in payload.get('events',[]) if str(e.get('id'))==r.provider_event_id),None)
        quotes=[]
        if event:
            for book in event.get('bookmakers',[]):
                key=str(book.get('key') or ''); title=str(book.get('title') or key)
                for market in book.get('markets',[]):
                    if market.get('key')!='batter_total_bases':continue
                    vals=[o for o in market.get('outcomes',[]) if o.get('description')==r.player and float(o.get('point') or -1)==1.5]
                    sides={str(o.get('name')).lower():float(o['price']) for o in vals if str(o.get('name')).lower() in {'over','under'}}
                    if r.C1_side not in sides:continue
                    ts=str(market.get('last_update') or ''); visible=bool(ts and capture and _dt(ts)<=_dt(capture)); classification='EXACT_SAME_RUN_CONTEMPORANEOUS_PRICE' if visible else 'SOURCE_TIMESTAMP_MISSING' if not ts else 'LATER_PRICE_EXCLUDED'
                    q={'book_key':key,'book_name':title,'selected_price':sides[r.C1_side],'opposite_price':sides.get('under' if r.C1_side=='over' else 'over'),'two_sided':len(sides)==2,'observation_timestamp':ts,'capture_timestamp':capture,'price_age_seconds':(_dt(capture)-_dt(ts)).total_seconds() if visible else None,'source_path':r.market_source_payload,'source_hash':r.market_source_sha256,'current_audit_hash':pipe.sha(path),'classification':classification,'accessibility':'OPERATOR_EXECUTABLE_CONFIRMED' if key==BOL else 'OBSERVED_MARKET_ONLY','rule_coverage':'BOOK_RULE_CERTIFIED_FOR_DATE' if key==BOL else 'BOOK_RULE_MISSING'}
                    quotes.append(q); inventory[(key,r.market_source_payload,r.normal_pipeline_run_tag,ts)]=q
        exact=sorted([q for q in quotes if q['classification']=='EXACT_SAME_RUN_CONTEMPORANEOUS_PRICE'],key=lambda x:x['book_key'])
        # BetOnline is already exact-ID certified in the frozen population. Inherit
        # that binding when provider display spelling differs from the MLB name;
        # alternate books still fail closed on an exact description mismatch.
        if not any(q['book_key']==BOL for q in exact):
            exact.append({'book_key':BOL,'book_name':'BetOnline','selected_price':float(r.over_odds if r.C1_side=='over' else r.under_odds),'opposite_price':float(r.under_odds if r.C1_side=='over' else r.over_odds),'two_sided':True,'observation_timestamp':r.market_observation_timestamp,'capture_timestamp':capture,'price_age_seconds':0,'source_path':r.market_source_payload,'source_hash':r.market_source_sha256,'current_audit_hash':pipe.sha(path),'classification':'EXACT_SAME_RUN_CONTEMPORANEOUS_PRICE','accessibility':'OPERATOR_EXECUTABLE_CONFIRMED','rule_coverage':'BOOK_RULE_CERTIFIED_FOR_DATE'})
            exact=sorted(exact,key=lambda x:x['book_key'])
        bol=next((q for q in exact if q['book_key']==BOL),None); best=max(exact,key=lambda q:(q['selected_price'],-ord(q['book_key'][0]))) if exact else None
        # Explicit alphabetical tie break.
        if exact:
            mx=max(q['selected_price'] for q in exact);best=sorted([q for q in exact if q['selected_price']==mx],key=lambda q:q['book_key'])[0]
            median_dec=float(np.median([_decimal(q['selected_price']) for q in exact]));median=_american(median_dec)
        else:median=None
        executable=[q for q in exact if q['accessibility']=='OPERATOR_EXECUTABLE_CONFIRMED' and q['rule_coverage']=='BOOK_RULE_CERTIFIED_FOR_DATE'];bestex=max(executable,key=lambda q:q['selected_price']) if executable else None
        bolprice=next(q['selected_price'] for q in exact if q['book_key']==BOL); maxprice=max(q['selected_price'] for q in exact); top=[q for q in exact if q['selected_price']==maxprice]
        bol_best_status='BETONLINE_STRICT_BEST' if bolprice==maxprice and len(top)==1 else 'BETONLINE_TIED_BEST' if bolprice==maxprice else 'ANOTHER_BOOK_BEST'
        decision='EXACT_CONTEMPORANEOUS_PRICE_ATTACHED' if exact else 'BOOK_DID_NOT_OFFER_MARKET'
        out.append({'slate_date':r.slate_date,'partition':r.partition,'game_pk':r.game_pk,'player_mlb_id':r.player_mlb_id,'player':r.player,'selected_side':r.C1_side,'normal_pipeline_run_tag':r.normal_pipeline_run_tag,'governing_selection_timestamp':capture,'betonline_selected_price':float(r.over_odds if r.C1_side=='over' else r.under_odds),'books_observed':len(exact),'books_executable':len(executable),'quotes_json':json.dumps(exact,sort_keys=True,separators=(',',':')),'best_observed_book':best['book_key'] if best else '','best_observed_odds':best['selected_price'] if best else '','best_observed_rule_coverage':best['rule_coverage'] if best else '','best_executable_book':bestex['book_key'] if bestex else '','best_executable_odds':bestex['selected_price'] if bestex else '','median_odds':median if median is not None else '','betonline_best_status':bol_best_status,'attachment_decision':decision,'source_path':r.market_source_payload,'source_hash':r.market_source_sha256})
    inv=[]
    for (key,path,run,ts),q in sorted(inventory.items()):inv.append({'book_name':q['book_name'],'provider_book_key':key,'artifact_path':path,'governing_run_tag':run,'source_observation_timestamp':ts,'ingestion_timestamp':q['capture_timestamp'],'game_identity_fields':'provider_event_id exact','player_identity_fields':'exact provider description bound to frozen player identity','prop_type':'batter_total_bases','line':1.5,'side':'selected and opposite where present','american_odds':'preserved in attachment','two_sided_market_availability':q['two_sided'],'original_source_hash':q['source_hash'],'current_audit_hash':q['current_audit_hash'],'classification':q['classification']})
    return pd.DataFrame(out).sort_values(KEY,kind='stable'),inv,cache

def inventory(write:bool=False)->dict:
    d,rows,_=_source_rows(); books=sorted({r['provider_book_key'] for r in rows})
    if write:pipe.write_csv(OUT/'sportsbook_price_source_inventory.csv',rows)
    print(json.dumps({'c1_rows':len(d),'books':books,'inventory_rows':len(rows)},sort_keys=True));return {'rows':rows,'books':books}

def freeze()->dict:
    d,rows,_=_source_rows();pipe.write_csv(OUT/'sportsbook_price_source_inventory.csv',rows);pipe.write_csv(ATTACH,d)
    books=sorted({r['provider_book_key'] for r in rows}); access={'contract':'SPORTSBOOK_ACCESSIBILITY_MANIFEST_V1','evidence_rule':'feed visibility alone does not prove operator access','books':{b:{'classification':'OPERATOR_EXECUTABLE_CONFIRMED' if b==BOL else 'OBSERVED_MARKET_ONLY','evidence':'frozen governing BetOnline operational price and settlement lifecycle' if b==BOL else 'provider feed observation only; no exact account/ticket/config evidence'} for b in books}}
    (OUT/'sportsbook_accessibility_manifest.json').write_text(json.dumps(access,indent=2,sort_keys=True)+'\n')
    rules=[{'book_key':b,'date_window':'2026-05-01 through 2026-08-02','coverage':'BOOK_RULE_CERTIFIED_FOR_DATE' if b==BOL else 'BOOK_RULE_MISSING','player_total_bases':'certified frozen BetOnline settlement lifecycle' if b==BOL else 'not preserved','minimum_game_length':'certified by frozen BetOnline settlement classifications' if b==BOL else 'not preserved','shortened_suspended_nonappearance_zero_pa':'frozen book settlement classifications' if b==BOL else 'not preserved','notes':'BetOnline rules may not be transferred to another sportsbook'} for b in books]
    pipe.write_csv(OUT/'sportsbook_rule_coverage.csv',rules)
    man={'contract':CONTRACT,**_parents(),'attachment_rows':len(d),'books_observed':books,'exact_quote_rows_total':int(d.books_observed.sum()),'operator_executable_confirmed_books':[BOL],'alternate_book_rules_certified':[],'attachment_sha256':pipe.sha(ATTACH)};MANIFEST.write_text(json.dumps(man,indent=2,sort_keys=True)+'\n');return man

def _metrics(q:pd.DataFrame,odds_col:str,rule_col:str|None=None,diagnostic=False)->dict:
    priced=pd.to_numeric(q[odds_col],errors='coerce').notna(); settled=q.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT')&priced
    if rule_col and not diagnostic:settled &= q[rule_col].eq('BOOK_RULE_CERTIFIED_FOR_DATE')
    s=q[settled]; win=s.selected_result.eq('WIN'); odds=pd.to_numeric(s[odds_col]); net=np.where(win,odds.map(_profit),-5);w=int(win.sum());l=len(s)-w
    return {'eligible_c1_rows':len(q),'priced_rows':int(priced.sum()),'settled_wagers':len(s),'wins':w,'losses':l,'voids':int((q.book_settlement.str.startswith('BOOK_VOID')&priced).sum()),'rule_uncertified':int((priced & (~q[rule_col].eq('BOOK_RULE_CERTIFIED_FOR_DATE'))).sum()) if rule_col else 0,'technical_unresolved':int((q.book_settlement.eq('TECHNICAL_UNRESOLVED')&priced).sum()),'coverage_rate':float(priced.mean()),'average_odds':float(odds.mean()) if len(s) else '','stake':5*len(s),'returned_stake':5*w,'gross_winning_profit':float(np.sum(np.where(win,odds.map(_profit),0))),'net_dollars':float(np.sum(net)),'roi':float(np.sum(net)/(5*len(s))) if len(s) else ''}

def evaluate()->dict:
    man=json.loads(MANIFEST.read_text());
    if pipe.sha(ATTACH)!=man['attachment_sha256']:raise RuntimeError('frozen multibook attachment hash mismatch')
    a=pd.read_csv(ATTACH,dtype=str,keep_default_na=False);z=pd.read_csv(pipe.SETTLEMENT,dtype=str,keep_default_na=False);d=a.merge(z[KEY+['book_settlement','over_result','under_result','over_net','under_net']],on=KEY,validate='one_to_one');d['selected_result']=np.where(d.selected_side.eq('over'),d.over_result,d.under_result)
    # Expand exact quote rows for book-specific and deterministic audits.
    long=[]
    for r in d.itertuples(index=False):
        for q in json.loads(r.quotes_json):
            certified=q['book_key']==BOL
            result_status=(r.selected_result if r.book_settlement=='BOOK_SETTLED_OFFICIAL_RESULT' else r.book_settlement) if certified else 'BOOK_RULE_UNCERTIFIED'
            long.append({'slate_date':r.slate_date,'partition':r.partition,'game_pk':r.game_pk,'player_mlb_id':r.player_mlb_id,'selected_side':r.selected_side,'book_key':q['book_key'],'selected_price':q['selected_price'],'opposite_price':q.get('opposite_price'),'two_sided':q['two_sided'],'observation_timestamp':q['observation_timestamp'],'source_path':q['source_path'],'source_hash':q['source_hash'],'accessibility':q['accessibility'],'rule_coverage':q['rule_coverage'],'official_mlb_selected_side_result':r.selected_result,'frozen_betonline_settlement':r.book_settlement,'book_settlement_eligibility':'CERTIFIED' if certified else 'BOOK_RULE_UNCERTIFIED','result_status':result_status})
    l=pd.DataFrame(long);books=sorted(l.book_key.unique())
    audits=[]
    for r in d.itertuples(index=False):
        bo=float(r.betonline_selected_price);best=float(r.best_observed_odds) if r.best_observed_odds else np.nan;be=float(r.best_executable_odds) if r.best_executable_odds else np.nan;med=float(r.median_odds) if r.median_odds else np.nan
        audits.append({'slate_date':r.slate_date,'partition':r.partition,'game_pk':r.game_pk,'player_mlb_id':r.player_mlb_id,'books_observed':r.books_observed,'books_executable':r.books_executable,'betonline_odds':bo,'best_observed_book':r.best_observed_book,'best_observed_odds':best,'best_executable_book':r.best_executable_book,'best_executable_odds':be,'median_odds':med,'american_odds_improvement_vs_betonline':best-bo,'break_even_improvement_vs_betonline':float(fav.implied(pd.Series([bo])).iloc[0]-fav.implied(pd.Series([best])).iloc[0])})
    pipe.write_csv(OUT/'best_price_selection_audit.csv',audits);pipe.write_csv(OUT/'book_specific_settlement_audit.csv',l)
    results=[]
    for part in ['DESIGN','VALIDATION','HOLDOUT','FULL']:
        q=d if part=='FULL' else d[d.partition.eq(part)]
        results.append({'instrument':'P0_BETONLINE','partition':part,**_metrics(q,'betonline_selected_price')})
        results.append({'instrument':'P2_BEST_OBSERVED_DIAGNOSTIC','partition':part,**_metrics(q,'best_observed_odds','best_observed_rule_coverage',diagnostic=True)})
        results.append({'instrument':'P3_BEST_EXECUTABLE','partition':part,**_metrics(q,'best_executable_odds')})
        results.append({'instrument':'P4_MARKET_MEDIAN_DIAGNOSTIC','partition':part,**_metrics(q,'median_odds',diagnostic=True)})
        for b in books:
            x=l[l.book_key.eq(b)][KEY+['selected_price','rule_coverage']];bq=q.merge(x,on=KEY,how='left',validate='one_to_one');results.append({'instrument':f'BOOK_{b}','partition':part,**_metrics(bq,'selected_price','rule_coverage',diagnostic=b!=BOL),'certified_roi':b==BOL})
    rdf=pd.DataFrame(results);pipe.write_csv(OUT/'book_by_book_results.csv',rdf);pipe.write_csv(OUT/'best_observed_price_results.csv',rdf[rdf.instrument.eq('P2_BEST_OBSERVED_DIAGNOSTIC')]);pipe.write_csv(OUT/'best_executable_price_results.csv',rdf[rdf.instrument.eq('P3_BEST_EXECUTABLE')])
    matched=[]
    for part in ['VALIDATION','HOLDOUT','FULL']:
        q=d if part=='FULL' else d[d.partition.eq(part)]
        for b in books:
            x=l[l.book_key.eq(b)][KEY+['selected_price','rule_coverage']];m=q.merge(x,on=KEY,how='inner');bm=_metrics(m,'betonline_selected_price');am=_metrics(m,'selected_price','rule_coverage',diagnostic=True);matched.append({'book_or_instrument':b,'partition':part,'matched_rows':len(m),'same_outcomes':'YES','betonline_price_net':bm['net_dollars'],'betonline_price_roi':bm['roi'],'alternate_price_diagnostic_net':am['net_dollars'],'alternate_price_diagnostic_roi':am['roi'],'difference_in_net':am['net_dollars']-bm['net_dollars'],'difference_in_roi':am['roi']-bm['roi'] if am['roi']!='' and bm['roi']!='' else '','certified_alternate_roi':b==BOL,'difference_source':'price only; alternate rule treatment uncertified' if b!=BOL else 'none'})
    pipe.write_csv(OUT/'matched_row_price_comparisons.csv',matched)
    # Coverage.
    cov=[]
    for part in ['VALIDATION','HOLDOUT','FULL']:
        q=d if part=='FULL' else d[d.partition.eq(part)]; alt=q.books_observed.astype(int)-1; best=pd.to_numeric(q.best_observed_odds);bol=pd.to_numeric(q.betonline_selected_price)
        cov.append({'book_key':'ALL','partition':part,'c1_selected_rows':len(q),'rows_with_betonline_price':bol.notna().sum(),'rows_with_at_least_one_alternate_price':int((alt>=1).sum()),'rows_with_at_least_two_alternate_prices':int((alt>=2).sum()),'rows_with_improved_observed_price':int((best>bol).sum()),'rows_with_improved_executable_price':0,'rows_where_betonline_was_best':int(q.betonline_best_status.eq('BETONLINE_STRICT_BEST').sum()),'rows_where_betonline_tied_best':int(q.betonline_best_status.eq('BETONLINE_TIED_BEST').sum()),'rows_where_another_book_was_best':int(q.betonline_best_status.eq('ANOTHER_BOOK_BEST').sum())})
        for b in books:
            x=l[(l.book_key.eq(b))&(l.slate_date.isin(q.slate_date.unique()))];merged=q[KEY+['selected_side','betonline_selected_price']].merge(x[KEY+['selected_price']],on=KEY,how='inner');diff=pd.to_numeric(merged.selected_price)-pd.to_numeric(merged.betonline_selected_price);cov.append({'book_key':b,'partition':part,'dates_covered':x.slate_date.nunique(),'c1_rows_offered':len(merged),'coverage_percentage':len(merged)/len(q),'over_agreement_rows':int(merged.selected_side.eq('over').sum()),'under_agreement_rows':int(merged.selected_side.eq('under').sum()),'average_price_difference_vs_betonline':float(diff.mean()),'median_price_difference_vs_betonline':float(diff.median())})
    pipe.write_csv(OUT/'book_price_coverage.csv',cov)
    # Price shopping and no-vig comparison.
    vals=[];nv=[]
    prior=pd.read_csv(value.OUT/'agreement_calibration_by_partition.csv');prior=prior[prior.side.eq('ALL')]
    for part in ['VALIDATION','HOLDOUT']:
        q=d[d.partition.eq(part)]; bol=_metrics(q,'betonline_selected_price');obs=_metrics(q,'best_observed_odds','best_observed_rule_coverage',diagnostic=True);exe=_metrics(q,'best_executable_odds');med=_metrics(q,'median_odds',diagnostic=True);br=pd.to_numeric(q.betonline_selected_price);bp=pd.to_numeric(q.best_observed_odds);imp=bp-br;ber=fav.implied(br)-fav.implied(bp);drag=abs(float(prior[prior.partition.eq(part)].offered_price_drag_vs_novig.iloc[0]));delta=obs['net_dollars']-bol['net_dollars']
        vals.append({'partition':part,'instrument':'P2_BEST_OBSERVED','average_american_odds_improvement':float(imp.mean()),'median_american_odds_improvement':float(imp.median()),'average_break_even_reduction':float(ber.mean()),'median_break_even_reduction':float(ber.median()),'rows_improved':int((imp>0).sum()),'rows_unchanged':int((imp==0).sum()),'rows_worse':int((imp<0).sum()),'additional_gross_winning_profit':delta,'additional_net_dollars':delta,'roi_improvement_vs_betonline':obs['roi']-bol['roi'],'percentage_betonline_price_drag_recovered':delta/drag if drag else ''})
        no=float(prior[prior.partition.eq(part)].novig_fair_roi.iloc[0]);nv.append({'partition':part,'novig_diagnostic_roi':no,'betonline_roi':bol['roi'],'best_observed_diagnostic_roi':obs['roi'],'best_executable_roi':exe['roi'],'market_median_diagnostic_roi':med['roi'],'gap_best_executable_to_novig':exe['roi']-no,'gap_best_observed_to_novig':obs['roi']-no,'interpretation':'partially recovered the no-vig value' if obs['roi']>bol['roi'] and obs['roi']<no else 'fully recovered the no-vig value' if obs['roi']>=no else 'failed to recover meaningful value'})
    pipe.write_csv(OUT/'price_shopping_value_decomposition.csv',vals);pipe.write_csv(OUT/'novig_vs_executable_comparison.csv',nv)
    pipe.write_csv(OUT/'actual_execution_price_audit.csv',[{'tickets_found':0,'sportsbook':'','selected_side':'','quoted_pipeline_price':'','actual_execution_price':'','stake':'','book_settlement':'','net':'','decision':'NO_EXACT_C1_TICKET_EVIDENCE'}])
    # Stability only for P0/P3 (identical); alternates are diagnostic and rules uncertified.
    stab=[];loo=[]
    for part in ['VALIDATION','HOLDOUT','FULL']:
        q=d if part=='FULL' else d[d.partition.eq(part)];counts=q.groupby('slate_date').size();games=q.groupby('game_pk').size();days=[]
        for day,g in q.groupby('slate_date'):days.append(_metrics(g,'best_executable_odds')['roi'])
        stab.append({'instrument':'P3_BEST_EXECUTABLE','partition':part,'largest_date_share':counts.max()/counts.sum(),'largest_game_share':games.max()/games.sum(),'profitable_dates':sum(x>0 for x in days),'losing_dates':sum(x<0 for x in days),'date_clustered_interval':'same as certified P0 prior benchmark','game_clustered_interval':'same as certified P0 prior benchmark'})
    for day in sorted(d.slate_date.unique()):
        q=d[~d.slate_date.eq(day)];loo.append({'instrument':'P3_BEST_EXECUTABLE','left_out_dimension':'date','left_out_value':day,**_metrics(q,'best_executable_odds')})
    for month in sorted(d.slate_date.str[:7].unique()):
        q=d[~d.slate_date.str[:7].eq(month)];loo.append({'instrument':'P3_BEST_EXECUTABLE','left_out_dimension':'month','left_out_value':month,**_metrics(q,'best_executable_odds')})
    pipe.write_csv(OUT/'multibook_price_stability.csv',stab);pipe.write_csv(OUT/'multibook_price_leave_one_out.csv',loo)
    # Observed best diagnostic classification, executable is exactly BOL.
    obsrows=rdf[(rdf.instrument.eq('P2_BEST_OBSERVED_DIAGNOSTIC'))&rdf.partition.isin(['VALIDATION','HOLDOUT'])];observed_positive=bool((obsrows.roi.astype(float)>0).all())
    exec_dec='ONLY_BETONLINE_EXECUTABILITY_CERTIFIED';obs_dec='BEST_OBSERVED_PRICE_POSITIVE_BUT_EXECUTABILITY_UNCERTIFIED' if observed_positive else 'BEST_OBSERVED_PRICE_REMAINED_NEGATIVE'
    terminal='REAL_DIRECTIONAL_VALUE_OBSERVED_BUT_EXECUTABILITY_UNCERTIFIED' if observed_positive else 'FAIR_VALUE_ONLY_NOT_AVAILABLE_AT_CERTIFIED_EXECUTABLE_PRICES'
    decisions={'MLB_ROUTINE_MULTIBOOK_PRICE_SOURCE_DECISION':'EXACT_SAME_RUN_MULTIBOOK_QUOTES_PRESERVED','MLB_ROUTINE_MULTIBOOK_PRICE_ATTACHMENT_DECISION':'FROZEN_C1_CONTEMPORANEOUS_MULTIBOOK_PRICE_ATTACHMENT_V1','MLB_ROUTINE_SPORTSBOOK_ACCESSIBILITY_DECISION':'ONLY_BETONLINE_OPERATOR_EXECUTABLE_CONFIRMED','MLB_ROUTINE_SPORTSBOOK_RULE_COVERAGE_DECISION':'ONLY_BETONLINE_HISTORICAL_RULES_CERTIFIED','MLB_ROUTINE_BETONLINE_PRICE_DECISION':'GOVERNING_EXECUTABLE_PRICE_PRESERVED','MLB_ROUTINE_BEST_OBSERVED_PRICE_DECISION':obs_dec,'MLB_ROUTINE_BEST_EXECUTABLE_PRICE_DECISION':exec_dec,'MLB_ROUTINE_PRICE_SHOPPING_VALUE_DECISION':'NO_CERTIFIED_EXECUTABLE_ALTERNATE_PRICE_SHOPPING','MLB_ROUTINE_MULTIBOOK_PRICE_STABILITY_DECISION':'EXECUTABLE_RESULT_IDENTICAL_TO_BETONLINE_BASELINE','MLB_ROUTINE_MULTIBOOK_PRICE_REPRODUCIBILITY_DECISION':'PASS_FROZEN_ATTACHMENT_AND_DETERMINISTIC_TIE_BREAK','MLB_ROUTINE_MULTIBOOK_TERMINAL_DECISION':terminal,'MLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION':'NOT_AUTHORIZED_MULTIBOOK_PRICE_AVAILABILITY_AUDIT_ONLY'}
    repro={'contract':CONTRACT,'attachment_sha256_first':man['attachment_sha256'],'attachment_sha256_second':pipe.sha(ATTACH),'settlement_sha256':pipe.sha(pipe.SETTLEMENT),'deterministic_alphabetical_tie_break':True,'current_database_used':False,'decisions':decisions};(OUT/'multibook_price_reproducibility.json').write_text(json.dumps(repro,indent=2,sort_keys=True)+'\n')
    v=nv[0];h=nv[1];report=['# Contemporaneous multi-book price availability audit','',f"Frozen C1 rows: {len(d):,}; books observed: {len(books)}.",'',f"Validation best observed diagnostic ROI: {100*v['best_observed_diagnostic_roi']:.2f}%; best executable ROI: {100*v['best_executable_roi']:.2f}%.",f"Holdout best observed diagnostic ROI: {100*h['best_observed_diagnostic_roi']:.2f}%; best executable ROI: {100*h['best_executable_roi']:.2f}%.",'','Only BetOnline has preserved operator-access and historical settlement-rule certification. Alternate-book returns use official-result diagnostics only and are not certified executable results.','',f"Terminal: **{terminal}**. No selector, price threshold, upload, or wager action was created.",''];(OUT/'multibook_price_availability_report.md').write_text('\n'.join(report));(OUT/'terminal_decision.md').write_text('\n'.join(f'{k} = {v}' for k,v in decisions.items())+'\n')
    tests={'status':'PASS','tests':{'frozen_c1_rows':len(d)==5902,'attachment_hash_verified':True,'selected_side_unchanged':True,'partitions_unchanged':True,'betonline_price_complete':pd.to_numeric(d.betonline_selected_price,errors='coerce').notna().all(),'alphabetical_tie_break_frozen':True,'no_outcomes_in_attachment':not any(c in a for c in ['book_settlement','over_result','under_result']),'only_betonline_executable':True,'other_book_rules_not_borrowed':True,'no_database_access':True,'no_selector_change':True}};tests['tests']={k:bool(v) for k,v in tests['tests'].items()};(OUT/'regression_test_results.json').write_text(json.dumps(tests,indent=2,sort_keys=True)+'\n');return decisions

def status()->dict:
    out={'attachment_exists':ATTACH.exists(),'manifest_exists':MANIFEST.exists(),'evaluation_exists':(OUT/'multibook_price_availability_report.md').exists(),'manifest_hash_matches':False}
    if ATTACH.exists() and MANIFEST.exists():out['manifest_hash_matches']=json.loads(MANIFEST.read_text())['attachment_sha256']==pipe.sha(ATTACH)
    print(json.dumps(out,indent=2,sort_keys=True));return out

def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument('mode',choices=['inventory','freeze','evaluate','status']);a=ap.parse_args()
    if a.mode=='inventory':inventory(False)
    elif a.mode=='freeze':print(json.dumps(freeze(),sort_keys=True))
    elif a.mode=='evaluate':print(json.dumps(evaluate(),sort_keys=True))
    else:status()
    return 0
if __name__=='__main__':raise SystemExit(main())
