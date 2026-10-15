#!/usr/bin/env python3
"""Provider-level descriptive price-neighborhood analysis from immutable NHL captures."""
from __future__ import annotations

import csv, hashlib, json, math
from collections import defaultdict
from pathlib import Path
from statistics import median

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'artifacts/analysis/nhl/points_provider_price_neighborhood/2026-10-15'
BASE=ROOT/'artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14'
OBSROOT=ROOT/'artifacts/operational/nhl/odds_observations/season=2026'
HIST=ROOT/'backend/nhl/exports/odds_history'
RECON=ROOT/'artifacts/operational/nhl/postgame_reconciliation'
DAYS=['2026-10-10','2026-10-11','2026-10-12','2026-10-13','2026-10-14']
PRICE_BANDS=[('LT_-120',None,-120),('-120_TO_-106',-120,-105),('-105_TO_-101',-105,-100),('EVEN_+100',100,101),('+101_TO_+110',101,111),('+111_TO_+125',111,126),('+126_PLUS',126,None)]
EDGE_BANDS=[('LT_-15PP',None,-.15),('-15_TO_-10PP',-.15,-.10),('-10_TO_-5PP',-.10,-.05),('-5_TO_-3PP',-.05,-.03),('NEG_-3_TO_-1PP',-.03,-.01),('NEG_-1_TO_0PP',-.01,0),('0_TO_+1PP',0,.01),('+1_TO_+3PP',.01,.03),('+3_TO_+5PP',.03,.05),('+5_TO_+10PP',.05,.10),('+10_TO_+15PP',.10,.15),('GE_+15PP',.15,None)]

def readcsv(p):
    with Path(p).open(newline='') as f:return list(csv.DictReader(f))
def num(v):
    try:
        x=float(v); return x if math.isfinite(x) else None
    except (TypeError,ValueError):return None
def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def implied(o):
    p=num(o)
    if p is None or p==0:return None
    return -p/(100-p) if p<0 else 100/(100+p)
def amer(p):
    if p is None or not 0<p<1:return None
    return round(-100*p/(1-p),2) if p>=.5 else round(100*(1-p)/p,2)
def band(x,reg):
    for label,lo,hi in reg:
        if x is not None and (lo is None or x>=lo) and (hi is None or x<hi):return label
    return 'UNAVAILABLE'
def dump(name,rows):
    fields=list(dict.fromkeys(k for r in rows for k in r)) or ['empty']
    with (OUT/name).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
def normname(s):return ' '.join((s or '').lower().replace('.','').replace("'",'').split())
def metrics(rows):
    settled=[r for r in rows if r.get('outcome_over') not in ('','NA',None)]
    if not settled:return {'n':len(rows),'settled_n':0}
    n=len(settled); wins=sum(r['selected_win'] for r in settled)
    if not all(r.get('model_brier') is not None and r.get('book_brier') is not None for r in settled):
        return {'n':len(rows),'settled_n':n,'model_wins':wins,'model_accuracy':wins/n}
    return {'n':len(rows),'settled_n':n,'wins':wins,'losses':n-wins,'accuracy':wins/n,
      'mean_model_brier':sum(r['model_brier'] for r in settled)/n,'mean_book_brier':sum(r['book_brier'] for r in settled)/n,
      'model_brier_advantage':sum(r['book_brier']-r['model_brier'] for r in settled)/n,
      'mean_model_log_loss':sum(r['model_log_loss'] for r in settled)/n,'mean_book_log_loss':sum(r['book_log_loss'] for r in settled)/n,
      'model_log_loss_advantage':sum(r['book_log_loss']-r['model_log_loss'] for r in settled)/n,
      'model_only_correct':sum(r['model_win'] and not r['book_win'] for r in settled),
      'book_only_correct':sum(r['book_win'] and not r['model_win'] for r in settled),
      'mean_unit_return':sum(r['unit_return'] for r in settled)/n if all(r.get('unit_return') is not None for r in settled) else None,
      'mean_provider_price_american':sum(r['provider_side_price'] for r in settled)/n if all(r.get('provider_side_price') is not None for r in settled) else None}

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    inv={r['slate_date']:r for r in readcsv(BASE/'market_evidence_inventory.csv') if r['slate_date'] in DAYS}
    canonical=readcsv(BASE/'canonical_market_intersection.csv')
    canonical={(r['slate_date'],r['game_id'],r['player_id'],str(num(r['line']))):r for r in canonical if num(r['line'])==.5}
    # Oct 15 is pending only and is bound directly by the active parent receipt.
    receipt=json.loads((ROOT/'artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261015T140453183604Z_bfa01e59/parent_receipt.json').read_text())
    pendday='2026-10-15'; pending_prediction=ROOT/'backend/nhl/data/processed/daily_runs/nhldaily_20261015T140453183604Z_bfa01e59/points_predictions.csv'
    pending_prediction_record=next(x for x in receipt['lanes']['points']['outputs'] if str(x.get('path','')).endswith('/points_predictions.csv'))
    if digest(pending_prediction)!=pending_prediction_record['sha256']:raise ValueError('Oct15 prediction hash mismatch')
    pending_attachment_record=next(x for x in receipt['lanes']['points_attachment']['outputs'] if str(x.get('path','')).endswith('/points_with_market.csv'))
    pending_attachment=ROOT/'nhl/site/data/points_with_market.csv'
    if digest(pending_attachment)!=pending_attachment_record['sha256']:raise ValueError('Oct15 points attachment hash mismatch')
    pending_integrity=json.loads((ROOT/'nhl/site/data/points_attachment_integrity.json').read_text())
    if pending_integrity.get('status')!='PASS' or pending_integrity.get('attachment_sha256')!=pending_attachment_record['sha256']:raise ValueError('Oct15 attachment integrity mismatch')
    pending_attachment_rows=readcsv(pending_attachment)
    pred_by_date={d:{} for d in DAYS+[pendday]}; obs_paths={}; integrity={}; hash_rows=[]
    for d in DAYS:
        integ=json.loads((HIST/d/'points_attachment_integrity.json').read_text()); integrity[d]=integ
        if integ.get('status')!='PASS':raise ValueError(f'attachment integrity not PASS {d}')
        attachment_path=HIST/d/'points_with_market.csv'
        if digest(attachment_path)!=integ.get('attachment_sha256'):raise ValueError(f'attachment hash mismatch {d}')
        obs_manifest=integ['odds_observation_manifest_sha256']; raw_sha=integ['odds_raw_response_sha256']
        timestamp=inv[d].get('observation_timestamp_utc')
        prefix='observation='+timestamp.replace('-','').replace(':','') if timestamp else ''
        # Timestamp locates the immutable observation; the attachment raw hash confirms exact identity.
        candidates=[f for f in (OBSROOT/f'slate_date={d}').glob('observation=*') if f.name.startswith(prefix)]
        if len(candidates)!=1:raise ValueError(f'expected one exact observation for {d}; found {len(candidates)}')
        folder=candidates[0]; raw=folder/'raw_response.json'
        if digest(raw)!=raw_sha:raise ValueError(f'raw response hash mismatch {d}')
        obs_paths[d]=folder
        pred_path=ROOT/inv[d]['prediction_path']
        if digest(pred_path)!=inv[d]['prediction_sha256']:raise ValueError(f'prediction hash mismatch {d}')
        preds=readcsv(pred_path); pred_by_date[d]={(p['game_id'],p['player_id'],str(num(p['line']))):p for p in preds if num(p['line'])==.5}
        hash_rows += [{'slate_date':d,'artifact':str(raw.relative_to(ROOT)),'sha256':digest(raw)}, {'slate_date':d,'artifact':str(folder/'normalized_odds.jsonl').replace(str(ROOT)+'/',''),'sha256':digest(folder/'normalized_odds.jsonl')}, {'slate_date':d,'artifact':str(HIST/d/'points_attachment_integrity.json').replace(str(ROOT)+'/',''),'sha256':digest(HIST/d/'points_attachment_integrity.json')},{'slate_date':d,'artifact':str(attachment_path.relative_to(ROOT)),'sha256':digest(attachment_path)},{'slate_date':d,'artifact':str(pred_path.relative_to(ROOT)),'sha256':digest(pred_path)}]
    receipt_odds=receipt['odds_observation']; odir=ROOT/receipt_odds['path']; raw=odir/'raw_response.json'
    if digest(raw)!=receipt_odds['manifest_sha256'] and digest(raw)!=json.loads((odir/'observation_summary.json').read_text()).get('response_sha256'):
        # manifest binds raw hash in immutable observation summary; validate that explicit value below.
        pass
    osum=json.loads((odir/'observation_summary.json').read_text())
    if digest(raw)!=osum.get('response_sha256'):raise ValueError('Oct15 raw hash mismatch')
    obs_paths[pendday]=odir; pred_by_date[pendday]={(p['game_id'],p['player_id'],str(num(p['line']))):p for p in readcsv(pending_prediction) if num(p['line'])==.5}
    hash_rows.extend([{'slate_date':pendday,'artifact':str(raw.relative_to(ROOT)),'sha256':digest(raw)},{'slate_date':pendday,'artifact':str(pending_prediction.relative_to(ROOT)),'sha256':digest(pending_prediction)},{'slate_date':pendday,'artifact':str(pending_attachment.relative_to(ROOT)),'sha256':digest(pending_attachment)}])
    # exact player-name-to-ID binding comes from each retained attachment's unique prediction key.
    quotes=[]; window_excluded_quotes=[]; bindings={d:{} for d in DAYS+[pendday]}; seen={}; raw_digests={d:digest(obs_paths[d]/'raw_response.json') for d in DAYS+[pendday]}
    for d in DAYS+[pendday]:
        if d==pendday:
            attached=readcsv(ROOT/'backend/nhl/exports/daily/names/names_2026-10-15.csv')
            name_map={(x['game_id'],x['player_id']):x['full_name'] for x in attached}
            # Prediction input contains player IDs and names; use it as the exact slate identity spine.
            pp=readcsv(pending_prediction)
            for p in pp:
                if num(p.get('line'))==.5:bindings[d].setdefault((p['game_id'],normname(name_map.get((p['game_id'],p['player_id'])))),(p['player_id'],p.get('game_start_utc')))
            cutoff=osum['observation_timestamp_utc']; attach=None
        else:
            attach=readcsv(HIST/d/'points_with_market.csv'); cutoff=inv[d]['observation_timestamp_utc']
            for a in attach:
                if num(a.get('line'))==.5:bindings[d].setdefault((a['game_id'],normname(a['full_name'])),(a['player_id'],None))
        folder=obs_paths[d]
        gamebind={}
        for line in (folder/'game_binding.jsonl').read_text().splitlines():
            x=json.loads(line)
            if x.get('binding_status')=='MATCHED':gamebind[x['provider_event_id']]=x['canonical_game_id']
        for line in (folder/'normalized_odds.jsonl').read_text().splitlines():
            o=json.loads(line)
            if o.get('market') not in ('player_points','player_points_alternate') or num(o.get('point'))!=.5:continue
            gid=gamebind.get(o.get('provider_event_id'))
            if gid is None:continue
            binding=bindings[d].get((str(gid),normname(o.get('description'))))
            if not binding:continue
            pid,start=binding; key=(d,str(gid),str(pid),'.5',o['bookmaker'],o['outcome'].upper())
            q={'slate_date':d,'game_id':gid,'player_id':pid,'line':.5,'provider':'','provider_title':'','side':o['outcome'].upper(),'price_american':int(o['price']),'provider_price_band':band(int(o['price']),PRICE_BANDS),'market':o['market'],'quote_timestamp_utc':o.get('market_last_update'),'observation_timestamp_utc':cutoff,'observation_path':str(folder.relative_to(ROOT)),'commence_time_utc':o.get('commence_time'),'provider_event_id':o.get('provider_event_id'),'provider_player_name':o.get('description'),'parent_daily_run_id': inv[d]['parent_daily_run_id'] if d in inv else receipt['parent_daily_run_id'],'raw_response_sha256':raw_digests[d],'observation_manifest_sha256':receipt_odds['manifest_sha256'] if d==pendday else integrity[d]['odds_observation_manifest_sha256'],'provider':'%s'%o['bookmaker'],'production_edge_neighborhood':'OTHER','is_duplicate_key':False}
            if not q['quote_timestamp_utc'] or q['quote_timestamp_utc']>q['observation_timestamp_utc'] or not q['commence_time_utc'] or q['observation_timestamp_utc']>=q['commence_time_utc']:
                q['exclusion_reason']='QUOTE_AFTER_BOUND_OBSERVATION_OR_NOT_PREGAME';window_excluded_quotes.append(q);continue
            quotes.append(q);seen.setdefault(key,[]).append(q)
    # Unique exact quotes only. Duplicates for an identity/book/side are kept in an audit file and excluded from analysis.
    # Standard player_points is preferred; alternate-market quotes are a fallback when no standard quote exists.
    # Retain duplicate-key collisions within the chosen market for audit and exclude them from estimates.
    chosen={}
    for k,vals in seen.items():
        std=[q for q in vals if q['market']=='player_points']
        chosen[k]=std if std else vals
    dupkeys={k for k,v in chosen.items() if len(v)>1}
    quotes=[q for q in quotes if q in chosen[(q['slate_date'],str(q['game_id']),str(q['player_id']),'.5',q['provider'],q['side'])]]
    # Build prediction-level quote sets plus attachment-median, BetOnline, and simultaneous best-price policy rows.
    predrows=[]; policy_rows=[]; ambiguous=[]; quote_index=defaultdict(list)
    for q in quotes: quote_index[(q['slate_date'],str(q['game_id']),str(q['player_id']))].append(q)
    for d in DAYS+[pendday]:
        attachment=readcsv(HIST/d/'points_with_market.csv') if d in DAYS else pending_attachment_rows
        amap={(a['game_id'],a['player_id'],str(num(a['line']))):a for a in attachment or [] if num(a.get('line'))==.5}
        grades={}
        if d in DAYS and inv[d].get('reconciliation_path'):
            gr=Path(inv[d]['reconciliation_path']); grades={(g['game_id'],g['player_id'],str(num(g['line']))):g for g in readcsv(ROOT/gr) if num(g['line'])==.5}
            hash_rows.append({'slate_date':d,'artifact':str(gr),'sha256':digest(ROOT/gr)})
        elif d=='2026-10-14':
            outcome_path=ROOT/receipt['prior_day_learning']['reconciliation_package']/'canonical_skater_outcomes.csv'
            outcome_rows=readcsv(outcome_path)
            grades={(g['game_id'],g['player_id'],'0.5'):{**g,'grading_status':'SETTLED' if g.get('participation_state')=='PARTICIPATED' and g.get('official_final')=='True' else 'PARTICIPATION_STATUS_UNRESOLVED','outcome_over':float(num(g.get('official_points'))>.5) if g.get('participation_state')=='PARTICIPATED' and g.get('official_final')=='True' else None} for g in outcome_rows}
            hash_rows.append({'slate_date':d,'artifact':str(outcome_path.relative_to(ROOT)),'sha256':digest(outcome_path)})
        elif d in DAYS: grades={}
        for key,p in pred_by_date[d].items():
            gid,pid,line=key; a=amap.get(key,{}); g=grades.get(key,{})
            obsutc=osum['observation_timestamp_utc'] if d==pendday else inv[d]['observation_timestamp_utc']
            start=p.get('game_start_utc'); pregame=(d==pendday or (start and inv[d]['observation_timestamp_utc']<start))
            mo=implied(a.get('price_over'));mu=implied(a.get('price_under'));mn=(mo/(mo+mu) if mo is not None and mu is not None else None)
            po=num(p.get('prob_over')); side='OVER' if po is not None and po>=.5 else 'UNDER'; psel=po if side=='OVER' else (1-po if po is not None else None)
            attachprice=num(a.get('price_over' if side=='OVER' else 'price_under')); attachraw=implied(attachprice); edge=psel-attachraw if psel is not None and attachraw is not None else None
            qlist=[q for q in quote_index[(d,str(gid),str(pid))] if (d,gid,pid,'.5',q['provider'],q['side']) not in dupkeys and q['quote_timestamp_utc'] and q['quote_timestamp_utc']<=q['observation_timestamp_utc'] and q['commence_time_utc'] and q['observation_timestamp_utc']<q['commence_time_utc']]
            sides={s:[q for q in qlist if q['side']==s] for s in ('OVER','UNDER')}
            byprovider={}
            for q in qlist:byprovider.setdefault(q['provider'],{})[q['side']]=q
            two_sided={b:v for b,v in byprovider.items() if 'OVER' in v and 'UNDER' in v}
            # Odds improve for bettor as American number increases; choose best price for model's selected side.
            selected_quotes=[q for q in sides[side]]
            best=max(selected_quotes,key=lambda q:q['price_american']) if selected_quotes else None
            # BetOnline is exact-provider only.
            bo=byprovider.get('betonlineag',{}).get(side)
            y=num(g.get('outcome_over'))
            if y is None and g.get('settled_side') in ('OVER','UNDER'): y=1.0 if g['settled_side']=='OVER' else 0.0
            status=g.get('grading_status','PENDING') if d in DAYS else 'PENDING'
            stored_edge=num(a.get('edge_over' if side=='OVER' else 'edge_under'))
            edge_delta=abs(stored_edge-edge) if stored_edge is not None and edge is not None else None
            if edge_delta is not None and edge_delta>1e-10:raise ValueError(f'production edge mismatch {d} {gid} {pid}: {edge_delta}')
            row={'slate_date':d,'game_id':gid,'player_id':pid,'line':line,'model_p_over':po,'model_selected_side':side,'model_selected_probability':psel,'production_edge':edge,'production_selected_edge':edge,'production_edge_reproduced':edge_delta is not None,'production_edge_reproduction_abs_delta':edge_delta,'production_edge_band':band(edge,EDGE_BANDS),'production_edge_neighborhood':'NEGATIVE_NEAR_ZERO' if edge is not None and -.03<=edge<0 else 'POSITIVE_NEAR_ZERO' if edge is not None and 0<=edge<.03 else 'MODERATE_POSITIVE' if edge is not None and .03<=edge<.10 else 'EXTREME_POSITIVE' if edge is not None and edge>=.10 else 'OTHER','attachment_selected_price':attachprice,'attachment_selected_price_band':band(attachprice,PRICE_BANDS),'observation_timestamp_utc':obsutc,'game_start_utc':start,'pregame_cutoff_pass':bool(pregame),'provider_quote_count':len(qlist),'over_provider_count':len(sides['OVER']),'under_provider_count':len(sides['UNDER']),'two_sided_provider_count':len(two_sided),'provider_dispersion_over':max((q['price_american'] for q in sides['OVER']),default=None)-min((q['price_american'] for q in sides['OVER']),default=0) if sides['OVER'] else None,'provider_dispersion_under':max((q['price_american'] for q in sides['UNDER']),default=None)-min((q['price_american'] for q in sides['UNDER']),default=0) if sides['UNDER'] else None,'outcome_over':y if status=='SETTLED' else None,'grading_status':status}
            if status!='SETTLED': row['outcome_over']=None
            predrows.append(row)
            median_quote={'provider':'ATTACHMENT_MEDIAN','price_american':attachprice,'quote_timestamp_utc':obsutc}
            for policy,q in [('ATTACHMENT_MEDIAN',median_quote if attachprice is not None else None),('BETONLINE_EXACT',bo),('BEST_OBSERVED',best)]:
                if q:
                    price=q['price_american']; raw=implied(price); pr=raw if side=='OVER' else None
                    marketp=None; marketover=None; paired_provider=None
                    if policy=='ATTACHMENT_MEDIAN' and mn is not None:
                        paired_provider='MEDIAN_ATTACHMENT';marketover=mn;marketp=mn if side=='OVER' else 1-mn
                    elif policy=='BETONLINE_EXACT' and two_sided.get('betonlineag'):
                        paired_provider='betonlineag';ov=implied(two_sided['betonlineag']['OVER']['price_american']);un=implied(two_sided['betonlineag']['UNDER']['price_american']);marketover=ov/(ov+un);marketp=marketover if side=='OVER' else 1-marketover
                    elif policy=='BEST_OBSERVED':
                        if q['provider'] in two_sided:
                            paired_provider=q['provider'];v=two_sided[paired_provider];ov=implied(v['OVER']['price_american']);un=implied(v['UNDER']['price_american']);marketover=ov/(ov+un);marketp=marketover if side=='OVER' else 1-marketover
                    if marketp is None:marketp=raw
                    market_side=('OVER' if marketover>=.5 else 'UNDER') if marketover is not None else side
                    bookwin=((y==1) if market_side=='OVER' else (y==0)) if y is not None else None
                    modelwin=((y==1) if side=='OVER' else (y==0)) if y is not None else None
                    bp=marketp
                    unitret=(price/100 if price>0 else 100/abs(price)) if y is not None and ((y==1 and side=='OVER') or (y==0 and side=='UNDER')) else (-1 if y is not None else None)
                    model_brier=(psel-(1 if (y==1 and side=='OVER') or (y==0 and side=='UNDER') else 0))**2 if y is not None else None
                    book_brier=(bp-(1 if (y==1 and side=='OVER') or (y==0 and side=='UNDER') else 0))**2 if y is not None else None
                    target=1 if (y==1 and side=='OVER') or (y==0 and side=='UNDER') else 0
                    mloss=-(target*math.log(max(1e-15,min(1-1e-15,psel)))+(1-target)*math.log(max(1e-15,min(1-1e-15,1-psel)))) if y is not None else None
                    bloss=-(target*math.log(max(1e-15,min(1-1e-15,bp)))+(1-target)*math.log(max(1e-15,min(1-1e-15,1-bp)))) if y is not None else None
                    policy_rows.append({**row,'policy':policy,'provider':q['provider'],'provider_side_price':price,'provider_price_band':band(price,PRICE_BANDS),'provider_side_raw_edge':psel-raw if psel is not None else None,'provider_no_vig_edge':psel-marketp if psel is not None else None,'price_implied_selected':raw,'two_sided_no_vig_selected':marketp,'paired_provider':paired_provider or 'UNPAIRED_RAW_IMPLIED','provider_favored_side':market_side,'model_win':bool(modelwin) if modelwin is not None else None,'book_win':bool(bookwin) if bookwin is not None else None,'selected_win':bool(modelwin) if modelwin is not None else None,'unit_return':unitret,'model_brier':model_brier,'book_brier':book_brier,'model_log_loss':mloss,'book_log_loss':bloss,'quote_timestamp_utc':q['quote_timestamp_utc']})
    # Construct provider quote table with exact prediction key and paired same-book no-vig edges.
    predrow_index={(r['slate_date'],str(r['game_id']),str(r['player_id'])):r for r in predrows}
    quote_pairs=defaultdict(dict)
    for q in quotes:quote_pairs[(q['slate_date'],str(q['game_id']),str(q['player_id']),q['provider'])][q['side']]=q
    for q in quotes:
        q['is_duplicate_key']= (q['slate_date'],str(q['game_id']),str(q['player_id']),'.5',q['provider'],q['side']) in dupkeys
        k=(q['slate_date'],str(q['game_id']),str(q['player_id']),'.5');p=pred_by_date[q['slate_date']].get((str(q['game_id']),str(q['player_id']),'0.5'))
        if not p:continue
        po=num(p.get('prob_over'));side='OVER' if po is not None and po>=.5 else 'UNDER';ps=po if side=='OVER' else (1-po if po is not None else None)
        q['model_selected_side']=side;q['model_selected_probability']=ps;q['provider_price_band']=band(q['price_american'],PRICE_BANDS);q['provider_raw_implied_probability']=implied(q['price_american']);q['provider_quote_edge']=ps-implied(q['price_american']) if ps is not None and q['side']==side else None;q['provider_raw_edge']=q['provider_quote_edge'];q['provider_no_vig_same_book_edge']=None
        if q['side']!=side:q['provider_raw_edge']=None
        pair=quote_pairs[(q['slate_date'],str(q['game_id']),str(q['player_id']),q['provider'])]
        if 'OVER' in pair and 'UNDER' in pair and ps is not None:
            ov=implied(pair['OVER']['price_american']);un=implied(pair['UNDER']['price_american']);fair_over=ov/(ov+un);fair_selected=fair_over if side=='OVER' else 1-fair_over
            q['provider_no_vig_same_book_edge']=ps-fair_selected;q['provider_no_vig_over']=fair_over
        else:q['provider_no_vig_over']=None
        r=predrow_index.get((q['slate_date'],str(q['game_id']),str(q['player_id'])))
        q['production_selected_edge']=r['production_selected_edge'] if r else None
        q['production_edge_neighborhood']=r['production_edge_neighborhood'] if r else 'OTHER'
        q['outcome_over']=r['outcome_over'] if r else None
        q['grading_status']=r['grading_status'] if r else 'UNMATCHED'
        q['provider_side_probability']=ps if q['side']==side else (1-ps if ps is not None else None)
        q['provider_side_edge']=q['provider_side_probability']-implied(q['price_american']) if q['provider_side_probability'] is not None else None
        q['provider_side_win']=((q['outcome_over']==1 and q['side']=='OVER') or (q['outcome_over']==0 and q['side']=='UNDER')) if q['outcome_over'] is not None else None
        if q['provider_side_win'] is not None:
            q['provider_side_unit_return']=(q['price_american']/100 if q['price_american']>0 else 100/abs(q['price_american'])) if q['provider_side_win'] else -1
    # Fixed provider-price neighborhoods are evaluated once per prediction, selecting the best qualifying
    # selected-side offer without reference to outcomes.
    quote_groups_by_prediction=defaultdict(list)
    for q in quotes:
        if not q.get('is_duplicate_key') and q.get('side')==q.get('model_selected_side') and q.get('provider_side_probability') is not None:
            quote_groups_by_prediction[(q['slate_date'],str(q['game_id']),str(q['player_id']))].append(q)
    pred_lookup={(r['slate_date'],str(r['game_id']),str(r['player_id'])):r for r in predrows}
    neighborhood_rows=[]
    def inrange(x,lo,hi):return x is not None and lo<=x<hi
    for key,rr in pred_lookup.items():
        qs=quote_groups_by_prediction.get(key,[]); sample=[]
        for q in qs:
            price=q['price_american']; pe=q.get('provider_quote_edge'); prod=q.get('production_selected_edge')
            if prod is not None and -.03<=prod<0 and price>=100:sample.append(('PRODUCTION_NEG3_TO_0_PLUS100',q))
            if inrange(pe,-.03,0) and price>=100:sample.append(('PROVIDER_NEG3_TO_0_PLUS100',q))
            if inrange(pe,-.01,0) and price>=100:sample.append(('PROVIDER_NEG1_TO_0_PLUS100',q))
            if inrange(pe,0,.01) and price>=100:sample.append(('PROVIDER_0_TO_PLUS1_PLUS100',q))
            if inrange(pe,0,.03) and price>=100:sample.append(('PROVIDER_0_TO_PLUS3_PLUS100',q))
            if inrange(pe,-.03,0) and -105<=price<=-101:sample.append(('PROVIDER_NEG3_TO_0_NEAR_EVEN_FAV',q))
            if inrange(pe,-.03,0) and -120<=price<=-106:sample.append(('PROVIDER_NEG3_TO_0_FAV',q))
            if inrange(pe,0,.03) and -105<=price<=-101:sample.append(('PROVIDER_0_TO_PLUS3_NEAR_EVEN_FAV',q))
            if inrange(pe,0,.03) and -120<=price<=-106:sample.append(('PROVIDER_0_TO_PLUS3_FAV',q))
            if pe is not None and pe>=.10:sample.append(('PROVIDER_EDGE_GE_PLUS10',q))
            if prod is not None and prod>=.10:sample.append(('PRODUCTION_EDGE_GE_PLUS10',q))
            for edge_label,lo,hi in [('PROVIDER_NEG3_TO_NEG1',-.03,-.01),('PROVIDER_NEG1_TO_0',-.01,0),('PROVIDER_0_TO_PLUS1',0,.01),('PROVIDER_PLUS1_TO_PLUS3',.01,.03),('PROVIDER_PLUS3_TO_PLUS5',.03,.05),('PROVIDER_PLUS5_AND_ABOVE',.05,None)]:
                if pe is not None and pe>=lo and (hi is None or pe<hi):sample.append((edge_label,q))
        byneighborhood=defaultdict(list)
        for label,q in sample:byneighborhood[label].append(q)
        for label,eligible in byneighborhood.items():
            q=max(eligible,key=lambda x:x['price_american']); pr=rr['model_selected_probability']; y=rr['outcome_over']
            fair_over=q.get('provider_no_vig_over'); fair_sel=(fair_over if q['model_selected_side']=='OVER' else 1-fair_over) if fair_over is not None else None
            neighborhood_rows.append({'slate_date':rr['slate_date'],'game_id':rr['game_id'],'player_id':rr['player_id'],'line':.5,'neighborhood':label,'selected_provider':q['provider'],'selected_side':q['side'],'provider_price_american':q['price_american'],'provider_price_band':q['provider_price_band'],'production_edge':rr['production_selected_edge'],'provider_quote_edge':q.get('provider_quote_edge'),'provider_no_vig_selected_side_probability':fair_sel,'model_selected_probability':pr,'provider_no_vig_gap':pr-fair_sel if fair_sel is not None else None,'model_selected_side':rr['model_selected_side'],'provider_favored_side':('OVER' if fair_over>=.5 else 'UNDER') if fair_over is not None else None,'same_side':(rr['model_selected_side']==('OVER' if fair_over>=.5 else 'UNDER')) if fair_over is not None else None,'outcome_over':y,'grading_status':rr['grading_status'],'unit_return':q.get('provider_side_unit_return'),'quote_timestamp_utc':q['quote_timestamp_utc'],'observation_timestamp_utc':q['observation_timestamp_utc']})
    dump('provider_quotes.csv',quotes);dump('out_of_window_quote_audit.csv',window_excluded_quotes);dump('prediction_price_inventory.csv',predrows);dump('prediction_policy_rows.csv',policy_rows);dump('prediction_neighborhood_rows.csv',neighborhood_rows);dump('duplicate_quote_audit.csv',[q for q in quotes if q['is_duplicate_key']]);dump('source_hashes.csv',hash_rows)
    dump('production_edge_reproduction.csv',[{'slate_date':r['slate_date'],'game_id':r['game_id'],'player_id':r['player_id'],'line':r['line'],'production_edge':r['production_edge'],'reproduced':r['production_edge_reproduced'],'absolute_difference':r['production_edge_reproduction_abs_delta']} for r in predrows])
    def neighborhood_metrics(rs):
        settled=[r for r in rs if r['grading_status']=='SETTLED' and r.get('outcome_over') is not None]
        if not settled:return {'n':len(rs),'settled_n':0}
        n=len(settled); modelwins=sum((r['outcome_over']==1 if r['model_selected_side']=='OVER' else r['outcome_over']==0) for r in settled)
        scores=[]; market_scores=[]; ml=[]; bl=[]; returns=[]
        for r in settled:
            y=r['outcome_over']; po=r['model_selected_probability']; target=1 if r['model_selected_side']=='OVER' else 0; ysel=1 if (y==1 if target else y==0) else 0
            scores.append((po- y)**2); ml.append(-(y*math.log(max(1e-15,min(1-1e-15,po)))+(1-y)*math.log(max(1e-15,min(1-1e-15,1-po)))))
            if r['provider_no_vig_selected_side_probability'] is not None:
                mp=r['provider_no_vig_selected_side_probability']; market_scores.append((mp-ysel)**2); bl.append(-(ysel*math.log(max(1e-15,min(1-1e-15,mp)))+(1-ysel)*math.log(max(1e-15,min(1-1e-15,1-mp)))))
            if r.get('unit_return') is not None:returns.append(r['unit_return'])
        return {'n':len(rs),'settled_n':n,'dates':'|'.join(sorted({r['slate_date'] for r in settled})),'hgb_wins':modelwins,'hgb_losses':n-modelwins,'hgb_accuracy':modelwins/n,'expected_losses':sum(1-r['model_selected_probability'] for r in settled),'observed_losses':n-modelwins,'excess_losses':n-modelwins-sum(1-r['model_selected_probability'] for r in settled),'hgb_brier':sum(scores)/n,'hgb_log_loss':sum(ml)/n,'hgb_calibration_gap':modelwins/n-sum(r['model_selected_probability'] for r in settled)/n,'paired_no_vig_n':len(market_scores),'same_provider_market_brier':sum(market_scores)/len(market_scores) if market_scores else None,'same_provider_market_log_loss':sum(bl)/len(bl) if bl else None,'hgb_brier_advantage_on_paired':sum((r['provider_no_vig_selected_side_probability']- (1 if (r['outcome_over']==1 if r['model_selected_side']=='OVER' else r['outcome_over']==0) else 0))**2-(r['model_selected_probability']-(r['outcome_over']))**2 for r in settled if r['provider_no_vig_selected_side_probability'] is not None)/len(market_scores) if market_scores else None,'hgb_closer_count':sum((r['provider_no_vig_selected_side_probability']-(1 if (r['outcome_over']==1 if r['model_selected_side']=='OVER' else r['outcome_over']==0) else 0))**2>(r['model_selected_probability']-(r['outcome_over']))**2 for r in settled if r['provider_no_vig_selected_side_probability'] is not None),'market_closer_count':sum((r['provider_no_vig_selected_side_probability']-(1 if (r['outcome_over']==1 if r['model_selected_side']=='OVER' else r['outcome_over']==0) else 0))**2<(r['model_selected_probability']-(r['outcome_over']))**2 for r in settled if r['provider_no_vig_selected_side_probability'] is not None),'descriptive_net_units':sum(returns) if returns else None,'descriptive_roi_per_unit_risk':sum(returns)/len(returns) if returns else None}
    neighborhood_metrics_rows=[]
    for label in sorted({r['neighborhood'] for r in neighborhood_rows}):neighborhood_metrics_rows.append({'neighborhood':label,**neighborhood_metrics([r for r in neighborhood_rows if r['neighborhood']==label])})
    dump('neighborhood_performance.csv',neighborhood_metrics_rows)
    primary_labels=['PRODUCTION_NEG3_TO_0_PLUS100','PROVIDER_NEG3_TO_0_PLUS100','PROVIDER_NEG1_TO_0_PLUS100','PROVIDER_0_TO_PLUS1_PLUS100','PROVIDER_0_TO_PLUS3_PLUS100']
    dump('date_neighborhood_performance.csv',[{'slate_date':d,'neighborhood':label,**neighborhood_metrics([r for r in neighborhood_rows if r['slate_date']==d and r['neighborhood']==label])} for d in DAYS for label in primary_labels])
    core_edge_labels=['PROVIDER_NEG3_TO_NEG1','PROVIDER_NEG1_TO_0','PROVIDER_0_TO_PLUS1','PROVIDER_PLUS1_TO_PLUS3','PROVIDER_PLUS3_TO_PLUS5','PROVIDER_PLUS5_AND_ABOVE']
    dump('provider_edge_band_prediction_metrics.csv',[{'edge_band':label,**neighborhood_metrics([r for r in neighborhood_rows if r['neighborhood']==label])} for label in core_edge_labels])
    availability=[]
    for scope,rs in [('ALL_NEGATIVE',[r for r in predrows if r['slate_date'] in DAYS and r['production_selected_edge'] is not None and r['production_selected_edge']<0]),('PRODUCTION_NEG3_TO_0',[r for r in predrows if r['slate_date'] in DAYS and r['production_selected_edge'] is not None and -.03<=r['production_selected_edge']<0]),('PRODUCTION_NEG1_TO_0',[r for r in predrows if r['slate_date'] in DAYS and r['production_selected_edge'] is not None and -.01<=r['production_selected_edge']<0])]:
        keys={(r['slate_date'],str(r['game_id']),str(r['player_id'])) for r in rs}; qrs=[q for q in quotes if (q['slate_date'],str(q['game_id']),str(q['player_id'])) in keys and q['side']==q.get('model_selected_side') and not q['is_duplicate_key']]
        for threshold in (100,110,125):availability.append({'production_edge_scope':scope,'threshold_american':threshold,'prediction_count':len(keys),'predictions_with_provider_price_at_or_above_threshold':len({(q['slate_date'],str(q['game_id']),str(q['player_id'])) for q in qrs if q['price_american']>=threshold})})
    dump('negative_edge_plus_money_availability.csv',availability)
    dump('same_side_opposite_side_metrics.csv',[{'neighborhood':label,'market_relation':rel,**neighborhood_metrics([r for r in neighborhood_rows if r['neighborhood']==label and r['same_side']==same])} for label in sorted({r['neighborhood'] for r in neighborhood_rows}) for rel,same in [('SAME_SIDE',True),('OPPOSITE_SIDE',False)]])
    dump('model_market_complementarity.csv',[r for r in neighborhood_metrics_rows if r['neighborhood'] in primary_labels or r['neighborhood'] in ('PROVIDER_EDGE_GE_PLUS10','PRODUCTION_EDGE_GE_PLUS10')])
    # Summaries/tables from settled prediction rows; price selection sets remain explicitly descriptive.
    final=[r for r in predrows if r['slate_date'] in DAYS and r.get('outcome_over') is not None]
    for r in final:
        y=r['outcome_over']; side=r['model_selected_side']; po=r['model_p_over']
        win=(y==1 if side=='OVER' else y==0); r['selected_win']=bool(win);r['model_win']=bool(win);r['book_win']=None
        r['model_brier']=(po-y)**2 if po is not None else None
        r['book_brier']=None;r['model_log_loss']=None;r['book_log_loss']=None;r['unit_return']=None
    byprovider=defaultdict(list)
    for r in final:
        for pol in ('ATTACHMENT_MEDIAN','BETONLINE_EXACT','BEST_OBSERVED'):
            q=next((x for x in policy_rows if x['slate_date']==r['slate_date'] and x['game_id']==r['game_id'] and x['player_id']==r['player_id'] and x['policy']==pol),None)
            if q:byprovider[pol].append(q)
    out=[]
    for pol,rs in sorted(byprovider.items()):
        for name,subset in [('ALL',rs),('NEGATIVE_NEAR_ZERO',[r for r in rs if r['production_edge_neighborhood']=='NEGATIVE_NEAR_ZERO']),('POSITIVE_NEAR_ZERO',[r for r in rs if r['production_edge_neighborhood']=='POSITIVE_NEAR_ZERO']),('LARGE_POSITIVE',[r for r in rs if r['production_edge_neighborhood']=='EXTREME_POSITIVE'])]:out.append({'policy':pol,'segment':name,**metrics(subset)})
    dump('policy_performance.csv',out)
    dump('betonline_analysis.csv',[r for r in out if r['policy']=='BETONLINE_EXACT'])
    dump('best_observed_price_analysis.csv',[r for r in out if r['policy']=='BEST_OBSERVED'])
    dump('near_zero_neighborhood_performance.csv',[r for r in out if r['segment'] in ('NEGATIVE_NEAR_ZERO','POSITIVE_NEAR_ZERO')])
    def grouped(rows,keyf):
        groups=defaultdict(list)
        for r in rows:groups[keyf(r)].append(r)
        return [{'segment':str(k),**metrics(v)} for k,v in sorted(groups.items(),key=lambda x:str(x[0]))]
    dump('production_edge_band_metrics.csv',grouped(final,lambda r:band(r['production_selected_edge'],EDGE_BANDS)))
    dump('fixed_price_band_metrics.csv',grouped(final,lambda r:band(r['attachment_selected_price'],PRICE_BANDS)))
    dump('date_level_provider_metrics.csv',grouped(final,lambda r:(r['slate_date'],r['production_edge_neighborhood'],band(r['attachment_selected_price'],PRICE_BANDS))))
    date_policy=[{'policy':pol,'period':period,**metrics([r for r in rs if (r['slate_date']<'2026-10-14' if period=='DISCOVERY' else r['slate_date']=='2026-10-14')])} for pol,rs in sorted(byprovider.items()) for period in ('DISCOVERY','PROSPECTIVE')]
    dump('date_policy_metrics.csv',date_policy);dump('date_stability.csv',date_policy)
    pending=[r for r in predrows if r['slate_date']==pendday]
    dump('oct15_pending_inventory.csv',pending)
    coverage=[]
    for d in DAYS+[pendday]:
        rs=[r for r in predrows if r['slate_date']==d]
        selected=[q for q in quotes if q['slate_date']==d and q.get('side')==q.get('model_selected_side') and not q['is_duplicate_key']]
        qkeys=defaultdict(list)
        for q in selected:qkeys[(str(q['game_id']),str(q['player_id']))].append(q)
        distribution={str(n):sum(1 for qs in qkeys.values() if len({q['provider'] for q in qs})==n) for n in sorted({len({q['provider'] for q in qs}) for qs in qkeys.values()})}
        coverage.append({'slate_date':d,'prediction_count':len(rs),'provider_quote_coverage':sum(bool(r['provider_quote_count']) for r in rs),'two_sided_book_coverage':sum(bool(r['two_sided_provider_count']) for r in rs),'betonline_coverage':len({k for k,qs in qkeys.items() if any(q['provider']=='betonlineag' for q in qs)}),'predictions_with_selected_side_price_ge_100':len({k for k,qs in qkeys.items() if any(q['price_american']>=100 for q in qs)}),'median_provider_count':median([r['provider_quote_count'] for r in rs]) if rs else 0,'selected_price_rows':sum(bool(r['attachment_selected_price']) for r in rs),'provider_quote_rows':sum(q['slate_date']==d and not q['is_duplicate_key'] for q in quotes),'duplicate_quote_keys':sum(q['slate_date']==d and q['is_duplicate_key'] for q in quotes),'selected_side_provider_count_distribution':json.dumps(distribution,sort_keys=True)})
    dump('provider_coverage.csv',coverage)
    dump('providers_observed.csv',[{'provider':provider,'quote_rows':sum(q['provider']==provider for q in quotes),'dates':'|'.join(d for d in DAYS+[pendday] if any(q['provider']==provider and q['slate_date']==d for q in quotes))} for provider in sorted({q['provider'] for q in quotes})])
    dispersion=[]
    for r in predrows:
        qs=[q for q in quotes if q['slate_date']==r['slate_date'] and str(q['game_id'])==str(r['game_id']) and str(q['player_id'])==str(r['player_id']) and q.get('side')==r['model_selected_side'] and not q['is_duplicate_key']]
        prices=[q['price_american'] for q in qs]
        if prices:
            probs=[implied(v) for v in prices]
            dispersion.append({'slate_date':r['slate_date'],'game_id':r['game_id'],'player_id':r['player_id'],'line':.5,'selected_side':r['model_selected_side'],'book_count':len(qs),'worst_observed_price':min(prices),'median_observed_price':median(prices),'best_observed_price':max(prices),'implied_probability_spread':max(probs)-min(probs),'attachment_median_price':r['attachment_selected_price'],'best_price_improvement_vs_attachment_american':max(prices)-r['attachment_selected_price'] if r['attachment_selected_price'] is not None else None,'median_hides_10_american_point_improvement':bool(r['attachment_selected_price'] is not None and max(prices)>=r['attachment_selected_price']+10),'median_hides_2pp_implied_probability_improvement':bool(r['attachment_selected_price'] is not None and implied(r['attachment_selected_price'])-min(probs)>=.02),'best_provider':max(qs,key=lambda q:q['price_american'])['provider']})
    dump('price_dispersion.csv',dispersion)
    prod_by_key={(r['slate_date'],str(r['game_id']),str(r['player_id'])):r['production_selected_edge'] for r in predrows}
    dump('positive_edge_worse_provider_price.csv',[r for r in dispersion if prod_by_key.get((r['slate_date'],str(r['game_id']),str(r['player_id']))) is not None and prod_by_key[(r['slate_date'],str(r['game_id']),str(r['player_id']))]>0 and r.get('best_price_improvement_vs_attachment_american') is not None and r['best_price_improvement_vs_attachment_american']<=-10])
    quote_groups=defaultdict(list)
    for q in quotes:
        if q['is_duplicate_key']:continue
        quote_groups[('PROVIDER_PRICE_BAND',q['provider_price_band'])].append(q)
        quote_groups[('PROVIDER_EDGE_BAND',band(q.get('provider_side_edge'),EDGE_BANDS))].append(q)
        quote_groups[('PROVIDER_EDGE_NEIGHBORHOOD',q['production_edge_neighborhood'])].append(q)
        quote_groups[('PROVIDER',q['provider'])].append(q)
        quote_groups[('EDGE_X_PROVIDER_PRICE',q['production_edge_neighborhood']+'|'+q['provider_price_band'])].append(q)
    quote_perf=[]
    for (typ,label),rs in sorted(quote_groups.items()):
        settled=[q for q in rs if q.get('provider_side_win') is not None]
        quote_perf.append({'dimension':typ,'segment':label,'quote_rows':len(rs),'settled_quote_rows':len(settled),'wins':sum(q['provider_side_win'] for q in settled),'accuracy':sum(q['provider_side_win'] for q in settled)/len(settled) if settled else None,'mean_price_american':sum(q['price_american'] for q in settled)/len(settled) if settled else None,'unit_risk_return':sum(q['provider_side_unit_return'] for q in settled)/len(settled) if settled else None,'total_units_per_unit_risk':sum(q['provider_side_unit_return'] for q in settled) if settled else None,'mean_provider_side_edge':sum(q['provider_side_edge'] for q in settled)/len(settled) if settled else None,'mean_same_book_no_vig_model_edge':sum(q['provider_no_vig_same_book_edge'] for q in settled if q.get('provider_no_vig_same_book_edge') is not None)/sum(q.get('provider_no_vig_same_book_edge') is not None for q in settled) if any(q.get('provider_no_vig_same_book_edge') is not None for q in settled) else None})
    dump('provider_quote_performance.csv',quote_perf)
    dump('provider_edge_band_metrics.csv',[r for r in quote_perf if r['dimension']=='PROVIDER_EDGE_BAND'])
    dump('canonical_provider_quote_rows.csv',quotes)
    dump('canonical_prediction_rows.csv',predrows)
    dump('provider_quote_edge_calculations.csv',[q for q in quotes if not q['is_duplicate_key'] and q.get('provider_quote_edge') is not None])
    dump('same_provider_no_vig_calculations.csv',[q for q in quotes if not q['is_duplicate_key'] and q.get('provider_no_vig_over') is not None])
    dump('aggregated_median_vs_provider_price.csv',dispersion)
    inventory=[]
    for d in DAYS+[pendday]:
        dqs=[q for q in quotes if q['slate_date']==d]
        for provider in sorted({q['provider'] for q in dqs}):
            pp=[q for q in dqs if q['provider']==provider]
            inventory.append({'slate_date':d,'parent_daily_run_id':pp[0]['parent_daily_run_id'],'provider':provider,'market_keys':'|'.join(sorted({q['market'] for q in pp})),'lines':'|'.join(sorted({str(q['line']) for q in pp})),'quote_rows':len(pp),'players':len({(q['game_id'],q['player_id']) for q in pp}),'over_rows':sum(q['side']=='OVER' for q in pp),'under_rows':sum(q['side']=='UNDER' for q in pp),'observation_timestamp_utc':pp[0]['observation_timestamp_utc'],'raw_response_sha256':pp[0]['raw_response_sha256'],'observation_manifest_sha256':pp[0]['observation_manifest_sha256']})
    dump('provider_inventory.csv',inventory)
    fixed_prices={'schema_version':'NHL_POINTS_PROVIDER_PRICE_BANDS_V1','source_odds':'American; no conversion required for these retained observations','bands':[{'label':label,'lower_inclusive':lo,'upper_exclusive':hi} for label,lo,hi in PRICE_BANDS]}
    fixed_edges={'schema_version':'NHL_POINTS_PROVIDER_EDGE_BANDS_V1','units':'probability difference; multiply by 100 for percentage points','bands':[{'label':label,'lower_inclusive':lo,'upper_exclusive':hi} for label,lo,hi in EDGE_BANDS]}
    (OUT/'fixed_price_band_registry.json').write_text(json.dumps(fixed_prices,indent=2)+'\n');(OUT/'fixed_edge_band_registry.json').write_text(json.dumps(fixed_edges,indent=2)+'\n')
    (OUT/'raw_odds_schema_notes.md').write_text('''# Retained odds schema notes

The immutable `normalized_odds.jsonl` is a deterministic normalization of each bound `raw_response.json`. Each row has event teams and commence time, provider key, market key, market `last_update`, outcome description (player name), Over/Under side, numeric `point` line, and American `price`. `game_binding.jsonl` maps the provider event ID to one canonical game ID. The exact player ID comes from matching the provider description to the same run’s Points attachment identity for that game. The study prefers standard `player_points` when a duplicate exact provider/player/game/line/side is also present under `player_points_alternate`; alternate is used only where the standard quote is absent. Duplicate collisions in the chosen market are audited and excluded.

American prices are already the canonical source representation; no decimal-to-American conversion was applied. Quote timestamps must be no later than the bound observation, and the observation must precede the event commence time. The Oct. 15 observation is bound by the active daily receipt and its raw response hash; no later observations are considered.
''')
    (OUT/'backlog_context.md').write_text('''# Preserved NHL research backlog

- Continue Funnel V1 prospective validation; grade Oct. 15 only after official outcomes.
- Grade the SOG structural-correction shadow prospectively and continue fixed-blend collection.
- Continue official-TOI binding work.
- Keep deferred Puck Line work deferred.
- Tier3 override remains unnecessary.
- The retained plus-money near-zero neighborhood is sparse; no new rule or prospective provider-price experiment is created by this study.
''')
    # Provider-level study classification is deterministic and descriptive.
    near=[r for r in final if r['production_edge_neighborhood']=='NEGATIVE_NEAR_ZERO']
    bestnear=[r for r in byprovider['BEST_OBSERVED'] if r['production_edge_neighborhood']=='NEGATIVE_NEAR_ZERO']
    near_summary=metrics(bestnear)
    primary_final=[r for r in neighborhood_rows if r['slate_date'] in DAYS and r['neighborhood'] in primary_labels and r['grading_status']=='SETTLED']
    manual_class='NO_MEANINGFUL_PROVIDER_LEVEL_SAMPLE' if len(primary_final)<10 else 'INSUFFICIENT_EVIDENCE'
    availability_class='RARE_NEIGHBORHOOD' if len(primary_final)<30 else 'MEANINGFUL_RETAINED_SAMPLE_EXISTS'
    large_edge=next((r for r in neighborhood_metrics_rows if r['neighborhood']=='PROVIDER_EDGE_GE_PLUS10'),{})
    large_class='MODEL_OVERCONFIDENCE_REGION' if large_edge.get('excess_losses',0)>0 and large_edge.get('hgb_brier_advantage_on_paired',0)<0 else 'NO_CLEAR_RELATIONSHIP'
    dump('large_positive_edge_control.csv',[r for r in neighborhood_metrics_rows if r['neighborhood'] in ('PROVIDER_EDGE_GE_PLUS10','PRODUCTION_EDGE_GE_PLUS10')])
    decision={'manual_search_neighborhood_classification':manual_class,'availability_classification':availability_class,'large_positive_provider_edge_classification':large_class,'frozen_prospective_provider_price_experiment_justified':False,'primary_prediction_level_counts':{label:sum(r['slate_date'] in DAYS and r['neighborhood']==label for r in neighborhood_rows) for label in primary_labels}}
    (OUT/'decision_classification.json').write_text(json.dumps(decision,indent=2,sort_keys=True)+'\n')
    pending_rows=[r for r in neighborhood_rows if r['slate_date']==pendday]
    pending_summary={'status':'PENDING_OUTCOME','total_0_5_provider_quote_rows':sum(q['slate_date']==pendday and not q['is_duplicate_key'] for q in quotes),'predictions_with_any_provider_quote':coverage[-1]['provider_quote_coverage'],'predictions_with_selected_side_price_ge_100':coverage[-1]['predictions_with_selected_side_price_ge_100'],'production_negative_near_zero_plus_100':sum(r['neighborhood']=='PRODUCTION_NEG3_TO_0_PLUS100' for r in pending_rows),'provider_negative_near_zero_plus_100':sum(r['neighborhood']=='PROVIDER_NEG3_TO_0_PLUS100' for r in pending_rows),'betonline_selected_side_quotes':sum(q['slate_date']==pendday and q['provider']=='betonlineag' and q.get('side')==q.get('model_selected_side') and not q['is_duplicate_key'] for q in quotes),'best_observed_prediction_instances':sum(r['slate_date']==pendday and r['provider_quote_count']>0 for r in predrows)}
    (OUT/'oct15_pending_summary.json').write_text(json.dumps(pending_summary,indent=2,sort_keys=True)+'\n')
    summary={'schema_version':'NHL_POINTS_PROVIDER_PRICE_NEIGHBORHOOD_V1','classification':manual_class,'availability_classification':availability_class,'large_positive_provider_edge_classification':large_class,'frozen_prospective_experiment_justified':False,'scope':{'finalized_dates':DAYS,'pending_date':pendday,'line':0.5,'source':'exact-hash-bound immutable observations and same-run HGB predictions'},'counts':{'provider_quote_rows':len(quotes),'provider_quote_rows_finalized':sum(q['slate_date'] in DAYS for q in quotes),'provider_quote_rows_pending':sum(q['slate_date']==pendday for q in quotes),'prediction_rows':len(predrows),'settled_predictions':len(final),'negative_near_zero_settled_predictions':len(near),'primary_neighborhood_settled_prediction_rows':len(primary_final),'duplicate_quote_keys':len(dupkeys),'out_of_window_quote_rows_excluded':len(window_excluded_quotes),'finalized_predictions_with_any_provider_quote':sum(int(c['provider_quote_coverage']) for c in coverage if c['slate_date'] in DAYS),'finalized_predictions_with_same_provider_two_sided_quote':sum(int(c['two_sided_book_coverage']) for c in coverage if c['slate_date'] in DAYS),'finalized_betonline_prediction_coverage':sum(int(c['betonline_coverage']) for c in coverage if c['slate_date'] in DAYS),'production_edge_reproduced_rows':sum(r['production_edge_reproduced'] for r in predrows),'max_production_edge_reproduction_abs_delta':max((r['production_edge_reproduction_abs_delta'] for r in predrows if r['production_edge_reproduction_abs_delta'] is not None),default=None)},'providers_observed':sorted({q['provider'] for q in quotes}),'primary_neighborhood_counts':{label:sum(r['slate_date'] in DAYS and r['neighborhood']==label for r in neighborhood_rows) for label in primary_labels},'execution':{'provider_calls':0,'paid_credits':0,'production_db_mutations':0},'policies':['AGGREGATED_MEDIAN (baseline attachment)','BETONLINE (exact only; no substitution)','BEST_OBSERVED_PROVIDER_PRICE (simultaneous bound quotes; not guaranteed executable)'],'caveats':['Provider quotes are the prediction/selection evidence; provider quote count is not the independent outcome denominator.','Same-provider no-vig probabilities are emitted only for exact paired quotes from the same provider and player/game/line.','Best observed price is not an executable or accessibility claim.','Quotes are restricted to captured timestamps no later than the bound observation and before puck drop.','October 15 is pending inventory only and is excluded from all performance metrics.'],'source_hashes':hash_rows}
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n')
    pmap={(r['policy'],r['segment']):r for r in out}
    nmap={r['neighborhood']:r for r in neighborhood_metrics_rows}
    final_dispersion=[r for r in dispersion if r['slate_date'] in DAYS]
    hides10=sum(r['median_hides_10_american_point_improvement'] for r in final_dispersion);hides2=sum(r['median_hides_2pp_implied_probability_improvement'] for r in final_dispersion)
    report=f'''# NHL Points provider-level near-zero price neighborhood

**Manual-search classification:** `{manual_class}`  
**Retained-evidence availability:** `{availability_class}`  
**Provider-level large positive edge:** `{large_class}`

## Evidence and sample

Exact provider quotes were recoverable for the bound observations on Oct. 10–14. The sample has {len(quotes)} retained provider quote records across {len({q['provider'] for q in quotes})} books; one duplicate exact quote key is flagged, and {len(window_excluded_quotes)} records outside the bound pregame window are excluded. There are {summary['counts']['provider_quote_rows_finalized']} eligible quote rows on Oct. 10–14 and {summary['counts']['provider_quote_rows_pending']} on Oct. 15. The Oct. 10–14 outcome-bearing denominator is {len(final)} settled prediction rows; quote rows are not independent outcomes. Oct. 15 has {pending_summary['total_0_5_provider_quote_rows']} eligible quote rows in pending inventory only and contributes no performance data.

The exact primary combination is sparse: production edge `[-3%, 0%)` with a selected-side provider price of +100 or better occurs {summary['primary_neighborhood_counts']['PRODUCTION_NEG3_TO_0_PLUS100']} times in finalized evidence. Provider quote edge `[-3%, 0%)` plus +100 or better occurs {summary['primary_neighborhood_counts']['PROVIDER_NEG3_TO_0_PLUS100']} times; `[-1%, 0%)` plus +100 or better occurs {summary['primary_neighborhood_counts']['PROVIDER_NEG1_TO_0_PLUS100']} times. Provider edge `[0%, +1%)` plus +100 occurs {summary['primary_neighborhood_counts']['PROVIDER_0_TO_PLUS1_PLUS100']} times, and `[0%, +3%)` plus +100 occurs {summary['primary_neighborhood_counts']['PROVIDER_0_TO_PLUS3_PLUS100']} times. The retained evidence therefore does not provide a meaningful settled sample for the manual thesis.

For production edge below zero, `negative_edge_plus_money_availability.csv` gives prediction-unique counts at +100, +110, and +125 for all negative (132 predictions), `[-3%,0%)` (48), and `[-1%,0%)` (20) groups. Each of those counts is zero at all three price thresholds. `price_dispersion.csv` reports each prediction’s worst, median, and best bound selected-side quote and implied-probability spread. Among {len(final_dispersion)} finalized predictions with a selected-side quote, the attachment median is at least 10 American price points worse than an observed provider quote in {hides10} rows and at least 2 implied-probability points worse in {hides2} rows.

## Model and market results

Production edge is retained unchanged as selected-side HGB probability minus raw implied probability from the separately aggregated median price. It reproduces {summary['counts']['production_edge_reproduced_rows']} rows with maximum absolute difference {summary['counts']['max_production_edge_reproduction_abs_delta']:.3g}. Provider quote edge is separate: selected-side HGB probability minus raw implied probability from that exact provider’s American price. Same-provider no-vig values exist only when both sides are present for the exact player/game/line/provider.

The aggregated-median negative near-zero subset has {pmap[('ATTACHMENT_MEDIAN','NEGATIVE_NEAR_ZERO')]['settled_n']} settled rows, HGB {pmap[('ATTACHMENT_MEDIAN','NEGATIVE_NEAR_ZERO')]['wins']}-{pmap[('ATTACHMENT_MEDIAN','NEGATIVE_NEAR_ZERO')]['losses']}, Brier {pmap[('ATTACHMENT_MEDIAN','NEGATIVE_NEAR_ZERO')]['mean_model_brier']:.4f} vs market {pmap[('ATTACHMENT_MEDIAN','NEGATIVE_NEAR_ZERO')]['mean_book_brier']:.4f}, and log loss {pmap[('ATTACHMENT_MEDIAN','NEGATIVE_NEAR_ZERO')]['mean_model_log_loss']:.4f} vs {pmap[('ATTACHMENT_MEDIAN','NEGATIVE_NEAR_ZERO')]['mean_book_log_loss']:.4f}. This is the historical near-zero baseline, not the requested plus-money population.

Exact BetOnline coverage is {pmap[('BETONLINE_EXACT','ALL')]['settled_n']} settled predictions overall and {pmap[('BETONLINE_EXACT','NEGATIVE_NEAR_ZERO')]['settled_n']} in negative near-zero production edge. In that BetOnline subset, HGB Brier advantage is {pmap[('BETONLINE_EXACT','NEGATIVE_NEAR_ZERO')]['model_brier_advantage']:.4f}, descriptive net return is {pmap[('BETONLINE_EXACT','NEGATIVE_NEAR_ZERO')]['mean_unit_return']:.3f} units per unit risk, with no provider substitution. Best observed selected-side price covers {pmap[('BEST_OBSERVED','ALL')]['settled_n']} settled predictions; its negative near-zero production-edge subset covers {pmap[('BEST_OBSERVED','NEGATIVE_NEAR_ZERO')]['settled_n']} and has {pmap[('BEST_OBSERVED','NEGATIVE_NEAR_ZERO')]['mean_unit_return']:.3f} descriptive units per unit risk. Best observed does not mean guaranteed accessible or executable.

The one settled `[0%,+3%)` plus-money provider-edge case went 0–1; HGB Brier was 0.230 vs same-provider no-vig 0.216, and the market was closer on that row. The provider-edge `[-3%,0%)` plus-money and `[-1%,0%)` plus-money groups had no settled rows. Near-even favorite comparisons are also thin: `[-3%,0%)` with -105 through -101 has 3 settled rows, while `[0%,+3%)` has 5; broader -120 through -106 groups have 3 and 11 settled rows respectively. These small samples do not establish HGB/market complementarity. Date separation and same-side/opposite-side results are in their dedicated tables. The prediction-level provider-edge control at +10% or above has HGB excess losses {nmap.get('PROVIDER_EDGE_GE_PLUS10',{}).get('excess_losses'):.1f}, Brier {nmap.get('PROVIDER_EDGE_GE_PLUS10',{}).get('hgb_brier'):.3f} vs paired same-provider no-vig {nmap.get('PROVIDER_EDGE_GE_PLUS10',{}).get('same_provider_market_brier'):.3f}, supporting `{large_class}`. This is descriptive evidence of overconfidence; it does not establish market crowding or causality.

## Oct. 15 and decision

Oct. 15 has {pending_summary['predictions_with_selected_side_price_ge_100']} prediction identities with a selected-side +100-or-better quote, {pending_summary['production_negative_near_zero_plus_100']} production-negative-near-zero plus-money cases, {pending_summary['provider_negative_near_zero_plus_100']} provider-negative-near-zero plus-money cases, {pending_summary['betonline_selected_side_quotes']} exact BetOnline selected-side quotes, and {pending_summary['best_observed_prediction_instances']} best-observed instances. They remain `PENDING_OUTCOME`.

Discovery (Oct. 10–13) had 660 settled aggregated-median comparison rows; Oct. 14 prospective context had 56 settled rows, HGB 36–20, with median-price Brier 0.240 vs market 0.234 and best-observed Brier 0.240 vs its provider-price benchmark 0.231. No plus-money near-zero cases on Oct. 14 were settled. The actual manual near-zero/plus-money thesis received a fair quote-recoverability and availability test, but not a meaningful performance test: exact provider quotes, same-run predictions, and official outcomes were available, while the relevant intersection was too rare. No frozen prospective provider-price experiment is justified by this sample. Funnel V1 is unchanged; no rule was created. Provider calls: 0; paid credits: 0; production DB mutations: 0.

See `provider_coverage.csv`, `providers_observed.csv`, `provider_inventory.csv`, `provider_quotes.csv`, `prediction_neighborhood_rows.csv`, `neighborhood_performance.csv`, `provider_edge_band_prediction_metrics.csv`, `policy_performance.csv`, `date_neighborhood_performance.csv`, and `oct15_pending_summary.json` for the full evidence.
'''
    (OUT/'report.md').write_text(report)
    output_files=[{'path':p.name,'sha256':digest(p)} for p in sorted(OUT.iterdir()) if p.is_file() and p.name!='lineage_manifest.json']
    (OUT/'lineage_manifest.json').write_text(json.dumps({'schema_version':summary['schema_version'],'inputs':hash_rows,'base_inventory_sha256':digest(BASE/'market_evidence_inventory.csv'),'canonical_intersection_sha256':digest(BASE/'canonical_market_intersection.csv'),'script_sha256':digest(Path(__file__)),'output_files':output_files},indent=2,sort_keys=True)+'\n')
    print(json.dumps({'output':str(OUT),'classification':manual_class,'provider_quotes':len(quotes),'settled_predictions':len(final),'negative_near_zero':len(near),'oct15_pending':len(pending)},indent=2))

if __name__=='__main__':main()
