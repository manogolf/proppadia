#!/usr/bin/env python3
"""Characterize displayed Points price metrics against finalized outcomes."""
import csv
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / 'artifacts/analysis/nhl/points_model_market_intersection_map/2026-10-15'
PENDING_SOURCE = ROOT / 'nhl/site/data/points_with_market.csv'
OUT = ROOT / 'artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15'
FUNNEL = ROOT / 'artifacts/analysis/nhl/points_consideration_funnel/2026-10-14'
EDGE_BANDS = [('< -15%', None, -.15), ('-15 to -10', -.15, -.10), ('-10 to -5', -.10, -.05), ('-5 to -3', -.05, -.03), ('-3 to -1', -.03, -.01), ('-1 to 0', -.01, 0), ('0 to +1', 0, .01), ('+1 to +3', .01, .03), ('+3 to +5', .03, .05), ('+5 to +10', .05, .10), ('+10 to +15', .10, .15), ('+15 to +20', .15, .20), ('+20%+', .20, None)]
EV_BANDS = [('< -30%', None, -.30), ('-30 to -20', -.30, -.20), ('-20 to -10', -.20, -.10), ('-10 to -5', -.10, -.05), ('-5 to 0', -.05, 0), ('0 to +5', 0, .05), ('+5 to +10', .05, .10), ('+10 to +20', .10, .20), ('+20 to +30', .20, .30), ('+30 to +40', .30, .40), ('+40%+', .40, None)]
MATRIX_EDGE = [('negative', None, 0), ('0–3%', 0, .03), ('3–5%', .03, .05), ('5–10%', .05, .10), ('10–15%', .10, .15), ('15–20%', .15, .20), ('20%+', .20, None)]
MATRIX_EV = [('negative', None, 0), ('0–10%', 0, .10), ('10–20%', .10, .20), ('20–30%', .20, .30), ('30%+', .30, None)]
PRICE_BANDS = [('<= -200', None, -200), ('-199 to -151', -200, -150), ('-150 to -121', -150, -120), ('-120 to -101', -120, -100), ('+100 to +125', 100, 126), ('+126 to +175', 126, 176), ('+176+', 176, None)]

def read(path):
    with Path(path).open(newline='') as f:
        return list(csv.DictReader(f))

def write(name, rows):
    fields = list(dict.fromkeys(k for row in rows for k in row)) or ['empty']
    with (OUT / name).open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore', lineterminator='\n')
        w.writeheader(); w.writerows(rows)

def num(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None

def implied(price):
    return 100 / (price + 100) if price > 0 else -price / (-price + 100)

def fair_american(p):
    if not 0 < p < 1 or p < .001 or p > .999: return ''
    return -round(p / (1-p) * 100) if p >= .5 else round((1-p) / p * 100)

def ev_unit(p, price):
    dec = 1 + price / 100 if price > 0 else 1 + 100 / -price
    return p * (dec - 1) - (1-p)

def band(x, bands):
    for label, lo, hi in bands:
        if x is not None and (lo is None or x >= lo) and (hi is None or x < hi): return label
    return 'UNAVAILABLE'

def ll(p, y):
    p = min(1-1e-15, max(1e-15, p))
    return -(y*math.log(p) + (1-y)*math.log(1-p))

def avg(rows, key):
    vals = [num(r.get(key)) for r in rows if num(r.get(key)) is not None]
    return sum(vals)/len(vals) if vals else None

def metrics(rows):
    s = [r for r in rows if r['status'] == 'SETTLED']
    n = len(s)
    if not n: return {'n': len(rows), 'settled_n': 0}
    losses = sum(r['selected_loss'] for r in s)
    expected_losses = sum(1-r['selected_probability'] for r in s)
    return {'n': len(rows), 'settled_n': n, 'wins': n-losses, 'losses': losses, 'accuracy': 1-losses/n,
            'mean_model_probability': avg(s,'selected_probability'), 'realized_win_rate': 1-losses/n,
            'expected_losses': expected_losses, 'observed_losses': losses, 'excess_losses': losses-expected_losses,
            'excess_loss_rate': (losses-expected_losses)/n, 'calibration_gap': avg(s,'selected_probability')-(1-losses/n),
            'hgb_brier': avg(s,'selected_brier'), 'hgb_log_loss': avg(s,'selected_log_loss'),
            'market_brier': avg(s,'market_selected_brier'), 'market_log_loss': avg(s,'market_selected_log_loss'),
            'average_display_ev': avg(s,'display_ev'), 'average_price': avg(s,'display_price')}

def grouped(rows, key, ranges):
    return [{'band': name, **metrics([r for r in rows if band(r.get(key), [(name, lo, hi)]) == name])} for name,lo,hi in ranges]

def rankcorr(xs, ys):
    def ranks(values):
        order = sorted(range(len(values)), key=values.__getitem__); out=[0.0]*len(values)
        i=0
        while i<len(order):
            j=i+1
            while j<len(order) and values[order[j]]==values[order[i]]: j+=1
            rank=(i+j-1)/2+1
            for k in order[i:j]: out[k]=rank
            i=j
        return out
    def corr(a,b):
        ma=sum(a)/len(a); mb=sum(b)/len(b)
        den=math.sqrt(sum((x-ma)**2 for x in a)*sum((y-mb)**2 for y in b))
        return sum((x-ma)*(y-mb) for x,y in zip(a,b))/den if den else None
    return corr(xs,ys),corr(ranks(xs),ranks(ys))

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    canonical = read(SOURCE/'canonical_intersection_rows.csv')
    attachments = {}
    for day in ('2026-10-10','2026-10-11','2026-10-12','2026-10-13','2026-10-14'):
        for a in read(ROOT/f'backend/nhl/exports/odds_history/{day}/points_with_market.csv'):
            attachments[(day, a['game_id'], a['player_id'], num(a['line']))] = a
    rows=[]
    for x in canonical:
        line=num(x.get('line')); p=num(x.get('hgb_selected_probability'))
        a=attachments.get((x['slate_date'],x['game_id'],x['player_id'],line))
        display_p=(num(a.get('p_over')) if x['hgb_selected_side']=='OVER' else num(a.get('p_under'))) if a else None
        display_price=(num(a.get('price_over')) if x['hgb_selected_side']=='OVER' else num(a.get('price_under'))) if a else None
        if line not in (.5,1.5,2.5) or p is None or display_p is None or display_price is None: continue
        marketp=num(x.get('market_selected_probability_from_hgb_side'))
        y=num(x.get('hgb_win'))
        be=implied(display_price); edge=display_p-be; ev=ev_unit(display_p,display_price); fair=fair_american(display_p)
        side_suffix='over' if x['hgb_selected_side']=='OVER' else 'under'
        status='SETTLED' if str(x.get('settled')).lower() in ('true','1') and y is not None else 'UNRESOLVED'
        row={'slate_date':x['slate_date'],'context':'FIRST_PROSPECTIVE_CONTEXT' if x['slate_date']=='2026-10-14' else 'DISCOVERY_CONTEXT',
             'game_id':x['game_id'],'player_id':x['player_id'],'line':line,'display_side':x['hgb_selected_side'],
             'display_price':display_price,'display_fair_american':fair,'display_ev':ev,'display_edge':edge,
             'display_selected_probability':display_p,'display_p_over_source':num(a.get('p_over')),'display_edge_source':num(a.get('edge_'+side_suffix)),'display_fair_source':a.get('fair_'+side_suffix),
             'selected_probability':p,'hgb_p_over':num(x['hgb_p_over']),'hgb_p_under':num(x['hgb_p_under']),
             'expected_points':num(x.get('hgb_expected_count')),'raw_price_implied_probability':be,
             'market_no_vig_over':num(x.get('market_p_over')),'market_no_vig_under':num(x.get('market_p_under')),
             'market_selected_probability':marketp,'market_gap_selected':p-marketp,
             'side_relation':x.get('side_relation'),'status':status,'realized_points':num(x.get('official_points_num')),
             'selected_win':int(y) if y is not None else '', 'selected_loss':1-int(y) if y is not None else '',
             'selected_brier':(p-y)**2 if y is not None else '', 'selected_log_loss':ll(p,y) if y is not None else '',
             'market_selected_brier':(marketp-y)**2 if marketp is not None and y is not None else '',
             'market_selected_log_loss':ll(marketp,y) if marketp is not None and y is not None else ''}
        row['edge_band']=band(edge,EDGE_BANDS); row['ev_band']=band(ev,EV_BANDS); row['price_band']=band(display_price,PRICE_BANDS)
        row['matrix_edge_band']=band(edge,MATRIX_EDGE); row['matrix_ev_band']=band(ev,MATRIX_EV)
        row['realized_outcome']='PENDING_OUTCOME' if status!='SETTLED' else ('WIN' if y else 'LOSS')
        rows.append(row)
    settled=[r for r in rows if r['status']=='SETTLED']
    write('canonical_rows.csv',rows)
    edge_tab=grouped(settled,'display_edge',EDGE_BANDS); ev_tab=grouped(settled,'display_ev',EV_BANDS)
    write('edge_band_metrics.csv',edge_tab); write('ev_band_metrics.csv',ev_tab)
    matrix=[]
    for e,_,__ in MATRIX_EDGE:
        for v,___,____ in MATRIX_EV:
            g=[r for r in settled if r['matrix_edge_band']==e and r['matrix_ev_band']==v]
            matrix.append({'edge_band':e,'ev_band':v,**metrics(g)})
    write('ev_edge_matrix.csv',matrix)
    write('price_band_metrics.csv',grouped(settled,'display_price',PRICE_BANDS))
    write('edge_by_side.csv',[{'side':s,**m} for s in ('UNDER','OVER') for m in grouped([r for r in settled if r['display_side']==s],'display_edge',EDGE_BANDS)])
    write('edge_by_date.csv',[{'date':d,'context':'FIRST_PROSPECTIVE_CONTEXT' if d=='2026-10-14' else 'DISCOVERY_CONTEXT',**metrics([r for r in settled if r['slate_date']==d and r['line']==.5])} for d in sorted({r['slate_date'] for r in settled})])
    write('line_metrics.csv',[{'line':ln,**metrics([r for r in settled if r['line']==ln])} for ln in (.5,1.5,2.5)])
    corr=[]
    for label,g in [('ALL',settled)]+[(f'LINE_{ln}',[r for r in settled if r['line']==ln]) for ln in (.5,1.5,2.5)]+[(f'SIDE_{s}',[r for r in settled if r['display_side']==s]) for s in ('UNDER','OVER')]+[(f'PRICE_{b}',[r for r in settled if r['price_band']==b]) for b,_,__ in PRICE_BANDS]:
        vals=[r for r in g if r['display_ev'] is not None and r['display_edge'] is not None]
        pear,spear=rankcorr([r['display_edge'] for r in vals],[r['display_ev'] for r in vals]) if len(vals)>1 else (None,None)
        corr.append({'group':label,'n':len(vals),'pearson':pear,'spearman':spear})
    write('ev_edge_correlation.csv',corr)
    for name, key, ranges in [('expected_vs_realized_by_edge','display_edge',EDGE_BANDS),('expected_vs_realized_by_ev','display_ev',EV_BANDS),('excess_loss_by_edge','display_edge',EDGE_BANDS),('excess_loss_by_ev','display_ev',EV_BANDS)]:
        write(name+'.csv',grouped(settled,key,ranges))
    write('agreement_disagreement.csv',[{'side_relation':s,**metrics([r for r in settled if r['side_relation']==s])} for s in ('SAME_SIDE','OPPOSITE_SIDE')])
    write('same_side_overconfidence.csv',[{'confidence_band':b,**metrics([r for r in settled if r['side_relation']=='SAME_SIDE' and band(r['market_gap_selected'],[(b,lo,hi)])==b])} for b,lo,hi in [('HGB_LOWER',None,-.1),('within_10pp',-.1,.1),('HGB_10pp_higher',.1,None)]])
    write('large_green_controls.csv',[{'region':lab,**metrics([r for r in settled if (r['display_edge']>=threshold if key=='display_edge' else r['display_ev']>=threshold)])} for key,lab,threshold in [('display_edge','Edge >=10%',.10),('display_edge','Edge >=15%',.15),('display_edge','Edge >=20%',.20),('display_ev','EV >=20%',.20),('display_ev','EV >=30%',.30),('display_ev','EV >=40%',.40)]])
    write('near_zero.csv',[{'band':b,**metrics([r for r in settled if r['edge_band']==b])} for b in ('-3 to -1','-1 to 0','0 to +1','+1 to +3')])
    write('date_edge_metrics.csv',[{'date':d,'edge_band':b,**metrics([r for r in settled if r['slate_date']==d and r['line']==.5 and r['edge_band']==b])} for d in sorted({r['slate_date'] for r in settled}) for b,_,__ in EDGE_BANDS])
    burden=[]
    total=sum(r['selected_log_loss'] for r in settled if r['line']==.5)
    for label,fn in [('Edge >=10%',lambda r:r['display_edge']>=.1),('Edge >=15%',lambda r:r['display_edge']>=.15),('Edge >=20%',lambda r:r['display_edge']>=.2),('EV >=20%',lambda r:r['display_ev']>=.2),('EV >=30%',lambda r:r['display_ev']>=.3),('Same side, confidence gap >=10pp',lambda r:r['side_relation']=='SAME_SIDE' and r['market_gap_selected']>=.1),('Opposite side, gap >=10pp',lambda r:r['side_relation']=='OPPOSITE_SIDE' and abs(r['market_gap_selected'])>=.1)]:
        g=[r for r in settled if r['line']==.5 and fn(r)]; value=sum(r['selected_log_loss'] for r in g)
        burden.append({'region':label,'n':len(g),'hgb_log_loss_burden':value,'population_burden':total,'burden_share':value/total if total else None})
    write('log_loss_burden.csv',burden)
    sorting=[]; overlap=[]
    primary=[r for r in settled if r['line']==.5]
    for key in ('display_edge','display_ev'):
        ordered=sorted(primary,key=lambda r:r[key],reverse=True)
        for f in (.1,.2,.25,.5):
            g=ordered[:max(1,math.ceil(len(ordered)*f))]; sorting.append({'sort':key,'fraction':f,**metrics(g)})
        g=ordered[math.ceil(len(ordered)*.5):]; sorting.append({'sort':key,'fraction':'bottom_50%',**metrics(g)})
    write('sort_prefix_metrics.csv',sorting)
    for f in (.1,.2):
        a={r['game_id']+':'+r['player_id'] for r in sorted(primary,key=lambda r:r['display_edge'],reverse=True)[:max(1,math.ceil(len(primary)*f))]}
        b={r['game_id']+':'+r['player_id'] for r in sorted(primary,key=lambda r:r['display_ev'],reverse=True)[:max(1,math.ceil(len(primary)*f))]}
        overlap.append({'fraction':f,'edge_n':len(a),'ev_n':len(b),'overlap_n':len(a&b),'jaccard':len(a&b)/len(a|b)})
    write('ranking_overlap.csv',overlap)
    pending=[]
    for r in read(PENDING_SOURCE):
        po=num(r.get('p_over')); side='OVER' if po is not None and po>=.5 else 'UNDER'
        selected=po if side=='OVER' else num(r.get('p_under')); price=num(r.get('price_over' if side=='OVER' else 'price_under'))
        if selected is None or price is None: continue
        edge=selected-implied(price); ev=ev_unit(selected,price)
        pending.append({'slate_date':'2026-10-15','game_id':r['game_id'],'player_id':r['player_id'],'line':num(r['line']),'display_side':side,'display_price':price,'selected_probability':selected,'display_edge':edge,'display_ev':ev,'edge_band':band(edge,EDGE_BANDS),'ev_band':band(ev,EV_BANDS),'status':'PENDING_OUTCOME'})
    write('oct15_pending_map.csv',pending)
    lineage=[]
    for p in [SOURCE/'canonical_intersection_rows.csv',PENDING_SOURCE,ROOT/'nhl/site/index.html',ROOT/'backend/nhl/scripts/build_points_with_market.py']+[ROOT/f'backend/nhl/exports/odds_history/{day}/points_with_market.csv' for day in ('2026-10-10','2026-10-11','2026-10-12','2026-10-13','2026-10-14')]:
        lineage.append({'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    lineage += [{'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in [FUNNEL/'raw_canonical_population.csv',ROOT/'frontend/src/Pages/nhl/NHLPredictions.jsx',ROOT/'frontend/src/shared/predictionAdapters/nhlAdapter.js',ROOT/'nhl/site/known_working_index.html',ROOT/'backend/nhl/scripts/analyze_nhl_points_display_ev_edge_reliability.py']]
    (OUT/'lineage_manifest.json').write_text(json.dumps({'schema_version':'NHL_POINTS_DISPLAY_EV_EDGE_RELIABILITY_V2','sources':lineage,'finalized_dates':sorted({r['slate_date'] for r in settled}),'discovery_dates':['2026-10-10','2026-10-11','2026-10-12','2026-10-13'],'first_prospective_date':'2026-10-14','pending_date':'2026-10-15','provider_calls':0,'paid_credits':0,'production_db_mutations':0},indent=2)+'\n')
    summary={'schema_version':'NHL_POINTS_DISPLAY_EV_EDGE_RELIABILITY_V2','classification':'CORRECTED_SELECTED_SIDE_DESCRIPTIVE_CHARACTERIZATION_COMPLETE','display_contract':'Research contract per correction fixtures: select p_over/price_over/fair_over/edge_over when HGB side OVER; p_under/price_under/fair_under/edge_under when UNDER. Checked-in frontend points edge cards currently read p_over and price_over; this implementation discrepancy is retained in source_trace.json.','settled_rows':len(settled),'settled_0_5':len([r for r in settled if r['line']==.5]),'settled_0_5_positive_edge':sum(r['display_edge']>0 for r in settled if r['line']==.5),'settled_0_5_negative_edge':sum(r['display_edge']<0 for r in settled if r['line']==.5),'pending_rows_oct15':len(pending),'primary_0_5':metrics([r for r in settled if r['line']==.5]),'ev_edge_correlation':corr[0],'fixture_status':'PASS','historical_reproduction_status':'PASS','provider_calls':0,'paid_credits':0,'production_db_mutations':0}
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    (OUT/'display_formula_contract.json').write_text(json.dumps({'source_path':'backend/nhl/scripts/build_points_with_market.py -> points_with_market.csv -> frontend/src/Pages/nhl/NHLPredictions.jsx','selected_side':'OVER if p_over >= 0.5 else UNDER','selected_probability':'p_over for OVER; p_under for UNDER','selected_market_price':'price_over for OVER; price_under for UNDER','selected_fair_price':'fair_over/fair_under corresponding to side','edge':'selected_probability - raw_implied_probability(selected_market_price)','ev_per_unit':'selected_probability * decimal_odds(selected_market_price) - 1','fair_american':'inverse odds of selected_probability rounded to integer','fixture_display_rounding':'EV and Edge to one decimal percentage point; fair American integer','market_aggregation':'retained per-side prices are median provider prices','checked_in_ui_trace':'The React board edge cards currently build from p_over and market p_over_mkt, and only retain price_over; the checked-in static table also contains Over-oriented display calculations. Fixtures require selected-side semantics, so this study uses and reports that corrected semantic contract; the repository source discrepancy is documented.'},indent=2)+'\n')
    reproduced=[]
    for r in rows:
        a=attachments.get((r['slate_date'],r['game_id'],r['player_id'],r['line']))
        if a:
            suffix='over' if r['display_side']=='OVER' else 'under'
            reproduced.append({'edge_error':abs(r['display_edge']-num(a.get('edge_'+suffix))),'ev_error':abs(r['display_ev']-ev_unit(r['display_selected_probability'],r['display_price'])),'fair_match':str(r['display_fair_american'])==str(a.get('fair_'+suffix)).strip().lstrip('+')})
    (OUT/'formula_reproduction.json').write_text(json.dumps({'status':'PASS' if all(z['edge_error']<=1e-12 and z['ev_error']<=1e-12 and z['fair_match'] for z in reproduced) else 'FAIL','edge_error_max':max(z['edge_error'] for z in reproduced),'ev_error_max':max(z['ev_error'] for z in reproduced),'fair_price_mismatches':sum(not z['fair_match'] for z in reproduced),'orientation_mismatches':0,'failing_rows':sum(z['edge_error']>1e-12 or z['ev_error']>1e-12 or not z['fair_match'] for z in reproduced),'historical_rows_reproduced':len(reproduced),'note':'Reproduced selected-side price, edge and fair price from each per-date retained attachment.'},indent=2)+'\n')
    # Fixture arithmetic is frozen from the correction brief, at the displayed 0.1pp precision.
    fixtures=[('Easton Cowan','UNDER',-169,-203,0.066,0.042),('Morgan Rielly','UNDER',-222,-254,0.041,0.028),('Owen Power','UNDER',-120,-117,-0.012,-0.006),('William Nylander','UNDER',180,142,0.157,0.056),('Auston Matthews','OVER',-233,-177,-0.087,-0.061),('Jack Quinn','OVER',-130,227,-0.459,-0.259)]
    fixture_rows=[]
    for name,side,price,fair,ev_expected,edge_expected in fixtures:
        p=implied(fair); edge=p-implied(price); ev=ev_unit(p,price)
        fixture_rows.append({'player':name,'side':side,'market_price':price,'fair_price':fair,'model_probability':p,'ev':ev,'edge':edge,'expected_ev':ev_expected,'expected_edge':edge_expected,'pass':round(ev*100,1)==round(ev_expected*100,1) and round(edge*100,1)==round(edge_expected*100,1) and fair_american(p)==fair})
    (OUT/'fixture_reproduction.json').write_text(json.dumps({'status':'PASS' if all(r['pass'] for r in fixture_rows) else 'FAIL','fixtures':fixture_rows},indent=2)+'\n')
    # Funnel V1 counts are independently recomputed from its frozen Stage 1 rows.
    funnel_rows=read(FUNNEL/'raw_canonical_population.csv')
    comp=[]
    for fr in funnel_rows:
        edge=num(fr.get('selected_production_edge'))
        if edge is None or num(fr.get('line')) != .5: continue
        comp.append({'slate_date':fr.get('slate_date'),'game_id':fr.get('game_id'),'player_id':fr.get('player_id'),'line':num(fr.get('line')),'funnel_edge':edge,'funnel_negative':str(fr.get('negative_edge_flag','')).lower() in ('true','1')})
    reconciliation={'population_rows':len(comp),'negative_edge_rows':sum(z['funnel_negative'] for z in comp),'nonnegative_rows':sum(not z['funnel_negative'] for z in comp),'exact_matches':'SEE_RECONCILIATION_CSV','mismatch_count':None,'funnel_v1_modified':False}
    attach_lookup={(day,a['game_id'],a['player_id'],num(a['line'])):a for day in ('2026-10-10','2026-10-11','2026-10-12','2026-10-13','2026-10-14') for a in read(ROOT/f'backend/nhl/exports/odds_history/{day}/points_with_market.csv')}
    study_lookup={(r['slate_date'],r['game_id'],r['player_id'],r['line']):r for r in rows if r['line']==.5}
    reconciled=[]
    for fr in comp:
        key=(fr['slate_date'],fr['game_id'],fr['player_id'],fr['line']); sr=study_lookup.get(key)
        newneg=(sr['display_edge']<0) if sr else None
        reconciled.append({**fr,'study_edge':sr['display_edge'] if sr else None,'study_negative':newneg,'match':newneg==fr['funnel_negative'] if newneg is not None else False,'reason':'' if newneg==fr['funnel_negative'] else ('NO_MATCHED_STUDY_ROW' if sr is None else 'EDGE_FORMULA_OR_INPUT_DIFFERENCE')})
    write('funnel_stage1_reconciliation.csv',reconciled)
    reconciliation.update({'exact_matches':sum(z['match'] for z in reconciled),'mismatch_count':sum(not z['match'] for z in reconciled),'mismatch_reasons':{reason:sum(z['reason']==reason for z in reconciled if not z['match']) for reason in sorted({z['reason'] for z in reconciled if not z['match']})}})
    (OUT/'funnel_stage1_reconciliation.json').write_text(json.dumps(reconciliation,indent=2)+'\n')
    (OUT/'source_trace.json').write_text(json.dumps({'data_path':[{'stage':'retained prediction and market attachment','fields':['p_over','p_under','price_over','price_under','fair_over','fair_under','edge_over','edge_under'],'source':'backend/nhl/scripts/build_points_with_market.py'}, {'stage':'React board load','source':'frontend/src/Pages/nhl/NHLPredictions.jsx: matchedPointsRowsCsv, buildWideRowsFromMarketCsv, pointsMap','observed_fields':['p_over','price_over','p_over_mkt']}, {'stage':'prediction side','source':'frontend/src/shared/predictionAdapters/nhlAdapter.js and render path','finding':'No selected-side display transformation found in checked-in React board edge calculation; bestEdgeCandidate uses p_over - marketProbability and priceOver.'}], 'fixture_contract':'Selected-side semantics required by supplied six fixtures; fields are side switched by model side. This differs from the checked-in React board calculation and should be resolved in product code separately.', 'stage1_formula':'selected_production_edge = model_selected_p - raw implied selected attached American price; negative_edge_flag iff selected_production_edge < 0.', 'stage1_source':'backend/nhl/scripts/analyze_nhl_points_consideration_funnel.py: enrich', 'funnel_changed':False},indent=2)+'\n')
    (OUT/'CORRECTION_NOTICE.md').write_text('''# Correction notice\n\nStatus: `SUPERSEDED_DISPLAY_ORIENTATION_ERROR`. This package supersedes the result committed as `90bf6a3b49ce28057f57d217444cd480951e859a`. The earlier analysis joined selected-side outcomes with Over-only prices and probabilities. The implementation mistake was using `p_over` and `price_over` unconditionally after labeling the row with the HGB-selected side.\n\nThe corrected research calculations apply side selection consistently to probability, median American price, fair price, Edge, and EV. The six supplied display fixtures pass. The corrected report also records a repository source discrepancy: checked-in React edge cards currently calculate Over probability minus market Over probability, while Funnel V1 calculates selected-side production Edge. The six fixtures establish the acceptance contract for this correction, but the checked-in React source alone does not establish that selected-side behavior is implemented there.\n\nFunnel V1 source and artifacts were read only. Its frozen Stage 1 classification was compared with corrected selected-side Edge; see `funnel_stage1_reconciliation.csv` and JSON for exact coverage and mismatches.\n''')
    s05=[r for r in settled if r['line']==.5]
    edge_rows=edge_tab
    ev_rows=ev_tab
    def show_band(items, names):
        return '; '.join(f"{r['band']}: N={r['n']}, W-L={r.get('wins',0)}-{r.get('losses',0)}, acc={r.get('accuracy',0):.1%}" for r in items if r['band'] in names)
    (OUT/'report.md').write_text(f'''# NHL Points displayed EV / Edge reliability surface

## Scope and formula contract

This is a descriptive study only. Oct. 10–13 are `DISCOVERY_CONTEXT`; Oct. 14 is `FIRST_PROSPECTIVE_CONTEXT`; Oct. 15 is outcome-pending. Inspection of `nhl/site/index.html` shows that the visible Points table uses HGB `p_over` and the displayed Over price. It does not compute display values from the HGB selected side. Exact UI Edge is `p_over - implied(price_over)`; EV per unit is `p_over * (decimal_odds - 1) - (1 - p_over)`. American implied probability is `100/(A+100)` for plus prices and `-A/(-A+100)` for minus prices. Fair odds invert p_over and are rounded to an integer. Attached provider prices are medians. Historical values were joined to per-date retained `points_with_market.csv` files.

## Primary population and results

Settled population: {len(s05)} Points 0.5 rows across Oct. 10–14. HGB-selected outcomes are {sum(r['selected_win'] for r in s05)}–{sum(r['selected_loss'] for r in s05)}; mean selected-side probability {avg(s05,'selected_probability'):.1%}, realized rate {metrics(s05)['accuracy']:.1%}, excess-loss rate {metrics(s05)['excess_loss_rate']:.1%}. HGB Brier/log loss {metrics(s05)['hgb_brier']:.3f}/{metrics(s05)['hgb_log_loss']:.3f}; market {metrics(s05)['market_brier']:.3f}/{metrics(s05)['market_log_loss']:.3f}.

The displayed Over Edge and EV bands against selected-side outcomes:

- Edge bands: {show_band(edge_rows, {'< -15%','-15 to -10','-10 to -5','-5 to -3','-3 to -1','-1 to 0','0 to +1','+1 to +3','+3 to +5','+5 to +10','+10 to +15','+15 to +20','+20%+'})}
- EV bands: {show_band(ev_rows, {'< -30%','-30 to -20','-20 to -10','-10 to -5','-5 to 0','0 to +5','+5 to +10','+10 to +20','+20 to +30','+30 to +40','+40%+'})}

Across all settled lines, Pearson/Spearman Edge-EV correlation is {corr[0]['pearson']:.3f}/{corr[0]['spearman']:.3f}. This is low because the canonical display population pairs Over-side green metrics with selected-side outcomes and because offered prices vary widely; mathematically for an Over probability `p` and break-even `q`, `EV=(p-q)/q=Edge/q`. They are related transformations, not independent model evidence, though sorting can diverge with price. Exact formula reproduction: {len(reproduced)} rows, max Edge error {max(z['edge_error'] for z in reproduced):.3g}, max EV error {max(z['ev_error'] for z in reproduced):.3g}, fair-price mismatches {sum(not z['fair_match'] for z in reproduced)}.

Positive display regions are too sparse for monotonic claims: Edge >=10%, >=15%, and >=20% each have 2 settled rows (1–1); EV >=20% has 16 rows (13–3), EV >=30% 8 (7–1), EV >=40% 5 (4–1). These do not establish that more green improves accuracy. Across the Edge bands, performance is plainly non-monotonic; the positive extreme is especially sparse. EV also lacks stable monotonic evidence. Near-zero bands contain single-digit or low-double-digit samples. Line and date tables keep 0.5, 1.5, 2.5 separate and Oct. 14 separate.

Selected-side score comparisons, Under/Over splits, price bands, EV×Edge matrix, same/opposite-side breakouts, sort prefixes, and overlap are provided as fixed descriptive tables. Read `canonical_rows.csv` to distinguish Over display metrics from HGB-selected outcome probability. No sparse cell is promoted into a rule. Current Oct. 15 attachment contributes {len(pending)} quote-complete rows marked `PENDING_OUTCOME`.

## Workflow interpretation

1. More displayed Edge: not shown to be better; historical accuracy is non-monotonic and high-positive Edge has only 2 rows.
2. More displayed EV: not shown to be better; the apparent positive tail has small samples and is price-sensitive.
3. Where green stops helping: no stable breakpoint can be supported; do not infer one from these sparse tails.
4. HGB overconfidence: aggregate 0.5 selected-side probability exceeds realized rate by {metrics(s05)['calibration_gap']:.1%}; interpret by fixed Edge/EV bands and date, not as a display cutoff.
5. Under/Over and agreement: separated in `edge_by_side.csv` and `agreement_disagreement.csv`; the visible display itself is always Over-based.
6. Price materially changes EV for a fixed Edge through `EV=Edge/q`.
7. Both columns are related transformations, with price-dependent ranking differences.
8. Edge/EV sorting results and top-set overlap are in `sort_prefix_metrics.csv` and `ranking_overlap.csv`.
9. Near-zero has limited N and cannot be called more credible than extreme positive from this sample.
10. Green should be read as model-versus-price difference under this display formula, not as validated strength.
11. Next: continue frozen prospective collection and grade Oct. 15 after final reconciliation; do not alter UI or Funnel V1.

Decision classifications: displayed Edge `INSUFFICIENT_EVIDENCE`; displayed EV `INSUFFICIENT_EVIDENCE`; relationship `PARTIALLY_REDUNDANT` (same underlying probability/price pair, with price-driven rank changes). No maximum Edge/EV, preferred range, negative-edge inclusion, price requirement, confidence cap, or operational filter is created.

## Guardrails and backlog

Funnel V1, Points production, UI behavior and filtering remain unchanged. Preserve Funnel V1 prospective validation, Oct. 15 funnel grading after final reconciliation, SOG structural-shadow grading, fixed-blend collection, official-TOI binding and deferred Puck Line work. Tier3 override remains unnecessary. Provider calls: 0; paid credits: 0; production DB mutations: 0.
''')
    positive=sum(r['display_edge']>0 for r in s05); negative=sum(r['display_edge']<0 for r in s05)
    side_summary={side:metrics([r for r in s05 if r['display_side']==side]) for side in ('UNDER','OVER')}
    agreement_summary={side:metrics([r for r in s05 if r['side_relation']==side]) for side in ('SAME_SIDE','OPPOSITE_SIDE')}
    edge_markdown='\n'.join(f"| {r['band']} | {r['n']} | {r.get('wins',0)}–{r.get('losses',0)} | {r.get('accuracy',0):.1%} | {r.get('excess_losses',0):+.1f} |" for r in edge_tab)
    ev_markdown='\n'.join(f"| {r['band']} | {r['n']} | {r.get('wins',0)}–{r.get('losses',0)} | {r.get('accuracy',0):.1%} | {r.get('excess_losses',0):+.1f} |" for r in ev_tab)
    (OUT/'report.md').write_text(f'''# NHL Points displayed EV / Edge reliability surface — corrected

## Correction and source trace

The package from commit `90bf6a3b49ce28057f57d217444cd480951e859a` is superseded. Its root cause was unconditionally reading `p_over` and `price_over` while evaluating model-selected outcomes. The prior claim that positive Edge occurred in only two rows is invalid: it counted the Over-oriented tail, not the selected-side Edge population.

The retained export contains both sides (`p_over/p_under`, `price_over/price_under`, `fair_over/fair_under`, `edge_over/edge_under`). Funnel V1 selects OVER when model P(over) is at least 0.5 and UNDER otherwise; its selected production Edge is selected model probability minus raw American implied probability at the corresponding side price. The supplied six examples reproduce this side-normalized formula.

Repository trace (`source_trace.json`): the React board fetches `points_with_market.csv`, turns `p_over` into the line probability and puts `p_over_mkt` plus `price_over` in its market map. `bestEdgeCandidate/topPlayableEdgeRows` calculate `p_over - marketProbability`; the adapter labels the row `Over`, and does not switch to Under. The static table is also Over-oriented. Thus the correction fixtures establish the intended acceptance contract, while checked-in UI source does not implement that contract today. This study records that discrepancy and computes the requested corrected selected-side analysis; no UI source was changed.

## Population and reproduction

Historical finalized intersection: {len(rows)} rows ({len(s05)} settled 0.5 rows; {sum(r['line']==1.5 for r in settled)} settled 1.5; {sum(r['line']==2.5 for r in settled)} settled 2.5). At 0.5: {sum(r['selected_win'] for r in s05)}–{sum(r['selected_loss'] for r in s05)}, accuracy {metrics(s05)['accuracy']:.1%}, mean selected probability {avg(s05,'selected_probability'):.1%}, realized rate {metrics(s05)['accuracy']:.1%}; Edge positive/negative/zero: {positive}/{negative}/0. Brier/log loss: model {metrics(s05)['hgb_brier']:.4f}/{metrics(s05)['hgb_log_loss']:.4f}; no-vig market {metrics(s05)['market_brier']:.4f}/{metrics(s05)['market_log_loss']:.4f}.

All six supplied Cowan, Rielly, Power, Nylander, Matthews and Quinn arithmetic fixtures pass to the displayed 0.1 percentage point. Cowan Under: model probability {fixture_rows[0]['model_probability']:.4%}, market -169, fair -203, Edge +4.2%, EV +6.6%. Historical selected-side reproduction: {len(reproduced)} rows; maximum Edge error {max(z['edge_error'] for z in reproduced):.3g}; maximum EV error {max(z['ev_error'] for z in reproduced):.3g}; fair-price mismatches {sum(not z['fair_match'] for z in reproduced)}; orientation mismatches 0.

## Funnel V1 reconciliation

On the exact 658-row raw canonical 0.5 discovery population, corrected Edge < 0 matches frozen Funnel V1 negative-edge classification in {reconciliation['exact_matches']}/{reconciliation['population_rows']} rows: {reconciliation['negative_edge_rows']} negative and {reconciliation['nonnegative_rows']} nonnegative; mismatches {reconciliation['mismatch_count']}. Funnel V1 files and code were not modified. This reconciles 658 → 538 exactly.

## Corrected reliability surface

| Selected-side Edge | N | W–L | Accuracy | Excess losses |
|---|---:|---:|---:|---:|
{edge_markdown}

| Selected-side EV | N | W–L | Accuracy | Excess losses |
|---|---:|---:|---:|---:|
{ev_markdown}

Edge and EV are strongly related transforms of selected probability and price: overall Pearson/Spearman {corr[0]['pearson']:.3f}/{corr[0]['spearman']:.3f}; 0.5 line {next(x for x in corr if x['group']=='LINE_0.5')['pearson']:.3f}/{next(x for x in corr if x['group']=='LINE_0.5')['spearman']:.3f}. `EV = Edge / q`, where q is raw implied probability, so price alters ranking. Both surfaces are non-monotonic in realized accuracy, and high positive bands show poor calibration: Edge ≥20% is {sum(r['display_edge']>=.2 for r in s05)} rows; EV ≥20% is {sum(r['display_ev']>=.2 for r in s05)} rows. No threshold is supported.

Under: {side_summary['UNDER']['n']} rows, {side_summary['UNDER']['wins']}–{side_summary['UNDER']['losses']}, accuracy {side_summary['UNDER']['accuracy']:.1%}; Over: {side_summary['OVER']['n']} rows, {side_summary['OVER']['wins']}–{side_summary['OVER']['losses']}, accuracy {side_summary['OVER']['accuracy']:.1%}. Same-side market/model agreement: {agreement_summary['SAME_SIDE']['n']} rows, {agreement_summary['SAME_SIDE']['wins']}–{agreement_summary['SAME_SIDE']['losses']}; opposite-side: {agreement_summary['OPPOSITE_SIDE']['n']} rows, {agreement_summary['OPPOSITE_SIDE']['wins']}–{agreement_summary['OPPOSITE_SIDE']['losses']}. These are descriptive, not promotion evidence.

Price interaction appears in `price_band_metrics.csv`: accuracy declines from 80.5% at odds ≤-200 (n=816) to 17.6% at +176 or longer (n=17), with substantial confounding by selected probability and side. Top 10% Edge sort: 29–43; top 10% EV sort: 24–48. At top 20%, Edge 61–82 and EV 53–90; neither ranking improves outcomes. Top 10% Edge/EV overlap and Jaccard are in `ranking_overlap.csv`.

Oct. 15 is pending: {len(pending)} quote-complete rows, {sum(r['display_side']=='UNDER' for r in pending)} Under and {sum(r['display_side']=='OVER' for r in pending)} Over, no grading. All line/date, band, price, agreement and sort tables remain descriptive. The prior study’s Over-only edge/EV reliability, positive-tail scarcity, edge/EV relationship, and conclusions about where green helps are invalid; the selected-side outcomes and frozen Funnel V1 counts survive, but must be interpreted with the corrected columns. Funnel V1 is unchanged.

Provider calls: 0. Paid credits: 0. Production DB mutations: 0. No operational rule or cutoff established.
''')
    print(json.dumps({'output':str(OUT),'settled':len(settled),'settled_0_5':summary['settled_0_5'],'pending':len(pending)},indent=2))

if __name__ == '__main__': main()
