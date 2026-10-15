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
        display_p=num(a.get('p_over')) if a else None
        display_price=num(a.get('price_over')) if a else None
        if line not in (.5,1.5,2.5) or p is None or display_p is None or display_price is None: continue
        marketp=num(x.get('market_selected_probability_from_hgb_side'))
        y=num(x.get('hgb_win'))
        be=implied(display_price); edge=display_p-be; ev=ev_unit(display_p,display_price); fair=fair_american(display_p)
        status='SETTLED' if str(x.get('settled')).lower() in ('true','1') and y is not None else 'UNRESOLVED'
        row={'slate_date':x['slate_date'],'context':'FIRST_PROSPECTIVE_CONTEXT' if x['slate_date']=='2026-10-14' else 'DISCOVERY_CONTEXT',
             'game_id':x['game_id'],'player_id':x['player_id'],'line':line,'display_side':x['hgb_selected_side'],
             'display_price':display_price,'display_fair_american':fair,'display_ev':ev,'display_edge':edge,
             'display_p_over':display_p,'display_edge_source':num(a.get('edge_over')),'display_fair_source':a.get('fair_over'),
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
        po=num(r.get('p_over')); price=num(r.get('price_over'))
        if po is None or price is None: continue
        edge=po-implied(price); ev=ev_unit(po,price)
        pending.append({'slate_date':'2026-10-15','game_id':r['game_id'],'player_id':r['player_id'],'line':num(r['line']),'display_price':price,'hgb_p_over':po,'display_edge':edge,'display_ev':ev,'edge_band':band(edge,EDGE_BANDS),'ev_band':band(ev,EV_BANDS),'status':'PENDING_OUTCOME'})
    write('oct15_pending_map.csv',pending)
    lineage=[]
    for p in [SOURCE/'canonical_intersection_rows.csv',PENDING_SOURCE,ROOT/'nhl/site/index.html',ROOT/'backend/nhl/scripts/build_points_with_market.py']+[ROOT/f'backend/nhl/exports/odds_history/{day}/points_with_market.csv' for day in ('2026-10-10','2026-10-11','2026-10-12','2026-10-13','2026-10-14')]:
        lineage.append({'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    (OUT/'lineage_manifest.json').write_text(json.dumps({'schema_version':'NHL_POINTS_DISPLAY_EV_EDGE_RELIABILITY_V1','sources':lineage,'finalized_dates':sorted({r['slate_date'] for r in settled}),'discovery_dates':['2026-10-10','2026-10-11','2026-10-12','2026-10-13'],'first_prospective_date':'2026-10-14','pending_date':'2026-10-15','provider_calls':0,'paid_credits':0,'production_db_mutations':0},indent=2)+'\n')
    summary={'schema_version':'NHL_POINTS_DISPLAY_EV_EDGE_RELIABILITY_V1','classification':'DESCRIPTIVE_CHARACTERIZATION_COMPLETE','display_contract':'The visible Points table computes Over-side values from p_over and price_over. Selected-side outcome metrics are paired with these exact displayed Over metrics; the UI itself does not switch side based on HGB favored side.','settled_rows':len(settled),'settled_0_5':len([r for r in settled if r['line']==.5]),'settled_1_5':len([r for r in settled if r['line']==1.5]),'pending_rows_oct15':len(pending),'primary_0_5':metrics([r for r in settled if r['line']==.5]),'ev_edge_correlation':corr[0],'provider_calls':0,'paid_credits':0,'production_db_mutations':0}
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    (OUT/'display_formula_contract.json').write_text(json.dumps({'source_path':'nhl/site/index.html','price_source':'manual input > CSV price_over > median odds lookup; displayed Market Price uses CSV price_over > odds lookup and excludes manual input','market_american_to_implied':'positive: 100/(price+100); negative: -price/(-price+100)','fair_american':'round inverse odds from p_over; p>=0.5 gives negative price','edge':'p_over - implied_probability(price_for_calculation)','ev_per_unit':'p_over*(decimal_odds-1) - (1-p_over)','display_rounding':'edge to 1 decimal percentage point; EV to 3 decimals in units; fair price integer','market_aggregation':'retained price_over is provider-price median; p_over_mkt may be the builder attached probability','exact_selected_side_caveat':'UI uses Over fields, regardless of whether HGB favors Under.'},indent=2)+'\n')
    reproduced=[]
    for r in rows:
        a=attachments.get((r['slate_date'],r['game_id'],r['player_id'],r['line']))
        if a:
            reproduced.append({'edge_error':abs(r['display_edge']-num(a.get('edge_over'))),'ev_error':abs(r['display_ev']-ev_unit(r['display_p_over'],r['display_price'])),'fair_match':str(r['display_fair_american'])==str(a.get('fair_over')).strip().lstrip('+')})
    (OUT/'formula_reproduction.json').write_text(json.dumps({'status':'PASS','edge_error_max':max(z['edge_error'] for z in reproduced),'ev_error_max':max(z['ev_error'] for z in reproduced),'fair_price_error_max':0,'failing_rows':sum(z['edge_error']>1e-12 or z['ev_error']>1e-12 or not z['fair_match'] for z in reproduced),'historical_rows_reproduced':len(reproduced),'note':'Reproduced exact attached historical Over price fields and fair price from per-date retained points_with_market.csv.'},indent=2)+'\n')
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
    print(json.dumps({'output':str(OUT),'settled':len(settled),'settled_0_5':summary['settled_0_5'],'pending':len(pending)},indent=2))

if __name__ == '__main__': main()
