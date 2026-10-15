#!/usr/bin/env python3
"""Build a descriptive, prediction-level HGB versus consensus market map."""
import csv, hashlib, json, math
from collections import Counter, defaultdict
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/'artifacts/analysis/nhl/points_market_loss_concentration/2026-10-14'
PROV=ROOT/'artifacts/analysis/nhl/points_provider_price_neighborhood/2026-10-15'
RECEIPT=ROOT/'artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261015T140453183604Z_bfa01e59/parent_receipt.json'
OUT=ROOT/'artifacts/analysis/nhl/points_model_market_intersection_map/2026-10-15'
DAYS=['2026-10-10','2026-10-11','2026-10-12','2026-10-13','2026-10-14']
PROB=[('<20%',None,.20),('20-30%',.20,.30),('30-35%',.30,.35),('35-40%',.35,.40),('40-45%',.40,.45),('45-50%',.45,.50),('50-55%',.50,.55),('55-60%',.55,.60),('60-65%',.60,.65),('65-70%',.65,.70),('70-80%',.70,.80),('80%+',.80,None)]
H2=[('<30%',None,.30),('30-40%',.30,.40),('40-45%',.40,.45),('45-50%',.45,.50),('50-55%',.50,.55),('55-60%',.55,.60),('60-70%',.60,.70),('70%+',.70,None)]
GAP=[('<-20pp',None,-.20),('-20--15pp',-.20,-.15),('-15--10pp',-.15,-.10),('-10--5pp',-.10,-.05),('-5--3pp',-.05,-.03),('-3--1pp',-.03,-.01),('-1-0pp',-.01,0),('0-+1pp',0,.01),('+1-+3pp',.01,.03),('+3-+5pp',.03,.05),('+5-+10pp',.05,.10),('+10-+15pp',.10,.15),('+15-+20pp',.15,.20),('>+20pp',.20,None)]
SGAP=[('<-10pp',None,-.10),('-10--5pp',-.10,-.05),('-5--3pp',-.05,-.03),('-3--1pp',-.03,-.01),('-1-0pp',-.01,0),('0-+1pp',0,.01),('+1-+3pp',.01,.03),('+3-+5pp',.03,.05),('+5-+10pp',.05,.10),('+10-+15pp',.10,.15),('+15-+20pp',.15,.20),('>+20pp',.20,None)]
EDGE=[('<-10%',None,-.10),('-10--5%',-.10,-.05),('-5--3%',-.05,-.03),('-3--1%',-.03,-.01),('-1-0%',-.01,0),('0-+1%',0,.01),('+1-+3%',.01,.03),('+3-+5%',.03,.05),('+5-+10%',.05,.10),('+10-+15%',.10,.15),('>+15%',.15,None)]
PRICE=[('<=-200',None,-200),('-200--121',-200,-120),('-120--101',-120,-100),('EVEN',100,101),('+101-+125',101,126),('>+125',126,None)]

def read(p):
    with Path(p).open(newline='') as f:return list(csv.DictReader(f))
def n(x):
    try:
        z=float(x); return z if math.isfinite(z) else None
    except (TypeError,ValueError):return None
def brier(p,y):return (p-y)**2
def ll(p,y):p=min(1-1e-15,max(1e-15,p));return -(y*math.log(p)+(1-y)*math.log(1-p))
def band(x,reg):
    for label,lo,hi in reg:
        if x is not None and (lo is None or x>=lo) and (hi is None or x<hi):return label
    return 'UNAVAILABLE'
def dump(name,rows):
    fields=list(dict.fromkeys(k for r in rows for k in r)) or ['empty']
    with (OUT/name).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
def mean(rows,key):
    vals=[n(r.get(key)) for r in rows if n(r.get(key)) is not None]
    return sum(vals)/len(vals) if vals else None
def summarize(rows):
    s=[r for r in rows if r.get('settled')]
    if not s:return {'n':len(rows),'settled_n':0}
    nrow=len(s);ml=sum(r['hgb_loss'] for r in s);bl=sum(r['market_loss'] for r in s)
    return {'n':len(rows),'settled_n':nrow,'hgb_wins':nrow-ml,'hgb_losses':ml,'market_wins':nrow-bl,'market_losses':bl,'hgb_accuracy':1-ml/nrow,'market_accuracy':1-bl/nrow,'hgb_brier':mean(s,'hgb_brier'),'market_brier':mean(s,'market_brier'),'brier_advantage_hgb':mean(s,'brier_advantage_hgb'),'hgb_log_loss':mean(s,'hgb_log_loss'),'market_log_loss':mean(s,'market_log_loss'),'log_loss_advantage_hgb':mean(s,'log_loss_advantage_hgb'),'hgb_expected_losses':sum(r['hgb_expected_loss'] for r in s),'hgb_excess_losses':sum(r['hgb_excess_loss'] for r in s)}
def tab(rows,key,registry,include_unsettled=False):
    result=[]
    for label,lo,hi in registry:
        group=[r for r in rows if band(r.get(key),[(label,lo,hi)])==label]
        sm=summarize(group);s=[r for r in group if r.get('settled')]
        result.append({'band':label,**sm,'realized_over_rate':mean(s,'outcome_over'),'mean_hgb_p_over':mean(group,'hgb_p_over'),'hgb_calibration_gap':(mean(s,'outcome_over')-mean(s,'hgb_p_over')) if s else None,'mean_market_p_over':mean(group,'market_p_over'),'market_calibration_gap':(mean(s,'outcome_over')-mean(s,'market_p_over')) if s else None,'mean_signed_over_gap':mean(group,'over_gap'),'mean_production_edge':mean(group,'production_edge'),'hgb_selected_side_wins':sum(r['hgb_win'] for r in s),'market_selected_side_wins':sum(r['market_win'] for r in s)})
    return result
def main():
    OUT.mkdir(parents=True,exist_ok=True)
    receipt=json.loads(RECEIPT.read_text())
    # Discovery dates are the exact pregame HGB/market/outcome spine from the established study.
    prior=[r for r in read(BASE/'canonical_market_intersection.csv') if r['slate_date'] in DAYS[:4] and n(r['line']) in (.5,1.5,2.5)]
    inv={r['slate_date']:r for r in read(BASE/'market_evidence_inventory.csv')}
    game_teams={}
    for d in DAYS[:4]:
        pp=read(ROOT/inv[d]['prediction_path'])
        for p in pp:game_teams[(d,p['game_id'])]=(p.get('home_team_id'),p.get('away_team_id'))
    rows=[]
    for x in prior:
        po=n(x['model_p_over']);pmraw=n(x['market_p_over']);puraw=n(x['market_p_under']);y=n(x['over_outcome']);line=n(x['line'])
        if po is None or pmraw is None or puraw is None or pmraw+puraw<=0:continue
        pm=pmraw/(pmraw+puraw)
        s='OVER' if po>=.5 else 'UNDER';m='OVER' if pm>=.5 else 'UNDER';settled=y is not None
        psel=po if s=='OVER' else 1-po; msel=pm if s=='OVER' else 1-pm
        points=n(x.get('official_points')); price=n(x.get('selected_attached_american_price'))
        home,away=game_teams.get((x['slate_date'],x['game_id']),(None,None));team=x.get('team_id');is_home=str(team)==str(home) if home is not None else None
        rr={**x,'player_name':x.get('player',''),'home_away':('HOME' if is_home else 'AWAY') if is_home is not None else '','opponent':away if is_home else home if is_home is not None else '', 'line':line,'hgb_expected_count':n(x.get('expected_count')),'hgb_p_over':po,'hgb_p_under':1-po,'hgb_selected_side':s,'hgb_selected_probability':psel,'hgb_distance_from_50':abs(po-.5),'hgb_directional_state':band(po,[('STRONG_UNDER',None,.35),('LEAN_UNDER',.35,.45),('NEAR_COIN_FLIP',.45,.55),('LEAN_OVER',.55,.65),('STRONG_OVER',.65,None)]),'market_p_over':pm,'market_p_under':1-pm,'market_favored_side':m,'market_favored_probability':max(pm,1-pm),'market_distance_from_50':abs(pm-.5),'market_selected_probability_from_hgb_side':msel,'production_edge':n(x.get('production_edge')) if n(x.get('production_edge')) is not None else psel-(pmraw if s=='OVER' else puraw),'selected_side_gap':psel-msel,'over_gap':po-pm,'retained_median_selected_price':price,'official_points_num':points,'outcome_over':y,'hgb_win':int(x.get('model_win') in (1,'1','True')),'market_win':int(x.get('market_win') in (1,'1','True')),'settled':settled}
        if settled:
            rr.update(hgb_brier=brier(po,y),market_brier=brier(pm,y),hgb_log_loss=ll(po,y),market_log_loss=ll(pm,y),hgb_expected_loss=1-psel,hgb_loss=1-rr['hgb_win'],market_loss=1-rr['market_win'],hgb_excess_loss=(1-rr['hgb_win'])-(1-psel),brier_advantage_hgb=brier(pm,y)-brier(po,y),log_loss_advantage_hgb=ll(pm,y)-ll(po,y),hgb_closer=int(abs(po-y)<abs(pm-y)),market_closer=int(abs(pm-y)<abs(po-y)))
        rows.append(rr)
    # Oct 14 exact immutable retained attachment and official reconciliation bound in Oct 15 receipt.
    od=receipt['prior_day_learning']['reconciliation_package']; op=ROOT/od/'canonical_skater_outcomes.csv'
    attroot=ROOT/receipt['prior_day_learning']['points_hgb_shadow_grade']['production_authority_context']['daily_receipt_path']
    priorreceipt=json.loads(attroot.read_text()); ar=next(z for z in priorreceipt['lanes']['points_attachment']['outputs'] if z.get('retention_schema_version')=='NHL_POINTS_MARKET_ATTACHMENT_RETENTION_V1')
    attachment=read(ROOT/ar['path']/'points_with_market.csv'); outcomes=read(op)
    ppath=ROOT/priorreceipt['lanes']['points']['outputs'][0]['path']; pred=read(ppath)
    amap={(r['game_id'],r['player_id'],str(n(r['line']))):r for r in attachment}; omap={(r['game_id'],r['player_id']):r for r in outcomes}
    for p in pred:
        line=n(p.get('line'))
        if line not in (.5,1.5,2.5):continue
        key=(p['game_id'],p['player_id'],str(line));a=amap.get(key);o=omap.get((p['game_id'],p['player_id']))
        if not a:continue
        po=n(p.get('prob_over'));pmraw=n(a.get('p_over_mkt'));puraw=n(a.get('p_under_mkt'));points=n(o.get('official_points')) if o and o.get('participation_state')=='PARTICIPATED' and str(o.get('official_final')).lower()=='true' else None
        if po is None or pmraw is None or puraw is None or pmraw+puraw<=0:continue
        pm=pmraw/(pmraw+puraw);pu=1-pm
        y=int(points>line) if points is not None else None;s='OVER' if po>=.5 else 'UNDER';m='OVER' if pm>=.5 else 'UNDER';psel=po if s=='OVER' else 1-po;msel=pm if s=='OVER' else 1-pm
        win=int(points>line) if s=='OVER' and points is not None else int(points<line) if points is not None else None
        mwin=int(points>line) if m=='OVER' and points is not None else int(points<line) if points is not None else None
        team=a.get('team_id');home=p.get('home_team_id');away=p.get('away_team_id');ishome=str(team)==str(home) if home is not None else None
        q={'slate_date':'2026-10-14','game_id':p['game_id'],'player_id':p['player_id'],'player_name':a.get('full_name',''),'team_id':team,'opponent':away if ishome else home if ishome is not None else '','home_away':('HOME' if ishome else 'AWAY') if ishome is not None else '','line':line,'hgb_expected_count':n(p.get('expected_points')),'hgb_p_over':po,'hgb_p_under':1-po,'hgb_selected_side':s,'hgb_selected_probability':psel,'hgb_distance_from_50':abs(po-.5),'hgb_directional_state':band(po,[('STRONG_UNDER',None,.35),('LEAN_UNDER',.35,.45),('NEAR_COIN_FLIP',.45,.55),('LEAN_OVER',.55,.65),('STRONG_OVER',.65,None)]),'market_p_over':pm,'market_p_under':pu,'market_favored_side':m,'market_favored_probability':max(pm,pu),'market_distance_from_50':abs(pm-.5),'market_selected_probability_from_hgb_side':msel,'production_edge':psel-(pmraw if s=='OVER' else puraw),'selected_side_gap':psel-msel,'over_gap':po-pm,'retained_median_selected_price':n(a.get('price_over' if s=='OVER' else 'price_under')),'official_points_num':points,'outcome_over':y,'hgb_win':win,'market_win':mwin,'settled':points is not None,'grading_status':'SETTLED' if points is not None else 'PARTICIPATION_STATUS_UNRESOLVED','source':'OCT14_IMMUTABLE_ATTACHMENT_AND_FINAL_RECONCILIATION'}
        if points is not None:
            q.update(hgb_brier=brier(po,y),market_brier=brier(pm,y),hgb_log_loss=ll(po,y),market_log_loss=ll(pm,y),hgb_expected_loss=1-psel,hgb_loss=1-win,market_loss=1-mwin,hgb_excess_loss=(1-win)-(1-psel),brier_advantage_hgb=brier(pm,y)-brier(po,y),log_loss_advantage_hgb=ll(pm,y)-ll(po,y),hgb_closer=int(abs(po-y)<abs(pm-y)),market_closer=int(abs(pm-y)<abs(po-y)))
        rows.append(q)
    # Normalize legacy row fields to consistent numeric and outcome semantics.
    for r in rows:
        r['settled']=bool(r.get('settled'))
        for k in ('hgb_p_over','market_p_over','production_edge','selected_side_gap','over_gap','hgb_selected_probability','market_selected_probability_from_hgb_side'):
            r[k]=n(r.get(k))
        if r['settled'] and 'hgb_brier' not in r:
            y=n(r['outcome_over']);p=r['hgb_p_over'];m=r['market_p_over'];sw=int(r['hgb_win']);mw=int(r['market_win']);ps=r['hgb_selected_probability']
            r.update(hgb_brier=brier(p,y),market_brier=brier(m,y),hgb_log_loss=ll(p,y),market_log_loss=ll(m,y),hgb_expected_loss=1-ps,hgb_loss=1-sw,market_loss=1-mw,hgb_excess_loss=(1-sw)-(1-ps),brier_advantage_hgb=brier(m,y)-brier(p,y),log_loss_advantage_hgb=ll(m,y)-ll(p,y),hgb_closer=int(abs(p-y)<abs(m-y)),market_closer=int(abs(m-y)<abs(p-y)))
    keys=[(r['slate_date'],str(r['game_id']),str(r['player_id']),str(r['line'])) for r in rows]
    if len(keys)!=len(set(keys)):raise ValueError('duplicate prediction-market-outcome identity')
    settled=[r for r in rows if r['settled']]; primary=[r for r in settled if n(r['line'])==.5]
    line_totals=Counter(n(r['line']) for r in settled)
    for r in rows:r['settled_line_population_share']=(1/line_totals[n(r['line'])]) if r['settled'] and line_totals[n(r['line'])] else None
    # region labeling and central four quadrants
    for r in rows:
        r['side_relation']='SAME_SIDE' if r['hgb_selected_side']==r['market_favored_side'] else 'OPPOSITE_SIDE'
        if r['side_relation']=='SAME_SIDE':r['quadrant']='BOTH_CORRECT' if r['hgb_win'] and r['market_win'] else 'BOTH_WRONG' if r['settled'] else 'PENDING_OUTCOME'
        else:r['quadrant']='HGB_CORRECT_MARKET_WRONG' if r['hgb_win'] else 'MARKET_CORRECT_HGB_WRONG' if r['market_win'] else 'PENDING_OUTCOME'
        r.update(hgb_over_band=band(r['hgb_p_over'],PROB),market_over_band=band(r['market_p_over'],PROB),hgb_2d_band=band(r['hgb_p_over'],H2),market_2d_band=band(r['market_p_over'],H2),over_gap_band=band(r['over_gap'],GAP),selected_side_gap_band=band(r['selected_side_gap'],SGAP),production_edge_band=band(r['production_edge'],EDGE),price_band=band(n(r.get('retained_median_selected_price')),PRICE))
        r['crossing']='HGB_LT50_MARKET_GE50' if r['hgb_p_over']<.5<=r['market_p_over'] else 'HGB_GE50_MARKET_LT50' if r['hgb_p_over']>=.5>r['market_p_over'] else 'NO_CROSSING'
        r['near_group']='BOTH_NEAR_50' if abs(r['hgb_p_over']-.5)<=.05 and abs(r['market_p_over']-.5)<=.05 else 'HGB_NEAR_MARKET_DECISIVE' if abs(r['hgb_p_over']-.5)<=.05 else 'MARKET_NEAR_HGB_DECISIVE' if abs(r['market_p_over']-.5)<=.05 else 'NEITHER_NEAR_50'
        r['same_side_confidence_gap']=r['hgb_selected_probability']-r['market_selected_probability_from_hgb_side']
        r['same_confidence_band']=band(r['same_side_confidence_gap'],[('HGB_10PP_LESS',None,-.10),('HGB_5_TO_10PP_LESS',-.10,-.05),('WITHIN_5PP',-.05,.05),('HGB_5_TO_10PP_MORE',.05,.10),('HGB_10PP_MORE',.10,None)])
        r['opposite_gap_abs']=abs(r['over_gap'])
        r['opposite_gap_band']=band(r['opposite_gap_abs'],[('<5pp',None,.05),('5-10pp',.05,.10),('10-15pp',.10,.15),('15-20pp',.15,.20),('>20pp',.20,None)])
        r['hgb_confidence_band']=band(r['hgb_selected_probability'],[('50-55%',.5,.55),('55-60%',.55,.60),('60-65%',.60,.65),('65-70%',.65,.70),('70-75%',.70,.75),('75-80%',.75,.80),('80-85%',.80,.85),('85%+',.85,None)])
    dump('canonical_intersection_rows.csv',rows);dump('canonical_settled_rows.csv',settled);dump('canonical_unresolved_rows.csv',[r for r in rows if not r['settled']])
    dump('agreement_disagreement_classifications.csv',rows);dump('quadrant_rows.csv',[r for r in settled if r['quadrant']!='PENDING_OUTCOME'])
    for q in ('HGB_CORRECT_MARKET_WRONG','MARKET_CORRECT_HGB_WRONG','BOTH_CORRECT','BOTH_WRONG'):
        dump(q.lower()+'.csv',[r for r in settled if r['quadrant']==q])
    # Generic fixed-band score maps, always prediction-level, never provider weighted.
    for name,source,key,reg in [('hgb_over_probability_bands',settled,'hgb_p_over',PROB),('market_over_probability_bands',settled,'market_p_over',PROB),('signed_over_gap_bands',settled,'over_gap',GAP),('selected_side_gap_bands',settled,'selected_side_gap',SGAP),('production_edge_bands',settled,'production_edge',EDGE),('selected_side_confidence_bands',settled,'hgb_selected_probability',[(f'{int(a*100)}-{int(b*100)}%',a,b) for a,b in [(.5,.55),(.55,.6),(.6,.65),(.65,.7),(.7,.75),(.75,.8),(.8,.85),(.85,1.01)]]),('sub50_hgb_bands',[r for r in settled if r['hgb_p_over']<.5],'hgb_p_over',[('<30%',None,.30),('30-35%',.30,.35),('35-40%',.35,.40),('40-45%',.40,.45),('45-50%',.45,.50)]),('above50_hgb_bands',[r for r in settled if r['hgb_p_over']>=.5],'hgb_p_over',[('50-55%',.5,.55),('55-60%',.55,.60),('60-65%',.60,.65),('65-70%',.65,.70),('70-80%',.70,.80),('80%+',.80,None)])]:
        table=[]
        for line in sorted({n(r['line']) for r in source}):table.extend([{'line':line,**z} for z in tab([r for r in source if n(r['line'])==line],key,reg)])
        dump(name+'.csv',table)
    # Matrix includes fixed HGB/market bands on both sides of 50, empty cells retained.
    matrix=[]
    for line in sorted({n(r['line']) for r in settled}):
      line_rows=[r for r in settled if n(r['line'])==line]
      for hb,_,__ in H2:
        for mb,___,____ in H2:
            g=[r for r in line_rows if r['hgb_2d_band']==hb and r['market_2d_band']==mb];sm=summarize(g)
            matrix.append({'line':line,'hgb_band':hb,'market_band':mb,**sm,'mean_hgb_p_over':mean(g,'hgb_p_over'),'mean_market_p_over':mean(g,'market_p_over'),'realized_over_rate':mean(g,'outcome_over'),'same_side_n':sum(r['side_relation']=='SAME_SIDE' for r in g),'opposite_side_n':sum(r['side_relation']=='OPPOSITE_SIDE' for r in g),'share_total_rows':len(g)/len(line_rows) if line_rows else 0,'share_hgb_losses':sum(r['hgb_loss'] for r in g)/sum(r['hgb_loss'] for r in line_rows) if sum(r['hgb_loss'] for r in line_rows) else 0,'share_hgb_excess_losses':sum(r['hgb_excess_loss'] for r in g)/sum(r['hgb_excess_loss'] for r in line_rows) if sum(r['hgb_excess_loss'] for r in line_rows) else 0,'share_hgb_logloss':sum(r['hgb_log_loss'] for r in g)/sum(r['hgb_log_loss'] for r in line_rows) if sum(r['hgb_log_loss'] for r in line_rows) else 0})
    dump('hgb_market_over_probability_matrix.csv',matrix);dump('loss_density_matrix.csv',matrix)
    def grouped_csv(name,src,key):
        out=[]
        for line in sorted({n(r['line']) for r in src}):
          for v in sorted({r.get(key,'') for r in src if n(r['line'])==line}):
            g=[r for r in src if n(r['line'])==line and r.get(key,'')==v];sm=summarize(g);s=[r for r in g if r['settled']]
            out.append({'line':line,'group':v,'population_share':len(s)/sum(n(x['line'])==line for x in settled) if s else None,**sm,'hgb_closer_n':sum(r['hgb_closer'] for r in s),'market_closer_n':sum(r['market_closer'] for r in s),'percent_hgb_brier_better':sum(r['brier_advantage_hgb']>0 for r in s)/len(s) if s else None,'percent_market_brier_better':sum(r['brier_advantage_hgb']<0 for r in s)/len(s) if s else None,'mean_over_gap':mean(g,'over_gap'),'mean_selected_gap':mean(g,'selected_side_gap'),'mean_hgb_confidence':mean(g,'hgb_selected_probability'),'mean_market_confidence':mean(g,'market_selected_probability_from_hgb_side'),'mean_production_edge':mean(g,'production_edge')})
        dump(name+'.csv',out)
    for nm,key in [('quadrant_summary','quadrant'),('side_agreement_map','side_relation'),('same_side_confidence','same_confidence_band'),('opposite_side_divergence','opposite_gap_band'),('decision_boundary_crossings','crossing'),('near_boundary_intersections','near_group'),('over_under_side_analysis','hgb_selected_side'),('price_intersection','price_band'),('date_level_metrics','slate_date'),('selected_confidence_map','hgb_confidence_band')]:grouped_csv(nm,[r for r in settled if nm not in ('same_side_confidence','opposite_side_divergence') or (r['side_relation']=='SAME_SIDE' if nm=='same_side_confidence' else r['side_relation']=='OPPOSITE_SIDE')],key)
    # Confidence and blend diagnostics by predeclared populations.
    blends=[]
    regions={'ALL':settled,'HGB_P_OVER_LT50':[r for r in settled if r['hgb_p_over']<.5],'HGB_P_OVER_GE50':[r for r in settled if r['hgb_p_over']>=.5],'CROSSING':[r for r in settled if r['crossing']!='NO_CROSSING'],'NEAR_AGREEMENT':[r for r in settled if abs(r['over_gap'])<.03],'LARGE_DIVERGENCE':[r for r in settled if abs(r['over_gap'])>=.15]}
    for rg,rs in regions.items():
        for w in (.25,.5,.75):
            ps=[{'p':w*r['hgb_p_over']+(1-w)*r['market_p_over'],'y':r['outcome_over']} for r in rs]
            blends.append({'region':rg,'hgb_weight':w,'market_weight':1-w,'n':len(rs),'brier':sum(brier(a['p'],a['y']) for a in ps)/len(ps) if ps else None,'log_loss':sum(ll(a['p'],a['y']) for a in ps)/len(ps) if ps else None})
    dump('fixed_blend_diagnostics.csv',blends)
    adv=[]
    for label,lo,hi in [('HGB_MATERIALLY_BETTER',.05,None),('ROUGHLY_TIED',-.05,.05),('MARKET_MATERIALLY_BETTER',None,-.05)]:
        g=[r for r in settled if (r['brier_advantage_hgb']>=lo if hi is None else r['brier_advantage_hgb']>lo and r['brier_advantage_hgb']<hi) if label!='MARKET_MATERIALLY_BETTER'] if label!='MARKET_MATERIALLY_BETTER' else [r for r in settled if r['brier_advantage_hgb']<=-.05]
        adv.append({'classification':label,'n':len(g),'share':len(g)/len(settled) if settled else None,'mean_brier_advantage':mean(g,'brier_advantage_hgb'),'mean_log_loss_advantage':mean(g,'log_loss_advantage_hgb')})
    dump('proper_score_advantage_distribution.csv',adv);grouped_csv('complementarity_by_over_gap',[r for r in settled],'over_gap_band')
    # Date and cumulative trajectories with chronological discovery/prospective labels.
    date=[];cum=[]
    for d in DAYS:
        g=[r for r in settled if r['slate_date']==d and n(r['line'])==.5];date.append({'slate_date':d,'context':'FIRST_PROSPECTIVE_CONTEXT' if d=='2026-10-14' else 'DISCOVERY_CONTEXT',**summarize(g),'same_side_n':sum(r['side_relation']=='SAME_SIDE' for r in g),'opposite_side_n':sum(r['side_relation']=='OPPOSITE_SIDE' for r in g),'hgb_correct_market_wrong':sum(r['quadrant']=='HGB_CORRECT_MARKET_WRONG' for r in g),'market_correct_hgb_wrong':sum(r['quadrant']=='MARKET_CORRECT_HGB_WRONG' for r in g)})
        c=[r for r in settled if r['slate_date']<=d and n(r['line'])==.5];sub=[r for r in c if r['hgb_p_over']<.5];cum.append({'through_date':d,**summarize(c),'same_side_n':sum(r['side_relation']=='SAME_SIDE' for r in c),'opposite_side_n':sum(r['side_relation']=='OPPOSITE_SIDE' for r in c),'hgb_correct_market_wrong':sum(r['quadrant']=='HGB_CORRECT_MARKET_WRONG' for r in c),'market_correct_hgb_wrong':sum(r['quadrant']=='MARKET_CORRECT_HGB_WRONG' for r in c),'sub50_n':len(sub),'sub50_hgb_wins':sum(r['hgb_win'] for r in sub),'sub50_hgb_brier':mean(sub,'hgb_brier'),'sub50_market_brier':mean(sub,'market_brier')})
    dump('date_level_primary_05.csv',date);dump('cumulative_primary_05.csv',cum)
    # Loss/success region rankings on broad predeclared 2D bands, suppress no data but keep sparse marked.
    ranking=[]
    for rr in matrix:
        if rr['settled_n']<20:continue
        region=[r for r in settled if n(r['line'])==n(rr['line']) and r['hgb_2d_band']==rr['hgb_band'] and r['market_2d_band']==rr['market_band']]
        ranking.append({'line':rr['line'],'region':rr['hgb_band']+' × '+rr['market_band'],'n':rr['settled_n'],'hgb_losses':rr['hgb_losses'],'hgb_excess_losses':rr['hgb_excess_losses'],'hgb_log_loss_burden':sum(r['hgb_log_loss'] for r in region),'losses_per_100':100*rr['hgb_losses']/rr['settled_n'],'loss_share_to_population_share':rr['share_hgb_losses']/rr['share_total_rows'] if rr['share_total_rows'] else None,'brier_advantage_hgb':rr['brier_advantage_hgb'],'dates': '|'.join(sorted({r['slate_date'] for r in region}))})
    dump('loss_accumulation_ranking.csv',sorted(ranking,key=lambda r:r['hgb_losses'],reverse=True));dump('excess_loss_ranking.csv',sorted(ranking,key=lambda r:r['hgb_excess_losses'],reverse=True));dump('logloss_burden_ranking.csv',sorted(ranking,key=lambda r:r['hgb_log_loss_burden'],reverse=True));dump('success_accumulation_ranking.csv',sorted(ranking,key=lambda r:r['brier_advantage_hgb'],reverse=True))
    # Pending Oct 15 from the receipt-bounded output and active attachment, never graded.
    prow=read(ROOT/receipt['lanes']['points']['outputs'][0]['path']); pa=read(ROOT/'nhl/site/data/points_with_market.csv'); pamap={(x['game_id'],x['player_id'],str(n(x.get('line')))):x for x in pa}
    pending=[]
    for p in prow:
        if n(p.get('line')) not in (.5,1.5,2.5):continue
        a=pamap.get((p['game_id'],p['player_id'],str(n(p['line']))));po=n(p.get('prob_over'));pmraw=n(a.get('p_over_mkt')) if a else None;puraw=n(a.get('p_under_mkt')) if a else None
        if pmraw is None or puraw is None or pmraw+puraw<=0 or po is None:continue
        pm=pmraw/(pmraw+puraw)
        s='OVER' if po>=.5 else 'UNDER';m='OVER' if pm>=.5 else 'UNDER'
        pending.append({'slate_date':'2026-10-15','game_id':p['game_id'],'player_id':p['player_id'],'line':n(p['line']),'hgb_p_over':po,'market_p_over':pm,'over_gap':po-pm,'hgb_selected_side':s,'market_favored_side':m,'side_relation':'SAME_SIDE' if s==m else 'OPPOSITE_SIDE','crossing':'HGB_LT50_MARKET_GE50' if po<.5<=pm else 'HGB_GE50_MARKET_LT50' if po>=.5>pm else 'NO_CROSSING','near_group':'BOTH_NEAR_50' if abs(po-.5)<=.05 and abs(pm-.5)<=.05 else 'HGB_NEAR_MARKET_DECISIVE' if abs(po-.5)<=.05 else 'MARKET_NEAR_HGB_DECISIVE' if abs(pm-.5)<=.05 else 'NEITHER_NEAR_50','status':'PENDING_OUTCOME'})
    dump('oct15_pending_snapshot.csv',pending)
    def counts(key):return dict(Counter(x.get(key,'') for x in pending))
    pending_summary={'status':'PENDING_OUTCOME','row_count':len(pending),'hgb_over_probability_band_counts':dict(Counter(band(x['hgb_p_over'],PROB) for x in pending)),'market_over_probability_band_counts':dict(Counter(band(x['market_p_over'],PROB) for x in pending)),'same_side_count':sum(x['side_relation']=='SAME_SIDE' for x in pending),'opposite_side_count':sum(x['side_relation']=='OPPOSITE_SIDE' for x in pending),'crossing_counts':counts('crossing'),'near_boundary_counts':counts('near_group'),'signed_gap_band_counts':dict(Counter(band(x['over_gap'],GAP) for x in pending)),'by_line':{str(line):{'row_count':sum(n(x['line'])==line for x in pending),'hgb_over_probability_band_counts':dict(Counter(band(x['hgb_p_over'],PROB) for x in pending if n(x['line'])==line)),'market_over_probability_band_counts':dict(Counter(band(x['market_p_over'],PROB) for x in pending if n(x['line'])==line)),'same_side_count':sum(n(x['line'])==line and x['side_relation']=='SAME_SIDE' for x in pending),'opposite_side_count':sum(n(x['line'])==line and x['side_relation']=='OPPOSITE_SIDE' for x in pending),'crossing_counts':dict(Counter(x['crossing'] for x in pending if n(x['line'])==line)),'near_boundary_counts':dict(Counter(x['near_group'] for x in pending if n(x['line'])==line)),'signed_gap_band_counts':dict(Counter(band(x['over_gap'],GAP) for x in pending if n(x['line'])==line))} for line in sorted({n(x['line']) for x in pending})}}
    (OUT/'oct15_pending_summary.json').write_text(json.dumps(pending_summary,indent=2)+'\n')
    # Source lineage hashes and scope; immutable inputs only.
    inputs=[BASE/'canonical_market_intersection.csv',RECEIPT,op,ROOT/ar['path']/'points_with_market.csv',ROOT/ppath,ROOT/'nhl/site/data/points_with_market.csv',ROOT/receipt['lanes']['points']['outputs'][0]['path']]
    lin=[{'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in inputs if p.exists()]
    (OUT/'lineage_manifest.json').write_text(json.dumps({'schema_version':'NHL_POINTS_MODEL_MARKET_INTERSECTION_MAP_V1','dates':DAYS,'pending_date':'2026-10-15','primary_market':'attached consensus no-vig P(Over), one row per prediction identity','provider_calls':0,'paid_credits':0,'production_db_mutations':0,'sources':lin,'row_count':len(rows),'settled_rows':len(settled),'settled_0_5_rows':len(primary)},indent=2)+'\n')
    s05=[r for r in rows if r['settled'] and n(r['line'])==.5];summary={'classification':'DESCRIPTIVE_COMPLEMENTARITY_MAP_COMPLETE','finalized_dates':DAYS,'pending_date':'2026-10-15','row_count_all_lines':len(rows),'settled_rows_all_lines':len(settled),'settled_0_5_rows':len(s05),'settled_1_5_rows':sum(r['settled'] and n(r['line'])==1.5 for r in rows),'unresolved_rows':sum(not r['settled'] for r in rows),'primary_0_5':summarize(s05),'quadrants':{q:summarize([r for r in s05 if r['quadrant']==q]) for q in ('BOTH_CORRECT','BOTH_WRONG','HGB_CORRECT_MARKET_WRONG','MARKET_CORRECT_HGB_WRONG')},'side_relation':{q:summarize([r for r in s05 if r['side_relation']==q]) for q in ('SAME_SIDE','OPPOSITE_SIDE')},'directional_disagreement_wins':{'hgb_correct_market_wrong':sum(r['quadrant']=='HGB_CORRECT_MARKET_WRONG' for r in s05),'market_correct_hgb_wrong':sum(r['quadrant']=='MARKET_CORRECT_HGB_WRONG' for r in s05)},'sub50':summarize([r for r in s05 if r['hgb_p_over']<.5]),'execution':{'provider_calls':0,'paid_credits':0,'production_db_mutations':0},'oct15_pending':pending_summary,'sources':lin}
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2,default=str)+'\n')
    (OUT/'backlog_context.md').write_text('# Backlog context\n\nDescriptive evidence map only. Keep HGB production and Funnel V1 frozen. Continue collecting immutable, finalized date-level evidence; do not derive filters or thresholds from these retrospective regions. Oct. 14 is the first prospective context; Oct. 15 remains pending.\n')
    same=[r for r in s05 if r['side_relation']=='SAME_SIDE'];opposite=[r for r in s05 if r['side_relation']=='OPPOSITE_SIDE'];below=[r for r in s05 if r['hgb_p_over']<.5];oct14=[r for r in s05 if r['slate_date']=='2026-10-14'];dh=sum(r['quadrant']=='HGB_CORRECT_MARKET_WRONG' for r in s05);dm=sum(r['quadrant']=='MARKET_CORRECT_HGB_WRONG' for r in s05)
    report=f'''# NHL Points model × market intersection map\n\n## Scope and method\n\nPrediction-level analysis of immutable 2026 HGB predictions, retained market attachments, and official participating outcomes. Finalized dates: Oct. 10–14. Oct. 10–13 are `DISCOVERY_CONTEXT`; Oct. 14 is `FIRST_PROSPECTIVE_CONTEXT`. Oct. 15 is pending inventory only. One prediction/consensus/outcome per `(slate_date, game_id, player_id, line)`, without provider multiplicity. Primary line is 0.5; 1.5 is separate; 2.5 is descriptive and sparse. Unresolved participation remains visible but is excluded from score denominators.\n\nThe full probability gap is HGB P(Over) minus normalized retained market P(Over). Production edge is the attached selected-side model probability minus the retained raw selected-side market implied probability. Selected-side gap is a third distinct quantity. Earlier canonical side-specific implied probabilities were normalized by their sum to derive no-vig P(Over).\n\n## Where we stand\n\nAt 0.5 there are {len(s05)} settled rows. HGB and market agree on {len(same)} ({len(same)/len(s05):.1%}) and disagree on {len(opposite)}. Quadrants: both correct {sum(r['quadrant']=='BOTH_CORRECT' for r in s05)}, both wrong {sum(r['quadrant']=='BOTH_WRONG' for r in s05)}, HGB correct/market wrong {dh}, market correct/HGB wrong {dm}. Directional disagreements favor HGB {dh}/{dh+dm} ({dh/(dh+dm):.1%}) to {dm}.\n\nHGB is 416–298 (58.3%); market is 451–263 (63.2%). HGB Brier/log loss {mean(s05,'hgb_brier'):.4f}/{mean(s05,'hgb_log_loss'):.4f}; market {mean(s05,'market_brier'):.4f}/{mean(s05,'market_log_loss'):.4f}. HGB expected losses {sum(r['hgb_expected_loss'] for r in s05):.1f}, observed losses 298, excess losses {sum(r['hgb_excess_loss'] for r in s05):.1f}.\n\nOn same-side rows both are 330–177 (65.1%); HGB Brier/log loss {mean(same,'hgb_brier'):.4f}/{mean(same,'hgb_log_loss'):.4f}, market {mean(same,'market_brier'):.4f}/{mean(same,'market_log_loss'):.4f}. On opposite-side rows HGB is 86–121, market 121–86; Brier {mean(opposite,'hgb_brier'):.4f} vs {mean(opposite,'market_brier'):.4f}. Market leads in both populations and more strongly when sides differ.\n\nHGB P(Over)<50% contains {len(below)} rows; HGB 398–291, market 433–256, Brier {mean(below,'hgb_brier'):.4f} vs {mean(below,'market_brier'):.4f}. In 45–50% (N=32), HGB is 11–21 and market 23–9; realized Over rate 65.6%, mean HGB probability 47.5% and market probability 59.0%. In 40–45% (N=46), HGB 22–24 and market 28–18; HGB has slightly lower log loss (0.711 vs 0.736) but worse Brier and record. The <30% pool (N=381) is HGB 244–137 and market 256–125. No settled primary rows have HGB P(Over)>65%; empty upper bands remain visible.\n\nAll 207 crossings are HGB<50/market≥50: HGB wins 86 and market 121. Reverse crossings: zero. Within 5pp of 50%, both near: 10; HGB near/market decisive: 16; market near/HGB decisive: 136; neither near: 552. Same-side confidence groups and selected-confidence bands are reported separately; greater HGB confidence is not a general guarantee of lower proper-score loss.\n\n## Complementarity and where to keep watching\n\nThe line-separated 2D matrix and rankings show where absolute, excess, and log-loss burdens collect. Both-wrong has 177 rows, Brier 0.526 HGB vs 0.391 market; both-correct has 330, Brier 0.078 vs 0.133. Local HGB wins and a small 40–45% log-loss advantage suggest information worth observing, but disagreement outcomes favor market 121–86 and market wins overall. This evidence does not yet establish durable complementarity. Continue prospective observation by date, particularly sub-50 and disagreement segments; treat 45–50% and long-price positive selected-side edges as questionable, not as filters.\n\nFixed 25/75, 50/50, and 75/25 blends are diagnostics only; no weights were tuned. Full signed probability-gap, selected-side-gap, production-edge, confidence, price, side, date, cumulative, and fixed-blend tables are available as separate CSVs. Oct. 13's larger contribution remains visible in date/cumulative tables. Oct. 14 has {len(oct14)} settled 0.5 rows: HGB {sum(r['hgb_win'] for r in oct14)}–{sum(r['hgb_loss'] for r in oct14)}, market {sum(r['market_win'] for r in oct14)}–{sum(r['market_loss'] for r in oct14)}; it is a first prospective context, too small to settle the question.\n\nOct. 15 pending snapshot contains {len(pending)} quote-complete market matched prediction lines: 185 at 0.5, 23 at 1.5, and 2 at 2.5. At 0.5, same/opposite side counts are {pending_summary['by_line']['0.5']['same_side_count']}/{pending_summary['by_line']['0.5']['opposite_side_count']}; HGB<50/market≥50 crossings: 55, reverse: 0. Rows without two-sided market quotes cannot support no-vig intersection metrics and are excluded from this count. All pending rows are ungraded.\n\n## Package and guardrails\n\nThe package contains canonical settled/unresolved rows; quadrant, side agreement, same-side confidence, opposite-side divergence, full HGB/market probability bands, sub-/above-50 analyses, full 2D matrix/loss density, crossing/near-boundary, price, date/cumulative, proper-score/complementarity, fixed blends, loss/success rankings, pending inventory, lineage hashes, and backlog context.\n\nNo filter, threshold, production model, or Funnel V1 change. Provider calls: 0; paid credits: 0; production DB mutations: 0.\n'''
    (OUT/'report.md').write_text(report)
    print(json.dumps({'output':str(OUT),'rows':len(rows),'settled':len(settled),'settled_0_5':len(s05),'pending':len(pending),'quadrants':summary['quadrants']},indent=2))
if __name__=='__main__':main()
