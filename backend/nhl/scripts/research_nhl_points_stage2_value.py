#!/usr/bin/env python3
"""Stage-2 Points shadow candidates on top of the stage-1 inputs (P2.58, 2026-11-16). Research only.

Do xG (ixg_d10_per60, sh_pct_shrunk) and the PP points component (pp_pts60_shrunk, exp_pp_min,
exp_pp_points) add anything once game lines and PP unit are in? Same recipe, rebuilt 2023-24 +
2024-25 training frames; 2025-26 holdout and live 2026-11-01..11-14 (the PP evidence ends 11-14).
"""
import json, sys, numpy as np, pandas as pd
sys.path.insert(0,".")
from scipy.stats import poisson
from backend.nhl import points_candidate_shadows as sh
from backend.nhl.scripts import freeze_nhl_points_candidate_shadows as fz
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze
from backend.nhl.scripts import research_nhl_points_pp_component as ppc
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
K=["game_id","player_id"]
ri=fz.research_inputs()
xg=pd.read_csv("artifacts/analysis/nhl/points_refit_candidates_2026-11-16/rolling_inputs.csv.gz")[K+["ixg_d10_per60","sh_pct_shrunk"]]
pp=ppc.add_pp_features(ppc.pp_table(),160.0)[K+ppc.PP_FEATURES]
R="artifacts/analysis/nhl/points_training_rebuild_2026-11-16/"
tr=pd.read_csv(R+"training_frame_2023_2024.csv.gz",float_precision="round_trip"); s25=pd.read_csv(R+"season_frame_2025.csv.gz",float_precision="round_trip")
lv=ms.frames()["live"].copy(); lv["slate_date"]=lv.game_date.astype(str)
tr,s25,lv=(d.merge(ri[K+sh.PPU+sh.TOT],on=K,how="left").merge(xg,on=K,how="left").merge(pp,on=K,how="left") for d in (tr,s25,lv))
for d in (tr,s25,lv): d["attempts_d10_per60"]=scorer.impute_unknown_attempts(d)
F=list(freeze.FEATURES)+sh.PPU+sh.TOT
specs={"BASE_TP":F,"TP_XG":F+["ixg_d10_per60","sh_pct_shrunk"],"TP_PPPTS":F+ppc.PP_FEATURES,"TP_XG_PPPTS":F+["ixg_d10_per60","sh_pct_shrunk"]+ppc.PP_FEATURES}
M={}
for k,f in specs.items():
    g=freeze.research.fit_count(tr,f,"goals"); a=freeze.research.fit_count(tr,f,"assists")
    M[k]={"structure":"GOALS_PLUS_ASSISTS","feature_columns":f,"fill_zero":True,"scaler":g["scaler"],"goals":g,"assists":a}
for d in (s25,lv):
    for k,m in M.items(): d[k]=scorer.score(d,m).expected_points.to_numpy()
s25["role_change"]=s25["role_up"]=s25["role_down"]=False
lv_ok=lv[lv.slate_date<="2026-11-14"].copy()
for part,d in (("holdout",s25),("live<=11-14",lv_ok)):
    for k in list(specs)[1:]:
        t=pmc.paired(d,k,"BASE_TP",{"all":np.ones(len(d),bool)})
        print(part,k,[(r.line,r.diff_x1000,r.ci_x1000) for r in t.itertuples()])
pr=pd.read_csv("artifacts/analysis/nhl/qualified_plays_vs_market_2026-11-16_close/priced_rows.csv.gz")
p05=pr[pr.lane.eq("points")&pr.line.eq(0.5)&pr.side.eq("Over")][K+["mkt_over"]]
mk=lv_ok.merge(p05,on=K); y=(mk.realized_points>=1).astype(float).to_numpy()
print("two-sided", len(mk), "market", round(float(base.log_loss_rows(mk.mkt_over.to_numpy(),y).mean()),4), {k:round(float(base.log_loss_rows(poisson.sf(0,mk[k].to_numpy()),y).mean()),4) for k in specs})
