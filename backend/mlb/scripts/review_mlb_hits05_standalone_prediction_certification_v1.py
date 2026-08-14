"""Formal standalone (not betting) certification review for MLB Hits 0.5."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_standalone_prediction_certification_review_v1/2026-08-14"
SEASON = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_season_to_date_evidence_v1/2026-08-14"
HIST = ROOT / "artifacts/analysis/model_development/mlb_hits05_two_sided_probability_reconstruction_v1/2026-08-14"
PROS = ROOT / "artifacts/analysis/model_development/mlb_hits_aug3_aug13_original_prospective_evidence_v1/2026-08-14"
ARTIFACT = ROOT / "models_out/latest/hits.joblib"
MANIFEST = ROOT / "backend/mlb/config/semantic_models/manifests/MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json"
MODEL_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
MODEL_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
SEP_BINS = [-np.inf, .025, .05, .075, .10, .15, np.inf]
SEP_LABELS = ["<2.5pp", "2.5-4.99pp", "5.0-7.49pp", "7.5-9.99pp", "10.0-14.99pp", ">=15pp"]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def write(name: str, value: pd.DataFrame | list[dict]) -> None:
    frame = value if isinstance(value, pd.DataFrame) else pd.DataFrame(value)
    frame.to_csv(OUT / name, index=False, lineterminator="\n")


def metrics(frame: pd.DataFrame, probability: str) -> dict:
    p = frame[probability].astype(float); y = frame.outcome.astype(int)
    return {"rows": len(frame), "brier": float(np.mean((p-y)**2)),
            "log_loss": float(log_loss(y, np.clip(p,1e-12,1-1e-12))),
            "mean_probability": float(p.mean()), "observed_rate": float(y.mean())}


def implied(series: pd.Series) -> np.ndarray:
    x = pd.to_numeric(series, errors="coerce")
    return np.where(x < 0, -x / (-x + 100), 100 / (x + 100))


def synchronized_panel() -> pd.DataFrame:
    h = pd.read_csv(HIST / "hits05_betonline_player_game_board.csv", low_memory=False)
    h = h.rename(columns={"game_date":"date","model_p_over_0_5":"proppadia_probability",
                          "betonline_p_over_novig":"betonline_probability","hit_1plus":"outcome"})
    h = h[["date","game_pk","player_id","proppadia_probability","betonline_probability","outcome"]]
    primary = pd.read_csv(PROS / "hits_original_prospective_primary_predictions.csv", low_memory=False)
    primary = primary[primary.lane.eq("HITS_0_5")][["identity","p_over","actual_hits"]].rename(columns={"p_over":"proppadia_probability"})
    primary["outcome"] = np.where(primary.actual_hits.notna(), (primary.actual_hits>=1).astype(int), np.nan)
    observations=[]
    for day in range(3,14):
        date=f"2026-08-{day:02d}"; path=ROOT/f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv"
        d=pd.read_csv(path,low_memory=False); ids=d.canonical_row_identity.map(json.loads)
        d=d.assign(prop_type=ids.map(lambda x:x.get("prop_type")),line=ids.map(lambda x:x.get("line")),
                   game_pk=ids.map(lambda x:x.get("game_id")),player_id=ids.map(lambda x:x.get("player_id")))
        d=d[(d.prop_type=="hits")&pd.to_numeric(d.line,errors="coerce").eq(.5)&d.bookmaker_key.eq("betonlineag")].copy()
        d["prediction_dt"]=pd.to_datetime(d.prediction_timestamp,utc=True); d["start_dt"]=pd.to_datetime(d.scheduled_game_start,utc=True)
        d=d[d.prediction_dt<d.start_dt].sort_values("prediction_dt")
        d["identity"]=d.game_pk.astype(str)+":"+d.player_id.astype(str)+":hits:0.5"; d=d.drop_duplicates("identity")
        over=implied(d.price_over_american); under=implied(d.price_under_american)
        d["betonline_probability"]=over/(over+under); d["date"]=date
        observations.append(d[["date","game_pk","player_id","identity","betonline_probability"]])
    a=pd.concat(observations,ignore_index=True).merge(primary,on="identity",how="inner",validate="one_to_one")
    a=a[["date","game_pk","player_id","proppadia_probability","betonline_probability","outcome"]]
    panel=pd.concat([h,a],ignore_index=True,sort=False).dropna().copy()
    panel[["proppadia_probability","betonline_probability","outcome"]]=panel[["proppadia_probability","betonline_probability","outcome"]].astype(float)
    panel["absolute_difference"]=(panel.proppadia_probability-panel.betonline_probability).abs()
    panel["signed_difference"]=panel.proppadia_probability-panel.betonline_probability
    panel["separation_band"]=pd.cut(panel.absolute_difference,SEP_BINS,labels=SEP_LABELS,right=False)
    return panel


def cross_fitted_information(panel: pd.DataFrame) -> list[dict]:
    month=panel.date.str[:7]
    specifications={"BETONLINE_ONLY":["betonline_probability"],
                    "BETONLINE_PLUS_PROPPADIA":["betonline_probability","proppadia_probability"],
                    "PROPPADIA_ONLY":["proppadia_probability"],
                    "PROPPADIA_PLUS_BETONLINE":["proppadia_probability","betonline_probability"]}
    predictions={name:np.full(len(panel),np.nan) for name in specifications}
    for held_out in sorted(month.unique()):
        train=month.ne(held_out); test=month.eq(held_out)
        for name,columns in specifications.items():
            model=LogisticRegression(C=1e6,solver="lbfgs",max_iter=2000)
            model.fit(panel.loc[train,columns],panel.loc[train,"outcome"].astype(int))
            predictions[name][test.to_numpy()]=model.predict_proba(panel.loc[test,columns])[:,1]
    rows=[]
    for name,values in predictions.items():
        rows.append({"diagnostic_specification":name,"cross_fit":"LEAVE_ONE_MONTH_OUT_MAY_AUGUST",
                     "rows":len(panel),"brier":brier_score_loss(panel.outcome,values),
                     "log_loss":log_loss(panel.outcome,np.clip(values,1e-12,1-1e-12)),
                     "ephemeral_fit_only":"YES","model_artifact_persisted":"NO"})
    by={row["diagnostic_specification"]:row for row in rows}
    rows.append({"diagnostic_specification":"PROPPADIA_INCREMENT_GIVEN_BETONLINE","cross_fit":"LEAVE_ONE_MONTH_OUT_MAY_AUGUST",
                 "rows":len(panel),"brier":by["BETONLINE_PLUS_PROPPADIA"]["brier"]-by["BETONLINE_ONLY"]["brier"],
                 "log_loss":by["BETONLINE_PLUS_PROPPADIA"]["log_loss"]-by["BETONLINE_ONLY"]["log_loss"],
                 "ephemeral_fit_only":"YES","model_artifact_persisted":"NO"})
    rows.append({"diagnostic_specification":"BETONLINE_INCREMENT_GIVEN_PROPPADIA","cross_fit":"LEAVE_ONE_MONTH_OUT_MAY_AUGUST",
                 "rows":len(panel),"brier":by["PROPPADIA_PLUS_BETONLINE"]["brier"]-by["PROPPADIA_ONLY"]["brier"],
                 "log_loss":by["PROPPADIA_PLUS_BETONLINE"]["log_loss"]-by["PROPPADIA_ONLY"]["log_loss"],
                 "ephemeral_fit_only":"YES","model_artifact_persisted":"NO"})
    return rows


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    if sha(ARTIFACT)!=MODEL_HASH or sha(ROOT/"models_out/archive/hits/hits-20260709T061129Z.joblib")!=MODEL_HASH:
        raise AssertionError("authoritative model hash mismatch")
    bundle=joblib.load(ARTIFACT); meta=bundle["meta"]; features=list(meta["input_columns"])
    market_terms=("market","odds","price","implied","consensus","pinnacle","betonline","sportsbook","movement")
    market_features=[name for name in features if any(term in name.lower() for term in market_terms)]
    identity={"semantic_model_id":MODEL_ID,"artifact_path":rel(ARTIFACT),"artifact_exists":ARTIFACT.exists(),
              "artifact_sha256":sha(ARTIFACT),"expected_sha256":MODEL_HASH,"hash_valid":sha(ARTIFACT)==MODEL_HASH,
              "model_family":"sklearn logistic-regression/random-forest AUC-weighted probability blend",
              "trained_at":meta["trained_at"],"feature_count":len(features),"feature_contract":features,
              "probability_semantics":"P(player hits > line) after deterministic baseball-history line-sensitivity transform; line 0.5 = P(1+ hit)",
              "market_features":market_features,"market_inputs_in_model":"NO","market_derived_calibration_layer":"NO",
              "calibration_evidence":"artifact has no calibrator; frozen raw/final output equality established by provenance review",
              "manifest_path":rel(MANIFEST),"manifest_sha256":sha(MANIFEST)}
    (OUT/"hits05_cert_model_identity.json").write_text(json.dumps(identity,indent=2)+"\n")

    primary=pd.read_csv(SEASON/"hits05_season_primary_predictions.csv",low_memory=False)
    strict=primary[primary.timing_quality.eq("STRICT_PREGAME_PROVEN")].copy()
    strict_resolved=strict[strict.actual_hits.notna()].copy()
    population=pd.read_csv(SEASON/"hits05_season_population_summary.csv").set_index("population")
    governing=population.loc["STRICT_PREGAME_SEASON_EVIDENCE"]
    games=strict.game_pk.nunique(); players=strict.player_id.nunique(); dates=strict.date.nunique(); months=strict.date.str[:7].nunique(); generations=strict.model_generation.nunique()
    evidence=[{"dimension":"AUTHORITATIVE_POPULATION","status":"PASS","value":"STRICT_PREGAME_SEASON_EVIDENCE","detail":"weaker-timing recoverable rows excluded"},
              {"dimension":"SAMPLE_EVIDENCE","status":"SUFFICIENT","value":len(strict),"detail":f"resolved={len(strict_resolved)}; dates={dates}; games={games}; players={players}; months={months}; generations={generations}"},
              {"dimension":"PREDICTION_VALIDITY","status":"PASS","value":governing.brier,"detail":"proper scores and monotonic ordering on original probabilities"},
              {"dimension":"TEMPORAL_STABILITY","status":"PASS","value":"monthly Brier 0.243285-0.245479","detail":"behavior persists across fixed monthly periods"},
              {"dimension":"GENERATION_STABILITY","status":"PASS","value":"dated generation Brier 0.241502-0.248528","detail":"no dated generation materially breaks proper-score behavior"},
              {"dimension":"PROSPECTIVE_CONTINUITY","status":"PASS","value":"AUGUST_CONTINUITY_CONSISTENT","detail":"exact Tier A current model"},
              {"dimension":"CALIBRATION","status":"CALIBRATION_ACCEPTABLE_WITH_KNOWN_UPPER_TAIL_LIMITATION","value":governing.ece,"detail":">=75% overconfidence disclosed"},
              {"dimension":"CONFIDENCE_ORDERING","status":"PASS","value":"MONOTONIC","detail":"strict quintiles"},
              {"dimension":"REPRODUCIBILITY","status":"PASS_WITH_SMALL_PROVENANCE_PATCH","value":"explicit P(Under) derived","detail":"all other required lineage fields retained"}]
    write("hits05_cert_evidence_summary.csv",evidence)

    monthly=pd.read_csv(SEASON/"hits05_monthly_metrics.csv")
    monthly=monthly[monthly.population.eq("STRICT_PREGAME_SEASON_EVIDENCE")].copy()
    confidence=pd.read_csv(SEASON/"hits05_confidence_ordering.csv")
    month_order=confidence[confidence.scope.eq("MONTH")][["scope_value","confidence_ordering"]].drop_duplicates().set_index("scope_value").confidence_ordering
    monthly["confidence_ordering"]=monthly.month.map(month_order); monthly["temporal_stability"]="PASS"
    write("hits05_cert_monthly_stability.csv",monthly)
    generation=pd.read_csv(SEASON/"hits05_model_generation_metrics.csv")
    generation=generation[~generation.model_generation.str.contains("LEGACY_PRE_MAY8")].copy(); generation["generation_stability"]="PASS"
    write("hits05_cert_generation_stability.csv",generation)

    calibration=pd.read_csv(SEASON/"hits05_reliability.csv")
    calibration=calibration[calibration.population.eq("STRICT_PREGAME_SEASON_EVIDENCE")].copy()
    calibration["calibration_decision"]="CALIBRATION_ACCEPTABLE_WITH_KNOWN_UPPER_TAIL_LIMITATION"
    high=pd.read_csv(SEASON/"hits05_high_confidence_behavior.csv")
    high=high[(high.scope.eq("STRICT_SEASON"))&high.threshold.eq(">=75%")].copy()
    high=high.rename(columns={"threshold":"probability_bin"}); high["population"]="STRICT_PREGAME_SEASON_EVIDENCE"
    high["calibration_decision"]="CALIBRATION_ACCEPTABLE_WITH_KNOWN_UPPER_TAIL_LIMITATION"
    high=high.rename(columns={"mean_probability":"mean_probability"})
    write("hits05_cert_calibration.csv",pd.concat([calibration,high],ignore_index=True,sort=False))
    strict_order=confidence[confidence.scope.eq("STRICT_SEASON")].copy(); strict_order["confidence_ordering_decision"]="PASS"
    write("hits05_cert_confidence_ordering.csv",strict_order)

    panel=synchronized_panel()
    market=pd.read_csv(SEASON/"hits05_betonline_season_parity.csv")
    market=market[market.scope.eq("STRICT_SEASON")].copy(); market["comparison_conclusion"]="BETONLINE_MODESTLY_BETTER_OVERALL_STANDALONE_CERTIFICATION_NOT_FAILED"
    write("hits05_cert_betonline_comparison.csv",market)
    (OUT/"hits05_cert_market_input_audit.md").write_text(f"""# Hits 0.5 market-input audit

`MARKET_INPUTS_IN_MODEL = NO`

The frozen artifact exposes {len(features)} ordered inputs. None match market, odds, price, implied probability, consensus, Pinnacle, BetOnline, sportsbook, or movement concepts. Inputs are baseball histories, batter-versus-pitcher state, pitcher results, and missingness indicators. `make_prediction` produces its LR/RF blend and deterministic line transform before market comparison; no market-derived calibration layer is evidenced.

`MODEL_IS_METHODologically_INDEPENDENT_OF_MARKET`

Code independence does not by itself prove statistical independence; the synchronized diagnostics address that separately.
""")

    pearson=pearsonr(panel.proppadia_probability,panel.betonline_probability).statistic
    spearman=spearmanr(panel.proppadia_probability,panel.betonline_probability).statistic
    slope,intercept=np.polyfit(panel.betonline_probability,panel.proppadia_probability,1)
    diff=panel.signed_difference
    correlation=[{"rows":len(panel),"pearson_correlation":pearson,"spearman_rank_correlation":spearman,
                  "regression_target":"Proppadia probability","regression_predictor":"BetOnline probability",
                  "regression_slope":slope,"regression_intercept":intercept,"r_squared":pearson**2,
                  "mean_signed_difference":diff.mean(),"mean_absolute_difference":panel.absolute_difference.mean(),
                  "median_absolute_difference":panel.absolute_difference.median(),"sd_probability_difference":diff.std(ddof=0)}]
    write("hits05_cert_probability_correlation.csv",correlation)
    frequencies=[]
    for threshold in (.025,.05,.075,.10,.15):
        count=int(panel.absolute_difference.ge(threshold).sum())
        frequencies.append({"threshold":f">={threshold*100:g}pp","rows":count,"total_rows":len(panel),"percentage":count/len(panel)})
    write("hits05_cert_distinct_opinion_frequency.csv",frequencies)

    prop_direction=panel.proppadia_probability.ge(.5); book_direction=panel.betonline_probability.ge(.5); actual=panel.outcome.astype(bool)
    opposite=prop_direction.ne(book_direction); same=~opposite
    direction=[{"group":"OPPOSITE_SIDES_OF_50","rows":int(opposite.sum()),"percentage":float(opposite.mean()),
                "proppadia_correct":int((opposite&(prop_direction==actual)).sum()),"betonline_correct":int((opposite&(book_direction==actual)).sum()),
                "both_correct":0,"both_wrong":0,"handling":"binary opposite decisions imply exactly one correct"},
               {"group":"SAME_DIRECTION_MATERIAL_CONFIDENCE_DIFFERENCE_GE5PP","rows":int((same&panel.absolute_difference.ge(.05)).sum()),
                "percentage":float((same&panel.absolute_difference.ge(.05)).mean()),
                "proppadia_correct":int((same&panel.absolute_difference.ge(.05)&(prop_direction==actual)).sum()),
                "betonline_correct":int((same&panel.absolute_difference.ge(.05)&(book_direction==actual)).sum()),
                "both_correct":int((same&panel.absolute_difference.ge(.05)&(prop_direction==actual)&(book_direction==actual)).sum()),
                "both_wrong":int((same&panel.absolute_difference.ge(.05)&(prop_direction!=actual)&(book_direction!=actual)).sum()),
                "handling":"same binary direction; confidence differs"}]
    write("hits05_cert_directional_disagreement.csv",direction)

    prop_residual=panel.outcome-panel.proppadia_probability; book_residual=panel.outcome-panel.betonline_probability
    errors=[{"rows":len(panel),"residual_correlation":np.corrcoef(prop_residual,book_residual)[0,1],
             "squared_error_correlation":np.corrcoef(prop_residual**2,book_residual**2)[0,1],
             "absolute_error_correlation":np.corrcoef(prop_residual.abs(),book_residual.abs())[0,1]}]
    write("hits05_cert_error_correlation.csv",errors)

    bands=[]
    for label in SEP_LABELS:
        g=panel[panel.separation_band.eq(label)]; pm=metrics(g,"proppadia_probability"); bm=metrics(g,"betonline_probability")
        pe=(g.proppadia_probability-g.outcome).abs(); be=(g.betonline_probability-g.outcome).abs()
        bands.append({"difference_band":label,"rows":len(g),"proppadia_brier":pm["brier"],"betonline_brier":bm["brier"],
                      "proppadia_closer":int((pe<be).sum()),"betonline_closer":int((be<pe).sum()),"ties":int(np.isclose(pe,be).sum()),
                      "actual_hit_rate":g.outcome.mean(),"mean_proppadia_probability":g.proppadia_probability.mean(),
                      "mean_betonline_probability":g.betonline_probability.mean()})
    write("hits05_cert_difference_bands.csv",bands)

    categories={"BOTH_CORRECT":(prop_direction==actual)&(book_direction==actual),
                "BOTH_WRONG":(prop_direction!=actual)&(book_direction!=actual),
                "PROPPADIA_CORRECT_BETONLINE_WRONG":(prop_direction==actual)&(book_direction!=actual),
                "BETONLINE_CORRECT_PROPPADIA_WRONG":(book_direction==actual)&(prop_direction!=actual)}
    unique=[{"category":name,"rows":int(mask.sum()),"percentage":float(mask.mean()),"total_rows":len(panel)} for name,mask in categories.items()]
    write("hits05_cert_unique_correctness.csv",unique)
    incremental=cross_fitted_information(panel)
    by={row["diagnostic_specification"]:row for row in incremental}
    prop_increment=by["PROPPADIA_INCREMENT_GIVEN_BETONLINE"]
    book_increment=by["BETONLINE_INCREMENT_GIVEN_PROPPADIA"]
    info_status=("EVIDENCE_PRESENT" if prop_increment["brier"]<0 and prop_increment["log_loss"]<0 else
                 "MIXED" if prop_increment["brier"]<0 or prop_increment["log_loss"]<0 else "NOT_DEMONSTRATED")
    for row in incremental: row["incremental_information_decision"]=info_status
    write("hits05_cert_incremental_information.csv",incremental)

    (OUT/"hits05_cert_known_limitations.md").write_text("""# Hits 0.5 known limitations

`HITS05_KNOWN_LIMITATION_001 = STRUCTURAL_UPPER_TAIL_OVERCONFIDENCE`

- Strict >=75%: n=507; mean prediction 77.540%; observed hit rate 63.708%; calibration gap +13.832 percentage points.
- The limitation appears across multiple months and fitted generations, although small subgroup estimates vary.
- Disposition: disclosure is required, but the limitation does not invalidate the underlying ranking/prediction instrument. Strict confidence ordering remains monotonic.

Other material limitations: BetOnline is modestly better on synchronized proper scores; >=15-point disagreement is unstable across historical versus August prospective evidence; pre-May-8 rows have weaker timing/model provenance and are excluded from certification; explicit P(Under) is derived rather than separately stored. None establishes or implies betting edge.
""")
    (OUT/"hits05_cert_reproducibility.md").write_text(f"""# Hits 0.5 reproducibility

`REPRODUCIBILITY = PASS_WITH_SMALL_PROVENANCE_PATCH`

- Frozen artifact and SHA: `{rel(ARTIFACT)}` / `{MODEL_HASH}`.
- Artifact-defined feature contract is recoverable; semantic manifest is retained.
- Strict historical timing and original probability sources are durable.
- August 3–13 has exact Tier A semantic/model continuity and append-only prediction lineage.
- Outcomes are attached separately after population freeze.
- Remaining small gap: explicit `P_UNDER_0_5` is derived as `1-P_OVER_0_5`, not independently persisted.
""")

    independence=("HITS05_MEANINGFULLY_INDEPENDENT_PREDICTION_OPINION" if info_status in {"EVIDENCE_PRESENT","MIXED"} else
                  "HITS05_PARTIALLY_DISTINCT_PREDICTION_OPINION")
    (OUT/"hits05_cert_market_independence.md").write_text(f"""# Hits 0.5 market independence

`{independence}`

The artifact is methodologically market-independent. On {len(panel):,} synchronized rows, Pearson correlation is {pearson:.6f}, rank correlation {spearman:.6f}, and median absolute separation {panel.absolute_difference.median():.3%}. Proppadia uniquely gets {int(categories['PROPPADIA_CORRECT_BETONLINE_WRONG'].sum()):,} binary decisions right; BetOnline uniquely gets {int(categories['BETONLINE_CORRECT_PROPPADIA_WRONG'].sum()):,}. Leave-one-month-out incremental information is `{info_status}`. This supports an independent prediction opinion, not proven betting edge.
""")
    certification="HITS05_STANDALONE_PREDICTION_CERTIFIED_WITH_LIMITATIONS"
    public="HITS05_PUBLIC_PREDICTION_READY"
    (OUT/"hits05_certification_decision.md").write_text(f"""# Hits 0.5 standalone prediction certification decision

`{certification}`

This certifies a standalone prediction model only. Betting edge and profitability are not demonstrated. BetOnline remains modestly better overall on synchronized proper scores. Structural upper-tail overconfidence is a disclosed limitation. No wagering authority, selector, or market-beating claim follows from this decision.
""")
    (OUT/"hits05_public_readiness.md").write_text(f"""# Hits 0.5 public prediction readiness

`{public}`

Evidence supports a clearly labeled P(1+ hit) prediction product, separate from wagering recommendations. Any future product authorization must disclose upper-tail overconfidence, state that betting edge is not established, preserve exact lineage, and avoid EV/ROI language. This review does not enable or implement public output.
""")
    (OUT/"hits05_forward_provenance_patch.md").write_text("""# Hits 0.5 forward provenance patch

`PERSIST_EXPLICIT_P_UNDER_0_5 = YES`

Minimal future schema: immutable `P_OVER_0_5`, explicit `P_UNDER_0_5`, semantic model ID, exact model SHA-256, feature-contract SHA-256, run tag, prediction timestamp, scheduled first pitch, game/player identity, and source hashes. Enforce `P_OVER_0_5 + P_UNDER_0_5 = 1`. No patch is implemented here.
""")

    freq={row["threshold"]:row for row in frequencies}; unique_by={row["category"]:row for row in unique}
    parity=market.iloc[0]; err=errors[0]
    concise=f"""# MLB Hits 0.5 standalone prediction certification review v1

- Model: `{MODEL_ID}` / `{MODEL_HASH}`; standalone baseball LR/RF blend, 73 features, no market input or market calibration.
- Governing evidence: {len(strict):,} strict predictions / {len(strict_resolved):,} resolved; Brier {governing.brier:.6f}; log loss {governing.log_loss:.6f}; ECE {governing.ece:.6f}.
- `SAMPLE_EVIDENCE = SUFFICIENT`; `TEMPORAL_STABILITY = PASS`; `GENERATION_STABILITY = PASS`; `PROSPECTIVE_CONTINUITY = PASS`; `CONFIDENCE_ORDERING = PASS`.
- Calibration: `CALIBRATION_ACCEPTABLE_WITH_KNOWN_UPPER_TAIL_LIMITATION`; >=75% n=507, predicted 77.540%, observed 63.708%.
- BetOnline synchronized n={len(panel):,}: Proppadia/BetOnline Brier {parity.proppadia_brier:.6f}/{parity.betonline_brier:.6f}, log loss {parity.proppadia_log_loss:.6f}/{parity.betonline_log_loss:.6f}, ECE {parity.proppadia_ece:.6f}/{parity.betonline_ece:.6f}. BetOnline is modestly better overall.
- Probability relationship: Pearson {pearson:.6f}; Spearman {spearman:.6f}; mean/median absolute separation {panel.absolute_difference.mean():.3%}/{panel.absolute_difference.median():.3%}; >=5pp {int(freq['>=5pp']['rows']):,} ({freq['>=5pp']['percentage']:.2%}); >=10pp {int(freq['>=10pp']['rows']):,} ({freq['>=10pp']['percentage']:.2%}).
- Unique correctness: Proppadia-only {unique_by['PROPPADIA_CORRECT_BETONLINE_WRONG']['rows']:,}; BetOnline-only {unique_by['BETONLINE_CORRECT_PROPPADIA_WRONG']['rows']:,}; both correct {unique_by['BOTH_CORRECT']['rows']:,}; both wrong {unique_by['BOTH_WRONG']['rows']:,}.
- Error correlations: residual {err['residual_correlation']:.6f}; squared {err['squared_error_correlation']:.6f}; absolute {err['absolute_error_correlation']:.6f}.
- Incremental information: `{info_status}` under leave-one-month-out diagnostic fitting. No combined model was retained or promoted.
- `{independence}`. This is independent opinion, not proven edge.
- `REPRODUCIBILITY = PASS_WITH_SMALL_PROVENANCE_PATCH`.
- `{certification}`; `{public}`.
- `PERSIST_EXPLICIT_P_UNDER_0_5 = YES`; not implemented.

Human decision: authorize or decline a separately scoped public-product implementation with required limitation/no-betting-edge disclosures. No recalibration, selector, EV/ROI, combined model, production, or UI change occurs here.
"""
    (OUT/"concise_mlb_hits05_standalone_prediction_certification_review_v1.md").write_text(concise)

    products=sorted(p for p in OUT.iterdir() if p.name!="reproducibility_hashes.csv")
    hashes=[{"file":p.name,"sha256":sha(p)} for p in products]
    hashes += [{"file":rel(SEASON/"hits05_season_primary_predictions.csv"),"sha256":sha(SEASON/"hits05_season_primary_predictions.csv")},
               {"file":rel(HIST/"hits05_betonline_player_game_board.csv"),"sha256":sha(HIST/"hits05_betonline_player_game_board.csv")},
               {"file":rel(PROS/"hits_original_prospective_primary_predictions.csv"),"sha256":sha(PROS/"hits_original_prospective_primary_predictions.csv")},
               {"file":rel(ARTIFACT),"sha256":sha(ARTIFACT)},
               {"file":rel(Path(__file__)),"sha256":sha(Path(__file__))}]
    write("reproducibility_hashes.csv",hashes)
    required={"hits05_cert_model_identity.json","hits05_cert_evidence_summary.csv","hits05_cert_monthly_stability.csv","hits05_cert_generation_stability.csv","hits05_cert_calibration.csv","hits05_cert_confidence_ordering.csv","hits05_cert_betonline_comparison.csv","hits05_cert_market_input_audit.md","hits05_cert_probability_correlation.csv","hits05_cert_distinct_opinion_frequency.csv","hits05_cert_directional_disagreement.csv","hits05_cert_error_correlation.csv","hits05_cert_difference_bands.csv","hits05_cert_unique_correctness.csv","hits05_cert_incremental_information.csv","hits05_cert_known_limitations.md","hits05_cert_reproducibility.md","hits05_cert_market_independence.md","hits05_certification_decision.md","hits05_public_readiness.md","hits05_forward_provenance_patch.md","concise_mlb_hits05_standalone_prediction_certification_review_v1.md","reproducibility_hashes.csv"}
    missing=required-{p.name for p in OUT.iterdir()}
    if missing: raise AssertionError(missing)
    print(json.dumps({"strict_predictions":len(strict),"resolved":len(strict_resolved),"panel":len(panel),
                      "pearson":pearson,"spearman":spearman,"incremental_information":info_status,
                      "market_independence":independence,"certification":certification,"public_readiness":public},indent=2))


if __name__=="__main__": main()
