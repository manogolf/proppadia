#!/usr/bin/env python3
"""Summarize retained NHL 2026 prospective Moneyline evidence (offline only)."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT = ROOT / "artifacts/analysis/nhl/all_candidate_evaluation_export/full_window_2026-10-17"
OLD = ROOT / "artifacts/analysis/nhl/moneyline_prospective_performance/2026-10-13"
OUT = ROOT / "artifacts/analysis/nhl/moneyline_favorites_pricing_2026-10-17"
V3 = "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3"
V2 = "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2"


def rows(path: Path):
    with path.open(newline="", encoding="utf-8-sig") as f:
        yield from csv.DictReader(f)


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def number(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def decimal(price):
    p = number(price)
    if p is None or p == 0:
        return None
    return 1 + p / 100 if p > 0 else 1 + 100 / abs(p)


def calibration(probabilities, outcomes):
    bins = []
    for lo in (0, .1, .2, .3, .4, .5, .6, .7, .8, .9):
        ix = [i for i, p in enumerate(probabilities) if lo <= p < lo + .1 or (lo == .9 and p == 1)]
        if ix:
            bins.append({"range": f"{lo:.1f}-{lo + .1:.1f}", "n": len(ix),
                         "mean_probability": sum(probabilities[i] for i in ix) / len(ix),
                         "observed_rate": sum(outcomes[i] for i in ix) / len(ix)})
    return bins


def metrics(probabilities, outcomes):
    n = len(outcomes)
    if not n:
        return {"n": 0}
    clip = lambda p: min(max(p, 1e-15), 1 - 1e-15)
    wins = sum((p >= .5) == bool(y) for p, y in zip(probabilities, outcomes))
    return {"n": n, "accuracy": wins / n,
            "brier": sum((p - y) ** 2 for p, y in zip(probabilities, outcomes)) / n,
            "log_loss": -sum(y * math.log(clip(p)) + (1-y) * math.log(1-clip(p)) for p, y in zip(probabilities, outcomes)) / n,
            "mean_probability": sum(probabilities) / n,
            "observed_rate": sum(outcomes) / n,
            "calibration_bins": calibration(probabilities, outcomes)}


def hit_rate_and_roi(wagers):
    n = len(wagers)
    if not n:
        return {"n": 0}
    net = sum((d - 1) if won else -1 for _, d, won in wagers)
    return {"n": n, "wins": sum(w for _, _, w in wagers), "losses": n - sum(w for _, _, w in wagers),
            "net_units": net, "roi": net / n}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    preds = [r for r in rows(EXPORT / "candidate_predictions.csv")
             if r["model_family"] == "moneyline"]
    quotes = [r for r in rows(EXPORT / "candidate_quotes.csv")
              if r["model_family"] == "moneyline"]
    bygame = defaultdict(dict)
    for p in preds:
        bygame[p["game_id"]][p["model_version"]] = p
    # Use the retained per-slate cross-market ledger for model probabilities
    # and two-sided prices. The export's quote source may instead be the raw
    # normalized observation file, which does not carry model predictions.
    ledger_paths = []
    for slate in sorted({p["slate_date"] for p in preds}):
        referenced = Counter((ROOT / q["source_path"]).resolve() for q in quotes
            if q["slate_date"] == slate and q["source_path"].endswith("moneyline_shot_finishing_challenger_v3_ledger.csv"))
        if referenced:
            ledger_paths.append(referenced.most_common(1)[0][0])
        else:
            candidates = sorted((ROOT / "artifacts/operational/nhl/cross_market_shadow/season=2026" /
                f"slate_date={slate}").glob("run_type=*/state=*/moneyline_shot_finishing_challenger_v3_ledger.csv"),
                key=lambda p: p.stat().st_mtime_ns)
            if candidates:
                ledger_paths.append(candidates[-1].resolve())
    ledger_paths = sorted(set(ledger_paths))
    ledger_path_by_date = {p.parent.parent.parent.name.split("=", 1)[1]: p for p in ledger_paths}
    ledger_hash_by_date = {d: sha(p) for d, p in ledger_path_by_date.items()}
    ledger = defaultdict(list)
    ledger_refs = []
    for path in ledger_paths:
        ledger_refs.append({"path": str(path.relative_to(ROOT)), "sha256": sha(path)})
        for r in rows(path):
            if "slate_date" not in r or "game_id" not in r:
                continue
            ledger[(r["slate_date"], r["game_id"])].append(r)

    # Bind the two frozen model probabilities to the actual daily ledger used by
    # the export's audited Moneyline price rows. Do not infer p from a side.
    games = {}
    issues = []
    for game_id, arms in bygame.items():
        if V3 not in arms or V2 not in arms:
            issues.append({"game_id": game_id, "reason": "MISSING_MODEL_ARM"})
            continue
        p3, p2 = arms[V3], arms[V2]
        lr = ledger[(p3["slate_date"], game_id)]
        unique = {(r.get("challenger_home_win_probability"), r.get("v2_home_win_probability")) for r in lr}
        unique.discard((None, None)); unique.discard(("", ""))
        pairs = {(number(a), number(b)) for a, b in unique}
        pairs.discard((None, None))
        if len(pairs) != 1:
            issues.append({"game_id": game_id, "reason": "LEDGER_PROBABILITY_NOT_UNIQUE", "pair_count": len(pairs)})
            continue
        ph3, ph2 = next(iter(pairs))
        y = 1 if p3["outcome_side"] == "HOME" else 0 if p3["outcome_side"] == "AWAY" else None
        games[game_id] = {"game_id": game_id, "slate_date": p3["slate_date"],
            "scheduled_start_utc": p3["scheduled_start_utc"], "prediction_timestamp_utc": p3["prediction_timestamp_utc"],
            "home_team": next((r.get("home_team") for r in lr if r.get("home_team")), ""),
            "away_team": next((r.get("away_team") for r in lr if r.get("away_team")), ""),
            "outcome_status": p3["outcome_status"], "outcome_side": p3["outcome_side"], "y_home": y,
            "v3_home_probability": ph3, "v2_home_probability": ph2,
            "v3_pick": "HOME" if ph3 >= .5 else "AWAY", "v2_pick": "HOME" if ph2 >= .5 else "AWAY",
            "v3_prediction_identity": p3["prediction_identity"], "v2_prediction_identity": p2["prediction_identity"],
            "prediction_source_path": p3["source_path"], "prediction_source_sha256": p3["source_sha256"],
            "prediction_parent_run_id": p3["parent_run_id"],
            "ledger_source_path": str(ledger_path_by_date[p3["slate_date"]].relative_to(ROOT)) if p3["slate_date"] in ledger_path_by_date else "",
            "ledger_source_sha256": ledger_hash_by_date.get(p3["slate_date"], ""),
            "betonline_home_price": None, "betonline_away_price": None,
            "betonline_home_novig": None, "betonline_away_novig": None,
            "market_favorite": "", "market_favorite_probability": None}
        # The retained audited ledger contains two-sided rows. Require a same
        # book and quote capture, both pregame-qualified, for market consensus.
        books = defaultdict(dict)
        for r in lr:
            if r.get("qualification_status") != "PREGAME_QUALIFIED":
                continue
            orient = r.get("side_orientation", "").upper()
            if orient not in ("HOME", "AWAY"):
                continue
            b = r.get("sportsbook_key", "")
            # Preserve the exact ledger quote timestamp/capture as its key.
            key = (r.get("source_update_timestamp_utc", ""), r.get("market_observation_timestamp_utc", ""))
            books[(b, key)][orient] = r
        selected = {}
        for (book, key), sides in books.items():
            if set(sides) != {"HOME", "AWAY"}:
                continue
            # Among multiple paired captures choose the latest market update
            # no later than the model prediction timestamp.
            try:
                source_time = datetime.fromisoformat((key[0]).replace("Z", "+00:00"))
                obs = datetime.fromisoformat((key[1] or key[0]).replace("Z", "+00:00"))
                predtime = datetime.fromisoformat(p3["prediction_timestamp_utc"].replace("Z", "+00:00"))
                starttime = datetime.fromisoformat(p3["scheduled_start_utc"].replace("Z", "+00:00"))
            except ValueError:
                continue
            if not (source_time <= obs <= predtime < starttime):
                continue
            if book not in selected or obs > selected[book][0]:
                selected[book] = (obs, sides)
        def novig(sides):
            dh, da = decimal(sides["HOME"].get("american_price")), decimal(sides["AWAY"].get("american_price"))
            if not dh or not da: return None
            ih, ia = 1/dh, 1/da
            return ih / (ih + ia)
        if "betonlineag" in selected:
            _, sides = selected["betonlineag"]
            games[game_id]["betonline_home_price"] = number(sides["HOME"].get("american_price"))
            games[game_id]["betonline_away_price"] = number(sides["AWAY"].get("american_price"))
            games[game_id]["betonline_home_novig"] = novig(sides)
            games[game_id]["betonline_away_novig"] = 1 - novig(sides) if novig(sides) is not None else None
        # Keep every all-book paired quote in separate summary list.
        games[game_id]["book_pairs"] = {}
        for book, (_, sides) in selected.items():
            games[game_id]["book_pairs"][book] = {
                "home_price": number(sides["HOME"].get("american_price")),
                "away_price": number(sides["AWAY"].get("american_price")),
                "home_novig": novig(sides),
                "away_novig": 1 - novig(sides) if novig(sides) is not None else None,
                "home_quote": sides["HOME"], "away_quote": sides["AWAY"]}
        mh = games[game_id]["betonline_home_novig"]
        if mh is not None:
            games[game_id]["market_favorite"] = "HOME" if mh > .5 else "AWAY" if mh < .5 else "PICKEM"
            games[game_id]["market_favorite_probability"] = max(mh, 1-mh)

    # Unfiltered one-row-per-model/game candidate table. Pricing remains in a
    # separate quote file to prevent the books from expanding prediction rows.
    cand_path = OUT / "moneyline_candidates_unfiltered.csv"
    fields = ["candidate_key", "slate_date", "game_id", "home_team", "away_team", "scheduled_start_utc", "prediction_timestamp_utc",
        "run_type", "candidate_policy_status", "canonical_phase", "model_role", "export_model_role", "model_version", "prediction_identity", "home_win_probability", "predicted_side",
        "outcome_status", "outcome_side", "y_home", "source_path", "source_sha256", "parent_run_id", "ledger_source_path", "ledger_source_sha256"]
    with cand_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n"); w.writeheader()
        for g in sorted(games.values(), key=lambda x:(x["slate_date"], x["game_id"])):
            for version, arm, prob, pick in ((V3, bygame[g["game_id"]][V3], g["v3_home_probability"], g["v3_pick"]),
                                             (V2, bygame[g["game_id"]][V2], g["v2_home_probability"], g["v2_pick"])):
                prediction_time = datetime.fromisoformat(arm["prediction_timestamp_utc"].replace("Z", "+00:00"))
                cutover = datetime.fromisoformat("2026-10-14T15:27:11+00:00")
                historical_role = ("CHALLENGER" if version == V3 else "REFERENCE") if prediction_time < cutover else ("PRODUCTION" if version == V3 else "SHADOW")
                candidate_key=f"2026|{g['slate_date']}|{g['game_id']}|moneyline_home||{version}|{arm['prediction_identity']}"
                w.writerow({"candidate_key":candidate_key,"slate_date":g["slate_date"],"game_id":g["game_id"],"home_team":g["home_team"],"away_team":g["away_team"],
                    "scheduled_start_utc":g["scheduled_start_utc"],"prediction_timestamp_utc":arm["prediction_timestamp_utc"],"model_role":historical_role,"export_model_role":arm["model_role"],
                    "run_type":arm["run_type"],"candidate_policy_status":arm["candidate_policy_status"],"canonical_phase":arm["canonical_phase"],
                    "model_version":version,"prediction_identity":arm["prediction_identity"],"home_win_probability":prob,"predicted_side":pick,
                    "outcome_status":arm["outcome_status"],"outcome_side":arm["outcome_side"],"y_home":g["y_home"],
                    "source_path":arm["source_path"],"source_sha256":arm["source_sha256"],"parent_run_id":arm["parent_run_id"],
                    "ledger_source_path":g["ledger_source_path"],"ledger_source_sha256":g["ledger_source_sha256"]})
    quote_path = OUT / "moneyline_model_side_quotes_unfiltered.csv"
    quote_fields = ["candidate_key", "slate_date", "game_id", "model_version", "model_role", "export_model_role", "prediction_identity", "predicted_side",
        "sportsbook_key", "american_price", "decimal_price", "no_vig_probability_selected_side",
        "model_probability_selected_side", "model_market_edge", "expected_value_per_unit",
        "source_quote_timestamp_utc", "quote_observation_timestamp_utc", "quote_qualification_status",
        "ledger_source_path", "ledger_source_sha256"]
    with quote_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=quote_fields, lineterminator="\n"); w.writeheader()
        for g in sorted(games.values(), key=lambda x:(x["slate_date"], x["game_id"])):
            for version, arm, side in ((V3, bygame[g["game_id"]][V3], g["v3_pick"]),
                                       (V2, bygame[g["game_id"]][V2], g["v2_pick"])):
                for book, sides in g.get("book_pairs", {}).items():
                    source = sides["home_quote"] if side == "HOME" else sides["away_quote"]
                    novig_side = g["book_pairs"][book]["home_novig"] if side == "HOME" else g["book_pairs"][book]["away_novig"]
                    pside = g["v3_home_probability"] if side == "HOME" else 1-g["v3_home_probability"]
                    if version == V2:
                        pside = g["v2_home_probability"] if side == "HOME" else 1-g["v2_home_probability"]
                    price_decimal = decimal(source.get("american_price"))
                    prediction_time = datetime.fromisoformat(arm["prediction_timestamp_utc"].replace("Z", "+00:00"))
                    historical_role = ("CHALLENGER" if version == V3 else "REFERENCE") if prediction_time < cutover else ("PRODUCTION" if version == V3 else "SHADOW")
                    w.writerow({"slate_date":g["slate_date"], "game_id":g["game_id"], "model_version":version,
                        "candidate_key":f"2026|{g['slate_date']}|{g['game_id']}|moneyline_home||{version}|{arm['prediction_identity']}",
                        "model_role":historical_role, "export_model_role":arm["model_role"], "prediction_identity":arm["prediction_identity"], "predicted_side":side,
                        "sportsbook_key":book, "american_price":source.get("american_price"),
                        "decimal_price":price_decimal, "no_vig_probability_selected_side":novig_side,
                        "model_probability_selected_side":pside,
                        "model_market_edge":pside-novig_side if novig_side is not None else "",
                        "expected_value_per_unit":pside*price_decimal-1 if price_decimal else "",
                        "source_quote_timestamp_utc":source.get("source_update_timestamp_utc"),
                        "quote_observation_timestamp_utc":source.get("market_observation_timestamp_utc"),
                        "quote_qualification_status":source.get("qualification_status"),
                        "ledger_source_path":g["ledger_source_path"], "ledger_source_sha256":g["ledger_source_sha256"]})

    settled = [g for g in games.values() if g["y_home"] is not None and g["outcome_status"] == "SETTLED"]
    result = {"paired_game_count":len(games),"settled_paired_game_count":len(settled),"unresolved_paired_game_count":len(games)-len(settled),
        "ledger_pairing_issues":issues,"probability_metrics":{},"market_metrics":{},"favorites":{},"disagreements":{},"roi":{},"sensitivity":{},
        "old_89_study_comparison":{},"frozen_export_quote_rows":len(quotes),
        "qualified_model_side_quote_rows":sum(1 for _ in rows(quote_path)),
        "book_coverage_games":{},"source_ledger_count":len(ledger_paths)}
    for label, col in (("V3", "v3_home_probability"),("V2", "v2_home_probability")):
        ps=[g[col] for g in settled]; ys=[g["y_home"] for g in settled]
        result["probability_metrics"][label]=metrics(ps,ys)
    priced=[g for g in settled if g["betonline_home_novig"] is not None]
    if priced:
        result["market_metrics"]["BetOnline no-vig market"] = metrics([g["betonline_home_novig"] for g in priced],[g["y_home"] for g in priced])
    for fav in ("HOME","AWAY"):
        cohort=[g for g in priced if g["market_favorite"]==fav]
        result["favorites"][fav]={"n":len(cohort),"favorite_win_rate":sum(g["y_home"]==(fav=="HOME") for g in cohort)/len(cohort) if cohort else None,
            "v3_accuracy":sum(g["v3_pick"]==g["outcome_side"] for g in cohort)/len(cohort) if cohort else None,
            "v2_accuracy":sum(g["v2_pick"]==g["outcome_side"] for g in cohort)/len(cohort) if cohort else None}
    book_names=sorted({b for g in games.values() for b in g.get("book_pairs",{})})
    for book in book_names:
        result["book_coverage_games"][book]=sum(book in g.get("book_pairs",{}) for g in settled)
    for model, probcol, pickcol in (("V3","v3_home_probability","v3_pick"),("V2","v2_home_probability","v2_pick")):
        wagers=[]
        for g in settled:
            quote=g.get("book_pairs",{}).get("betonlineag")
            if not quote: continue
            side=g[pickcol]; price=quote["home_price"] if side=="HOME" else quote["away_price"]
            d=decimal(price)
            if d: wagers.append((side,d,side==g["outcome_side"]))
        result["roi"][model+" BetOnline selected side"]=hit_rate_and_roi(wagers)
        for book in book_names:
            bets=[]
            for g in settled:
                quote=g.get("book_pairs",{}).get(book)
                if not quote: continue
                side=g[pickcol]; price=quote["home_price"] if side=="HOME" else quote["away_price"]
                d=decimal(price)
                if d: bets.append((side,d,side==g["outcome_side"]))
            result["roi"][model+" "+book]=hit_rate_and_roi(bets)
        # Deterministic, predeclared threshold grid; descriptive only.
        for kind, thresholds in (("edge",[0,.025,.05,.10]),("ev",[0,.025,.05])):
            entries=[]
            for threshold in thresholds:
                bets=[]
                for g in settled:
                    q=g.get("book_pairs",{}).get("betonlineag")
                    if not q: continue
                    side=g[pickcol]; p=g[probcol] if side=="HOME" else 1-g[probcol]
                    mp=q["home_novig"] if side=="HOME" else q["away_novig"]
                    price=q["home_price"] if side=="HOME" else q["away_price"]; d=decimal(price)
                    value=(p-mp) if kind=="edge" else (p*d-1 if d else None)
                    if value is not None and value>=threshold and d:
                        bets.append((side,d,side==g["outcome_side"]))
                entries.append({"minimum":threshold,**hit_rate_and_roi(bets)})
            result["sensitivity"][model+"_minimum_"+kind]=entries
        price_entries=[]
        for floor in (-300,-200,-150):
            bets=[]
            for g in settled:
                q=g.get("book_pairs",{}).get("betonlineag")
                if not q: continue
                side=g[pickcol]; price=q["home_price"] if side=="HOME" else q["away_price"]
                d=decimal(price)
                if d and price >= floor:
                    bets.append((side,d,side==g["outcome_side"]))
            price_entries.append({"american_price_floor":floor, **hit_rate_and_roi(bets)})
        result["sensitivity"][model+"_american_price_floor"]=price_entries
    disagreements=[g for g in settled if g["v3_pick"]!=g["v2_pick"]]
    result["disagreements"]={"n":len(disagreements),"v3_correct":sum(g["v3_pick"]==g["outcome_side"] for g in disagreements),
        "v2_correct":sum(g["v2_pick"]==g["outcome_side"] for g in disagreements),"v3_favors_market_underdog":sum(g["market_favorite"] in ("HOME","AWAY") and g["v3_pick"]!=g["market_favorite"] for g in priced),
        "v3_market_gap_bands":[]}
    v3_dog=[g for g in priced if g["market_favorite"] in ("HOME","AWAY") and g["v3_pick"] != g["market_favorite"]]
    result["disagreements"]["v3_market_underdog_picks"]={"n":len(v3_dog),
        "wins":sum(g["v3_pick"]==g["outcome_side"] for g in v3_dog),
        "win_rate":sum(g["v3_pick"]==g["outcome_side"] for g in v3_dog)/len(v3_dog) if v3_dog else None,
        "mean_absolute_model_market_gap":sum(abs(g["v3_home_probability"]-g["betonline_home_novig"]) for g in v3_dog)/len(v3_dog) if v3_dog else None}
    for lo,hi in ((0,.025),(.025,.05),(.05,.10),(.10,1.01)):
        cohort=[g for g in priced if lo <= abs(g["v3_home_probability"]-g["betonline_home_novig"]) < hi]
        result["disagreements"]["v3_market_gap_bands"].append({"absolute_gap_pp":[lo*100,hi*100],"n":len(cohort),
            "v3_accuracy":sum(g["v3_pick"]==g["outcome_side"] for g in cohort)/len(cohort) if cohort else None,
        "v3_favors_market_underdog":sum(g["market_favorite"] in ("HOME","AWAY") and g["v3_pick"]!=g["market_favorite"] for g in cohort)})

    old_rows=list(rows(OLD/"moneyline_game_level_results.csv"))
    old_by_id={r["game_id"]:r for r in old_rows}
    overlap=[g for g in settled if g["game_id"] in old_by_id]
    result["old_89_study_comparison"]={"reported_paired_games":89,"old_file_rows":len(old_rows),"overlap_games_in_full_settled":len(overlap),
        "new_settled_games_outside_old_89":len(settled)-len(overlap),"overlap_dates":"2026-09-29 through 2026-10-12",
        "role_cutover_note":"After 2026-10-14, V3/V2 labels are production/shadow; paired probabilities remain separate model identities and are evaluated as contemporaneous outputs, not as same-role production comparisons."}
    result["old_89_study_comparison"]["overlap_probability_metrics"]={}
    for label, col in (("V3","v3_home_probability"),("V2","v2_home_probability")):
        result["old_89_study_comparison"]["overlap_probability_metrics"][label]=metrics([g[col] for g in overlap],[g["y_home"] for g in overlap])
    old_prices=[]
    for g in overlap:
        q=g.get("book_pairs",{}).get("betonlineag")
        if q: old_prices.append(g)
    result["old_89_study_comparison"]["overlap_betonline_roi"]={}
    for model,pickcol in (("V3","v3_pick"),("V2","v2_pick")):
        bets=[]
        for g in old_prices:
            side=g[pickcol];q=g["book_pairs"]["betonlineag"];d=decimal(q["home_price"] if side=="HOME" else q["away_price"])
            if d:bets.append((side,d,side==g["outcome_side"]))
        result["old_89_study_comparison"]["overlap_betonline_roi"][model]=hit_rate_and_roi(bets)
    cutover=datetime.fromisoformat("2026-10-14T15:27:11+00:00")
    pre_cutover=[g for g in settled if datetime.fromisoformat(g["prediction_timestamp_utc"].replace("Z", "+00:00")) < cutover]
    post_cutover=[g for g in settled if datetime.fromisoformat(g["prediction_timestamp_utc"].replace("Z", "+00:00")) >= cutover]
    result["cutover_period_metrics"]={"pre_cutover":{"n":len(pre_cutover)},"post_cutover":{"n":len(post_cutover)}}
    for label, cohort in (("pre_cutover",pre_cutover),("post_cutover",post_cutover)):
        for model,col in (("V3","v3_home_probability"),("V2","v2_home_probability")):
            result["cutover_period_metrics"][label][model]=metrics([g[col] for g in cohort],[g["y_home"] for g in cohort])
            bets=[]
            pickcol="v3_pick" if model=="V3" else "v2_pick"
            for g in cohort:
                q=g.get("book_pairs",{}).get("betonlineag")
                if not q: continue
                side=g[pickcol];d=decimal(q["home_price"] if side=="HOME" else q["away_price"])
                if d:bets.append((side,d,side==g["outcome_side"]))
            result["cutover_period_metrics"][label][model+"_BetOnline_ROI"]=hit_rate_and_roi(bets)
        market_cohort=[g for g in cohort if g["betonline_home_novig"] is not None]
        result["cutover_period_metrics"][label]["BetOnline_market"]=metrics(
            [g["betonline_home_novig"] for g in market_cohort],[g["y_home"] for g in market_cohort])

    # Add predeclared market favorites price bands and competing book range.
    result["favorite_price_bands"]=[]
    for lo,hi in ((.50,.55),(.55,.60),(.60,.65),(.65,.70),(.70,.80),(.80,1.001)):
        cohort=[g for g in priced if g["market_favorite"] in ("HOME","AWAY") and lo <= g["market_favorite_probability"] < hi]
        result["favorite_price_bands"].append({"favorite_novig_probability":[lo,hi],"n":len(cohort),
            "favorite_wins":sum(g["y_home"]==(g["market_favorite"]=="HOME") for g in cohort),
            "favorite_win_rate":sum(g["y_home"]==(g["market_favorite"]=="HOME") for g in cohort)/len(cohort) if cohort else None,
            "v3_correct":sum(g["v3_pick"]==g["outcome_side"] for g in cohort),
            "v3_accuracy":sum(g["v3_pick"]==g["outcome_side"] for g in cohort)/len(cohort) if cohort else None,
            "v2_correct":sum(g["v2_pick"]==g["outcome_side"] for g in cohort),
            "v2_accuracy":sum(g["v2_pick"]==g["outcome_side"] for g in cohort)/len(cohort) if cohort else None})
    result["favorite_side_split"]={}
    for model, pickcol in (("V3","v3_pick"),("V2","v2_pick")):
        result["favorite_side_split"][model]={}
        for fave in ("HOME","AWAY"):
            cohort=[g for g in priced if g["market_favorite"]==fave]
            result["favorite_side_split"][model][fave]={"n":len(cohort),
                "model_picked_market_favorite":sum(g[pickcol]==fave for g in cohort),
                "model_picked_market_underdog":sum(g[pickcol]!=fave for g in cohort),
                "accuracy":sum(g[pickcol]==g["outcome_side"] for g in cohort)/len(cohort) if cohort else None}
    result["model_side_price_split"]={}
    for model, pickcol in (("V3","v3_pick"),("V2","v2_pick")):
        result["model_side_price_split"][model]={}
        for label, pick_favorite in (("MARKET_FAVORITE",True),("MARKET_UNDERDOG",False)):
            bets=[]
            for g in priced:
                side=g[pickcol]
                if g["market_favorite"] not in ("HOME","AWAY"): continue
                if (side==g["market_favorite"]) != pick_favorite: continue
                q=g["book_pairs"]["betonlineag"]
                d=decimal(q["home_price"] if side=="HOME" else q["away_price"])
                if d: bets.append((side,d,side==g["outcome_side"]))
            result["model_side_price_split"][model][label]=hit_rate_and_roi(bets)
    favorite_bets=[]
    underdog_bets=[]
    for g in priced:
        if g["market_favorite"] not in ("HOME","AWAY"): continue
        q=g["book_pairs"]["betonlineag"]
        side=g["market_favorite"]
        price=q["home_price"] if side=="HOME" else q["away_price"]
        d=decimal(price)
        if d: favorite_bets.append((side,d,side==g["outcome_side"]))
        side="AWAY" if side=="HOME" else "HOME"
        price=q["home_price"] if side=="HOME" else q["away_price"]
        d=decimal(price)
        if d: underdog_bets.append((side,d,side==g["outcome_side"]))
    result["roi"]["BetOnline market favorite"]=hit_rate_and_roi(favorite_bets)
    result["roi"]["BetOnline market underdog"]=hit_rate_and_roi(underdog_bets)

    sources=[EXPORT/"candidate_predictions.csv",EXPORT/"candidate_quotes.csv",EXPORT/"manifest.json",OLD/"moneyline_game_level_results.csv",OLD/"moneyline_prospective_performance.json",
        ROOT/"artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261017T232609902832Z_68bc386f/parent_receipt.json"]
    result["source_artifacts"]=[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in sources]
    result["ledger_sources"]=ledger_refs
    result["checks"]={"model_arm_pairing_issues":len(issues),"v2_candidate_probability_max_abs_difference":max(
        (abs(number(bygame[g][V2]["probability_over"])-games[g]["v2_home_probability"]) for g in games), default=0),
        "old_89_probability_replay_max_abs_difference":max((
            max(abs(g["v3_home_probability"]-number(old_by_id[g["game_id"]]["challenger_home_probability"])),
                abs(g["v2_home_probability"]-number(old_by_id[g["game_id"]]["reference_home_probability"])) )
            for g in overlap), default=0),
        "duplicate_candidate_natural_keys":len(games)*2-len(set((game_id,v) for game_id in games for v in (V3,V2)))}
    result["population_contract"]="One prediction row per model/game; separate quote rows are keyed to that model's probability-derived side. Only settled official outcomes enter predictive metrics and ROI. No profitable filter applied. BetOnline is the declared primary book; alternatives are reported book-by-book, not pooled as independent wagers. Market price eligibility requires PREGAME_QUALIFIED and source update <= observation <= prediction < scheduled start. Historical roles use the authority effective timestamp 2026-10-14T15:27:11Z; export role labels are retained alongside them."
    summary_path=OUT/"analysis_summary.json"
    summary_path.write_text(json.dumps(result,indent=2,sort_keys=True)+"\n")
    output_manifest={"schema_version":"NHL_MONEYLINE_FAVORITES_PRICING_ANALYSIS_V1",
        "frozen_export_manifest_sha256":sha(EXPORT/"manifest.json"),
        "outputs":[{"path":p.name,"sha256":sha(p),"rows":sum(1 for _ in rows(p)) if p.suffix==".csv" else None}
            for p in (cand_path,quote_path,summary_path)]}
    (OUT/"OUTPUT_MANIFEST.json").write_text(json.dumps(output_manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps({k:result[k] for k in ("paired_game_count","settled_paired_game_count","unresolved_paired_game_count","probability_metrics","market_metrics","favorites","disagreements","roi","sensitivity","old_89_study_comparison","book_coverage_games")},indent=2))


if __name__ == "__main__":
    main()
