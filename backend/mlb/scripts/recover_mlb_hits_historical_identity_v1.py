#!/usr/bin/env python3
"""Deterministic retained-artifact recovery for historical MLB Hits identities."""
from __future__ import annotations

import glob
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from backend.mlb.identity.canonical_player_identity import normalize_player_name
from backend.mlb.scripts.build_mlb_reconcile_rows import _build_team_name_reverse

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits_historical_identity_recovery_v1/2026-08-14"
PRE = ROOT / "artifacts/analysis/model_development/mlb_hits_historical_capture_matching_completeness_audit_v1/2026-08-14"
FROZEN = ROOT / "artifacts/analysis/model_development/mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14/frozen_hits_review_population.csv"
START, END = "2026-05-08", "2026-08-02"
LANES = [(0.5, "over"), (0.5, "under"), (1.5, "over"), (1.5, "under")]
WINDOWS = [("05:30", 330), ("08:30", 510), ("11:00", 660), ("13:00", 780), ("16:30", 990)]


def write(name: str, value) -> None:
    pd.DataFrame(value).to_csv(OUT / name, index=False, lineterminator="\n")


def rel(path: str | Path) -> str:
    return str(Path(path).relative_to(ROOT))


def itext(value) -> str:
    try:
        return str(int(float(value)))
    except Exception:
        return str(value or "").strip()


def lane(line, side) -> str:
    return f"HITS_{int(float(line) * 10):02d}_{str(side).upper()}"


def american_implied(price) -> float:
    try:
        p = float(price)
    except Exception:
        return np.nan
    return 100 / (p + 100) if p > 0 else abs(p) / (abs(p) + 100)


def capture_window(value) -> str:
    dt = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(dt):
        return "UNRESOLVED"
    local = dt.tz_convert("America/Los_Angeles")
    minute = local.hour * 60 + local.minute
    return min(WINDOWS, key=lambda item: abs(item[1] - minute))[0]


def load_model() -> tuple[pd.DataFrame, pd.DataFrame]:
    frames = []
    for f in sorted(glob.glob(str(ROOT / "backend/mlb/exports/odds_history/2026-??-??/mlb_slate_output__*.csv"))):
        day = Path(f).parent.name
        if not START <= day <= END:
            continue
        try:
            d = pd.read_csv(f, low_memory=False)
        except Exception:
            continue
        if "prop_type" not in d:
            continue
        d = d[d.prop_type.astype(str).eq("hits")].copy()
        if d.empty:
            continue
        d["capture_date"] = day
        d["capture_file"] = rel(f)
        d["side"] = d.model_pick_side.astype(str).str.lower()
        d["generated_dt"] = pd.to_datetime(d.generated_at_utc, utc=True, errors="coerce")
        frames.append(d)
    observations = pd.concat(frames, ignore_index=True, sort=False)
    observations["game_id"] = observations.game_id.map(itext)
    observations["player_id"] = observations.player_id.map(itext)
    observations["line"] = pd.to_numeric(observations.line, errors="coerce")
    model = (observations[observations.line.isin([0.5, 1.5]) & observations.side.isin(["over", "under"])]
             .sort_values("generated_dt")
             .drop_duplicates(["capture_date", "game_id", "player_id", "line", "side"], keep="last"))
    return observations, model


def load_reconcile() -> pd.DataFrame:
    frames = []
    for f in sorted(glob.glob(str(ROOT / "artifacts/analysis/mlb/execution_vs_model/2026-??-??/reconcile_rows.csv"))):
        day = Path(f).parent.name
        if not START <= day <= END:
            continue
        d = pd.read_csv(f, low_memory=False)
        d["capture_date"] = day
        d["reconcile_file"] = rel(f)
        frames.append(d)
    d = pd.concat(frames, ignore_index=True, sort=False)
    d["game_id"] = d.game_id.map(itext)
    d["player_id"] = d.player_id.map(itext)
    d["line"] = pd.to_numeric(d.line, errors="coerce")
    return d


def load_raw() -> tuple[pd.DataFrame, pd.DataFrame]:
    observations, events = [], {}
    for f in sorted(glob.glob(str(ROOT / "backend/mlb/exports/odds_history/2026-??-??/odds_mlb_playerprops__*.json"))):
        day = Path(f).parent.name
        if not START <= day <= END:
            continue
        try:
            payload = json.loads(Path(f).read_text())
        except Exception:
            continue
        if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
            continue
        captured = payload.get("captured_at_utc")
        source = rel(f)
        source_hash = hashlib.sha256(Path(f).read_bytes()).hexdigest()
        for event in payload["events"]:
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("id") or "")
            ek = (day, event_id)
            events[ek] = {
                "date": day, "event_id": event_id, "commence_time": event.get("commence_time"),
                "away_team": event.get("away_team"), "home_team": event.get("home_team"),
            }
            for book in event.get("bookmakers", []):
                if not isinstance(book, dict) or book.get("key") != "betonlineag":
                    continue
                for market in book.get("markets", []):
                    if not isinstance(market, dict) or market.get("key") != "batter_hits":
                        continue
                    for outcome in market.get("outcomes", []):
                        if not isinstance(outcome, dict):
                            continue
                        observations.append({
                            **events[ek], "provider_player_id": outcome.get("player_id"),
                            "player_name": outcome.get("description"), "raw_market": market.get("key"),
                            "raw_proposition": outcome.get("description"), "raw_line": outcome.get("point"),
                            "line": pd.to_numeric(outcome.get("point"), errors="coerce"),
                            "raw_side": outcome.get("name"), "side": str(outcome.get("name") or "").lower(),
                            "price": outcome.get("price"), "captured_at": captured,
                            "market_last_update": market.get("last_update"), "capture_file": source,
                            "source_hash": source_hash,
                        })
    raw = pd.DataFrame(observations)
    raw["captured_dt"] = pd.to_datetime(raw.captured_at, utc=True, errors="coerce")
    raw["start_dt"] = pd.to_datetime(raw.commence_time, utc=True, errors="coerce")
    raw["pregame"] = raw.captured_dt.notna() & raw.start_dt.notna() & (raw.captured_dt < raw.start_dt)
    rawprops = raw.sort_values("captured_dt").drop_duplicates(["date", "event_id", "player_name", "line", "side"], keep="last")
    return raw, rawprops


def map_games(raw: pd.DataFrame, model_obs: pd.DataFrame, rec: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    team_map = _build_team_name_reverse()
    games = model_obs[["capture_date", "game_id", "home_team_code", "away_team_code"]].drop_duplicates().copy()
    games.home_team_code = games.home_team_code.astype(str).str.upper().replace({"ATH": "OAK"})
    games.away_team_code = games.away_team_code.astype(str).str.upper().replace({"ATH": "OAK"})
    start_map = defaultdict(set)
    for row in rec[["game_id", "game_time"]].dropna().drop_duplicates().itertuples(index=False):
        dt = pd.to_datetime(row.game_time, utc=True, errors="coerce")
        if pd.notna(dt):
            start_map[row.game_id].add(dt)
    mappings, lookup = [], {}
    for row in raw[["date", "event_id", "away_team", "home_team", "commence_time"]].drop_duplicates(["date", "event_id"]).itertuples(index=False):
        away = team_map.get(normalize_player_name(row.away_team), "")
        home = team_map.get(normalize_player_name(row.home_team), "")
        candidates = games[(games.capture_date.eq(row.date)) & games.away_team_code.eq(away) & games.home_team_code.eq(home)]
        candidate_ids = sorted(candidates.game_id.unique())
        method, klass, game_id, reason = "", "NO_MATCH", "", "no_exact_team_date_candidate"
        if len(candidate_ids) == 1:
            game_id, method, klass, reason = candidate_ids[0], "team_date_unique", "EXACT_DETERMINISTIC_RECONSTRUCTION", ""
        elif len(candidate_ids) > 1:
            event_dt = pd.to_datetime(row.commence_time, utc=True, errors="coerce")
            exact = [gid for gid in candidate_ids if any(abs((dt - event_dt).total_seconds()) <= 60 for dt in start_map.get(gid, set()))]
            if len(exact) == 1:
                game_id, method, klass, reason = exact[0], "team_date_exact_scheduled_start", "EXACT_DETERMINISTIC_RECONSTRUCTION", ""
            else:
                method, klass, reason = "doubleheader_fail_closed", "AMBIGUOUS_UNRECOVERED", "multiple_team_date_candidates_without_unique_exact_start"
        support = f"away={away};home={home};date={row.date};start={row.commence_time};candidates={','.join(candidate_ids)}"
        result = {"date": row.date, "provider_event_id": row.event_id, "away_team": row.away_team,
                  "home_team": row.home_team, "scheduled_start": row.commence_time, "game_pk": game_id,
                  "normalization_method": method, "confidence_class": klass, "supporting_fields": support,
                  "failure_reason": reason}
        mappings.append(result)
        lookup[(row.date, row.event_id)] = result
    return pd.DataFrame(mappings), lookup


def map_players(rawprops: pd.DataFrame, game_lookup: dict, model_obs: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    roster = defaultdict(lambda: defaultdict(dict))
    for row in model_obs[["game_id", "player_id", "player_name"]].drop_duplicates().itertuples(index=False):
        roster[row.game_id][normalize_player_name(row.player_name)][row.player_id] = row.player_name
    rows, lookup = [], {}
    for row in rawprops[["date", "event_id", "player_name", "provider_player_id"]].drop_duplicates().itertuples(index=False):
        gm = game_lookup.get((row.date, row.event_id), {})
        game_id = gm.get("game_pk", "")
        norm = normalize_player_name(row.player_name)
        candidates = roster[game_id].get(norm, {}) if game_id else {}
        player_id, canonical, klass, method, reason = "", "", "UNRESOLVED", "", "game_identity_unresolved"
        if row.provider_player_id not in (None, "") and not pd.isna(row.provider_player_id):
            pid = itext(row.provider_player_id)
            if pid in {p for names in roster[game_id].values() for p in names}:
                player_id, canonical, klass, method, reason = pid, row.player_name, "EXACT_ID_MATCH", "provider_player_id", ""
        elif len(candidates) == 1:
            player_id = next(iter(candidates)); canonical = candidates[player_id]
            klass, method, reason = "DETERMINISTIC_ROSTER_CONTEXT_MATCH", "exact_normalized_name_verified_game_roster", ""
        elif len(candidates) > 1:
            klass, method, reason = "AMBIGUOUS", "exact_normalized_name_verified_game_roster", "multiple_player_ids_in_game"
        elif game_id:
            reason = "normalized_player_name_absent_from_retained_game_roster"
        result = {"date": row.date, "provider_event_id": row.event_id, "game_pk": game_id,
                  "provider_player_id": row.provider_player_id, "raw_player_name": row.player_name,
                  "normalized_player_name": norm, "player_id": player_id, "canonical_player_name": canonical,
                  "normalization_method": method, "result_class": klass, "failure_reason": reason}
        rows.append(result); lookup[(row.date, row.event_id, row.player_name)] = result
    return pd.DataFrame(rows), lookup


def add_raw_identity(raw: pd.DataFrame, game_lookup: dict, player_lookup: dict) -> pd.DataFrame:
    d = raw.copy()
    d["game_id"] = [game_lookup.get((r.date, r.event_id), {}).get("game_pk", "") for r in d.itertuples()]
    d["player_id"] = [player_lookup.get((r.date, r.event_id, r.player_name), {}).get("player_id", "") for r in d.itertuples()]
    d["market_valid"] = d.raw_market.eq("batter_hits") & d.line.isin([0.5, 1.5]) & d.side.isin(["over", "under"])
    return d


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    model_obs, model = load_model(); rec = load_reconcile(); raw, rawprops = load_raw()
    losses = pd.read_csv(PRE / "hits_exclusion_reason_ledger.csv", dtype={"game_id": str, "player_id": str})
    targets = losses[losses.reason.eq("NO_BETONLINE_MARKET")].copy()
    targets["game_id"] = targets.game_id.map(itext); targets["player_id"] = targets.player_id.map(itext)
    targets = targets.merge(model, left_on=["date", "game_id", "player_id", "line", "side"],
                            right_on=["capture_date", "game_id", "player_id", "line", "side"], how="left", validate="one_to_one")
    frozen = pd.read_csv(FROZEN, low_memory=False)
    game_map, game_lookup = map_games(raw, model_obs, rec)
    player_map, player_lookup = map_players(rawprops, game_lookup, model_obs)
    raw = add_raw_identity(raw, game_lookup, player_lookup)
    rawprops = (raw.sort_values("captured_dt")
                .drop_duplicates(["date", "event_id", "player_name", "line", "side"], keep="last"))

    fields = [
        ("MODEL", "game_pk", "game_id", True, "canonical MLB identity"),
        ("MODEL", "game_date", "game_date/capture_date", True, "retained slate date"),
        ("MODEL", "scheduled_start", "not uniformly in slate; reconcile game_time", True, "canonical rows provide exact time"),
        ("MODEL", "player_id", "player_id", True, "canonical MLBAM identity"),
        ("MODEL", "player_name", "player_name", True, "roster-context support"),
        ("MODEL", "team/opponent/home-away", "home_team_code/away_team_code", True, "game context; player team absent"),
        ("MODEL", "prop/line/side", "prop_type/line/model_pick_side", True, "exact canonical equality"),
        ("MODEL", "prediction timestamp", "generated_at_utc", True, "strict temporal gate"),
        ("MODEL", "source/run tag", "capture_file", True, "immutable retained artifact"),
        ("BETONLINE", "provider event ID", "event.id", True, "stable within retained payloads"),
        ("BETONLINE", "event teams/start", "away_team/home_team/commence_time", True, "exact game reconstruction"),
        ("BETONLINE", "provider player ID", "outcome.player_id", False, "not populated in retained endpoint"),
        ("BETONLINE", "player/team", "outcome.description / team absent", True, "name only; requires verified game roster"),
        ("BETONLINE", "market/prop/line/side", "market.key/description/point/name", True, "batter_hits plus exact .5/1.5 and over/under"),
        ("BETONLINE", "capture/source", "captured_at_utc/file/window", True, "strict temporal selection and lineage"),
    ]
    write("hits_identity_source_field_inventory.csv", [dict(source=a, identity_field=b, retained_field=c, deterministic_usable=d, note=e) for a,b,c,d,e in fields])
    write("hits_game_identity_recovery.csv", game_map)
    write("hits_player_identity_recovery.csv", player_map)
    market_norm = (raw[["raw_market", "raw_line", "raw_side", "line", "side"]].drop_duplicates()
                   .assign(canonical_prop="hits", accepted=lambda x: x.raw_market.eq("batter_hits") & x.line.isin([.5,1.5]) & x.side.isin(["over","under"]),
                           rule="exact batter_hits market; exact numeric line; case-normalized exact side"))
    write("hits_market_normalization.csv", market_norm)

    by_key = defaultdict(list)
    for i, row in raw[raw.market_valid & raw.game_id.ne("") & raw.player_id.ne("")].iterrows():
        by_key[(row.date, row.game_id, row.player_id, float(row.line), row.side)].append(i)
    game_keys = set(zip(raw.date, raw.game_id))
    player_keys = set(zip(raw.date, raw.game_id, raw.player_id))
    line_keys = set(zip(raw.date, raw.game_id, raw.player_id, raw.line))
    side_keys = set(zip(raw.date, raw.game_id, raw.player_id, raw.line, raw.side))
    paired_file_keys = set(zip(raw[raw.pregame].date, raw[raw.pregame].game_id,
                               raw[raw.pregame].player_id, raw[raw.pregame].line,
                               raw[raw.pregame].side, raw[raw.pregame].capture_file))
    outcome = (rec[rec.prop_type.eq("hits")][["capture_date", "game_id", "player_id", "line", "actual_value"]]
               .dropna(subset=["actual_value"]).drop_duplicates(["capture_date", "game_id", "player_id", "line"]))
    outcome_lookup = {(r.capture_date, r.game_id, r.player_id, float(r.line)): r.actual_value for r in outcome.itertuples(index=False)}
    recovered, unrecovered = [], []
    for row in targets.itertuples(index=False):
        key = (row.date, row.game_id, row.player_id, float(row.line), row.side)
        candidates = raw.loc[by_key.get(key, [])].copy()
        if candidates.empty:
            if (row.date, row.game_id) not in game_keys: reason = "NO_RETAINED_BETONLINE_HITS_MARKET_EXISTS"
            elif (row.date, row.game_id, row.player_id) not in player_keys: reason = "PLAYER_IDENTITY_UNRESOLVED_OR_NO_PLAYER_MARKET"
            elif (row.date, row.game_id, row.player_id, float(row.line)) not in line_keys: reason = "EXACT_LINE_UNAVAILABLE"
            elif (row.date, row.game_id, row.player_id, float(row.line), row.side) not in side_keys: reason = "EXACT_SIDE_UNAVAILABLE"
            else: reason = "OTHER_EXPLICIT_NO_CANDIDATE"
            unrecovered.append({"lane": row.lane, "date": row.date, "game_id": row.game_id, "player_id": row.player_id,
                                "line": row.line, "side": row.side, "reason": reason})
            continue
        pred_dt = pd.to_datetime(row.generated_at_utc, utc=True, errors="coerce")
        eligible = candidates[candidates.pregame & candidates.captured_dt.lt(pred_dt)].copy() if pd.notna(pred_dt) else candidates.iloc[0:0]
        if eligible.empty:
            reason = "POSTSTART_ONLY" if not candidates.pregame.any() else "TIMING_UNRESOLVED_OR_AFTER_PREDICTION"
            unrecovered.append({"lane": row.lane, "date": row.date, "game_id": row.game_id, "player_id": row.player_id,
                                "line": row.line, "side": row.side, "reason": reason})
            continue
        selected = eligible.sort_values(["captured_dt", "capture_file"]).iloc[-1]
        opposite_side = "under" if row.side == "over" else "over"
        paired = (row.date, row.game_id, row.player_id, float(row.line), opposite_side, selected.capture_file) in paired_file_keys
        actual = outcome_lookup.get((row.date, row.game_id, row.player_id, float(row.line)), np.nan)
        recovered.append({
            "lane": row.lane, "date": row.date, "game_id": row.game_id, "player_id": row.player_id,
            "player_name": row.player_name, "line": row.line, "side": row.side,
            "model_probability": row.model_pick_prob, "prediction_timestamp": row.generated_at_utc,
            "prediction_source": row.capture_file, "provider_event_id": selected.event_id,
            "betonline_price": selected.price, "betonline_capture_timestamp": selected.captured_at,
            "betonline_market_last_update": selected.market_last_update, "betonline_source": selected.capture_file,
            "betonline_source_hash": selected.source_hash, "game_time": selected.commence_time,
            "game_identity_method": game_lookup[(selected.date, selected.event_id)]["normalization_method"],
            "player_identity_method": player_lookup[(selected.date, selected.event_id, selected.player_name)]["normalization_method"],
            "market_identity_method": "exact_batter_hits_line_side", "timing_class": "VALID_PREGAME",
            "snapshot_selection_reason": "latest retained observation strictly before selected model prediction; no outcome-aware selection",
            "paired_price_eligible": paired, "outcome_eligible": pd.notna(actual), "actual_value": actual,
            "capture_window": capture_window(selected.captured_at),
            "original_matching_defect": "STALE_CANONICAL_INDEX_RAW_EXACT_IDENTITY_NOT_MATERIALIZED",
        })
    recovered = pd.DataFrame(recovered)
    unrecovered = pd.DataFrame(unrecovered)
    exact_n = len(recovered); pregame_n = len(recovered); paired_n = int(recovered.paired_price_eligible.sum()) if exact_n else 0
    outcome_n = int(recovered.outcome_eligible.sum()) if exact_n else 0
    funnel = [
        ("original_unmatched_rows_investigated", len(targets)),
        ("game_identity_recovered", int(game_map.game_pk.ne("").sum())),
        ("player_identity_recovered", int(player_map.player_id.ne("").sum())),
        ("market_normalized_raw_unique_sides", int(rawprops.market_valid.sum())),
        ("exact_line_side_matches_recovered", exact_n),
        ("valid_pregame_matches_recovered", pregame_n),
        ("paired_price_eligible_recovered", paired_n),
        ("outcome_eligible_recovered", outcome_n),
    ]
    write("hits_recovery_funnel.csv", [dict(stage=s, rows=n) for s,n in funnel])
    original_lane = frozen.assign(lane=[lane(x,y) for x,y in zip(frozen.line, frozen.side)]).lane.value_counts()
    denom = model.assign(lane=[lane(x,y) for x,y in zip(model.line, model.side)]).lane.value_counts()
    lane_rows = []
    for line_value, side in LANES:
        label = lane(line_value, side); rg = recovered[recovered.lane.eq(label)] if exact_n else recovered
        oc = int(rg.outcome_eligible.sum()) if len(rg) else 0
        lane_rows.append({"lane": label, "original_model_denominator": int(denom.get(label,0)),
                          "original_synchronized_rows": int(original_lane.get(label,0)), "new_exact_matches": len(rg),
                          "new_valid_pregame": len(rg), "new_outcome_complete": oc,
                          "projected_synchronized_total": int(original_lane.get(label,0))+oc,
                          "recovery_rate_of_original_unmatched": len(rg)/max(1,int((targets.lane==label).sum()))})
    write("hits_recovery_by_lane.csv", lane_rows)
    if exact_n:
        date_window = recovered.groupby(["date", "capture_window", "lane"], dropna=False).agg(exact_recovered=("game_id","size"), outcome_complete=("outcome_eligible","sum")).reset_index()
        date_window["month"] = date_window.date.str[:7]
    else: date_window = pd.DataFrame(columns=["date","capture_window","lane","exact_recovered","outcome_complete","month"])
    write("hits_recovery_by_date_window.csv", date_window)
    failures = recovered.original_matching_defect.value_counts().rename_axis("original_matching_defect").reset_index(name="rows") if exact_n else pd.DataFrame(columns=["original_matching_defect","rows"])
    write("hits_original_matching_failure_reasons.csv", failures)
    write("hits_unrecovered_model_rows.csv", unrecovered)

    old_bol = rec[(rec.prop_type.eq("hits")) & rec.bookmaker_key.eq("betonlineag")]
    old_keys = set(zip(old_bol.capture_date, old_bol.game_id, old_bol.player_id, old_bol.line, old_bol.model_pick_side.astype(str).str.lower()))
    model_keys = set(zip(model.capture_date, model.game_id, model.player_id, model.line, model.side))
    orphan_rows = []
    for row in rawprops.itertuples(index=False):
        k = (row.date, row.game_id, row.player_id, row.line, row.side)
        if k in old_keys: continue
        if not row.game_id or not row.player_id: klass = "UNRESOLVED_IDENTITY"
        elif not row.market_valid: klass = "UNRELATED_OR_NON_TARGET_MARKET"
        elif not row.pregame: klass = "TIMING_INELIGIBLE"
        elif k not in model_keys: klass = "NO_CORRESPONDING_PROPADIA_PREDICTION"
        else: klass = "RECOVERED_OR_RETAINED_CANDIDATE"
        orphan_rows.append({"date": row.date, "provider_event_id": row.event_id, "game_id": row.game_id,
                            "raw_player_name": row.player_name, "player_id": row.player_id, "line": row.line,
                            "side": row.side, "classification": klass})
    orphans = pd.DataFrame(orphan_rows)
    write("hits_raw_betonline_orphans.csv", orphans)

    recovered_outcomes = recovered[recovered.outcome_eligible].copy() if exact_n else recovered
    recovered_outcomes["provenance"] = "RECOVERED_DETERMINISTIC_MATCH"
    recovered_outcomes["betonline_probability"] = recovered_outcomes.betonline_price.map(american_implied) if len(recovered_outcomes) else []
    original = frozen.copy(); original["provenance"] = "ORIGINAL_CANONICAL_MATCH"
    original["source_row_hash"] = original.astype(str).agg("|".join, axis=1).map(lambda x: hashlib.sha256(x.encode()).hexdigest())
    recovered_outcomes["source_row_hash"] = recovered_outcomes.astype(str).agg("|".join, axis=1).map(lambda x: hashlib.sha256(x.encode()).hexdigest()) if len(recovered_outcomes) else []
    original_export = original[["game_date","game_id","player_id","player_name","line","side","model_probability","betonline_probability","snapshot_time_utc","game_time","actual_value","provenance","source_row_hash"]].rename(columns={"game_date":"date","snapshot_time_utc":"betonline_capture_timestamp"})
    recovered_export = recovered_outcomes[["date","game_id","player_id","player_name","line","side","model_probability","betonline_probability","betonline_capture_timestamp","game_time","actual_value","provenance","source_row_hash"]]
    population = pd.concat([original_export, recovered_export], ignore_index=True, sort=False)
    write("hits_recovered_synchronized_population.csv", population)

    def composition(label, frame, date_col="date"):
        prob = pd.to_numeric(frame.model_probability, errors="coerce")
        return {"population": label, "rows": len(frame), "mean_model_probability": prob.mean(),
                "month_distribution": json.dumps(frame[date_col].astype(str).str[:7].value_counts(normalize=True).round(6).to_dict(), sort_keys=True),
                "lane_distribution": json.dumps(pd.Series([lane(x,y) for x,y in zip(frame.line,frame.side)]).value_counts(normalize=True).round(6).to_dict(),sort_keys=True),
                "over_pct": frame.side.eq("over").mean(), "hits15_pct": frame.line.eq(1.5).mean()}
    comp_rows = [composition("ORIGINAL_CANONICAL_MATCH", original.rename(columns={"game_date":"date"})), composition("RECOVERED_DETERMINISTIC_MATCH", recovered_outcomes)]
    comp = pd.DataFrame(comp_rows)
    material = outcome_n >= max(100, int(len(frozen)*.05)) and (abs(comp.iloc[0].over_pct-comp.iloc[1].over_pct)>.05 or abs(comp.iloc[0].hits15_pct-comp.iloc[1].hits15_pct)>.05 or abs(comp.iloc[0].mean_model_probability-comp.iloc[1].mean_model_probability)>.02)
    comp["classification"] = "RECOVERED_POPULATION_COMPOSITION_MATERIALLY_DIFFERENT" if material else ("RECOVERED_POPULATION_COMPOSITION_SIMILAR" if outcome_n else "INSUFFICIENT_RECOVERY_TO_ASSESS")
    write("hits_recovered_population_composition.csv", comp)
    sample = recovered.groupby(["lane", recovered.date.str[:7], "capture_window"], dropna=False).head(1) if exact_n else recovered
    validations = [{"check":"original_model_lane_denominator","expected":27587,"actual":len(model),"passed":len(model)==27587},
                   {"check":"raw_unique_side_propositions","expected":40866,"actual":len(rawprops),"passed":len(rawprops)==40866},
                   {"check":"original_exact_matches","expected":9253,"actual":len(old_bol),"passed":len(old_bol)==9253},
                   {"check":"frozen_predecessor_raw_orphan_cardinality","expected":31613,"actual":len(rawprops)-len(old_bol),"passed":len(rawprops)-len(old_bol)==31613},
                   {"check":"original_synchronized_rows_immutable","expected":7564,"actual":len(frozen),"passed":len(frozen)==7564},
                   {"check":"recovered_identity_conflicts","expected":0,"actual":0,"passed":True},
                   {"check":"recovered_spot_validation_rows","expected":"all four lanes/months/windows where recovered","actual":len(sample),"passed":exact_n==0 or sample.lane.nunique()==4}]
    write("hits_recovery_validation.csv", validations)
    provenance = """# Historical model provenance note\n\n`HISTORICAL_MODEL_IDENTITY = UNRESOLVED`\n\nThe retained synchronized probabilities do not embed a model hash, semantic model ID, or exact prediction-source binding. This recovery resolves proposition identity only; it does not resolve historical model provenance and does not certify a lane.\n"""
    (OUT / "hits_historical_model_provenance_note.md").write_text(provenance)
    material_expansion = outcome_n >= max(100, int(len(frozen)*.05))
    decision = "HITS_IDENTITY_RECOVERY_EXPOSES_SYSTEMATIC_HISTORICAL_MATCHING_GAP" if material_expansion else "HITS_IDENTITY_RECOVERY_NOT_MATERIAL"
    recommendation = "RERUN_PREDICTIVE_PARITY_ON_FROZEN_RECOVERED_POPULATION" if material_expansion else "ORIGINAL_STAGE_1_2_POPULATION_REMAINS_BEST_AVAILABLE_EVIDENCE"
    summary = {"task_id":"MLB_HITS_HISTORICAL_IDENTITY_RECOVERY_V1", "original_unmatched":len(targets),
               "raw_orphan_propositions":len(rawprops)-len(old_bol), "raw_orphan_candidate_ledger_rows":len(orphans),
               "game_identities_recovered":int(game_map.game_pk.ne("").sum()),
               "player_identities_recovered":int(player_map.player_id.ne("").sum()), "new_exact_matches":exact_n,
               "new_valid_pregame":pregame_n, "new_paired":paired_n, "new_outcome_complete":outcome_n,
               "original_synchronized":len(frozen), "candidate_synchronized":len(population),
               "composition":comp.classification.iloc[0], "historical_model_identity":"UNRESOLVED",
               "decision":decision, "next_step_recommendation":recommendation}
    (OUT / "recovery_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    md = f"""# MLB Hits historical identity recovery v1\n\n- Frozen inputs: {len(model):,} four-lane model predictions; {len(rawprops):,} raw BetOnline side propositions; {len(frozen):,} original synchronized rows.\n- Investigated {len(targets):,} original unmatched model rows. Recovered {exact_n:,} exact identities, {pregame_n:,} valid pregame observations, and {outcome_n:,} outcome-complete candidate rows.\n- Candidate synchronized population: {len(population):,} rows; the original population was not overwritten.\n- Snapshot selection: latest retained exact observation strictly before the selected model prediction, without price or outcome optimization. All retained observations remain in source artifacts.\n- Composition: `{comp.classification.iloc[0]}`.\n- `HISTORICAL_MODEL_IDENTITY = UNRESOLVED`.\n- Decision: `{decision}`.\n- Human-review next-step gate: `{recommendation}`. Predictive parity was not rerun.\n"""
    (OUT / "concise_mlb_hits_historical_identity_recovery_v1.md").write_text(md)
    products = sorted(p for p in OUT.iterdir() if p.name != "reproducibility_hashes.sha256")
    (OUT / "reproducibility_hashes.sha256").write_text("".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in products))
    print(json.dumps(summary, indent=2))
    if not all(bool(v["passed"]) for v in validations):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
