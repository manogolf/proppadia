#!/usr/bin/env python3
"""Read-only inventory of the local MLB expected-quality/Statcast platform."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT = ROOT / "artifacts/analysis/model_development/mlb_expected_quality_feature_platform_inventory_v1/2026-08-12"
POP = ROOT / "artifacts/analysis/model_development/mlb_lineup_confirmed_scoring_prediction_v2/2026-08-12/historical_lineup_population_manifest.csv"
PLATFORM = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22"
RAW = ROOT / "backend/mlb/data/external/statcast/raw"
SCRIPT = Path(__file__).resolve()


def write_csv(name: str, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(OUT / name, index=False)


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def load_statcast() -> pd.DataFrame:
    cols = ["game_date", "game_pk", "at_bat_number", "pitch_number", "batter", "pitcher", "events",
            "description", "stand", "p_throws", "pitch_type", "release_speed", "launch_speed",
            "launch_angle", "estimated_ba_using_speedangle", "estimated_woba_using_speedangle",
            "estimated_slg_using_speedangle", "woba_value", "woba_denom", "launch_speed_angle",
            "delta_run_exp", "release_spin_rate", "pfx_x", "pfx_z", "release_pos_x", "release_pos_z",
            "release_extension", "effective_speed", "zone", "plate_x", "plate_z", "bb_type"]
    frames = []
    for year in (2025, 2026):
        for path in sorted((RAW / str(year)).glob("*/statcast_search.csv")):
            frames.append(pd.read_csv(path, usecols=cols, low_memory=False))
    d = pd.concat(frames, ignore_index=True)
    d["game_date"] = pd.to_datetime(d.game_date).dt.strftime("%Y-%m-%d")
    for c in ["game_pk", "at_bat_number", "pitch_number", "batter", "pitcher"]:
        d[c] = pd.to_numeric(d[c], errors="coerce").astype("Int64")
    d = d.dropna(subset=["game_pk", "at_bat_number", "pitch_number", "batter", "pitcher"])
    d = d.drop_duplicates(["game_pk", "at_bat_number", "pitch_number"], keep="last")
    return d


def coverage(stat: pd.DataFrame, pop: pd.DataFrame) -> tuple[list[dict], dict]:
    # Strict-prior state is advanced once per completed date, never before games on that date.
    batter = defaultdict(Counter)
    pitcher = defaultdict(Counter)
    pair = Counter()
    daily = {k: v for k, v in stat.groupby("game_date", sort=True)}

    def add_day(day: pd.DataFrame) -> None:
        for pid, g in day.groupby("batter"):
            c = batter[int(pid)]
            c["pitches"] += len(g)
            c["pa"] += int(g.events.notna().sum())
            c["xq"] += int(g.estimated_woba_using_speedangle.notna().sum())
            c["bip"] += int(g.launch_speed.notna().sum())
            c["hard"] += int((pd.to_numeric(g.launch_speed, errors="coerce") >= 95).sum())
            c["barrel"] += int((pd.to_numeric(g.launch_speed_angle, errors="coerce") == 6).sum())
            c["pitch_types"] += int(g.pitch_type.notna().sum())
        for pid, g in day.groupby("pitcher"):
            c = pitcher[int(pid)]
            c["pitches"] += len(g)
            c["bf"] += int(g.events.notna().sum())
            c["xq"] += int(g.estimated_woba_using_speedangle.notna().sum())
            c["bip"] += int(g.launch_speed.notna().sum())
            c["velo"] += int(g.release_speed.notna().sum())
            c["games"] += int(g.game_pk.nunique())
            c["pitch_types"] += int(g.pitch_type.notna().sum())
        for (b, p), g in day.groupby(["batter", "pitcher"]):
            pair[(int(b), int(p))] += len(g)

    prior_days = sorted(x for x in daily if x < pop.date.min())
    for day in prior_days:
        add_day(daily[day])
    rows = []
    batter_player_games = batter_covered = batter_sparse = 0
    all18 = ge16 = 0
    both = one = neither = combined = 0
    starter_total = starter_covered = starter_change = 0
    matchup_total = matchup_direct10 = matchup_profile = 0
    for date, games in pop.sort_values(["date", "game_pk"]).groupby("date", sort=True):
        for _, game in games.iterrows():
            away = json.loads(game.away_starting_lineup_json)
            home = json.loads(game.home_starting_lineup_json)
            ids = [int(x["player_id"]) for x in away + home]
            ncovered = sum(batter[x]["xq"] > 0 for x in ids)
            n30 = sum(batter[x]["pa"] >= 30 for x in ids)
            batter_player_games += 18
            batter_covered += ncovered
            batter_sparse += sum(batter[x]["pa"] < 30 for x in ids)
            all18 += ncovered == 18
            ge16 += ncovered >= 16
            sp = [int(game.away_starting_pitcher_id), int(game.home_starting_pitcher_id)]
            sc = sum(pitcher[x]["pitches"] >= 100 and pitcher[x]["xq"] > 0 for x in sp)
            both += sc == 2; one += sc == 1; neither += sc == 0
            combined += n30 >= 16 and sc == 2
            starter_total += 2; starter_covered += sc
            starter_change += sum(pitcher[x]["pitches"] >= 100 and pitcher[x]["games"] >= 2 for x in sp)
            for lineup, opp in ((away, sp[1]), (home, sp[0])):
                for x in lineup:
                    bid = int(x["player_id"]); matchup_total += 1
                    matchup_direct10 += pair[(bid, opp)] >= 10
                    matchup_profile += batter[bid]["pitches"] >= 100 and pitcher[opp]["pitches"] >= 100
            rows.append({"game_pk": int(game.game_pk), "date": date, "lineup_players_with_prior_xwoba": ncovered,
                         "lineup_players_with_30_prior_pa": n30, "both_starters_100_prior_pitches_and_xwoba": sc == 2,
                         "sufficiently_complete_descriptive": n30 >= 16 and sc == 2})
        if date in daily:
            add_day(daily[date])
    n = len(pop)
    metrics = {
        "population_games": n, "player_games": batter_player_games, "all_18_batters_expected_quality": all18,
        "at_least_16_batters_expected_quality": ge16, "batter_expected_quality_player_games": batter_covered,
        "batter_sparse_under_30_pa": batter_sparse, "both_starters_covered": both, "one_starter_covered": one,
        "neither_starter_covered": neither, "starter_instances_covered": starter_covered,
        "starter_instances_change_ready": starter_change, "combined_sufficient_games": combined,
        "matchup_instances": matchup_total, "direct_pair_10_pitch": matchup_direct10,
        "pitch_family_profile_ready": matchup_profile,
    }
    return rows, metrics


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pop = pd.read_csv(POP, dtype={"date": str})
    stat = load_statcast()
    detail, m = coverage(stat, pop)
    pct = lambda a, b: round(100 * a / b, 3) if b else 0
    raw_files = list(RAW.glob("*/*/statcast_search.csv"))
    research_files = list((RAW / "2025").glob("*/statcast_search.csv")) + list((RAW / "2026").glob("*/statcast_search.csv"))
    inventory = [
        {"asset": "Baseball Savant Statcast raw archive", "path": "backend/mlb/data/external/statcast/raw/<year>/<date_range>/statcast_search.csv", "concepts": "xBA;xSLG;xwOBA;EV;LA;hard-hit;barrel;pitch type;velocity;spin;movement;run value;handedness", "grain": "pitch", "coverage": "2022-04-07 through 2026-07-27", "status": "LOCAL_AUTHORITATIVE_RAW"},
        {"asset": "Statcast acquisition metadata", "path": "backend/mlb/data/external/statcast/raw/<year>/<date_range>/metadata.json", "concepts": "request URL;timestamp;HTTP status;row count;SHA256;duplicate keys;schema", "grain": "download chunk", "coverage": "2022-2026", "status": "LOCAL_LINEAGE"},
        {"asset": "External batter-event platform certification", "path": str(PLATFORM.relative_to(ROOT)), "concepts": "schema;coverage;identity;validation;source contracts", "grain": "platform", "coverage": "2022 through 2026-07-21 certification", "status": "CERTIFIED"},
        {"asset": "MLB StatsAPI retained feeds", "path": "backend/mlb/data/external/statsapi/raw/2026/<gamePk>/feed_live.json", "concepts": "lineups;starters;game starts;PA outcomes;hitData", "grain": "game/play", "coverage": "2026 retained history", "status": "LOCAL_AUTHORITATIVE_RAW"},
        {"asset": "Lineup-confirmed scoring v2 population", "path": str(POP.relative_to(ROOT)), "concepts": "18 starters;starting pitchers;F5 and full-game outcomes", "grain": "game", "coverage": "2026-03-26 through 2026-07-27; 1,594 games", "status": "ACCEPTED_TARGET_SPINE"},
        {"asset": "Contact quality readiness audit", "path": "backend/mlb/scripts/audit_mlb_contact_quality_data_readiness.py", "concepts": "feed hitData EV/LA and prior contact-quality feasibility", "grain": "audit", "coverage": "prior project audit", "status": "DERIVATIVE"},
        {"asset": "Statcast/contact pilots", "path": "backend/mlb/scripts/run_mlb_empirical_contact_quality_conversion_pilot.py", "concepts": "contact conversion and expected-hit reconstruction", "grain": "research derivative", "coverage": "bounded pilots", "status": "DERIVATIVE"},
    ]
    write_csv("expected_quality_repository_inventory.csv", inventory)
    lineage = [
        {"source": "Baseball Savant Statcast Search CSV", "authority": "baseballsavant.mlb.com", "storage": inventory[0]["path"], "grain": "pitch", "date_coverage": "2022-04-07..2026-07-27", "strict_prior": "YES", "raw_retained": "YES", "deterministic_replay": "YES_FROM_IMMUTABLE_LOCAL_BYTES"},
        {"source": "MLB StatsAPI feed/live", "authority": "statsapi.mlb.com", "storage": inventory[3]["path"], "grain": "game/play/pitch", "date_coverage": "2026 retained population", "strict_prior": "YES", "raw_retained": "YES", "deterministic_replay": "YES_FROM_IMMUTABLE_LOCAL_BYTES"},
        {"source": "Feature engineering", "authority": "repository scripts", "storage": "artifacts/analysis/model_development", "grain": "player-game/team-game", "date_coverage": "varies", "strict_prior": "DEPENDS_ON_SCRIPT", "raw_retained": "N/A", "deterministic_replay": "YES_WHEN_INPUT_HASHES_FROZEN"},
        {"source": "pybaseball", "authority": "none found as governing retained source", "storage": "N/A", "grain": "N/A", "date_coverage": "N/A", "strict_prior": "N/A", "raw_retained": "NO", "deterministic_replay": "N/A"},
        {"source": "local database", "authority": "existing outcomes/opportunity tables", "storage": "mlb schema", "grain": "game/player-game", "date_coverage": "varies", "strict_prior": "YES_FOR_TIMESTAMPED_ROWS", "raw_retained": "SOURCE_DEPENDENT", "deterministic_replay": "SOURCE_DEPENDENT"},
    ]
    write_csv("expected_quality_source_lineage.csv", lineage)
    concepts = {
        "estimated_ba_using_speedangle": "xBA on eligible contact", "estimated_slg_using_speedangle": "xSLG on eligible contact",
        "estimated_woba_using_speedangle": "xwOBA on eligible contact/PA", "woba_value": "observed wOBA value",
        "launch_speed": "exit velocity; hard-hit derived >=95 mph", "launch_angle": "launch angle; sweet spot derived with declared band",
        "launch_speed_angle": "Statcast batted-ball category; barrel category=6", "events": "terminal PA outcome for K/BB/HR rates",
        "description": "pitch result for swing/whiff/contact/chase derivation with zone", "pitch_type": "pitch family and usage",
        "release_speed": "velocity", "effective_speed": "effective velocity", "release_spin_rate": "spin",
        "pfx_x": "horizontal movement", "pfx_z": "vertical movement", "release_pos_x": "release position x",
        "release_pos_z": "release position z", "release_extension": "extension", "zone": "rule-zone bucket/chase derivation",
        "plate_x": "horizontal location", "plate_z": "vertical location", "delta_run_exp": "pitch-level run expectancy change",
        "bb_type": "GB/FB/LD/popup state", "stand": "batter handedness", "p_throws": "pitcher handedness",
        "batter": "MLB batter identity", "pitcher": "MLB pitcher identity", "game_pk": "MLB game identity",
        "game_date": "date boundary", "at_bat_number": "PA identity", "pitch_number": "pitch identity",
    }
    sample = pd.read_csv(sorted((RAW / "2026").glob("*/statcast_search.csv"))[0], nrows=0).columns
    field_rows = [{"field": f, "verified_present": f in sample, "concept": c, "native_grain": "pitch" if f not in {"game_date", "game_pk", "at_bat_number"} else "identifier", "strict_prior_aggregation": "YES", "caveat": "expected/contact fields populate only eligible events" if f.startswith("estimated_") or f in {"launch_speed", "launch_angle", "launch_speed_angle", "bb_type"} else ""} for f, c in concepts.items()]
    write_csv("expected_quality_field_contract.csv", field_rows)
    feasibility = []
    for entity, family, windows, grain, note in [
        ("batter", "expected/contact quality", "season-to-date; rolling 15/30/60/100 PA", "pitch/PA -> pregame player state", "feasible; window availability is descriptive"),
        ("batter", "handedness split", "season-to-date and rolling when sample permits", "pitch/PA -> split state", "feasible but sparse for short windows"),
        ("starter", "expected quality allowed", "season-to-date; rolling 2/3/5 starts", "pitch/PA -> pregame player state", "feasible"),
        ("starter", "pitch mix/velocity/stuff", "season-to-date; rolling pitches and 2/3/5 starts", "pitch -> pregame player state", "feasible"),
        ("bullpen", "underlying quality", "team/relief aggregate strictly before game", "pitch -> team-game", "feasible; exact reliever availability intentionally not required"),
        ("matchup", "lineup vs pitch family", "prior batter pitch-family and starter mix", "pitch -> matchup/team-game", "feasible with shrinkage/archetypes; direct pairs sparse"),
        ("batter", "xwOBA/contact quality availability", ">=1 eligible prior expected-quality observation; also report 15/30/60/100 PA", "pitch/PA -> pregame player state", "availability counts only; no reliability threshold claimed"),
        ("batter", "K-BB/whiff/contact availability", "15/30/60/100 prior PA or rolling pitches", "pitch/PA -> pregame player state", "availability counts only; split cells will be sparser"),
        ("starter", "velocity/pitch-mix availability", ">=100 prior pitches and >=2 prior games for reported change-state coverage", "pitch -> pregame player state", "descriptive rule only; no stability threshold claimed"),
        ("starter/bullpen", "contact suppression availability", "prior BIP; rolling 2/3/5 starts for starters; team relief S-T-D", "pitch/PA -> pregame player/team state", "BIP-driven fields are naturally sparser than pitch fields"),
    ]:
        feasibility.append({"entity": entity, "feature_family": family, "supported_windows": windows, "reconstruction": grain, "strict_prior": "YES", "history_required": note})
    write_csv("expected_quality_strict_prior_feasibility.csv", feasibility)
    cov = [
        {"section": "population", "metric": "exact games", "count": m["population_games"], "denominator": m["population_games"], "percent": 100},
        {"section": "batter", "metric": "games all 18 with prior xwOBA observation", "count": m["all_18_batters_expected_quality"], "denominator": m["population_games"], "percent": pct(m["all_18_batters_expected_quality"],m["population_games"])},
        {"section": "batter", "metric": "games >=16 with prior xwOBA observation", "count": m["at_least_16_batters_expected_quality"], "denominator": m["population_games"], "percent": pct(m["at_least_16_batters_expected_quality"],m["population_games"])},
        {"section": "batter", "metric": "player-games with prior xwOBA observation", "count": m["batter_expected_quality_player_games"], "denominator": m["player_games"], "percent": pct(m["batter_expected_quality_player_games"],m["player_games"])},
        {"section": "batter", "metric": "sparse player-games under 30 prior PA", "count": m["batter_sparse_under_30_pa"], "denominator": m["player_games"], "percent": pct(m["batter_sparse_under_30_pa"],m["player_games"])},
        {"section": "starter", "metric": "games both starters >=100 prior pitches plus prior xwOBA allowed", "count": m["both_starters_covered"], "denominator": m["population_games"], "percent": pct(m["both_starters_covered"],m["population_games"])},
        {"section": "starter", "metric": "games exactly one starter covered", "count": m["one_starter_covered"], "denominator": m["population_games"], "percent": pct(m["one_starter_covered"],m["population_games"])},
        {"section": "starter", "metric": "games neither starter covered", "count": m["neither_starter_covered"], "denominator": m["population_games"], "percent": pct(m["neither_starter_covered"],m["population_games"])},
        {"section": "combined", "metric": ">=16 batters >=30 PA and both starters covered", "count": m["combined_sufficient_games"], "denominator": m["population_games"], "percent": pct(m["combined_sufficient_games"],m["population_games"])},
    ]
    write_csv("expected_quality_population_coverage.csv", cov)
    novelty = [
        ("xBA/xSLG/xwOBA", "GENUINELY_NEW", "contact quality independent of realized hit/run result"), ("exit velocity/launch angle/hard-hit/barrel", "GENUINELY_NEW", "batted-ball process quality"),
        ("pitch velocity/spin/movement/release/pitch mix", "GENUINELY_NEW", "stuff and repertoire state"), ("whiff/chase/contact by pitch family", "GENUINELY_NEW", "pitch interaction process"),
        ("ERA/runs allowed/recent runs", "TRANSFORMATION_OF_EXISTING_OUTCOMES", "realized scoring"), ("BA/hits/SLG/HR", "TRANSFORMATION_OF_EXISTING_OUTCOMES", "realized batting outcomes"),
        ("rolling variants of existing runs/hits/workload", "HIGHLY_REDUNDANT", "rearranges already represented outcome/opportunity state"),
    ]
    write_csv("expected_quality_novelty_classification.csv", [{"feature_family":a,"classification":b,"rationale":c} for a,b,c in novelty])
    bundles = [
        ("A", "Batter contact quality", "rolling xwOBA; EV; hard-hit%; barrel%; K-BB%; whiff%", "HIGH"),
        ("B", "Starter underlying quality", "xwOBA allowed; hard-hit/barrel suppression; K-BB%; velocity; whiff%; compact pitch mix", "HIGH"),
        ("C", "Matchup quality", "handedness-adjusted lineup quality; lineup pitch-family profiles weighted by starter mix", "HIGH"),
        ("D", "Bullpen underlying quality", "strict-prior team relief xwOBA allowed; K-BB%; hard-hit/barrel suppression", "MEDIUM"),
    ]
    write_csv("expected_quality_feature_bundles.csv", [{"bundle":a,"name":b,"small_feature_set":c,"conceptual_novelty":d,"fit_model_now":"NO"} for a,b,c,d in bundles])
    write_csv("pitch_mix_matchup_feasibility.csv", [
        {"measure":"local deduplicated pitches loaded (2025-2026 through July 27)","count":len(stat),"denominator":len(stat),"percent":100,"assessment":"HIGH_VOLUME"},
        {"measure":"lineup-batter/opposing-starter instances with both >=100 prior pitches", "count":m["pitch_family_profile_ready"],"denominator":m["matchup_instances"],"percent":pct(m["pitch_family_profile_ready"],m["matchup_instances"]),"assessment":"PRACTICAL_FOR_PITCH_FAMILY_PROFILES"},
        {"measure":"direct batter-starter instances with >=10 prior head-to-head pitches", "count":m["direct_pair_10_pitch"],"denominator":m["matchup_instances"],"percent":pct(m["direct_pair_10_pitch"],m["matchup_instances"]),"assessment":"SPARSE_DIRECT_MATCHUP"},
        {"measure":"recommended representation","count":"","denominator":"","percent":"","assessment":"starter mix x batter pitch-family/archetype; shrink sparse cells"},
    ])
    write_csv("velocity_skill_change_feasibility.csv", [
        {"signal":"starter velocity change vs baseline","availability_rule":">=100 prior pitches and >=2 prior games (descriptive)","covered_instances":m["starter_instances_change_ready"],"denominator":2*m["population_games"],"percent":pct(m["starter_instances_change_ready"],2*m["population_games"]),"strict_prior":"YES","assessment":"FEASIBLE"},
        {"signal":"pitch usage change","availability_rule":"same", "covered_instances":m["starter_instances_change_ready"],"denominator":2*m["population_games"],"percent":pct(m["starter_instances_change_ready"],2*m["population_games"]),"strict_prior":"YES","assessment":"FEASIBLE"},
        {"signal":"whiff/contact-quality change","availability_rule":"rolling pitches/BIP; descriptive counts, no reliability claim", "covered_instances":m["starter_instances_change_ready"],"denominator":2*m["population_games"],"percent":pct(m["starter_instances_change_ready"],2*m["population_games"]),"strict_prior":"YES","assessment":"FEASIBLE_WITH_BIP_SPARSITY"},
    ])
    write_csv("expected_quality_acquisition_sizing.csv", [
        {"stage":"NONE_REQUIRED_FOR_RESEARCH", "date_range":"local 2025 + 2026 through 2026-07-27", "approx_pitch_rows":len(stat), "storage_bytes":sum(p.stat().st_size for p in research_files), "runtime":"0 retrieval; local aggregation under one minute on this host", "retrieval_path":"retained raw CSV", "execute_now":"NO"},
        {"stage":"OPTIONAL_INCREMENTAL_REFRESH", "date_range":"2026-07-28 through latest complete date", "approx_pitch_rows":"~4,000 per MLB day", "storage_bytes":"~2.5-3.0 MB per MLB day observed", "runtime":"seconds to low minutes/day with 1.5s pacing", "retrieval_path":"existing acquire_mlb_statcast_chunks.py / Baseball Savant CSV", "execute_now":"NO"},
        {"stage":"OPTIONAL_EXTRA_BASELINE", "date_range":"2024 already local", "approx_pitch_rows":710631, "storage_bytes":487186897, "runtime":"0 retrieval; local aggregation minutes", "retrieval_path":"retained raw CSV", "execute_now":"NO"},
    ])
    score = [
        ("A Batter contact quality","HIGH","HIGH","HIGH","MEDIUM","MEDIUM","HIGH","HIGH","HIGH"),
        ("B Starter underlying quality","HIGH","HIGH","HIGH","MEDIUM","MEDIUM","HIGH","HIGH","MEDIUM"),
        ("C Matchup quality","MEDIUM","HIGH","HIGH","MEDIUM","HIGH","HIGH","HIGH","MEDIUM"),
        ("D Bullpen underlying quality","HIGH","HIGH","MEDIUM","MEDIUM","MEDIUM","LOW","MEDIUM","HIGH"),
    ]
    write_csv("information_novelty_scorecard.csv", [{"bundle":a,"historical_coverage":b,"strict_prior_reproducibility":c,"conceptual_novelty":d,"expected_sparsity":e,"implementation_effort":f,"likely_utility_f5":g,"likely_utility_team_scoring":h,"likely_utility_full_game":i} for a,b,c,d,e,f,g,h,i in score])
    (OUT / "statcast_recovery_feasibility.md").write_text(f"""# Statcast recovery feasibility\n\nNo recovery is required for research readiness. Authoritative Baseball Savant Statcast Search CSV responses are retained locally at pitch grain for 2022-04-07 through 2026-07-27. The 2025+2026 slice contains {len(stat):,} deduplicated pitches from {stat.batter.nunique():,} batters and {stat.pitcher.nunique():,} pitchers and occupies {sum(p.stat().st_size for p in research_files):,} bytes. The full 2022-2026 archive occupies {sum(p.stat().st_size for p in raw_files):,} bytes. Raw request metadata, timestamps, URLs, row counts, response hashes, schema, and duplicate-key validation permit deterministic replay from local bytes.\n\nIf freshness beyond July 27 is needed later, use the existing bounded daily acquisition script against `https://baseballsavant.mlb.com/statcast_search/csv` with its retained query contract and 1.5-second pacing. Expected volume is roughly 4,000 pitch rows and 2.5–3.0 MB per MLB day, with seconds to low minutes of retrieval per day. No probe was necessary because an actual 119-column retained schema and validated raw responses already establish availability.\n\nBatter and pitcher identity are MLB IDs on every retained pitch. Expected metrics are event-conditional and therefore naturally sparse relative to all pitches; that is semantic missingness, not source failure.\n""", encoding="utf-8")
    (OUT / "current_platform_information_gap.md").write_text("""# Current platform information gap\n\nThe recent scoring models predominantly recombined realized outcomes (runs, hits, ERA), opportunity/workload, lineup identity, park, and schedule state. Those are useful controls but add little new information about the latent quality of contact or current pitch arsenal, explaining why richer architectures could still fail to beat constants. Market information was deliberately outside those scoring foundations and this audit.\n\nThe important weak or unused dimensions are strict-prior contact quality (xwOBA, EV, barrels, hard-hit), pitcher contact suppression, pitch velocity/movement/repertoire change, whiff/chase process, and lineup compatibility with a starter's pitch families. The raw inputs are not absent: they are already retained. The gap is a governed pregame feature layer that aggregates them without same-day leakage and handles young-player/split sparsity.\n\nThe existing outcome spine already supplies away/home F5 runs, F5 total, away/home full-game runs, and full-game total. No new outcome acquisition is required.\n""", encoding="utf-8")
    cov_text = "; ".join(f"{r['metric']}: {r['count']}/{r['denominator']} ({r['percent']}%)" for r in cov[1:])
    summary = f"""# MLB Expected-Quality Feature Platform Inventory v1\n\n**Decision:** `EXPECTED_QUALITY_PLATFORM_ALREADY_AVAILABLE`  \n**EXPECTED_QUALITY_MODEL_RESEARCH_READY:** `YES`\n\nThe local authoritative Statcast archive contains xBA, xSLG, xwOBA, exit velocity, launch angle, hard-hit/barrel inputs, pitch type, velocity, spin, movement, location, pitch run value, and handedness at pitch/PA grain. Outcome-only ERA, runs, hits, BA, and their rolling transforms are not genuinely new information.\n\nAgainst the accepted 1,594-game lineup-confirmed population, using date-strict prior history (the current date is never admitted): {cov_text}. The combined line is a descriptive availability rule, not a fitted or accepted qualification threshold.\n\nBullpen underlying quality is reconstructable as strict-prior team relief state, without solving individual availability. Pitch-family lineup profiles are practical for {m['pitch_family_profile_ready']}/{m['matchup_instances']} ({pct(m['pitch_family_profile_ready'],m['matchup_instances'])}%) batter-matchup instances under a descriptive 100-pitch rule; direct batter-pitcher history >=10 pitches is only {m['direct_pair_10_pitch']}/{m['matchup_instances']} ({pct(m['direct_pair_10_pitch'],m['matchup_instances'])}%), so archetype/pitch-family aggregation is preferable. Velocity/usage change state is available for {m['starter_instances_change_ready']}/{2*m['population_games']} ({pct(m['starter_instances_change_ready'],2*m['population_games'])}%) starter instances under the descriptive >=100-pitch, >=2-game rule.\n\nStrict-prior xwOBA/contact-quality and pitcher underlying-quality states are reconstructable. The smallest justified acquisition is none: 2025 plus 2026 through July 27 is already local; an optional freshness-only daily increment is about 4,000 rows and 2.5–3.0 MB per MLB day. Highest-novelty next bundles are batter contact quality, starter underlying quality, and shrunk lineup-vs-pitch-family matchup state. No model was fit.\n"""
    (OUT / "concise_mlb_expected_quality_feature_platform_inventory_v1.md").write_text(summary, encoding="utf-8")
    # Detail is intentionally retained for reproducibility but not part of the minimum named outputs.
    pd.DataFrame(detail).to_csv(OUT / "strict_prior_game_coverage_detail.csv", index=False)
    inputs = [POP, PLATFORM / "baseball_savant_schema.csv", PLATFORM / "critical_field_coverage.csv", SCRIPT]
    outputs = sorted(p for p in OUT.iterdir() if p.name != "sha256_manifest.csv")
    write_csv("sha256_manifest.csv", [{"kind":"input" if p in inputs else "output", "path":str(p.relative_to(ROOT)), "sha256":sha(p), "bytes":p.stat().st_size} for p in inputs + outputs])
    print(json.dumps({"decision":"EXPECTED_QUALITY_PLATFORM_ALREADY_AVAILABLE", "research_ready":"YES", **m}, indent=2))


if __name__ == "__main__":
    main()
