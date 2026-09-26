from __future__ import annotations

import pandas as pd

from backend.nhl.saves_shadow.core import (
    NHL_COM_PROJECTED_LINEUP_SOURCE,
    apply_starter_gate,
    qualify_starter_evidence,
)


RUN = "2026-09-20T20:00:00Z"
START = "2026-09-20T23:00:00Z"
AUTHORITY = ("fixture-authority",)


def parents():
    games = pd.DataFrame([{
        "canonical_season": 2026, "slate_date": "2026-09-20", "game_id": 9001,
        "home_team": "AAA", "away_team": "BBB", "scheduled_start_time_utc": START,
    }])
    goalies = pd.DataFrame([
        {"game_id": 9001, "team": "AAA", "goalie_id": 101, "goalie_name": "Alex Goalie"},
        {"game_id": 9001, "team": "AAA", "goalie_id": 102, "goalie_name": "Backup Goalie"},
        {"game_id": 9001, "team": "BBB", "goalie_id": 201, "goalie_name": "Other Goalie"},
    ])
    return games, goalies


def event(goalie_id=101, *, source="fixture-authority", status="CONFIRMED",
          source_time="2026-09-20T19:30:00Z", capture_time="2026-09-20T19:31:00Z"):
    return {
        "canonical_season": 2026, "slate_date": "2026-09-20", "game_id": 9001,
        "team": "AAA", "goalie_id": goalie_id, "goalie_status": status,
        "source": source, "source_record_id": f"record-{goalie_id}",
        "source_timestamp_utc": source_time, "capture_timestamp_utc": capture_time,
        "raw_payload_sha256": "a" * 64,
    }


def nhl_article_event(*, goalie_name="Alex Goalie", team="AAA", capture_time="2026-09-20T19:31:00Z",
                      designation_text="Alex Goalie is expected to start in goal for AAA.", **overrides):
    row = event(goalie_id=None, source=NHL_COM_PROJECTED_LINEUP_SOURCE, status="PROJECTED",
                source_time=None, capture_time=capture_time)
    row.update({
        "article_url": "https://www.nhl.com/news/aaa-bbb-game-preview-september-20-2026",
        "article_published_date": "2026-09-20", "article_published_at_utc": None,
        "article_game_date": "2026-09-20", "article_home_team": "AAA", "article_away_team": "BBB",
        "article_team": team, "goalie_name": goalie_name, "designation_text": designation_text,
    })
    row.update(overrides)
    row["source_record_id"] = "aaa-bbb-game-preview-september-20-2026"
    return row


def test_confirmed_fresh_pregame_source_preserves_timestamps_and_qualifies_identity():
    games, goalies = parents()
    result = qualify_starter_evidence(
        games=games, goalies=goalies, evidence=pd.DataFrame([event()]),
        run_timestamp_utc=RUN, authorized_sources=AUTHORITY,
    )
    home = result[result.team.eq("AAA")].iloc[0]
    assert home.starter_identity_state == "STARTER_CONFIRMED"
    assert home.starter_evidence_status == "CONFIRMED"
    assert home.selected_starter_goalie_id == 101
    assert home.starter_source_timestamp_utc == "2026-09-20T19:30:00Z"
    assert home.starter_capture_timestamp_utc == "2026-09-20T19:31:00Z"
    assert result[result.team.eq("BBB")].iloc[0].starter_identity_state == "STARTER_UNKNOWN_MISSING_EVIDENCE"


def test_projected_pregame_evidence_qualifies_without_becoming_confirmed():
    games, goalies = parents()
    result = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([event(status="PROJECTED")]),
        run_timestamp_utc=RUN, authorized_sources=AUTHORITY)
    home = result[result.team.eq("AAA")].iloc[0]
    assert home.starter_identity_state == "STARTER_PROJECTED"
    assert home.starter_evidence_status == "PROJECTED"
    assert home.selected_starter_goalie_id == 101
    assert home.starter_source_timestamp_utc == "2026-09-20T19:30:00Z"


def test_nhl_com_projected_article_binds_name_and_retains_date_only_provenance():
    games, goalies = parents()
    result = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([nhl_article_event()]), run_timestamp_utc=RUN)
    home = result[result.team.eq("AAA")].iloc[0]
    assert home.starter_identity_state == "STARTER_PROJECTED"
    assert home.starter_evidence_status == "PROJECTED"
    assert home.selected_starter_goalie_id == 101
    assert home.starter_source_timestamp_utc is None
    assert home.starter_source_url == "https://www.nhl.com/news/aaa-bbb-game-preview-september-20-2026"
    assert home.starter_source_published_date == "2026-09-20"
    assert pd.isna(home.starter_source_published_at_utc)
    assert home.starter_capture_timestamp_utc == "2026-09-20T19:31:00Z"
    assert home.starter_source_content_sha256 == "a" * 64
    assert home.starter_source_record_id == "aaa-bbb-game-preview-september-20-2026"


def test_nhl_com_article_rejects_ambiguous_missing_or_unbound_goalie_names():
    games, goalies = parents()
    ambiguous_goalies = pd.concat([goalies, pd.DataFrame([{
        "game_id": 9001, "team": "AAA", "goalie_id": 103, "goalie_name": "Alex Goalie"}])], ignore_index=True)
    ambiguous = qualify_starter_evidence(games=games, goalies=ambiguous_goalies,
        evidence=pd.DataFrame([nhl_article_event()]), run_timestamp_utc=RUN)
    assert ambiguous.loc[ambiguous.team.eq("AAA"), "starter_reason"].iloc[0] == "NHL_COM_GOALIE_NAME_AMBIGUOUS"

    missing = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([nhl_article_event(goalie_name="Unknown Goalie",
            designation_text="Unknown Goalie is expected to start in goal.")]), run_timestamp_utc=RUN)
    assert missing.loc[missing.team.eq("AAA"), "starter_reason"].iloc[0] == "NHL_COM_GOALIE_NAME_NOT_BOUND"

    mismatch = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([nhl_article_event(article_home_team="CCC")]), run_timestamp_utc=RUN)
    assert mismatch.loc[mismatch.team.eq("AAA"), "starter_reason"].iloc[0] == "NHL_COM_ARTICLE_GAME_MISMATCH"


def test_nhl_com_article_requires_unique_explicit_projected_designation_and_pregame_capture():
    games, goalies = parents()
    vague = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([nhl_article_event(designation_text="Alex Goalie is listed in the lineup.")]), run_timestamp_utc=RUN)
    assert vague.loc[vague.team.eq("AAA"), "starter_reason"].iloc[0] == "NHL_COM_STARTER_NOT_EXPLICITLY_DESIGNATED"

    conflicting = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([nhl_article_event(), nhl_article_event(
            goalie_name="Backup Goalie", designation_text="Backup Goalie is expected to start in goal.")]),
        run_timestamp_utc=RUN)
    assert conflicting.loc[conflicting.team.eq("AAA"), "starter_identity_state"].iloc[0] == "STARTER_UNKNOWN_CONFLICTING_EVIDENCE"

    poststart = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([nhl_article_event(capture_time="2026-09-20T23:01:00Z")]),
        run_timestamp_utc="2026-09-20T23:02:00Z")
    assert poststart.loc[poststart.team.eq("AAA"), "starter_identity_state"].iloc[0] == "STARTER_UNKNOWN_INVALID_TIMING"


def test_missing_stale_unauthorized_and_poststart_evidence_fail_closed():
    games, goalies = parents()
    missing = qualify_starter_evidence(games=games, goalies=goalies, evidence=None,
        run_timestamp_utc=RUN, authorized_sources=AUTHORITY)
    assert missing.starter_identity_state.eq("STARTER_UNKNOWN_MISSING_EVIDENCE").all()

    stale = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([event(source_time="2026-09-19T10:00:00Z", capture_time="2026-09-19T10:01:00Z")]),
        run_timestamp_utc=RUN, authorized_sources=AUTHORITY)
    assert stale.loc[stale.team.eq("AAA"), "starter_identity_state"].iloc[0] == "STARTER_UNKNOWN_STALE_EVIDENCE"

    unauthorized = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([event(source="unlisted-source")]),
        run_timestamp_utc=RUN, authorized_sources=AUTHORITY)
    assert unauthorized.loc[unauthorized.team.eq("AAA"), "starter_identity_state"].iloc[0] == "STARTER_UNKNOWN_UNAUTHORIZED_SOURCE"

    poststart = qualify_starter_evidence(games=games, goalies=goalies,
        evidence=pd.DataFrame([event(source_time="2026-09-20T23:01:00Z", capture_time="2026-09-20T23:02:00Z")]),
        run_timestamp_utc="2026-09-20T23:03:00Z", authorized_sources=AUTHORITY)
    assert poststart.loc[poststart.team.eq("AAA"), "starter_identity_state"].iloc[0] == "STARTER_UNKNOWN_INVALID_TIMING"


def test_conflicting_authoritative_sources_fail_closed():
    games, goalies = parents()
    evidence = pd.DataFrame([
        event(goalie_id=101, source="authority-a"),
        event(goalie_id=102, source="authority-b"),
    ])
    result = qualify_starter_evidence(games=games, goalies=goalies, evidence=evidence,
        run_timestamp_utc=RUN, authorized_sources=("authority-a", "authority-b"))
    home = result[result.team.eq("AAA")].iloc[0]
    assert home.starter_identity_state == "STARTER_UNKNOWN_CONFLICTING_EVIDENCE"
    assert pd.isna(home.selected_starter_goalie_id)


def test_market_listed_goalie_without_confirmed_source_cannot_be_selected():
    market_selection = pd.DataFrame([{
        "game_id": 9001, "team": "AAA", "decision": "SELECTED",
        "selected_goalie_id": 101, "reason": "POLICY_C_UNIQUE_TOP_MULTIBOOK",
    }])
    # An unresolved source decision must block a would-be market selection.
    states = pd.DataFrame([{
        "game_id": 9001, "team": "AAA", "starter_identity_state": "STARTER_UNKNOWN_MISSING_EVIDENCE",
        "starter_reason": "NO_SOURCE_EVIDENCE", "selected_starter_goalie_id": None,
    }])
    gated = apply_starter_gate(market_selection, states)
    assert gated.iloc[0].decision == "BLOCKED"
    assert pd.isna(gated.iloc[0].selected_goalie_id)


def test_market_selection_cannot_override_a_different_confirmed_starter():
    market_selection = pd.DataFrame([{
        "game_id": 9001, "team": "AAA", "decision": "SELECTED",
        "selected_goalie_id": 102, "reason": "POLICY_C_UNIQUE_TOP_MULTIBOOK",
    }])
    states = pd.DataFrame([{
        "game_id": 9001, "team": "AAA", "starter_identity_state": "STARTER_CONFIRMED",
        "starter_reason": "AUTHORIZED_FRESH_PREGAME_CONFIRMATION", "selected_starter_goalie_id": 101,
    }])
    gated = apply_starter_gate(market_selection, states)
    assert gated.iloc[0].decision == "BLOCKED"
    assert gated.iloc[0].reason == "MARKET_LISTED_GOALIE_DIFFERS_FROM_SOURCE_PREGAME_STARTER"
    assert gated.iloc[0].starter_market_agreement == "DISAGREES_MARKET_DIAGNOSTIC_ONLY"
    assert pd.isna(gated.iloc[0].selected_goalie_id)


def test_projected_source_remains_eligible_when_market_candidate_disagrees():
    market_selection = pd.DataFrame([{
        "game_id": 9001, "team": "AAA", "decision": "SELECTED",
        "selected_goalie_id": 102, "reason": "POLICY_C_UNIQUE_TOP_MULTIBOOK",
    }])
    states = pd.DataFrame([{
        "game_id": 9001, "team": "AAA", "starter_identity_state": "STARTER_PROJECTED",
        "starter_evidence_status": "PROJECTED", "starter_reason": "AUTHORIZED_FRESH_PREGAME_PROJECTED",
        "selected_starter_goalie_id": 101,
    }])
    gated = apply_starter_gate(market_selection, states)
    assert gated.iloc[0].starter_identity_state == "STARTER_PROJECTED"
    assert gated.iloc[0].starter_market_agreement == "DISAGREES_MARKET_DIAGNOSTIC_ONLY"
    assert gated.iloc[0].decision == "BLOCKED"
