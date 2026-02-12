// 📁 extractStatFromPlays.js

export function extractStatFromPlays(plays, playerId) {
  const stats = {
    batting: {},
    pitching: {},
  };

  for (const play of plays) {
    const batterId = play.matchup?.batter?.id;
    const pitcherId = play.matchup?.pitcher?.id;
    const result = play.result?.eventType;

    // Track batting events
    if (batterId === playerId) {
      switch (result) {
        case "single":
          stats.batting.singles = (stats.batting.singles || 0) + 1;
          stats.batting.hits = (stats.batting.hits || 0) + 1;
          break;
        case "double":
          stats.batting.doubles = (stats.batting.doubles || 0) + 1;
          stats.batting.hits = (stats.batting.hits || 0) + 1;
          break;
        case "triple":
          stats.batting.triples = (stats.batting.triples || 0) + 1;
          stats.batting.hits = (stats.batting.hits || 0) + 1;
          break;
        case "home_run":
          stats.batting.home_runs = (stats.batting.home_runs || 0) + 1;
          stats.batting.hits = (stats.batting.hits || 0) + 1;
          stats.batting.rbis =
            (stats.batting.rbis || 0) + (play.result?.rbi || 1);
          break;
        case "walk":
        case "intent_walk":
          stats.batting.base_on_balls = (stats.batting.base_on_balls || 0) + 1;
          break;
        case "strikeout":
          stats.batting.strikeouts = (stats.batting.strikeouts || 0) + 1;
          break;
        case "hit_by_pitch":
          stats.batting.hit_by_pitch = (stats.batting.hit_by_pitch || 0) + 1;
          break;
      }

      if (play.result?.rbi) {
        stats.batting.rbis = (stats.batting.rbis || 0) + play.result.rbi;
      }

      if (play.result?.awayScore > 0 || play.result?.homeScore > 0) {
        if (play.runners?.some((r) => r.details?.isScoringEvent)) {
          stats.batting.runs = (stats.batting.runs || 0) + 1;
        }
      }

      if (play.runners?.some((r) => r.details?.event === "stolen_base")) {
        stats.batting.stolen_bases = (stats.batting.stolen_bases || 0) + 1;
      }
    }

    // Track pitching events
    if (pitcherId === playerId) {
      switch (result) {
        case "strikeout":
          stats.pitching.strikeouts = (stats.pitching.strikeouts || 0) + 1;
          break;
        case "walk":
        case "intent_walk":
          stats.pitching.walks_allowed =
            (stats.pitching.walks_allowed || 0) + 1;
          break;
        case "hit_by_pitch":
          stats.pitching.hit_batters = (stats.pitching.hit_batters || 0) + 1;
          break;
        case "single":
        case "double":
        case "triple":
        case "home_run":
          stats.pitching.hits_allowed = (stats.pitching.hits_allowed || 0) + 1;
          break;
      }

      if (play.result?.rbi) {
        stats.pitching.earned_runs =
          (stats.pitching.earned_runs || 0) + play.result.rbi;
      }

      const outs = play.count?.outs || 0;
      if (outs > 0) {
        stats.pitching.outs_recorded =
          (stats.pitching.outs_recorded || 0) + outs;
      }
    }
  }

  return stats;
}
