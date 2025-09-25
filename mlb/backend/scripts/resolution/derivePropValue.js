// backend/scripts/resolution/derivePropValue.js

export function derivePropValue(propType, stats) {
  const { batting = {}, pitching = {} } = stats || {};

  switch (propType) {
    case "hits":
      return batting.hits;
    case "runs":
    case "runs_scored":
      return batting.runs;
    case "rbis":
      return batting.rbi;
    case "home_runs":
      return batting.homeRuns;
    case "stolen_bases":
      return batting.stolenBases;
    case "walks":
      return batting.baseOnBalls;
    case "strikeouts_batting":
      return batting.strikeOuts;
    case "total_bases":
      return batting.totalBases;
    case "hits_runs_rbis": {
      const hasBattingInputs =
        typeof batting?.hits === "number" ||
        typeof batting?.runs === "number" ||
        typeof batting?.rbi === "number";

      if (!hasBattingInputs) return null;

      const hits = batting?.hits ?? 0;
      const runs = batting?.runs ?? 0;
      const rbi = batting?.rbi ?? 0;

      const isMissing = [batting?.hits, batting?.runs, batting?.rbi].some(
        (v) => v === undefined
      );

      if (isMissing) {
        console.warn("[⚠️ COMBO] Missing input(s) for hits_runs_rbis:", {
          hits: batting?.hits,
          runs: batting?.runs,
          rbi: batting?.rbi,
        });
      } else {
        console.log("[COMBO] hits_runs_rbis input:", { hits, runs, rbi });
      }

      return hits + runs + rbi;
    }

    case "runs_rbis":
      return batting.runs + batting.rbi;
    case "singles":
      return (
        batting.hits - batting.doubles - batting.triples - batting.homeRuns
      );
    case "doubles":
      return batting.doubles;
    case "triples":
      return batting.triples;
    case "strikeouts_pitching":
      return pitching.strikeOuts;
    case "walks_allowed":
      return pitching.baseOnBalls;
    case "hits_allowed":
      return pitching.hits;
    case "earned_runs":
      return pitching.earnedRuns;
    case "outs_recorded":
      return pitching.outs;
    default:
      console.warn("⚠️ Unknown propType in derivePropValue:", propType);
      return;
  }
}
