// PlayerProfileDashboard.js
import React, { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import Skeleton from "react-loading-skeleton";
import "react-loading-skeleton/dist/skeleton.css";
import { getPropDisplayLabel } from "../../shared/propUtils.js";
import { getBaseURL } from "../../shared/getBaseURL.js";

export default function PlayerProfileDashboard() {
  const { playerId } = useParams();
  const [profileData, setProfileData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchProfile() {
      try {
        const res = await fetch(`${getBaseURL()}/player-profile/${playerId}`);
        const data = await res.json();
        setProfileData(data);
        console.log("👀 Profile data:", data);
      } catch (err) {
        console.error("Error fetching player profile:", err);
      } finally {
        setLoading(false);
      }
    }
    fetchProfile();
  }, [playerId]);

  if (loading) {
    return (
      <div className="p-6 max-w-5xl mx-auto">
        <Skeleton height={36} width={300} />
        <Skeleton count={10} height={20} style={{ marginTop: 12 }} />
      </div>
    );
  }

  if (!profileData)
    return (
      <div className="p-4 text-red-600">Failed to load player profile</div>
    );

  const groupedStatFields = {
    "Batting Summary": [
      "avg",
      "obp",
      "slg",
      "ops",
      "babip",
      "atBats",
      "hits",
      "doubles",
      "triples",
      "homeRuns",
    ],
    "Run Production": [
      "rbi",
      "runs",
      "totalBases",
      "plateAppearances",
      "baseOnBalls",
      "intentionalWalks",
      "hitByPitch",
    ],
    Situational: [
      "sacBunts",
      "sacFlies",
      "groundIntoDoublePlay",
      "leftOnBase",
      "catchersInterference",
    ],
    "Strikeouts & Outs": [
      "strikeOuts",
      "groundOuts",
      "airOuts",
      "groundOutsToAirouts",
      "numberOfPitches",
      "atBatsPerHomeRun",
    ],
    Speed: ["stolenBases", "caughtStealing", "stolenBasePercentage"],
    "Games Played": ["gamesPlayed"],
  };

  const renderStatGroup = (title, stats) => {
    if (!stats || Object.keys(stats).length === 0) return null;

    return (
      <div className="mb-4">
        <h4 className="font-semibold text-gray-800 mb-2">{title}</h4>
        <div className="space-y-1 text-sm font-mono text-gray-800 bg-yellow-50 border border-gray-100 rounded px-4 py-2">
          {Object.entries(stats).map(([key, value]) => {
            const label = key
              .replace(/([a-z])([A-Z])/g, "$1 $2")
              .replace(/\b\w/g, (char) => char.toUpperCase());

            return (
              <div key={key} className="flex justify-between">
                <span className="text-gray-600">{label}:</span>
                <span className="text-right">{String(value)}</span>
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-2xl font-bold">
          Player Profile: {profileData?.player_info?.player_name || playerId} (
          {profileData?.player_info?.team || ""})
          <span className="text-sm font-normal text-gray-500"></span>
        </h1>
        <Link to="/players" className="text-blue-600 hover:underline text-sm">
          ← Back to Player List
        </Link>
      </div>
      <section className="mb-6">
        <div className="p-2 border border-gray-200 rounded bg-red-100 shadow-sm">
          <h2 className="text-xl font-semibold mb-2">Current Streaks</h2>
          {profileData.streaks?.length > 0 ? (
            <ul className="space-y-2">
              {profileData.streaks.map((s, i) => (
                <li key={i} className="p-2 bg-yellow-100 rounded shadow">
                  <span className="font-semibold">
                    {getPropDisplayLabel(s.prop_type)}
                  </span>
                  : {s.streak_type} streak of {s.streak_count}
                </li>
              ))}
            </ul>
          ) : (
            <p>No current streaks found.</p>
          )}
        </div>
      </section>
      <section className="mb-6">
        <div className="p-2 border border-gray-200 rounded bg-blue-50 shadow-sm">
          <h2 className="text-xl font-semibold mb-2">Recent Props</h2>
          {profileData.recent_props?.length > 0 ? (
            <ul className="space-y-1">
              {profileData.recent_props.map((prop, i) => (
                <li
                  key={i}
                  className="p-2 bg-blue-100 rounded shadow-sm text-sm"
                >
                  <div>
                    <span className="font-semibold text-blue-800">
                      {prop.game_date}
                    </span>
                    : {getPropDisplayLabel(prop.prop_type)} → {prop.outcome}
                  </div>
                  <div className="text-sm text-gray-600">
                    {prop.over_under.toLowerCase()} {prop.prop_value}
                    {"\u00A0\u00A0"}
                    {prop.confidence_score && (
                      <span className="ml-2 text-blue-600">
                        {Math.round(prop.confidence_score * 100)}% confident
                      </span>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <p>No recent props available.</p>
          )}
        </div>
      </section>
      <section className="mb-6">
        <div className="p-2 border border-gray-200 rounded bg-green-50 shadow-sm">
          <h2 className="text-xl font-semibold mb-2">Stat-Derived Props</h2>
          {profileData.stat_derived?.length > 0 ? (
            <ul className="space-y-2">
              {profileData.stat_derived.map((prop, i) => (
                <li key={i} className="p-2 bg-green-100 rounded shadow">
                  <div>
                    <span className="font-semibold">{prop.game_date}</span>:{" "}
                    {getPropDisplayLabel(prop.prop_type)} → {prop.result}
                    {prop.outcome && (
                      <span className="ml-2 text-sm text-gray-600">
                        ({prop.outcome})
                      </span>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <p>No stat-derived props recorded.</p>
          )}
        </div>
      </section>
      <section className="mb-6">
        <div className="p-2 border border-gray-200 rounded bg-gray-100 shadow-sm">
          <h2 className="text-xl font-semibold mb-2">Training Summary</h2>
          {profileData.training_summary?.length > 0 ? (
            <ul className="space-y-1">
              {profileData.training_summary.map((entry, i) => (
                <li
                  key={i}
                  className="px-2 py-1 bg-gray-50 rounded shadow-sm text-sm"
                >
                  {getPropDisplayLabel(entry.prop_type)}: {entry.count} props
                  used in training
                </li>
              ))}
            </ul>
          ) : (
            <p>No training data recorded.</p>
          )}
        </div>
      </section>
      <div className="flex mb-6 gap-4 items-stretch">
        {profileData.season_stats?.hitting &&
          (() => {
            const s = profileData.season_stats.hitting;
            return (
              <div className="flex-1 flex flex-col">
                <div className="flex flex-col flex-grow p-4 border border-gray-200 rounded bg-blue-50 shadow-sm">
                  <h3 className="text-xl font-semibold text-blue-700 mb-2">
                    Season Stats
                  </h3>
                  {renderStatGroup("Batting Summary", {
                    avg: s.avg,
                    obp: s.obp,
                    slg: s.slg,
                    ops: s.ops,
                    babip: s.babip,
                    atBats: s.atBats,
                    hits: s.hits,
                    doubles: s.doubles,
                    triples: s.triples,
                    homeRuns: s.homeRuns,
                  })}
                  {renderStatGroup("Run Production", {
                    rbi: s.rbi,
                    runs: s.runs,
                    totalBases: s.totalBases,
                    plateAppearances: s.plateAppearances,
                    baseOnBalls: s.baseOnBalls,
                    intentionalWalks: s.intentionalWalks,
                    hitByPitch: s.hitByPitch,
                  })}
                  {renderStatGroup("Situational", {
                    sacBunts: s.sacBunts,
                    sacFlies: s.sacFlies,
                    groundIntoDoublePlay: s.groundIntoDoublePlay,
                    leftOnBase: s.leftOnBase,
                    catchersInterference: s.catchersInterference,
                  })}
                  {renderStatGroup("Strikeouts & Outs", {
                    strikeOuts: s.strikeOuts,
                    groundOuts: s.groundOuts,
                    airOuts: s.airOuts,
                    groundOutsToAirouts: s.groundOutsToAirouts,
                    numberOfPitches: s.numberOfPitches,
                  })}
                </div>
              </div>
            );
          })()}

        {profileData.career_stats?.hitting &&
          (() => {
            const c = profileData.career_stats.hitting;
            return (
              <div className="flex-1 flex flex-col">
                <div className="flex flex-col flex-grow p-4 border border-gray-200 rounded bg-green-50 shadow-sm">
                  <h3 className="text-xl font-semibold text-green-700 mb-2">
                    Career Stats
                  </h3>
                  {renderStatGroup("Batting Summary", {
                    avg: c.avg,
                    obp: c.obp,
                    slg: c.slg,
                    ops: c.ops,
                    babip: c.babip,
                    atBats: c.atBats,
                    hits: c.hits,
                    doubles: c.doubles,
                    triples: c.triples,
                    homeRuns: c.homeRuns,
                  })}
                  {renderStatGroup("Run Production", {
                    rbi: c.rbi,
                    runs: c.runs,
                    totalBases: c.totalBases,
                    plateAppearances: c.plateAppearances,
                    baseOnBalls: c.baseOnBalls,
                    intentionalWalks: c.intentionalWalks,
                    hitByPitch: c.hitByPitch,
                  })}
                  {renderStatGroup("Situational", {
                    sacBunts: c.sacBunts,
                    sacFlies: c.sacFlies,
                    groundIntoDoublePlay: c.groundIntoDoublePlay,
                    leftOnBase: c.leftOnBase,
                    catchersInterference: c.catchersInterference,
                  })}
                  {renderStatGroup("Strikeouts & Outs", {
                    strikeOuts: c.strikeOuts,
                    groundOuts: c.groundOuts,
                    airOuts: c.airOuts,
                    groundOutsToAirouts: c.groundOutsToAirouts,
                    numberOfPitches: c.numberOfPitches,
                  })}
                </div>
              </div>
            );
          })()}
      </div>
    </div>
  );
}
