// PlayerProfileDashboard.js
import React, { useEffect, useState } from "react";
import { useParams, Link, useLocation } from "react-router-dom";
import Skeleton from "react-loading-skeleton";
import "react-loading-skeleton/dist/skeleton.css";
import { getPropDisplayLabel } from "../shared/propUtils.js";
import { getBaseURL } from "../shared/getBaseURL.js";

export default function PlayerProfileDashboard() {
  const { playerId } = useParams();
  const location = useLocation();
  const [profileData, setProfileData] = useState(null);
  const [todayMarketData, setTodayMarketData] = useState(null);
  const [todayMarketLoading, setTodayMarketLoading] = useState(false);
  const [todayMarketError, setTodayMarketError] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const routedPlayerName = String(location.state?.player_name || "").trim();
  const routedTeam = String(location.state?.team || "").trim();
  const routedSport = String(location.state?.sport || "").trim().toLowerCase();
  const profileSport = location.pathname.startsWith("/nhl/")
    ? "nhl"
    : location.pathname.startsWith("/mlb/")
      ? "mlb"
      : routedSport === "nhl"
        ? "nhl"
        : "mlb";
  const profilePlayerName = String(profileData?.player_info?.player_name || "").trim();
  const profileTeam = String(profileData?.player_info?.team || "").trim();
  const displayPlayerName = routedPlayerName || profilePlayerName || "";
  const displayTeam = routedTeam || profileTeam || "";

  useEffect(() => {
    async function fetchProfile() {
      try {
        setError("");
        const endpoint =
          profileSport === "nhl"
            ? `${getBaseURL()}/api/nhl/player-profile/${playerId}`
            : `${getBaseURL()}/api/player-profile/${playerId}`;
        const res = await fetch(endpoint);
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(`${res.status}: ${txt}`);
        }
        const data = await res.json();
        setProfileData(data);
      } catch (err) {
        setProfileData(null);
        setError(err?.message || "Failed to load player profile.");
      } finally {
        setLoading(false);
      }
    }
    fetchProfile();
  }, [playerId, profileSport]);

  useEffect(() => {
    if (profileSport !== "mlb") {
      setTodayMarketData(null);
      setTodayMarketError("");
      setTodayMarketLoading(false);
      return;
    }
    let isMounted = true;
    async function fetchTodayMarket() {
      try {
        setTodayMarketLoading(true);
        setTodayMarketError("");
        const params = new URLSearchParams({
          player_id: String(playerId),
          limit: "200",
          offset: "0",
        });
        const res = await fetch(`${getBaseURL()}/api/mlb/today/workspace?${params.toString()}`);
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(`${res.status}: ${txt}`);
        }
        const data = await res.json();
        if (isMounted) setTodayMarketData(data);
      } catch (err) {
        if (isMounted) {
          setTodayMarketData(null);
          setTodayMarketError(err?.message || "Unable to load today's market.");
        }
      } finally {
        if (isMounted) setTodayMarketLoading(false);
      }
    }
    fetchTodayMarket();
    return () => {
      isMounted = false;
    };
  }, [playerId, profileSport]);

  if (loading) {
    return (
      <div className="p-6 max-w-5xl mx-auto">
        <Skeleton height={36} width={300} />
        <Skeleton count={10} height={20} style={{ marginTop: 12 }} />
      </div>
    );
  }

  if (error)
    return (
      <div className="p-4 text-rose-600">
        Failed to load player profile: {error}
      </div>
    );

  if (!profileData)
    return (
      <div className="p-4 text-rose-600">Failed to load player profile</div>
    );

  const latestRecentPropDate =
    profileData?.recent_props?.map((p) => p?.game_date).find((d) => d) || null;
  const latestDerivedDate =
    profileData?.stat_derived?.map((p) => p?.game_date).find((d) => d) || null;
  const freshnessDate = latestRecentPropDate || latestDerivedDate;
  const freshnessSource = String(profileData?.freshness_metadata?.source || "").trim();
  const freshnessMaxDate = String(profileData?.freshness_metadata?.max_game_date || "").trim();
  const freshnessLabel = freshnessDate || freshnessMaxDate;
  const todayMarketRows = Array.isArray(todayMarketData?.rows) ? todayMarketData.rows : [];
  const todayMarketDate = todayMarketData?.active_slate_date || todayMarketData?.requested_slate_date || null;
  const PLAYABLE_ODDS_LIMIT = 500;

  const fmtMarketPrice = (value) => {
    if (value === null || value === undefined || value === "") return "—";
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return n > 0 ? `+${Math.round(n)}` : String(Math.round(n));
  };

  const fmtLine = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return Number.isInteger(n) ? String(n) : n.toFixed(1);
  };

  const isPlayableOdds = (value) => {
    if (value === null || value === undefined || value === "") return false;
    const n = Number(value);
    return Number.isFinite(n) && Math.abs(n) <= PLAYABLE_ODDS_LIMIT;
  };

  const pricePointsFromRow = (row) => {
    const best = Number(row?.best_price);
    const span = Number(row?.market_range);
    if (!Number.isFinite(best)) return [];
    const points = [best];
    if (Number.isFinite(span) && span > 0) {
      points.push(best - span);
    }
    return points;
  };

  const playableSummaryFromRow = (row) => {
    const rawPoints = pricePointsFromRow(row);
    const playablePoints = rawPoints.filter((price) => isPlayableOdds(price));
    if (!playablePoints.length) {
      return {
        best: null,
        range: null,
        rawCount: rawPoints.length,
        playableCount: 0,
        filteredOut: rawPoints.length > 0,
      };
    }
    return {
      best: Math.max(...playablePoints),
      range: {
        min: Math.min(...playablePoints),
        max: Math.max(...playablePoints),
      },
      rawCount: rawPoints.length,
      playableCount: playablePoints.length,
      filteredOut: rawPoints.length > playablePoints.length,
    };
  };

  const mergeRange = (current, next) => {
    if (!next) return current || null;
    if (!current) return next;
    return {
      min: Math.min(current.min, next.min),
      max: Math.max(current.max, next.max),
    };
  };

  const fmtRange = (range) => {
    if (!range) return "";
    const min = Number(range.min);
    const max = Number(range.max);
    if (!Number.isFinite(min) || !Number.isFinite(max)) return "";
    if (Math.round(min) === Math.round(max)) return fmtMarketPrice(max);
    return `${fmtMarketPrice(min)} to ${fmtMarketPrice(max)}`;
  };

  const mergeSideSummary = (existing, summary) => {
    const currentBest = Number(existing.best);
    const nextBest = Number(summary.best);
    return {
      best:
        Number.isFinite(nextBest) && (!Number.isFinite(currentBest) || nextBest > currentBest)
          ? nextBest
          : existing.best,
      range: mergeRange(existing.range, summary.range),
      filteredOut: Boolean(existing.filteredOut || summary.filteredOut),
      playableCount: (Number(existing.playableCount) || 0) + (Number(summary.playableCount) || 0),
      rawCount: (Number(existing.rawCount) || 0) + (Number(summary.rawCount) || 0),
    };
  };

  const todayMarketGroups = Array.from(
    todayMarketRows.reduce((acc, row) => {
      const key = `${row.player_id || playerId}|${row.prop_type || ""}|${fmtLine(row.line)}`;
      const side = String(row.side || "").trim().toUpperCase();
      const existing = acc.get(key) || {
        player_id: row.player_id || playerId,
        prop_type: row.prop_type,
        line: row.line,
        best_over_price: null,
        best_under_price: null,
        over_price_range: null,
        under_price_range: null,
        over_book_count: null,
        under_book_count: null,
        over_filtered_out: false,
        under_filtered_out: false,
        over_playable_count: 0,
        under_playable_count: 0,
        coverage_quality_label: row.coverage_quality_label,
        timing_signal: row.timing_signal,
        regime_context_label: row.regime_context_label,
      };

      if (!existing.coverage_quality_label && row.coverage_quality_label) {
        existing.coverage_quality_label = row.coverage_quality_label;
      }
      if (!existing.timing_signal && row.timing_signal) {
        existing.timing_signal = row.timing_signal;
      }
      if (!existing.regime_context_label && row.regime_context_label) {
        existing.regime_context_label = row.regime_context_label;
      }

      if (side === "OVER") {
        const summary = mergeSideSummary(
          {
            best: existing.best_over_price,
            range: existing.over_price_range,
            filteredOut: existing.over_filtered_out,
            playableCount: existing.over_playable_count,
            rawCount: existing.over_raw_count,
          },
          playableSummaryFromRow(row)
        );
        existing.best_over_price = summary.best;
        existing.over_price_range = summary.range;
        existing.over_filtered_out = summary.filteredOut;
        existing.over_playable_count = summary.playableCount;
        existing.over_raw_count = summary.rawCount;
        existing.over_book_count = Math.max(
          Number(existing.over_book_count) || 0,
          Number(row.book_count) || 0
        );
      } else if (side === "UNDER") {
        const summary = mergeSideSummary(
          {
            best: existing.best_under_price,
            range: existing.under_price_range,
            filteredOut: existing.under_filtered_out,
            playableCount: existing.under_playable_count,
            rawCount: existing.under_raw_count,
          },
          playableSummaryFromRow(row)
        );
        existing.best_under_price = summary.best;
        existing.under_price_range = summary.range;
        existing.under_filtered_out = summary.filteredOut;
        existing.under_playable_count = summary.playableCount;
        existing.under_raw_count = summary.rawCount;
        existing.under_book_count = Math.max(
          Number(existing.under_book_count) || 0,
          Number(row.book_count) || 0
        );
      }

      acc.set(key, existing);
      return acc;
    }, new Map()).values()
  ).sort((a, b) => {
    const propCmp = getPropDisplayLabel(a.prop_type).localeCompare(getPropDisplayLabel(b.prop_type));
    if (propCmp !== 0) return propCmp;
    return Number(a.line || 0) - Number(b.line || 0);
  });

  const fmtMarketRange = (row) => {
    const over = fmtRange(row.over_price_range);
    const under = fmtRange(row.under_price_range);
    if (over && under) return `O ${over} · U ${under}`;
    if (over) return `O ${over}`;
    if (under) return `U ${under}`;
    return "—";
  };

  const renderStatGroup = (title, stats) => {
    if (!stats || Object.keys(stats).length === 0) return null;

    return (
      <div className="mb-4">
        <h4 className="font-semibold text-slate-800 mb-2">{title}</h4>
        <div className="space-y-1 text-sm font-mono text-slate-800 bg-slate-50 border border-slate-200 rounded px-4 py-2">
          {Object.entries(stats).map(([key, value]) => {
            const label = key
              .replace(/([a-z])([A-Z])/g, "$1 $2")
              .replace(/\b\w/g, (char) => char.toUpperCase());

            return (
              <div key={key} className="flex justify-between">
                <span className="text-slate-600">{label}:</span>
                <span className="text-right">{String(value)}</span>
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="min-h-screen pp-page p-6 max-w-5xl mx-auto">
      <div className="flex justify-between items-center mb-4">
        <div>
          {displayPlayerName ? (
            <div className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500 mb-1">
              {displayPlayerName}
              {displayTeam ? ` · ${displayTeam}` : ""}
            </div>
          ) : null}
          <h1 className="text-2xl font-bold text-slate-900">
            Player Profile: {playerId}
          </h1>
          <div className="text-sm text-slate-600 mt-1">
            Data freshness:{" "}
            {freshnessLabel ? `last prop date ${freshnessLabel}` : "no recent prop history"}
            {freshnessSource ? ` · source: ${freshnessSource}` : ""}
          </div>
        </div>
        <Link to="/players" className="text-slate-700 hover:underline text-sm">
          ← Back to Player List
        </Link>
      </div>
      {profileSport === "mlb" ? (
        <section className="mb-6">
          <div className="pp-card p-3">
            <div className="flex items-baseline justify-between gap-3 mb-2">
              <h2 className="text-xl font-semibold text-slate-900">Today&apos;s Market</h2>
              {todayMarketDate ? (
                <span className="text-xs text-slate-500">Slate {todayMarketDate}</span>
              ) : null}
            </div>
            {todayMarketLoading ? (
              <div className="text-sm text-slate-500">Loading today&apos;s market...</div>
            ) : todayMarketError ? (
              <div className="text-sm text-rose-600">{todayMarketError}</div>
            ) : todayMarketGroups.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs uppercase tracking-wide text-slate-500 border-b border-slate-200">
                      <th className="py-2 pr-3 font-medium">Prop</th>
                      <th className="py-2 pr-3 font-medium">Line</th>
                      <th className="py-2 pr-3 font-medium">Best Over</th>
                      <th className="py-2 pr-3 font-medium">Best Under</th>
                      <th className="py-2 pr-3 font-medium">Range</th>
                      <th className="py-2 pr-3 font-medium">Context</th>
                    </tr>
                  </thead>
                  <tbody>
                    {todayMarketGroups.map((row) => {
                      const contextParts = [
                        row.coverage_quality_label,
                        row.timing_signal,
                        row.regime_context_label,
                        (row.over_filtered_out && !row.over_playable_count) ||
                        (row.under_filtered_out && !row.under_playable_count)
                          ? "Thin market"
                          : null,
                      ].filter(Boolean);
                      return (
                        <tr
                          key={`${row.player_id}-${row.prop_type}-${fmtLine(row.line)}`}
                          className="border-b border-slate-100 last:border-0"
                        >
                          <td className="py-2 pr-3 text-slate-800 font-medium">
                            {getPropDisplayLabel(row.prop_type)}
                          </td>
                          <td className="py-2 pr-3 text-slate-700">{fmtLine(row.line)}</td>
                          <td className="py-2 pr-3 text-slate-700">
                            {fmtMarketPrice(row.best_over_price)}
                            {row.best_over_price !== null && row.over_book_count ? (
                              <span className="ml-1 text-xs text-slate-400">({row.over_book_count})</span>
                            ) : null}
                          </td>
                          <td className="py-2 pr-3 text-slate-700">
                            {fmtMarketPrice(row.best_under_price)}
                            {row.best_under_price !== null && row.under_book_count ? (
                              <span className="ml-1 text-xs text-slate-400">({row.under_book_count})</span>
                            ) : null}
                          </td>
                          <td className="py-2 pr-3 text-slate-700">
                            {fmtMarketRange(row)}
                          </td>
                          <td className="py-2 pr-3 text-slate-600">
                            {contextParts.length ? contextParts.join(" · ") : "Context pending"}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-sm text-slate-600">
                No current market lines available for today&apos;s slate.
              </p>
            )}
          </div>
        </section>
      ) : null}
      <section className="mb-6">
        <div className="pp-card p-3">
          <h2 className="text-xl font-semibold mb-2 text-slate-900">Current Streaks</h2>
          {profileData.streaks?.length > 0 ? (
            <ul className="space-y-2">
              {profileData.streaks.map((s, i) => (
                <li key={i} className="pp-chip p-2">
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
        <div className="pp-card p-3">
          <h2 className="text-xl font-semibold mb-2 text-slate-900">Recent Props</h2>
          {profileData.recent_props?.length > 0 ? (
            <ul className="space-y-1">
              {profileData.recent_props.map((prop, i) => (
                <li key={i} className="pp-chip p-2 text-sm">
                  <div>
                    <span className="font-semibold text-slate-800">
                      {prop.game_date}
                    </span>
                    : {getPropDisplayLabel(prop.prop_type)} → {prop.outcome || "pending"}
                  </div>
                  <div className="text-sm text-slate-600">
                    {String(prop.over_under || "").toLowerCase() || "—"}{" "}
                    {prop.prop_value ?? "—"}
                    {"\u00A0\u00A0"}
                    {prop.confidence_score && (
                      <span className="ml-2 text-slate-700">
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
        <div className="pp-card p-3">
          <h2 className="text-xl font-semibold mb-2 text-slate-900">Stat-Derived Props</h2>
          {profileData.stat_derived?.length > 0 ? (
            <ul className="space-y-2">
              {profileData.stat_derived.map((prop, i) => (
                <li key={i} className="pp-chip p-2">
                  <div>
                    <span className="font-semibold">{prop.game_date}</span>:{" "}
                    {getPropDisplayLabel(prop.prop_type)} → {prop.result}
                    {prop.outcome && (
                      <span className="ml-2 text-sm text-slate-600">
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
        <div className="pp-card p-3">
          <h2 className="text-xl font-semibold mb-2 text-slate-900">Training Summary</h2>
          {profileData.training_summary?.length > 0 ? (
            <ul className="space-y-1">
              {profileData.training_summary.map((entry, i) => (
                <li key={i} className="pp-chip px-2 py-1 text-sm">
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
                <div className="flex flex-col flex-grow p-4 pp-card">
                  <h3 className="text-xl font-semibold text-slate-900 mb-2">
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
                <div className="flex flex-col flex-grow p-4 pp-card">
                  <h3 className="text-xl font-semibold text-slate-900 mb-2">
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
