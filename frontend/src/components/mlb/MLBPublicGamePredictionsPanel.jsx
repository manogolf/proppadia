import React, { useEffect, useState } from "react";
import { getBaseURL } from "../../shared/getBaseURL.js";

const pct = (value) => (Number.isFinite(Number(value)) ? `${(Number(value) * 100).toFixed(1)}%` : "—");
const runs = (value) => (Number.isFinite(Number(value)) ? Number(value).toFixed(2) : "—");
const stamp = (value) => value ? new Date(value).toLocaleString() : "—";

export default function MLBPublicGamePredictionsPanel({ gameDate }) {
  const [state, setState] = useState({ loading: true, enabled: false, rows: [], error: "" });

  useEffect(() => {
    let mounted = true;
    async function load() {
      try {
        const qs = new URLSearchParams({ game_date: gameDate });
        const response = await fetch(`${getBaseURL()}/api/mlb/game-predictions?${qs}`, { credentials: "include" });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload?.detail || "Game predictions unavailable");
        if (mounted) setState({ loading: false, enabled: Boolean(payload.enabled), rows: payload.rows || [], error: "" });
      } catch (error) {
        if (mounted) setState({ loading: false, enabled: false, rows: [], error: error?.message || "Unavailable" });
      }
    }
    load();
    return () => { mounted = false; };
  }, [gameDate]);

  // Feature-flag off preserves the existing public workspace exactly.
  if (state.loading || !state.enabled) return null;

  return (
    <section className="pp-card p-4 mb-4" data-testid="mlb-public-game-predictions">
      <div className="flex flex-wrap items-start justify-between gap-2 mb-3">
        <div>
          <h3 className="text-sm font-semibold text-slate-900">Game predictions</h3>
          <p className="text-xs text-slate-500">Confidence reflects model separation from 50%, not expected betting value.</p>
        </div>
        <span className="text-xs rounded border border-slate-300 bg-slate-50 px-2 py-1 text-slate-600">Pythagorean/Log5 v1</span>
      </div>
      {state.error ? <div className="text-sm text-slate-600">Predictions unavailable: {state.error}</div> : null}
      {!state.error && !state.rows.length ? <div className="text-sm text-slate-600">No eligible pregame predictions are available.</div> : null}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        {state.rows.map((row) => (
          <article key={`${row.game_date}-${row.game_id}`} className="rounded border border-slate-200 bg-white p-3">
            {row.admission_status !== "ADMITTED_SHADOW" ? (
              <div className="text-sm text-slate-600">{row.away_team || "Away"} @ {row.home_team || "Home"}: unavailable ({row.failure_reason})</div>
            ) : (
              <>
                <div className="font-medium text-slate-900">{row.away_team} @ {row.home_team}</div>
                <div className="mt-1 text-xs text-slate-500">{row.game_date} · {stamp(row.scheduled_start_utc)}</div>
                <div className="mt-1 text-sm text-slate-700">Prediction: {row.predicted_winner}</div>
                <div className="mt-1 text-sm text-slate-700">Away {pct(row.away_win_probability)} · Home {pct(row.home_win_probability)}</div>
                {row.score_prediction_status === "UNAVAILABLE_NO_QUALIFIED_SCORE_MODEL" ? (
                  <div className="mt-1 text-xs font-medium text-slate-600">UNAVAILABLE — NO QUALIFIED SCORE MODEL</div>
                ) : (
                  <div className="mt-1 text-xs text-slate-600">Expected score: {row.away_team} {runs(row.expected_away_runs)}, {row.home_team} {runs(row.expected_home_runs)} · Total {runs(row.expected_total_runs)}</div>
                )}
                <div className="mt-2 flex flex-wrap gap-2">
                  <span className="inline-flex rounded border border-slate-300 bg-slate-50 px-2 py-0.5 text-xs text-slate-700">{row.confidence_band}</span>
                  <span className="inline-flex rounded border border-slate-300 bg-slate-50 px-2 py-0.5 text-xs text-slate-700">{row.grading_status}</span>
                </div>
                {row.grading_status === "GRADED" ? <div className="mt-2 text-xs text-slate-700">Official: {row.away_team} {row.official_away_runs}, {row.home_team} {row.official_home_runs} · {row.official_winner} · {row.prediction_correct ? "Correct" : "Incorrect"}</div> : null}
                <div className="mt-2 text-xs text-slate-500">Data: {row.data_quality_status}</div>
                <div className="text-xs text-slate-500">Model: Pythagorean/Log5 · {row.model_version}</div>
                <div className="text-xs text-slate-500">Predicted: {stamp(row.prediction_timestamp_utc)}</div>
              </>
            )}
          </article>
        ))}
      </div>
      <details className="mt-3 text-xs text-slate-600">
        <summary className="cursor-pointer font-medium">Historical evaluation and limitations</summary>
        <div className="mt-2 space-y-1">
          <div>2025 frozen validation · Accuracy 55.57% · Brier 0.24424 · Log loss 0.68136</div>
          <div>Early 2026 · Accuracy 53.64% · Brier 0.25040 · Log loss 0.69401</div>
          <div>Late-2026 holdout · Accuracy 59.41% · Brier 0.23717 · Log loss 0.66693</div>
          <div>Early-season uncertainty remains material; probability confidence is not a guarantee of correctness.</div>
          <div>Historical market value was not tested.</div>
        </div>
      </details>
      <div className="mt-3 text-xs font-medium text-slate-700">MODEL PREDICTION — BETTING EDGE NOT DEMONSTRATED</div>
    </section>
  );
}
