import {
  checkPredictionContract,
  createPredictionRecord,
} from "../../config/predictionSchema.js";

function asProbability(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function formatNhlPredictionLine(record, fallback = "Prediction unavailable") {
  if (!record?.lineLabel) return fallback;
  return record.lineLabel;
}

export function adaptNhlBoardPrediction({
  propType,
  row,
  bestLine,
  market,
  modelUpdatedAt,
  marketUpdatedAt,
  modelSource,
  marketSource,
} = {}) {
  const marketType = String(propType || "").trim().toLowerCase();
  const playerName = row?.player_name || row?.player_id || null;
  const side = "over";
  const line = bestLine?.line ?? null;
  const lineLabel =
    playerName && line != null
      ? `${playerName} • Over ${line}`
      : "Prediction unavailable";

  const record = createPredictionRecord({
    sport: "nhl",
    marketType: marketType || null,
    propType: marketType || null,
    line,
    side,
    lineLabel,
    playerId: row?.player_id,
    playerName: row?.player_name,
    team: row?.team_abbr || row?.team || null,
    gameId: row?.game_id,
    gameDate: row?.game_date,
    modelProbability: asProbability(bestLine?.p ?? bestLine?.probability),
    marketProbability: asProbability(market?.marketProbability),
    modelSource: modelSource || "NHL model",
    marketSource: marketSource || "OddsAPI market median",
    modelUpdatedAt: modelUpdatedAt || null,
    marketUpdatedAt: marketUpdatedAt || null,
    features: row || {},
    raw: { row, bestLine, market },
  });

  const contract = checkPredictionContract(record, { label: "nhlAdapter" });
  return { ...record, contract };
}
