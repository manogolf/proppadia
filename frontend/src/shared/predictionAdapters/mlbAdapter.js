import {
  checkPredictionContract,
  createPredictionRecord,
} from "../../config/predictionSchema.js";

export function formatMlbPredictionLine(record, fallback = "Prediction unavailable") {
  if (!record?.propType) return fallback;
  const side = record.side || "";
  const line = record.line != null ? record.line : "";
  return `${record.propType} • ${side} ${line}`.trim();
}

export function adaptMlbPrediction(raw) {
  if (!raw || typeof raw !== "object") return null;

  const features = raw.features && typeof raw.features === "object" ? raw.features : {};
  const record = createPredictionRecord({
    sport: "mlb",
    marketType: features.prop_type || null,
    propType: features.prop_type || raw.propType || null,
    line: features.prop_value,
    side: features.over_under || null,
    lineLabel: formatMlbPredictionLine(
      {
        propType: features.prop_type || raw.propType || null,
        side: features.over_under || null,
        line: features.prop_value,
      },
      "Prediction unavailable"
    ),
    playerId: features.player_id,
    playerName: features.player_name,
    team: features.team,
    gameId: features.game_id,
    gameDate: features.game_date,
    modelProbability: raw.probability,
    marketProbability: raw.marketProbability,
    recommendation: raw.recommendation,
    modelSource: raw.model || "MLB model",
    marketSource: raw.marketSource,
    modelUpdatedAt: raw.updatedAt,
    marketUpdatedAt: raw.marketUpdatedAt,
    features,
    raw,
  });

  const contract = checkPredictionContract(record, { label: "mlbAdapter" });
  return { ...record, contract };
}
