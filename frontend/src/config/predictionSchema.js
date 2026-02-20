export const PREDICTION_SCHEMA_VERSION = "prediction.v1";
export const PREDICTION_SPORTS = Object.freeze(["mlb", "nhl"]);
export const PREDICTION_SIDES = Object.freeze(["over", "under"]);

function toText(value) {
  if (value == null) return null;
  const text = String(value).trim();
  return text ? text : null;
}

function toFiniteNumber(value) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toProbability(value) {
  const parsed = toFiniteNumber(value);
  if (parsed == null) return null;
  return Math.max(0, Math.min(1, parsed));
}

function toIsoTimestamp(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function normalizeSide(value) {
  const text = toText(value);
  if (!text) return null;
  const normalized = text.toLowerCase();
  return PREDICTION_SIDES.includes(normalized) ? normalized : null;
}

export function createPredictionRecord(input = {}) {
  return {
    schemaVersion: PREDICTION_SCHEMA_VERSION,
    sport: toText(input.sport)?.toLowerCase() || null,
    marketType: toText(input.marketType)?.toLowerCase() || null,
    propType: toText(input.propType)?.toLowerCase() || null,
    line: toFiniteNumber(input.line),
    side: normalizeSide(input.side),
    lineLabel: toText(input.lineLabel),
    playerId: toText(input.playerId),
    playerName: toText(input.playerName),
    team: toText(input.team),
    gameId: toText(input.gameId),
    gameDate: toText(input.gameDate),
    modelProbability: toProbability(input.modelProbability),
    marketProbability: toProbability(input.marketProbability),
    recommendation: toText(input.recommendation)?.toLowerCase() || null,
    modelSource: toText(input.modelSource),
    marketSource: toText(input.marketSource),
    modelUpdatedAt: toIsoTimestamp(input.modelUpdatedAt),
    marketUpdatedAt: toIsoTimestamp(input.marketUpdatedAt),
    confidenceLabel: toText(input.confidenceLabel),
    features:
      input.features && typeof input.features === "object" ? input.features : {},
    raw: input.raw ?? null,
  };
}

export function validatePredictionRecord(record) {
  const errors = [];
  if (!record || typeof record !== "object") {
    return { ok: false, errors: ["record_not_object"] };
  }
  if (record.schemaVersion !== PREDICTION_SCHEMA_VERSION) {
    errors.push("schema_version_mismatch");
  }
  if (!record.sport || !PREDICTION_SPORTS.includes(record.sport)) {
    errors.push("invalid_sport");
  }
  if (!record.propType) {
    errors.push("missing_prop_type");
  }
  if (record.side && !PREDICTION_SIDES.includes(record.side)) {
    errors.push("invalid_side");
  }
  return { ok: errors.length === 0, errors };
}

export function checkPredictionContract(record, { label = "prediction" } = {}) {
  const result = validatePredictionRecord(record);
  if (!result.ok && typeof console !== "undefined") {
    console.warn(
      `[prediction-schema] ${label} failed contract checks: ${result.errors.join(", ")}`
    );
  }
  return result;
}
