function formatTimestamp(ts) {
  if (!ts) return "-";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

function hasNumericProbability(value) {
  return value != null && Number.isFinite(Number(value));
}

export function buildMarketContext({
  marketProbability,
  marketSource,
  marketUpdatedAt,
  modelUpdatedAt,
  marketSourceFallback = "OddsAPI market",
  modelSourceFallback = "Model output",
}) {
  const hasMarket = hasNumericProbability(marketProbability);
  const sourceKind = hasMarket ? "market" : "model";
  const sourceLabel = hasMarket ? (marketSource || marketSourceFallback) : modelSourceFallback;
  const updatedLabel = formatTimestamp(hasMarket ? (marketUpdatedAt || modelUpdatedAt) : modelUpdatedAt);
  return { hasMarket, sourceKind, sourceLabel, updatedLabel };
}
