function formatTimestamp(ts) {
  if (!ts) return "-";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

export function buildMarketContext({
  marketProbability,
  marketSource,
  marketUpdatedAt,
  modelUpdatedAt,
  marketSourceFallback = "OddsAPI market",
  modelSourceFallback = "Model output",
}) {
  const hasMarket = marketProbability != null && Number.isFinite(Number(marketProbability));
  const sourceLabel = hasMarket
    ? (marketSource || marketSourceFallback)
    : modelSourceFallback;
  const updatedLabel = formatTimestamp(hasMarket ? (marketUpdatedAt || modelUpdatedAt) : modelUpdatedAt);
  return { sourceLabel, updatedLabel };
}

