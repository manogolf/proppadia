export function normalizeFeatureKeys(features) {
  return {
    rolling_result_avg_7: features.rolling_result_avg_7 ?? 0,
    hit_streak: features.hit_streak ?? 0,
    win_streak: features.win_streak ?? 0,
    is_home: features.is_home ?? 0,
    opponent_avg_win_rate:
      features.opponent_avg_win_rate ?? features.opponent_win_rate ?? 0,
    player_id: features.player_id ?? "unknown-player",
  };
}
