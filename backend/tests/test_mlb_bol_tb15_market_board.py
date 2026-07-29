import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


class NeutralBolTb15BoardTest(unittest.TestCase):
  def test_includes_every_valid_identity_bound_two_sided_row(self):
    with tempfile.TemporaryDirectory() as raw:
     tmp_path=Path(raw)
     wide = tmp_path / "wide.csv"
     odds = tmp_path / "odds.json"
     out = tmp_path / "bol"
     pd.DataFrame([{
        "player_id": 10, "player_name": "Exact Player", "team": "ATH", "opponent": "BOS",
        "game_id": 20, "prop_type": "total_bases", "p_over_1_5": .4,
        "away_team_code": "BOS", "home_team_code": "OAK",
     }]).to_csv(wide, index=False)
     odds.write_text(json.dumps({
        "captured_at_utc": "2026-07-29T12:00:00Z",
        "events": [{"home_team": "Athletics", "away_team": "Boston Red Sox",
          "bookmakers": [{"key": "betonlineag", "markets": [{
            "key": "batter_total_bases", "last_update": "2026-07-29T11:59:00Z",
            "outcomes": [
              {"name": "Over", "description": "Exact Player", "price": 125, "point": 1.5},
              {"name": "Under", "description": "Exact Player", "price": -165, "point": 1.5}
            ]}]}]}]
     }))
     subprocess.run([
        sys.executable, "-m", "backend.mlb.scripts.build_mlb_bol_tb15_market_board",
        "--date", "2026-07-29", "--wide-csv", str(wide), "--odds-json", str(odds),
        "--output-root", str(out), "--run-tag", "test_run",
     ], check=True)
     got = pd.read_csv(out/"2026-07-29/bol_tb15_market_board_2026-07-29.csv")
     self.assertEqual(len(got),1)
     self.assertEqual(got.iloc[0].game,"BOS @ ATH")
     self.assertEqual(got.iloc[0].over_odds,125)
     self.assertEqual(got.iloc[0].under_odds,-165)
     self.assertNotIn("ubo5","|".join(got.columns).lower())


if __name__ == "__main__":
    unittest.main()
