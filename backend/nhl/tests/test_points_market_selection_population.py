import unittest

import pandas as pd

from backend.nhl.scripts.build_nhl_points_market_selection_population import classify_quote_rows


class QuoteStatusTests(unittest.TestCase):
    def test_quote_status_uses_retained_prices_and_keeps_partial_separate(self):
        rows = pd.DataFrame({
            "price_over": [-110.0, None, None, None],
            "price_under": [-105.0, -115.0, None, None],
            "source_quote_count_over": [2, 0, 0, 0],
            "source_quote_count_under": [3, 1, 0, 0],
        })
        self.assertEqual(classify_quote_rows(rows).tolist(), [
            "EXACT_TWO_SIDED", "ONE_SIDED_PARTIAL", "ATTACHMENT_NO_QUOTE", "ATTACHMENT_NO_QUOTE"
        ])

    def test_identity_alone_cannot_assign_quote_status(self):
        rows = pd.DataFrame({
            "player_id": [8471214], "game_id": [2026020101],
            "price_over": [None], "price_under": [None],
            "source_quote_count_over": [0], "source_quote_count_under": [0],
        })
        self.assertEqual(classify_quote_rows(rows).iloc[0], "ATTACHMENT_NO_QUOTE")


if __name__ == "__main__":
    unittest.main()
