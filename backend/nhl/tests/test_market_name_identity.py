"""Book names bind to one slate player; shared names price nobody unless reviewed."""
import unittest

from backend.nhl import market_name_identity as identity
from backend.nhl.scripts import build_points_with_market as points_builder

SLATE = [(8480012, "Elias Pettersson"), (8483678, "Elias Pettersson"),
         (8475314, "Anders Lee"), (8481732, "Andre Lee"), (8478402, "Connor McDavid")]


class SlateNamesTests(unittest.TestCase):
    def test_reviewed_binding_gives_the_shared_full_name_to_one_player(self):
        names = identity.SlateNames(SLATE, {"elias pettersson": 8480012})
        self.assertEqual(names.player_for("Elias Pettersson"), 8480012)
        self.assertEqual(names.keys_for(8480012, "Elias Pettersson"), ["elias pettersson"])
        self.assertEqual(names.keys_for(8483678, "Elias Pettersson"), [])
        # The abbreviated form stays shared: no guess.
        self.assertIsNone(names.player_for("E. Pettersson"))

    def test_shared_name_without_review_prices_nobody(self):
        names = identity.SlateNames(SLATE, {})
        self.assertIsNone(names.player_for("Elias Pettersson"))
        self.assertEqual(names.report()["shared_keys"]["elias pettersson"], [8480012, 8483678])

    def test_initial_last_is_used_only_when_one_player_owns_it(self):
        names = identity.SlateNames(SLATE, {})
        self.assertEqual(names.player_for("Anders Lee"), 8475314)
        self.assertEqual(names.keys_for(8481732, "Andre Lee"), ["andre lee"])
        self.assertIsNone(names.player_for("A. Lee"))
        self.assertEqual(names.player_for("C. McDavid"), 8478402)
        self.assertEqual(names.keys_for(8478402, "Connor McDavid"), ["connor mcdavid", "c mcdavid"])

    def test_committed_binding_file_loads(self):
        self.assertEqual(identity.load_bindings()["elias pettersson"], 8480012)


class PointsOddsTests(unittest.TestCase):
    def test_points_quotes_go_to_the_owning_player_only(self):
        outcome = lambda name, side, price: {"name": side, "description": name, "point": 0.5, "price": price}
        raw = [{"key": "book", "markets": [{"key": "player_points", "outcomes": [
            outcome("Elias Pettersson", "Over", -150), outcome("Elias Pettersson", "Under", 115),
            outcome("Anders Lee", "Over", 140), outcome("Anders Lee", "Under", -180)]}]}]
        names = identity.SlateNames(SLATE, {"elias pettersson": 8480012})
        med = points_builder.parse_points_odds(raw, names)
        self.assertEqual(sorted(med["player_id"]), [8475314, 8480012])
        unreviewed = points_builder.parse_points_odds(raw, identity.SlateNames(SLATE, {}))
        self.assertEqual(list(unreviewed["player_id"]), [8475314])


if __name__ == "__main__":
    unittest.main()
