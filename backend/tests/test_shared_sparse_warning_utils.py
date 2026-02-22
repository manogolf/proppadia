import unittest
from dataclasses import dataclass

from backend.shared.scripts.sparse_warning_utils import find_sparse_warnings


@dataclass
class _Check:
    name: str
    detail: str


class TestSharedSparseWarningUtils(unittest.TestCase):
    def test_sparse_warning_rules(self):
        checks = [
            _Check(name="players_lookup", detail='{"found": false}'),
            _Check(name="players_search", detail='{"count": 0}'),
            _Check(name="player_profile", detail='{"player_name": null}'),
        ]
        warns = find_sparse_warnings(
            checks,
            [
                ("players_lookup", "missing", '"found": true', "lookup sparse"),
                ("players_search", "contains", '"count": 0', "search sparse"),
                ("player_profile", "contains", '"player_name": null', "profile sparse"),
            ],
        )
        self.assertEqual(warns, ["lookup sparse", "search sparse", "profile sparse"])


if __name__ == "__main__":
    unittest.main()
