import unittest
from unittest.mock import patch

from backend.scripts.strict_data_gate import enforce_strict_data_gate


class TestSharedStrictDataGate(unittest.TestCase):
    def test_strict_data_gate(self):
        with patch("builtins.print"):
            self.assertEqual(
                enforce_strict_data_gate(require_data=False, allow_sparse=False, warns=["x"]),
                0,
            )
            self.assertEqual(
                enforce_strict_data_gate(require_data=True, allow_sparse=True, warns=["x"]),
                0,
            )
            self.assertEqual(
                enforce_strict_data_gate(require_data=True, allow_sparse=False, warns=["x"]),
                1,
            )


if __name__ == "__main__":
    unittest.main()
