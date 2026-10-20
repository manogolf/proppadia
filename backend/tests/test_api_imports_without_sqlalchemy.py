"""The hosted API must start without sqlalchemy, which Render does not install.

On 2026-10-19 a module-level sqlalchemy import in backend/shared/db/mlb_research.py,
reached through the public game-predictions durable store, stopped the Render
build from starting; the previous build kept serving.
"""
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class HostedApiImportTests(unittest.TestCase):
    def test_api_server_imports_without_sqlalchemy(self):
        code = (
            "import sys; sys.modules['sqlalchemy'] = None; "
            "import backend.app.api_server"
        )
        env = {"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"}
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env,
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr[-600:])


if __name__ == "__main__":
    unittest.main()
