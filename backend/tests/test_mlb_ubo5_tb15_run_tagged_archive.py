from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class Ubo5RunTaggedArchiveTest(unittest.TestCase):
    def test_route_artifacts_receive_run_tagged_copies(self):
        with tempfile.TemporaryDirectory() as raw:
            temp = Path(raw)
            sources = {}
            for name in (
                "wide.csv", "slate.csv", "upload.csv", "odds.json",
                "feature.parquet", "route.csv", "health.json",
            ):
                path = temp / name
                path.write_text(f"{name}\n")
                sources[name] = path
            archive = temp / "archive"
            subprocess.run([
                sys.executable, "backend/mlb/scripts/archive_mlb_slate_artifacts.py",
                "--slate-date", "2026-07-26", "--odds-root", str(archive),
                "--pred-csv", str(sources["wide.csv"]),
                "--slate-csv", str(sources["slate.csv"]),
                "--book-upload-csv", str(sources["upload.csv"]),
                "--odds-snapshot-json", str(sources["odds.json"]),
                "--ubo5-feature-ledger", str(sources["feature.parquet"]),
                "--ubo5-route-ledger", str(sources["route.csv"]),
                "--ubo5-route-health", str(sources["health.json"]),
                "--run-tag", "local_daily_20260726T183000Z",
                "--strict",
            ], cwd=ROOT, check=True, capture_output=True, text=True)
            dated = archive / "2026-07-26"
            self.assertTrue((dated / "feature__local_daily_20260726T183000Z.parquet").is_file())
            self.assertTrue((dated / "route__local_daily_20260726T183000Z.csv").is_file())
            self.assertTrue((dated / "health__local_daily_20260726T183000Z.json").is_file())


if __name__ == "__main__":
    unittest.main()
