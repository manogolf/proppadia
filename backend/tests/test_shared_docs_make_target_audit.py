import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from backend.scripts import check_docs_make_targets as audit


class TestSharedDocsMakeTargetAudit(unittest.TestCase):
    def test_pass_when_doc_targets_exist(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            makefile = root / "Makefile"
            docs = root / "docs"
            docs.mkdir(parents=True, exist_ok=True)

            makefile.write_text("foo:\nbar:\n", encoding="utf-8")
            (docs / "x.md").write_text("Run `make foo` then make bar BAZ=1\n", encoding="utf-8")

            out = StringIO()
            with redirect_stdout(out):
                rc = audit.main(["--makefile", str(makefile), "--docs-root", str(docs), "--json"])
            self.assertEqual(rc, 0)
            payload = json.loads(out.getvalue())
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["summary"]["unknown_refs"], 0)

    def test_fail_when_unknown_target_present(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            makefile = root / "Makefile"
            docs = root / "docs"
            docs.mkdir(parents=True, exist_ok=True)

            makefile.write_text("foo:\n", encoding="utf-8")
            (docs / "x.md").write_text("Use `make missing_target` now\n", encoding="utf-8")

            out = StringIO()
            with redirect_stdout(out):
                rc = audit.main(["--makefile", str(makefile), "--docs-root", str(docs), "--json"])
            self.assertEqual(rc, 1)
            payload = json.loads(out.getvalue())
            self.assertFalse(payload["ok"])
            self.assertIn("missing_target", payload["unknown"])


if __name__ == "__main__":
    unittest.main()
