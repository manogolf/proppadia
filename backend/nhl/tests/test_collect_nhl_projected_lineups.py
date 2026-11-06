"""NHL.com projected lineups trial: parse the article layout and match names by team."""
import unittest

import pandas as pd

from backend.nhl.scripts import collect_nhl_projected_lineups as lineups

PAGE = """<html><head><script type="application/ld+json">
{"@type":"NewsArticle","datePublished":"2026-11-06T15:00:00Z","dateModified":"2026-11-06T19:54:30Z"}
</script></head><body><main>
<h1>Projected lineups, starting goalies for today</h1>
<p>BLUE JACKETS (7-7-0) at SABRES (7-5-3)</p>
<p>7 p.m. ET; CBJHN</p>
<p>Blue Jackets projected lineup</p>
<p>Matthew Knies -- Adam Fantilli -- Kent Johnson</p>
<p>Valeri Nichushkin -- Luca Del Bel Belluz -- Conor Garland</p>
<p>Zach Werenski -- Damon Severson</p>
<p>Jet Greaves</p>
<p>Cam Talbot</p>
<p><strong>Scratched:</strong>&nbsp;Erik Gudbranson, Jack Williams</p>
<p><strong>Injured:</strong>&nbsp;Isac Lundestrom (lower body),<br>Jake Smith (hip)</p>
<p>Sabres projected lineup</p>
<p>Zach Benson -- Peyton Krebs -- Tage Thompson</p>
<p>Rasmus Dahlin -- Mattias Samuelsson</p>
<p>Ukko-Pekka Luukkonen</p>
<p>Colten Ellis</p>
<p><strong>Scratched:</strong> None</p>
<p><strong>Injured:</strong> None</p>
<p>Status report</p>
<p>Greaves will start for Columbus after Talbot made 27 saves.</p>
<p>SATURDAY</p>
<p>PANTHERS (9-5-2) at CAPITALS (8-6-0)</p>
<p>Panthers projected lineup</p>
<p>Brady Tkachuk -- Sam Bennett -- Matthew Tkachuk</p>
</main></body></html>"""


def names():
    rows = [("Matthew Knies", "Knies", "CBJ"), ("Adam Fantilli", "Fantilli", "CBJ"),
            ("Kent Johnson", "Johnson", "CBJ"), ("Valeri Nichushkin", "Nichushkin", "CBJ"),
            ("Luca Del Bel Belluz", "Del Bel Belluz", "CBJ"), ("Conor Garland", "Garland", "CBJ"),
            ("Zach Werenski", "Werenski", "CBJ"), ("Damon Severson", "Severson", "CBJ"),
            ("Jet Greaves", "Greaves", "CBJ"), ("Cam Talbot", "Talbot", "CBJ"),
            ("Zach Benson", "Benson", "BUF"), ("Peyton Krebs", "Krebs", "BUF"),
            ("Tage Thompson", "Thompson", "BUF"), ("Rasmus Dahlin", "Dahlin", "BUF"),
            ("Mattias Samuelsson", "Samuelsson", "BUF"), ("Ukko-Pekka Luukkonen", "Luukkonen", "BUF"),
            ("Colten Ellis", "Ellis", "BUF"), ("Jacob Trouba", "Trouba", "BUF"),
            ("Jack Quinn", "Quinn", "BUF"), ("Ryan Quinn", "Quinn", "BUF")]
    return pd.DataFrame([{"player_id": i + 1, "full_name": f, "last_name": l, "team_code": t, "game_id": 1}
                         for i, (f, l, t) in enumerate(rows)])


class ParseTests(unittest.TestCase):
    def setUp(self):
        self.rows = pd.DataFrame(lineups.parse(PAGE))

    def test_lines_pairs_and_goalies(self):
        cbj = self.rows[self.rows.team_code.eq("CBJ")]
        self.assertEqual(cbj[cbj.unit.eq("F1")].name.tolist(), ["Matthew Knies", "Adam Fantilli", "Kent Johnson"])
        self.assertEqual(cbj[cbj.unit.eq("D1")].name.tolist(), ["Zach Werenski", "Damon Severson"])
        self.assertEqual(cbj[cbj.unit.eq("G1")].name.tolist(), ["Jet Greaves"])
        self.assertEqual(cbj[cbj.unit.eq("G2")].name.tolist(), ["Cam Talbot"])

    def test_split_labels_wrapped_lists_and_none(self):
        cbj = self.rows[self.rows.team_code.eq("CBJ")]
        self.assertEqual(cbj[cbj.unit.eq("SCRATCHED")].name.tolist(), ["Erik Gudbranson", "Jack Williams"])
        injured = cbj[cbj.unit.eq("INJURED")]
        self.assertEqual(injured.name.tolist(), ["Isac Lundestrom", "Jake Smith"])
        self.assertEqual(injured.note.tolist(), ["lower body", "hip"])
        buf = self.rows[self.rows.team_code.eq("BUF")]
        self.assertFalse(buf.unit.isin(["SCRATCHED", "INJURED"]).any())

    def test_later_days_are_marked_and_status_report_ends_a_team(self):
        self.assertEqual(set(self.rows[self.rows.team_code.eq("FLA")].day), {"SATURDAY"})
        self.assertEqual(set(self.rows[self.rows.team_code.isin(["CBJ", "BUF"])].day), {"TODAY"})
        self.assertFalse(self.rows.name.str.contains("Greaves will").any())

    def test_article_times(self):
        self.assertEqual(lineups.article_times(PAGE)["date_modified"], "2026-11-06T19:54:30Z")


class MatchTests(unittest.TestCase):
    def test_full_last_prefix_and_ambiguous(self):
        rows = pd.DataFrame([{"team_code": "BUF", "name": n} for n in
                             ("Tage Thompson", "T. Thompson", "Jacob Troub", "J. Quinn", "Kent Johnson")])
        matched = lineups.match_names(rows, names())
        self.assertEqual(matched.match.tolist(),
                         ["FULL_NAME", "LAST_NAME", "LAST_NAME_PREFIX", "UNMATCHED", "UNMATCHED"])
        self.assertEqual(int(matched.player_id[2]), 18)   # Trouba, by unique prefix on his team
        # Two Quinns on Buffalo, and Kent Johnson is not a Sabre: never guessed.


if __name__ == "__main__":
    unittest.main()
