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

    def test_neutral_site_vs_header_and_day_header_ends_the_game(self):
        # 2026-11-11: the next day's Global Series game ("vs.", not "at") was filed under
        # Nashville at Anaheim; on 2026-11-12 it was the only game listed and nothing parsed.
        page = """<html><body><main>
<p>PREDATORS (9-4-5) at DUCKS (10-7-1)</p>
<p>Predators projected lineup</p>
<p>Filip Forsberg -- Ryan O'Reilly -- Steven Stamkos</p>
<p>Status report</p>
<p>THURSDAY</p>
<p>HURRICANES (14-3-2) vs. KRAKEN (9-6-1)</p>
<p>2026 NHL Global Series Finland</p>
<p>At Veikkaus Arena, Helsinki</p>
<p>Hurricanes projected lineup</p>
<p>Taylor Hall -- Mark Jankowski -- Seth Jarvis</p>
<p>BROKEN HEADER LINE</p>
</main></body></html>"""
        rows = pd.DataFrame(lineups.parse(page))
        car = rows[rows.team_code.eq("CAR")]
        self.assertEqual(car[["day", "away", "home", "unit"]].drop_duplicates().values.tolist(),
                         [["THURSDAY", "CAR", "SEA", "F1"]])
        self.assertEqual(set(rows[rows.team_code.eq("NSH")].day), {"TODAY"})
        today = pd.DataFrame(lineups.parse(page.replace("<p>THURSDAY</p>", "")))
        self.assertEqual(today[today.team_code.eq("CAR")].home.unique().tolist(), ["SEA"])

    def test_teams_after_an_unread_day_header_are_dropped_not_misfiled(self):
        page = """<html><body><main>
<p>PREDATORS (9-4-5) at DUCKS (10-7-1)</p>
<p>Predators projected lineup</p>
<p>Filip Forsberg -- Ryan O'Reilly -- Steven Stamkos</p>
<p>FRIDAY</p>
<p>HURRICANES in a header the parser cannot read</p>
<p>Hurricanes projected lineup</p>
<p>Taylor Hall -- Mark Jankowski -- Seth Jarvis</p>
</main></body></html>"""
        rows = pd.DataFrame(lineups.parse(page))
        self.assertEqual(rows.team_code.unique().tolist(), ["NSH"])
        # Same day: the status report closes the game (Jets at Stars filed under Buffalo at
        # Chicago on 2026-11-12 because its header read "AT").
        same_day = page.replace("<p>FRIDAY</p>", "<p>Status report</p>")
        self.assertEqual(pd.DataFrame(lineups.parse(same_day)).team_code.unique().tolist(), ["NSH"])
        upper = same_day.replace("HURRICANES in a header the parser cannot read",
                                 "HURRICANES (14-3-2) AT KRAKEN (9-6-1)")
        car = pd.DataFrame(lineups.parse(upper)).query("team_code == 'CAR'")
        self.assertEqual(car[["away", "home"]].drop_duplicates().values.tolist(), [["CAR", "SEA"]])

    def test_extra_skater_suspended_player_and_empty_injured_label(self):
        # 2026-11-13: Luke Schenn listed alone ahead of Vancouver's goalies was read as G1;
        # 2026-11-08: Larkin under "Suspended:" was read as a goalie; an empty "Injured:"
        # swallowed "Status report" so the page's trailing text stayed under the team.
        page = """<html><body><main>
<p>RANGERS (8-8-2) at CANUCKS (7-9-2)</p>
<p>Canucks projected lineup</p>
<p>Jake DeBrusk -- Elias Pettersson -- Brock Boeser</p>
<p>Zeev Buium -- Filip Hronek</p>
<p>Luke Schenn</p>
<p>Leevi Merilainen</p>
<p>Kevin Lankinen</p>
<p><strong>Scratched:</strong> None</p>
<p><strong>Suspended:</strong></p>
<p>Dylan Larkin</p>
<p><strong>Injured:</strong></p>
<p>Status report</p>
<p>Fantasy Hockey Rankings and Picks</p>
</main></body></html>"""
        van = pd.DataFrame(lineups.parse(page))
        self.assertEqual(van[van.unit.isin(["EXTRA", "G1", "G2", "SUSPENDED"])][["unit", "name"]].values.tolist(),
                         [["EXTRA", "Luke Schenn"], ["G1", "Leevi Merilainen"], ["G2", "Kevin Lankinen"],
                          ["SUSPENDED", "Dylan Larkin"]])
        self.assertFalse(van.name.str.contains("Status|Fantasy").any())

    def test_loose_separators_and_dashes(self):
        # 2026-11-13: "Lauzon--  Andersson" became an EXTRA goalie-slot name and an en dash
        # turned Seattle's second line into a defense pair.
        page = """<html><body><main>
<p>PREDATORS (10-4-5) at GOLDEN KNIGHTS (11-5-2)</p>
<p>Golden Knights projected lineup</p>
<p>Kaapo Kakko -- Berkly Catton – Mackie Samoskevich</p>
<p>Shea Theodore --  Brayden McNabb</p>
<p>Jeremy Lauzon--  Rasmus Andersson</p>
<p>Ukko-Pekka Luukkonen</p>
<p>Carter Hart</p>
</main></body></html>"""
        rows = pd.DataFrame(lineups.parse(page))
        self.assertEqual(rows.groupby("unit", sort=False)["name"].agg(list).to_dict(),
                         {"F1": ["Kaapo Kakko", "Berkly Catton", "Mackie Samoskevich"],
                          "D1": ["Shea Theodore", "Brayden McNabb"],
                          "D2": ["Jeremy Lauzon", "Rasmus Andersson"],
                          "G1": ["Ukko-Pekka Luukkonen"], "G2": ["Carter Hart"]})

    def test_two_name_fourth_line_with_seven_defensemen(self):
        page = """<html><body><main>
<p>LIGHTNING (8-6-1) at JETS (8-8-3)</p>
<p>Lightning projected lineup</p>
<p>Jake Guentzel -- Brayden Point -- Nikita Kucherov</p>
<p>Brandon Hagel -- Anthony Cirelli -- Gage Goncalves</p>
<p>Ilya Mikheyev -- Conor Geekie -- Pontus Holmberg</p>
<p>Jeffrey Viel -- Zemgus Girgensons</p>
<p>J.J. Moser -- John Carlson</p>
<p>Ryan McDonagh -- Erik Cernak</p>
<p>Victor Hedman -- Emil Lilleberg</p>
<p>Charle-Edouard D'Astous</p>
<p>Andrei Vasilevskiy</p>
<p>Dennis Hildeby</p>
</main></body></html>"""
        rows = pd.DataFrame(lineups.parse(page))
        units = rows.groupby("unit", sort=False)["name"].agg(list).to_dict()
        self.assertEqual(units["F4"], ["Jeffrey Viel", "Zemgus Girgensons"])
        self.assertEqual(units["D1"], ["J.J. Moser", "John Carlson"])
        self.assertEqual(units["D3"], ["Victor Hedman", "Emil Lilleberg"])
        self.assertNotIn("D4", units)
        self.assertEqual((units["EXTRA"], units["G1"]), (["Charle-Edouard D'Astous"], ["Andrei Vasilevskiy"]))

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
