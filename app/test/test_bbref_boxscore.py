import json
import unittest
from pathlib import Path
from lxml import html
from app.main.data.scrape.bbref.scrape_boxscores_for_date import __parse_bbref_boxscore as parse_bbref_boxscore
from app.main.data.scrape.bbref.models.boxscore import BBRefBoxScore
from app.main.util.file_util import write_bbref_boxscore_to_file, read_bbref_boxscore_from_file
from app.main.util.json_decoders import decode_bbref_boxscore
from app.test.base import BaseTestCase

class TestBBRefBoxScore(BaseTestCase):
    APP_TEST_FOLDER = Path.cwd() / 'app' / 'test'
    BOXSCORE_URL = 'https://www.baseball-reference.com/boxes/ARI/ARI201803290.shtml'
    BOXSCORE_HTML = APP_TEST_FOLDER / 'test_files' / 'bbref_boxscore.xml'
    GAME_ID = 'ARI201803290'

    def test_scrape_bbref_boxscore(self):
        """Verify BBRefBoxScore object is correctly parsed from webpage."""
        response = html.parse(str(self.BOXSCORE_HTML))
        boxscore_parsed = parse_bbref_boxscore(response, self.BOXSCORE_URL, silent=True)

        result = write_bbref_boxscore_to_file(
            boxscore_parsed,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result['success'])
        filepath = result['filepath']

        self.assertEqual(filepath.name, f'{self.GAME_ID}.json')
        boxscore = read_bbref_boxscore_from_file(
            self.GAME_ID,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(boxscore.scrape_success)
        self.assertEqual(boxscore.boxscore_url, self.BOXSCORE_URL)
        self.assertEqual(boxscore.bbref_game_id, 'ARI201803290')
        self.assertEqual(boxscore.away_team_id_br, 'COL')
        self.assertEqual(boxscore.home_team_id_br, 'ARI')
        self.assertEqual(boxscore.away_team_runs, 2)
        self.assertEqual(boxscore.home_team_runs, 8)
        self.assertEqual(boxscore.away_team_wins_before_game, 0)
        self.assertEqual(boxscore.away_team_losses_before_game, 1)
        self.assertEqual(boxscore.home_team_wins_before_game, 1)
        self.assertEqual(boxscore.home_team_losses_before_game, 0)

        self.assertEqual(boxscore.attendance, 48703)
        self.assertEqual(boxscore.park_name, 'Chase Field')
        self.assertEqual(boxscore.game_duration, '3:36')
        self.assertEqual(boxscore.day_night, 'Night Game')
        self.assertEqual(boxscore.field_type, 'On Grass')
        self.assertEqual(boxscore.first_pitch_temperature, 82)
        self.assertEqual(boxscore.first_pitch_wind, 'Wind 0mph')
        self.assertEqual(boxscore.first_pitch_clouds, 'Night')
        self.assertEqual(boxscore.first_pitch_precipitation, 'No Precipitation')

        away_team_linescore_innings = boxscore.away_team_linescore_innings
        away_team_linescore_innings[0].runs = "1"
        away_team_linescore_innings[1].runs = "0"
        away_team_linescore_innings[2].runs = "0"
        away_team_linescore_innings[3].runs = "0"
        away_team_linescore_innings[4].runs = "0"
        away_team_linescore_innings[5].runs = "1"
        away_team_linescore_innings[6].runs = "0"
        away_team_linescore_innings[7].runs = "0"
        away_team_linescore_innings[8].runs = "0"

        home_team_linescore_innings = boxscore.home_team_linescore_innings
        home_team_linescore_innings[0].runs = "3"
        home_team_linescore_innings[1].runs = "0"
        home_team_linescore_innings[2].runs = "0"
        home_team_linescore_innings[3].runs = "0"
        home_team_linescore_innings[4].runs = "0"
        home_team_linescore_innings[5].runs = "3"
        home_team_linescore_innings[6].runs = "2"
        home_team_linescore_innings[7].runs = "0"
        home_team_linescore_innings[8].runs = "X"

        away_team_linescore_totals = boxscore.away_team_linescore_totals
        away_team_linescore_totals.total_runs = 2
        away_team_linescore_totals.total_hits = 9
        away_team_linescore_totals.total_errors = 0

        home_team_linescore_totals = boxscore.home_team_linescore_totals
        home_team_linescore_totals.total_runs = 8
        home_team_linescore_totals.total_hits = 12
        home_team_linescore_totals.total_errors = 0

        batting_stats = boxscore.batting_stats
        self.assertEqual(len(batting_stats), 32)

        pitching_stats = boxscore.pitching_stats
        self.assertEqual(len(pitching_stats), 11)

        pbp_events = boxscore.play_by_play
        self.assertEqual(len(pbp_events), 80)

        filepath.unlink()
        self.assertFalse(filepath.exists())