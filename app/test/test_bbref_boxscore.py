import json
import unittest
from pathlib import Path

from lxml import html

from app.main.scrape.bbref.scrape_bbref_boxscores_for_date import (
    __parse_bbref_boxscore as parse_bbref_boxscore
)
from app.main.scrape.bbref.models.boxscore import BBRefBoxscore
from app.main.util.file_util import (
    write_bbref_boxscore_to_file, read_bbref_boxscore_from_file
)
from app.main.util.json_decoders import decode_bbref_boxscore
from app.test.base import BaseTestCase

class TestBBRefBoxscore(BaseTestCase):
    APP_TEST_FOLDER = Path.cwd() / 'app' / 'test'
    BOXSCORE_URL = 'https://www.baseball-reference.com/boxes/ATL/ATL201803290.shtml'
    BOXSCORE_HTML = APP_TEST_FOLDER / 'test_files' / 'ATL201803290.xml'
    GAME_ID = 'ATL201803290'

    def test_scrape_bbref_boxscore(self):
        """Verify BBRefBoxscore object is correctly parsed from webpage."""
        response = html.parse(str(self.BOXSCORE_HTML))
        result = parse_bbref_boxscore(response, self.BOXSCORE_URL, silent=True)
        self.assertTrue(result.success)
        boxscore_parsed = result.value

        result = write_bbref_boxscore_to_file(
            boxscore_parsed,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        filepath = result.value
        self.assertEqual(filepath.name, f'{self.GAME_ID}.json')

        result = read_bbref_boxscore_from_file(
            self.GAME_ID,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        boxscore = result.value

        self.assertEqual(boxscore.boxscore_url, self.BOXSCORE_URL)
        self.assertEqual(boxscore.bbref_game_id, 'ATL201803290')
        self.assertEqual(boxscore.away_team_data.team_id_br, 'PHI')
        self.assertEqual(boxscore.home_team_data.team_id_br, 'ATL')
        self.assertEqual(boxscore.away_team_data.total_runs_scored_by_team, 5)
        self.assertEqual(boxscore.away_team_data.total_runs_scored_by_opponent, 8)
        self.assertEqual(boxscore.home_team_data.total_runs_scored_by_team, 8)
        self.assertEqual(boxscore.home_team_data.total_runs_scored_by_opponent, 5)
        self.assertEqual(boxscore.away_team_data.total_wins_before_game, 0)
        self.assertEqual(boxscore.away_team_data.total_losses_before_game, 1)
        self.assertEqual(boxscore.home_team_data.total_wins_before_game, 1)
        self.assertEqual(boxscore.home_team_data.total_losses_before_game, 0)

        self.assertEqual(boxscore.game_meta_info.attendance, 40208)
        self.assertEqual(boxscore.game_meta_info.park_name, 'SunTrust Park')
        self.assertEqual(boxscore.game_meta_info.game_duration, '3:28')
        self.assertEqual(boxscore.game_meta_info.day_night, 'Day Game')
        self.assertEqual(boxscore.game_meta_info.field_type, 'On Grass')
        self.assertEqual(boxscore.game_meta_info.first_pitch_temperature, 74)
        self.assertEqual(boxscore.game_meta_info.first_pitch_wind, 'Wind 16mph from Left to Right')
        self.assertEqual(boxscore.game_meta_info.first_pitch_clouds, 'Cloudy')
        self.assertEqual(boxscore.game_meta_info.first_pitch_precipitation, 'No Precipitation')

        self.assertEqual(boxscore.away_team_data.total_runs_scored_by_team, 5)
        self.assertEqual(boxscore.away_team_data.total_runs_scored_by_opponent, 8)
        self.assertEqual(boxscore.away_team_data.total_hits_by_team, 6)
        self.assertEqual(boxscore.away_team_data.total_hits_by_opponent, 9)
        self.assertEqual(boxscore.away_team_data.total_errors_by_team, 1)
        self.assertEqual(boxscore.away_team_data.total_errors_by_opponent, 0)

        self.assertEqual(boxscore.home_team_data.total_runs_scored_by_team, 8)
        self.assertEqual(boxscore.home_team_data.total_runs_scored_by_opponent, 5)
        self.assertEqual(boxscore.home_team_data.total_hits_by_team, 9)
        self.assertEqual(boxscore.home_team_data.total_hits_by_opponent, 6)
        self.assertEqual(boxscore.home_team_data.total_errors_by_team, 0)
        self.assertEqual(boxscore.home_team_data.total_errors_by_opponent, 1)

        away_team_pitch_stats = boxscore.away_team_data.pitching_stats
        home_team_pitch_stats = boxscore.home_team_data.pitching_stats
        away_team_total_pitches = sum(pitch_stats.pitch_count for pitch_stats in away_team_pitch_stats)
        home_team_total_pitches = sum(pitch_stats.pitch_count for pitch_stats in home_team_pitch_stats)
        total_pitches = away_team_total_pitches + home_team_total_pitches
        total_pitch_appearances = len(away_team_pitch_stats) + len(home_team_pitch_stats)

        filepath.unlink()
        self.assertFalse(filepath.exists())
