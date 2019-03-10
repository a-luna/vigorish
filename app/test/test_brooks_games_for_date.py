import json
import unittest
from datetime import datetime
from dateutil import tz
from pathlib import Path

from lxml import html

from app.main.constants import BROOKS_DASHBOARD_DATE_FORMAT
from app.main.scrape.brooks.scrape_brooks_games_for_date import (
    __parse_daily_dash_page as parse_daily_dash_page
)
from app.main.scrape.brooks.models.game_info import BrooksGameInfo
from app.main.scrape.brooks.models.games_for_date import BrooksGamesForDate
from app.main.util.file_util import (
    write_brooks_games_for_date_to_file, read_brooks_games_for_date_from_file
)
from app.main.util.json_decoders import decode_brooks_games_for_date
from app.test.base import BaseTestCase

class TestBrooksGamesForDate(BaseTestCase):
    GAME_DATE = datetime(2018, 4, 17)
    GAME_DATE_STR = '2018-04-17'
    APP_TEST_FOLDER = Path.cwd() / 'app' / 'test'
    DAILY_DASH_URL = 'http://www.brooksbaseball.net/dashboard.php?dts=04/17/2018'
    DAILY_DASH_HTML = APP_TEST_FOLDER / 'test_files' / 'brooks_daily_dash.xml'

    def test_scrape_brooks_games_for_date(self):
        """Verify BrooksGameInfo objects are correctly parsed from webpage."""
        response = html.parse(str(self.DAILY_DASH_HTML))
        result = parse_daily_dash_page(self.session, response, self.GAME_DATE, self.DAILY_DASH_URL)
        self.assertTrue(result.success)

        games_for_date = result.value
        self.assertEqual(games_for_date.dashboard_url, self.DAILY_DASH_URL)
        self.assertTrue(isinstance(games_for_date, BrooksGamesForDate))
        self.assertEqual(games_for_date.game_date_str, self.GAME_DATE_STR)
        self.assertEqual(games_for_date.game_count, 16)

        games = games_for_date.games
        self.assertEqual(len(games), 16)

        game0 = games[0]
        self.assertIsInstance(game0, BrooksGameInfo)
        self.assertEqual(game0.game_date_year, 2018)
        self.assertEqual(game0.game_date_month, 4)
        self.assertEqual(game0.game_date_day, 17)
        self.assertEqual(game0.game_time_hour, 3)
        self.assertEqual(game0.game_time_minute, 7)
        self.assertEqual(game0.time_zone_name, 'America/New_York')
        self.assertEqual(game0.bb_game_id, 'gid_2018_04_17_kcamlb_tormlb_1')
        self.assertEqual(game0.away_team_id_bb, 'KCA')
        self.assertEqual(game0.home_team_id_bb, 'TOR')
        self.assertEqual(game0.game_number_this_day, '1')
        self.assertEqual(game0.pitcher_appearance_count, 7)

        g0_pitch_dict = game0.pitcher_appearance_dict
        self.assertEqual(len(g0_pitch_dict), 7)

        game1 = games[1]
        self.assertIsInstance(game1, BrooksGameInfo)
        self.assertEqual(game1.game_date_year, 2018)
        self.assertEqual(game1.game_date_month, 4)
        self.assertEqual(game1.game_date_day, 17)
        self.assertEqual(game1.game_time_hour, 0)
        self.assertEqual(game1.game_time_minute, 0)
        self.assertEqual(game1.time_zone_name, 'America/New_York')
        self.assertEqual(game1.bb_game_id, 'gid_2018_04_17_kcamlb_tormlb_2')
        self.assertEqual(game1.away_team_id_bb, 'KCA')
        self.assertEqual(game1.home_team_id_bb, 'TOR')
        self.assertEqual(game1.game_number_this_day, '2')
        self.assertEqual(game1.pitcher_appearance_count, 12)

        g1_pitch_dict = game1.pitcher_appearance_dict
        self.assertEqual(len(g1_pitch_dict), 12)

        game8 = games[8]
        self.assertIsInstance(game8, BrooksGameInfo)
        self.assertEqual(game8.game_date_year, 2018)
        self.assertEqual(game8.game_date_month, 4)
        self.assertEqual(game8.game_date_day, 17)
        self.assertEqual(game8.game_time_hour, 7)
        self.assertEqual(game8.game_time_minute, 35)
        self.assertEqual(game8.time_zone_name, 'America/New_York')
        self.assertEqual(game8.bb_game_id, 'gid_2018_04_17_phimlb_atlmlb_1')
        self.assertEqual(game8.away_team_id_bb, 'PHI')
        self.assertEqual(game8.home_team_id_bb, 'ATL')
        self.assertEqual(game8.game_number_this_day, '1')
        self.assertEqual(game8.pitcher_appearance_count, 13)

        g8_pitch_dict = game8.pitcher_appearance_dict
        self.assertEqual(len(g8_pitch_dict), 13)

        game15 = games[15]
        self.assertIsInstance(game15, BrooksGameInfo)
        self.assertEqual(game15.game_date_year, 2018)
        self.assertEqual(game15.game_date_month, 4)
        self.assertEqual(game15.game_date_day, 17)
        self.assertEqual(game15.game_time_hour, 10)
        self.assertEqual(game15.game_time_minute, 10)
        self.assertEqual(game15.time_zone_name, 'America/New_York')
        self.assertEqual(game15.bb_game_id, 'gid_2018_04_17_lanmlb_sdnmlb_1')
        self.assertEqual(game15.away_team_id_bb, 'LAN')
        self.assertEqual(game15.home_team_id_bb, 'SDN')
        self.assertEqual(game15.game_number_this_day, '1')
        self.assertEqual(game15.pitcher_appearance_count, 15)

        g15_pitch_dict = game15.pitcher_appearance_dict
        self.assertEqual(len(g15_pitch_dict), 15)

        g5_pitch_dict = games[5].pitcher_appearance_dict
        self.assertTrue('543606' in g5_pitch_dict)
        self.assertEqual(g5_pitch_dict['543606'], 'http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=543606.xml&game=gid_2018_04_17_clemlb_minmlb_1/&prevGame=gid_2018_04_17_clemlb_minmlb_1/')

        g6_pitch_dict = games[6].pitcher_appearance_dict
        self.assertTrue('630023' in g6_pitch_dict)
        self.assertEqual(g6_pitch_dict['630023'], 'http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=630023.xml&game=gid_2018_04_17_texmlb_tbamlb_1/&prevGame=gid_2018_04_17_texmlb_tbamlb_1/')

        g10_pitch_dict = games[10].pitcher_appearance_dict
        self.assertTrue('453344' in g10_pitch_dict)
        self.assertEqual(g10_pitch_dict['453344'], 'http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=453344.xml&game=gid_2018_04_17_slnmlb_chnmlb_1/&prevGame=gid_2018_04_17_slnmlb_chnmlb_1/')

        g12_pitch_dict = games[12].pitcher_appearance_dict
        self.assertTrue('502327' in g12_pitch_dict)
        self.assertEqual(g12_pitch_dict['502327'], 'http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=502327.xml&game=gid_2018_04_17_chamlb_oakmlb_1/&prevGame=gid_2018_04_17_chamlb_oakmlb_1/')

    def test_persist_brooks_games_for_date(self):
        """Verify BrooksGamesForDate and BrooksGameInfo objects can be written to file and read from file."""
        response = html.parse(str(self.DAILY_DASH_HTML))
        result = parse_daily_dash_page(self.session, response, self.GAME_DATE, self.DAILY_DASH_URL)
        self.assertTrue(result.success)

        games_for_date_in = result.value
        result = write_brooks_games_for_date_to_file(
            games_for_date_in,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        filepath = result.value

        self.assertEqual(filepath.name, 'brooks_games_for_date_2018-04-17.json')
        result = read_brooks_games_for_date_from_file(
            self.GAME_DATE,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        games_for_date_out = result.value

        d = games_for_date_out.get_game_id_dict()

        self.assertTrue(isinstance(games_for_date_out, BrooksGamesForDate))
        self.assertEqual(games_for_date_out.dashboard_url, self.DAILY_DASH_URL)
        self.assertEqual(games_for_date_out.game_date, self.GAME_DATE)
        self.assertEqual(games_for_date_out.game_date_str, self.GAME_DATE_STR)
        self.assertEqual(games_for_date_out.game_count, 16)

        game0 = games_for_date_out.games[0]
        self.assertEqual(game0.time_zone_name, 'America/New_York')
        edt = tz.gettz(game0.time_zone_name)
        game_start = datetime(2018, 4, 17, hour=3, minute=7, tzinfo=edt)
        self.assertEqual(game0.game_start_time, game_start)

        game1 = games_for_date_out.games[1]
        game_start = datetime(2018, 4, 17, tzinfo=edt)
        self.assertEqual(game1.game_start_time, game_start)

        filepath.unlink()
        self.assertTrue(result.success)
        self.assertFalse(filepath.exists())
