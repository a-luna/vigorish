from datetime import datetime
from pathlib import Path

from lxml import html

from app.main.scrape.brooks.scrape_brooks_games_for_date import parse_daily_dash_page
from app.main.scrape.brooks.scrape_brooks_pitch_logs_for_date import parse_pitch_log
from app.main.scrape.brooks.models.pitch_logs_for_game import BrooksPitchLogsForGame
from app.main.scrape.brooks.models.pitch_log import BrooksPitchLog
from app.main.util.file_util import (
    read_bbref_games_for_date_from_file,
    read_brooks_pitch_logs_for_game_from_file,
    write_brooks_pitch_logs_for_game_to_file,
)
from app.main.util.string_functions import validate_bbref_game_id_list
from app.test.base import BaseTestCase


class TestBrooksPitchLogsForDate(BaseTestCase):
    GAME_DATE = datetime(2018, 4, 28)
    GAME_DATE_STR = "2018-04-28"
    GAME_ID = "gid_2018_04_28_detmlb_balmlb_1"
    PITCHER_ID = "612434"
    APP_TEST_FOLDER = Path.cwd() / "app" / "test"
    TEST_FILES_FOLDER = APP_TEST_FOLDER / "test_files"
    DAILY_DASH_URL = "http://www.brooksbaseball.net/dashboard.php?dts=04/28/2018"
    DAILY_DASH_HTML = TEST_FILES_FOLDER / "brooks_dashboard_2018-04-28.html"
    GAME_LOG_URL = "http://www.brooksbaseball.net/pfxVB/pfx.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=28&pitchSel=612434.xml&game=gid_2018_04_28_detmlb_balmlb_1/&prevGame=gid_2018_04_28_detmlb_balmlb_1/"
    GAME_LOG_HTML = TEST_FILES_FOLDER / "brooks_game_log.html"

    def test_scrape_brooks_games_for_date(self):
        """Verify BrooksGameInfo objects are correctly parsed from webpage."""
        result = read_bbref_games_for_date_from_file(
            self.GAME_DATE, folderpath=self.TEST_FILES_FOLDER
        )
        self.assertTrue(result.success)
        bbref_games_for_date = result.value
        game_ids = [Path(url).stem for url in bbref_games_for_date.boxscore_urls]
        required_game_data = validate_bbref_game_id_list(game_ids)

        response = html.parse(str(self.DAILY_DASH_HTML))
        result = parse_daily_dash_page(
            self.session, response, self.GAME_DATE, self.DAILY_DASH_URL, required_game_data,
        )
        self.assertTrue(result.success)
        games_for_date = result.value
        game = games_for_date.games[11]

        pitch_logs_for_game = BrooksPitchLogsForGame()
        pitch_logs_for_game.brooks_game_id = game.brooks_game_id
        pitch_logs_for_game.bbref_game_id = game.bbref_game_id
        pitch_logs_for_game.pitch_log_count = game.pitcher_appearance_count

        response = html.parse(str(self.GAME_LOG_HTML))
        result = parse_pitch_log(response, game, self.PITCHER_ID, self.GAME_LOG_URL)
        self.assertTrue(result.success)
        pitch_log = result.value
        pitch_logs_for_game.pitch_logs = [pitch_log]

        result = write_brooks_pitch_logs_for_game_to_file(
            pitch_logs_for_game, folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        filepath = result.value
        self.assertEqual(filepath.name, f"{self.GAME_ID}.json")

        result = read_brooks_pitch_logs_for_game_from_file(
            self.GAME_ID, folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        pitch_logs_for_game_out = result.value

        filepath.unlink()
        self.assertFalse(filepath.exists())
