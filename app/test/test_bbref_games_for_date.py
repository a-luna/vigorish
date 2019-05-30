import json
import unittest
from datetime import datetime
from dateutil import tz
from pathlib import Path

from lxml import html

from app.main.scrape.bbref.scrape_bbref_games_for_date import parse_bbref_dashboard_page
from app.main.scrape.bbref.models.games_for_date import BBRefGamesForDate
from app.main.util.file_util import (
    write_bbref_games_for_date_to_file,
    read_bbref_games_for_date_from_file,
)
from app.main.util.json_decoders import decode_bbref_games_for_date
from app.test.base import BaseTestCase


class TestBBRefGamesForDate(BaseTestCase):
    GAME_DATE = datetime(2018, 7, 26)
    GAME_DATE_STR = "2018-07-26"
    APP_TEST_FOLDER = Path.cwd() / "app" / "test"
    DAILY_DASH_URL = (
        "https://www.baseball-reference.com/boxes/?month=7&day=26&year=2018"
    )
    DAILY_DASH_HTML = APP_TEST_FOLDER / "test_files" / "bbref_daily_dash.html"

    def test_scrape_bbref_games_for_date(self):
        """Verify BBRefGameInfo object is correctly parsed from webpage."""
        response = html.parse(str(self.DAILY_DASH_HTML))
        result = parse_bbref_dashboard_page(response, self.GAME_DATE, self.DAILY_DASH_URL)
        self.assertTrue(result.success)

        games_for_date = result.value
        self.assertEqual(games_for_date.dashboard_url, self.DAILY_DASH_URL)
        self.assertTrue(isinstance(games_for_date, BBRefGamesForDate))
        self.assertEqual(games_for_date.game_date_str, self.GAME_DATE_STR)
        self.assertEqual(games_for_date.game_count, 11)

        boxscore_urls = games_for_date.boxscore_urls
        self.assertEqual(len(boxscore_urls), 11)

        url0 = "https://www.baseball-reference.com/boxes/ANA/ANA201807260.shtml"
        url1 = "https://www.baseball-reference.com/boxes/ATL/ATL201807260.shtml"
        url2 = "https://www.baseball-reference.com/boxes/BAL/BAL201807260.shtml"
        url3 = "https://www.baseball-reference.com/boxes/BOS/BOS201807260.shtml"
        url4 = "https://www.baseball-reference.com/boxes/CHN/CHN201807260.shtml"
        url5 = "https://www.baseball-reference.com/boxes/CIN/CIN201807260.shtml"
        url6 = "https://www.baseball-reference.com/boxes/MIA/MIA201807260.shtml"
        url7 = "https://www.baseball-reference.com/boxes/NYA/NYA201807260.shtml"
        url8 = "https://www.baseball-reference.com/boxes/PIT/PIT201807260.shtml"
        url9 = "https://www.baseball-reference.com/boxes/SFN/SFN201807260.shtml"
        url10 = "https://www.baseball-reference.com/boxes/TEX/TEX201807260.shtml"

        self.assertEqual(boxscore_urls[0], url0)
        self.assertEqual(boxscore_urls[1], url1)
        self.assertEqual(boxscore_urls[2], url2)
        self.assertEqual(boxscore_urls[3], url3)
        self.assertEqual(boxscore_urls[4], url4)
        self.assertEqual(boxscore_urls[5], url5)
        self.assertEqual(boxscore_urls[6], url6)
        self.assertEqual(boxscore_urls[7], url7)
        self.assertEqual(boxscore_urls[8], url8)
        self.assertEqual(boxscore_urls[9], url9)
        self.assertEqual(boxscore_urls[10], url10)

    def test_persist_bbref_games_for_date(self):
        """Verify BBRefGamesForDate object can be written to file and read from file."""
        response = html.parse(str(self.DAILY_DASH_HTML))
        result = parse_bbref_dashboard_page(response, self.GAME_DATE, self.DAILY_DASH_URL)
        self.assertTrue(result.success)

        games_for_date_in = result.value
        result = write_bbref_games_for_date_to_file(
            games_for_date_in, folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        filepath = result.value

        self.assertEqual(filepath.name, "bbref_games_for_date_2018-07-26.json")
        result = read_bbref_games_for_date_from_file(
            self.GAME_DATE, folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        games_for_date_out = result.value

        self.assertTrue(isinstance(games_for_date_out, BBRefGamesForDate))
        self.assertEqual(games_for_date_out.dashboard_url, self.DAILY_DASH_URL)
        self.assertEqual(games_for_date_out.game_date, self.GAME_DATE)
        self.assertEqual(games_for_date_out.game_date_str, self.GAME_DATE_STR)
        self.assertEqual(games_for_date_out.game_count, 11)

        boxscore_urls = games_for_date_out.boxscore_urls
        self.assertEqual(len(boxscore_urls), 11)

        url0 = "https://www.baseball-reference.com/boxes/ANA/ANA201807260.shtml"
        url1 = "https://www.baseball-reference.com/boxes/ATL/ATL201807260.shtml"
        url2 = "https://www.baseball-reference.com/boxes/BAL/BAL201807260.shtml"
        url3 = "https://www.baseball-reference.com/boxes/BOS/BOS201807260.shtml"
        url4 = "https://www.baseball-reference.com/boxes/CHN/CHN201807260.shtml"
        url5 = "https://www.baseball-reference.com/boxes/CIN/CIN201807260.shtml"
        url6 = "https://www.baseball-reference.com/boxes/MIA/MIA201807260.shtml"
        url7 = "https://www.baseball-reference.com/boxes/NYA/NYA201807260.shtml"
        url8 = "https://www.baseball-reference.com/boxes/PIT/PIT201807260.shtml"
        url9 = "https://www.baseball-reference.com/boxes/SFN/SFN201807260.shtml"
        url10 = "https://www.baseball-reference.com/boxes/TEX/TEX201807260.shtml"

        self.assertEqual(boxscore_urls[0], url0)
        self.assertEqual(boxscore_urls[1], url1)
        self.assertEqual(boxscore_urls[2], url2)
        self.assertEqual(boxscore_urls[3], url3)
        self.assertEqual(boxscore_urls[4], url4)
        self.assertEqual(boxscore_urls[5], url5)
        self.assertEqual(boxscore_urls[6], url6)
        self.assertEqual(boxscore_urls[7], url7)
        self.assertEqual(boxscore_urls[8], url8)
        self.assertEqual(boxscore_urls[9], url9)
        self.assertEqual(boxscore_urls[10], url10)

        filepath.unlink()
        self.assertFalse(filepath.exists())
