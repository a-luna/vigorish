import json
import unittest
from datetime import datetime
from dateutil import tz
from pathlib import Path
from string import Template

from lxml import html

from app.main.scrape.bbref.scrape_bbref_games_for_date import parse_bbref_dashboard_page
from app.main.scrape.bbref.models.games_for_date import BBRefGamesForDate
from app.main.util.file_util import (
    write_bbref_games_for_date_to_file,
    read_bbref_games_for_date_from_file,
)
from app.main.util.json_decoders import decode_bbref_games_for_date
from app.main.util.list_functions import compare_lists
from app.test.base import BaseTestCase


class TestBBRefGamesForDate(BaseTestCase):
    APP_TEST_FOLDER = Path.cwd() / "app" / "test" / "test_files"
    _T_BBREF_URL = 'https://www.baseball-reference.com/boxes/?month=${m}&day=${d}&year=${y}'

    def get_game_date_str(self, game_date):
        return game_date.strftime('%Y-%m-%d')

    def get_bbref_url_for_date(self, game_date):
        year = game_date.year
        month = game_date.month
        day = game_date.day
        return Template(self._T_BBREF_URL).substitute(m=month, d=day, y=year)

    def scrape_games_for_date_from_html(self, game_date, html_path):
        bbref_url = self.get_bbref_url_for_date(game_date)
        contents = html_path.read_text()
        response = html.fromstring(contents)
        result = parse_bbref_dashboard_page(response, game_date, bbref_url)
        self.assertTrue(result.success)
        games_for_date = result.value
        self.assertTrue(isinstance(games_for_date, BBRefGamesForDate))
        return games_for_date

    def test_scrape_bbref_games_for_date(self):
        """Verify BBRefGameInfo object is correctly parsed from webpage."""
        html_path = self.APP_TEST_FOLDER / "bbref_daily_dash.html"
        game_date = datetime(2018, 7, 26)
        game_date_str = self.get_game_date_str(game_date)
        bbref_url = self.get_bbref_url_for_date(game_date)
        expected_game_count = 11
        games_for_date = self.scrape_games_for_date_from_html(game_date, html_path)
        self.assertEqual(games_for_date.dashboard_url, bbref_url)
        self.assertEqual(games_for_date.game_date_str, game_date_str)
        self.assertEqual(games_for_date.game_count, expected_game_count)
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
        html_path = self.APP_TEST_FOLDER / "bbref_daily_dash.html"
        game_date = datetime(2018, 7, 26)
        game_date_str = self.get_game_date_str(game_date)
        bbref_url = self.get_bbref_url_for_date(game_date)
        expected_game_count = 11
        games_for_date_in = self.scrape_games_for_date_from_html(game_date, html_path)
        result = write_bbref_games_for_date_to_file(
            games_for_date_in, folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        filepath = result.value

        self.assertEqual(filepath.name, "bbref_games_for_date_2018-07-26.json")
        result = read_bbref_games_for_date_from_file(
            game_date, folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result.success)
        games_for_date_out = result.value

        self.assertTrue(isinstance(games_for_date_out, BBRefGamesForDate))
        self.assertEqual(games_for_date_out.dashboard_url, bbref_url)
        self.assertEqual(games_for_date_out.game_date, game_date)
        self.assertEqual(games_for_date_out.game_date_str, game_date_str)
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

    def test_bbref_games_req_sel(self):
        """Verify selenium and requests produce the same data for bbref_games_for_date."""
        html_path_req = self.APP_TEST_FOLDER / "bbref_games_req_sel" / "2019-06-13_req.html"
        html_path_sel = self.APP_TEST_FOLDER / "bbref_games_req_sel" / "2019-06-13_sel.html"
        game_date = datetime(2019, 6, 13)
        game_date_str = self.get_game_date_str(game_date)
        bbref_url = self.get_bbref_url_for_date(game_date)
        expected_game_count = 11
        games_for_date_req = self.scrape_games_for_date_from_html(game_date, html_path_req)
        games_for_date_sel = self.scrape_games_for_date_from_html(game_date, html_path_sel)
        self.assertEqual(games_for_date_req.dashboard_url, bbref_url)
        self.assertEqual(games_for_date_req.game_date_str, game_date_str)
        self.assertEqual(games_for_date_req.game_count, expected_game_count)
        self.assertEqual(games_for_date_sel.dashboard_url, bbref_url)
        self.assertEqual(games_for_date_sel.game_date_str, game_date_str)
        self.assertEqual(games_for_date_sel.game_count, expected_game_count)
        boxscore_urls_req = games_for_date_req.boxscore_urls
        boxscore_urls_sel = games_for_date_sel.boxscore_urls
        self.assertEqual(len(boxscore_urls_req), 11)
        self.assertEqual(len(boxscore_urls_sel), 11)
        self.assertTrue(compare_lists(boxscore_urls_req, boxscore_urls_req))

    def test_bbref_nightmare(self):
        game_dates = [
            (datetime(2019, 3, 28), 15),
            (datetime(2019, 3, 29), 8),
            (datetime(2019, 3, 30), 14),
            (datetime(2019, 6, 13), 11),
            (datetime(2019, 6, 14), 15),
            (datetime(2019, 6, 15), 15),
            (datetime(2019, 6, 16), 15)
        ]

        for gd, game_count in game_dates:
            html_path = self.APP_TEST_FOLDER / "bbref_nightmare" / f"{gd.strftime('%Y%m%d')}.html"
            game_date_str = self.get_game_date_str(gd)
            bbref_url = self.get_bbref_url_for_date(gd)
            games_for_date = self.scrape_games_for_date_from_html(gd, html_path)
            self.assertEqual(games_for_date.dashboard_url, bbref_url)
            self.assertEqual(games_for_date.game_date_str, game_date_str)
            self.assertEqual(games_for_date.game_count, game_count)
            boxscore_urls = games_for_date.boxscore_urls
            self.assertEqual(len(boxscore_urls), game_count)
