import json
import unittest
from pathlib import Path
from lxml import html
from app.main.data.scrape.bbref.scrape_boxscores_for_date import __parse_bbref_boxscore as parse_bbref_boxscore
from app.main.data.scrape.bbref.models.boxscore import BBRefBoxscore
from app.main.util.file_util import write_bbref_boxscore_to_file, read_bbref_boxscore_from_file
from app.main.util.json_decoders import decode_bbref_boxscore
from app.test.base import BaseTestCase

class TestBBRefBoxscore(BaseTestCase):
    APP_TEST_FOLDER = Path.cwd() / 'app' / 'test'
    BOXSCORE_URL = 'https://www.baseball-reference.com/boxes/ARI/ARI201803290.shtml'
    BOXSCORE_HTML = APP_TEST_FOLDER / 'test_files' / 'bbref_boxscore.xml'
    GAME_ID = 'ARI201803290'

    def test_scrape_bbref_boxscore(self):
        """Verify BBRefBoxscore object is correctly parsed from webpage."""
        response = html.parse(str(self.BOXSCORE_HTML))
        result = parse_bbref_boxscore(response, self.BOXSCORE_URL, silent=True)
        self.assertTrue(result['success'])
        boxscore_parsed = result['result']

        result = write_bbref_boxscore_to_file(
            boxscore_parsed,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result['success'])
        filepath = result['filepath']

        self.assertEqual(filepath.name, f'{self.GAME_ID}.json')
        result = read_bbref_boxscore_from_file(
            self.GAME_ID,
            folderpath=self.APP_TEST_FOLDER
        )
        self.assertTrue(result['success'])
        boxscore = result['result']
        satan = 666