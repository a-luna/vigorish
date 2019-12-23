from datetime import datetime
from pathlib import Path

from lxml import html

from app.main.scrape.brooks.scrape_brooks_pitchfx import parse_pitchfx_log
from app.main.scrape.brooks.models.pitchfx_log import BrooksPitchFxLog
from app.main.scrape.brooks.models.pitchfx import BrooksPitchFxData
from app.main.util.file_util import (
    read_brooks_pitch_logs_for_game_from_file,
    write_brooks_pitchfx_log_to_file,
    read_brooks_pitchfx_log_from_file
)
from app.test.base import BaseTestCase

class TestBrooksPitchFxLog(BaseTestCase):
    APP_TEST_FOLDER = Path.cwd() / "app" / "test"
    TEST_FILES_FOLDER = APP_TEST_FOLDER / "test_files"
    #PITCH_APP_ID = "98147d5e-197c-457e-bc09-44cf5372f071"
    #GAME_ID = "gid_2018_04_22_houmlb_chamlb_1"
    #PITCHER_ID = "621121"
    #PITCHFX_HTML = TEST_FILES_FOLDER /  "CHA201804220_621121.html"

    PITCH_APP_ID = "c7bb3db7-9be5-437c-b887-1655063a5bbc"
    GAME_ID = "gid_2019_06_01_nynmlb_arimlb_1"
    PITCHER_ID = "594798"
    PITCHFX_HTML = TEST_FILES_FOLDER /  "ARI201906010_594798.html"

    #PITCH_APP_ID = "4f511c83-ce28-448e-a478-e3dffbb06210"
    #GAME_ID = "gid_2019_06_01_miamlb_sdnmlb_1"
    #PITCHER_ID = "476589"
    #PITCHFX_HTML = TEST_FILES_FOLDER /  "SDN201906010_476589.html"

    def test_scrape_brooks_pitchfx_log(self):
        """Verify BrooksPitchFxLog objects are correctly parsed from webpage."""
        result = read_brooks_pitch_logs_for_game_from_file(self.GAME_ID, folderpath=self.TEST_FILES_FOLDER)
        self.assertTrue(result.success)
        pitch_logs_for_game = result.value
        pitch_log = list(set([
            plog for plog in pitch_logs_for_game.pitch_logs
            if plog.pitch_app_id == self.PITCH_APP_ID]))[0]

        response = html.parse(str(self.PITCHFX_HTML))
        result = parse_pitchfx_log(response, pitch_log)
        self.assertTrue(result.success)
        pitchfx_log = result.value

        result = write_brooks_pitchfx_log_to_file(pitchfx_log, folderpath=self.APP_TEST_FOLDER)
        self.assertTrue(result.success)
        filepath = result.value
        self.assertEqual(filepath.name, f"{self.PITCH_APP_ID}.json")

        result = read_brooks_pitchfx_log_from_file(self.PITCH_APP_ID, folderpath=self.APP_TEST_FOLDER)
        self.assertTrue(result.success)
        pitchfx_log_out = result.value

        filepath.unlink()
        self.assertFalse(filepath.exists())
