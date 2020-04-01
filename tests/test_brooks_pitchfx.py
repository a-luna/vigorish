from datetime import datetime

from lxml import html

from vigorish.enums import DataSet
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.util.result import Result

DATA_SET = DataSet.BROOKS_PITCHFX
GAME_DATE = datetime(2018, 4, 1)
BB_GAME_ID = "gid_2018_04_01_anamlb_oakmlb_1"
PITCH_APP_ID = "OAK201804010_660271"


def parse_brooks_pitchfx_from_html(scraped_data):
    result = scraped_data.get_brooks_pitch_logs_for_game(BB_GAME_ID)
    assert result.success
    pitch_logs = result.value
    pitch_log = [plog for plog in pitch_logs.pitch_logs if plog.pitch_app_id == PITCH_APP_ID][0]
    html_path = scraped_data.get_html(DATA_SET, PITCH_APP_ID)
    html_text = html_path.read_text()
    page_content = html.fromstring(html_text, base_url=pitch_log.pitchfx_url)
    result = parse_pitchfx_log(page_content, pitch_log)
    assert result.success
    pitchfx_log = result.value
    return pitchfx_log


def test_parse_brooks_pitchfx(scraped_data):
    pitchfx_log = parse_brooks_pitchfx_from_html(scraped_data)
    assert isinstance(pitchfx_log, BrooksPitchFxLog)
    result = verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log)
    assert result.success


def test_persist_brooks_pitchfx(scraped_data):
    pitchfx_log_parsed = parse_brooks_pitchfx_from_html(scraped_data)
    assert isinstance(pitchfx_log_parsed, BrooksPitchFxLog)
    result = scraped_data.save_json(DATA_SET, pitchfx_log_parsed)
    assert result.success
    json_filepath = result.value
    assert json_filepath.name == "OAK201804010_660271.json"
    result = scraped_data.get_brooks_pitchfx_log(PITCH_APP_ID)
    assert result.success
    pitchfx_log_decoded = result.value
    assert isinstance(pitchfx_log_decoded, BrooksPitchFxLog)
    result = verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log):
    return Result.Ok()
