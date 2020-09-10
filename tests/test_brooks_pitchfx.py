from datetime import datetime

from vigorish.config.database import PitchAppScrapeStatus
from vigorish.enums import DataSet
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from vigorish.util.result import Result

from tests.util import reset_pitch_app_scrape_status_after_parsed_pitchfx

DATA_SET = DataSet.BROOKS_PITCHFX
GAME_DATE = datetime(2018, 4, 1)
BB_GAME_ID = "gid_2018_04_01_anamlb_oakmlb_1"
PITCH_APP_ID = "OAK201804010_660271"


def parse_brooks_pitchfx_from_html(scraped_data):
    pitch_logs = scraped_data.get_brooks_pitch_logs_for_game(BB_GAME_ID)
    pitch_log = [plog for plog in pitch_logs.pitch_logs if plog.pitch_app_id == PITCH_APP_ID][0]
    html_path = scraped_data.get_html(DATA_SET, PITCH_APP_ID)
    result = parse_pitchfx_log(html_path.read_text(), pitch_log)
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
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath.name == "OAK201804010_660271.json"
    pitchfx_log_decoded = scraped_data.get_brooks_pitchfx_log(PITCH_APP_ID)
    assert isinstance(pitchfx_log_decoded, BrooksPitchFxLog)
    result = verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def test_update_database_pitchfx(db_session, scraped_data):
    pitchfx_log = parse_brooks_pitchfx_from_html(scraped_data)
    assert isinstance(pitchfx_log, BrooksPitchFxLog)
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, PITCH_APP_ID)
    assert pitch_app_status
    assert pitch_app_status.scraped_pitchfx == 0
    assert pitch_app_status.pitch_count_pitchfx == 0
    result = update_pitch_appearance_status_records(db_session, pitchfx_log)
    assert result.success
    assert pitch_app_status.scraped_pitchfx == 1
    assert pitch_app_status.pitch_count_pitchfx == 92
    reset_pitch_app_scrape_status_after_parsed_pitchfx(db_session, PITCH_APP_ID)


def verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log):
    return Result.Ok()
