import pytest

from tests.util import GAME_DATE_PFX as GAME_DATE
from tests.util import GAME_ID_PFX as BBREF_GAME_ID
from tests.util import (
    parse_brooks_pitchfx_from_html,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
)
from vigorish.config.database import PitchAppScrapeStatus
from vigorish.enums import DataSet
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from vigorish.util.result import Result

DATA_SET = DataSet.BROOKS_PITCHFX
BB_GAME_ID = "gid_2018_04_01_anamlb_oakmlb_1"
PITCH_APP_ID = "OAK201804010_660271"


@pytest.fixture(scope="module", autouse=True)
def create_test_data(db_session, scraped_data):
    """Initialize DB with data to verify test functions in test_brooks_pitchfx module."""
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_boxscore(db_session, scraped_data, BBREF_GAME_ID)
    update_scraped_pitch_logs(db_session, scraped_data, GAME_DATE, BBREF_GAME_ID)
    return True


def test_parse_brooks_pitchfx(scraped_data):
    pitchfx_log = parse_brooks_pitchfx_from_html(scraped_data, BB_GAME_ID, PITCH_APP_ID)
    assert isinstance(pitchfx_log, BrooksPitchFxLog)
    result = verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log)
    assert result.success


def test_persist_brooks_pitchfx(scraped_data):
    pitchfx_log_parsed = parse_brooks_pitchfx_from_html(scraped_data, BB_GAME_ID, PITCH_APP_ID)
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
    pitchfx_log = parse_brooks_pitchfx_from_html(scraped_data, BB_GAME_ID, PITCH_APP_ID)
    assert isinstance(pitchfx_log, BrooksPitchFxLog)
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, PITCH_APP_ID)
    assert pitch_app_status
    assert pitch_app_status.scraped_pitchfx == 0
    assert pitch_app_status.pitch_count_pitchfx == 0
    result = update_pitch_appearance_status_records(db_session, pitchfx_log)
    assert result.success
    assert pitch_app_status.scraped_pitchfx == 1
    assert pitch_app_status.pitch_count_pitchfx == 92
    db_session.commit()


def verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log):
    return Result.Ok()
