import pytest

from tests.util import GAME_DATE_PFX as GAME_DATE
from tests.util import GAME_ID_PFX as BBREF_GAME_ID
from tests.util import (
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
)
from vigorish.database import PitchAppScrapeStatus
from vigorish.enums import DataSet
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from vigorish.util.result import Result

DATA_SET = DataSet.BROOKS_PITCHFX
BB_GAME_ID = "gid_2018_04_01_anamlb_oakmlb_1"
PITCH_APP_ID = "OAK201804010_660271"


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_brooks_pitchfx module."""
    update_scraped_bbref_games_for_date(vig_app, GAME_DATE)
    update_scraped_brooks_games_for_date(vig_app, GAME_DATE)
    update_scraped_boxscore(vig_app, BBREF_GAME_ID)
    update_scraped_pitch_logs(vig_app, GAME_DATE, BBREF_GAME_ID)
    return True


def parse_brooks_pitchfx_from_html(vig_app, bb_game_id, pitch_app_id):
    pitch_logs = vig_app.scraped_data.get_brooks_pitch_logs_for_game(bb_game_id)
    pitch_log = [plog for plog in pitch_logs.pitch_logs if plog.pitch_app_id == pitch_app_id][0]
    html_path = vig_app.scraped_data.get_html(DataSet.BROOKS_PITCHFX, pitch_app_id)
    result = parse_pitchfx_log(html_path.read_text(), pitch_log)
    assert result.success
    pitchfx_log = result.value
    return pitchfx_log


def test_parse_brooks_pitchfx(vig_app):
    pitchfx_log = parse_brooks_pitchfx_from_html(vig_app, BB_GAME_ID, PITCH_APP_ID)
    assert isinstance(pitchfx_log, BrooksPitchFxLog)
    result = verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log)
    assert result.success


def test_persist_brooks_pitchfx(vig_app):
    pitchfx_log_parsed = parse_brooks_pitchfx_from_html(vig_app, BB_GAME_ID, PITCH_APP_ID)
    assert isinstance(pitchfx_log_parsed, BrooksPitchFxLog)
    result = vig_app.scraped_data.save_json(DATA_SET, pitchfx_log_parsed)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath.name == "OAK201804010_660271.json"
    pitchfx_log_decoded = vig_app.scraped_data.get_brooks_pitchfx_log(PITCH_APP_ID)
    assert isinstance(pitchfx_log_decoded, BrooksPitchFxLog)
    result = verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def test_update_database_pitchfx(vig_app):
    pitchfx_log = parse_brooks_pitchfx_from_html(vig_app, BB_GAME_ID, PITCH_APP_ID)
    assert isinstance(pitchfx_log, BrooksPitchFxLog)
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(vig_app.db_session, PITCH_APP_ID)
    assert pitch_app_status
    assert pitch_app_status.scraped_pitchfx == 0
    assert pitch_app_status.pitch_count_pitchfx == 0
    result = update_pitch_appearance_status_records(vig_app.db_session, pitchfx_log)
    assert result.success
    assert pitch_app_status.scraped_pitchfx == 1
    assert pitch_app_status.pitch_count_pitchfx == 92
    vig_app.db_session.commit()


def verify_brooks_pitchfx_OAK201804010_660271(pitchfx_log):
    assert pitchfx_log.pitcher_name == "Shohei Ohtani"
    assert pitchfx_log.pitcher_id_mlb == 660271
    assert pitchfx_log.pitch_app_id == "OAK201804010_660271"
    assert pitchfx_log.total_pitch_count == 92
    assert pitchfx_log.pitcher_team_id_bb == "ANA"
    assert pitchfx_log.opponent_team_id_bb == "OAK"
    assert pitchfx_log.bb_game_id == "gid_2018_04_01_anamlb_oakmlb_1"
    assert pitchfx_log.bbref_game_id == "OAK201804010"
    assert pitchfx_log.game_date_year == 2018
    assert pitchfx_log.game_date_month == 4
    assert pitchfx_log.game_date_day == 1
    assert pitchfx_log.game_time_hour == 16
    assert pitchfx_log.game_time_minute == 5
    assert pitchfx_log.time_zone_name == "America/New_York"
    assert len(pitchfx_log.pitchfx_log) == 92
    return Result.Ok()
