import pytest

from tests.util import GAME_DATE_PLOG as GAME_DATE
from tests.util import GAME_ID_PLOG as BBREF_GAME_ID
from tests.util import (
    parse_brooks_pitch_logs_for_game_from_html,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
)
from vigorish.config.database import GameScrapeStatus, PitchAppScrapeStatus
from vigorish.enums import DataSet
from vigorish.scrape.brooks_pitch_logs.models.pitch_logs_for_game import BrooksPitchLogsForGame
from vigorish.status.update_status_brooks_pitch_logs import update_status_brooks_pitch_logs_for_game
from vigorish.util.result import Result

DATA_SET = DataSet.BROOKS_PITCH_LOGS
BB_GAME_ID = "gid_2018_06_17_wasmlb_tormlb_1"
PITCH_APP_ID = "TOR201806170_461325"


@pytest.fixture(scope="module", autouse=True)
def create_test_data(db_session, scraped_data):
    """Initialize DB with data to verify test functions in test_brooks_pitch_logs module."""
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_boxscore(db_session, scraped_data, BBREF_GAME_ID)
    return True


def test_parse_brooks_pitch_logs_for_game(scraped_data):
    pitch_logs_for_game = parse_brooks_pitch_logs_for_game_from_html(
        scraped_data, GAME_DATE, BBREF_GAME_ID
    )
    assert isinstance(pitch_logs_for_game, BrooksPitchLogsForGame)
    result = verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game)
    assert result.success


def test_persist_brooks_pitch_logs_for_game(scraped_data):
    pitch_logs_for_game_parsed = parse_brooks_pitch_logs_for_game_from_html(
        scraped_data, GAME_DATE, BBREF_GAME_ID
    )
    assert isinstance(pitch_logs_for_game_parsed, BrooksPitchLogsForGame)
    result = verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game_parsed)
    result = scraped_data.save_json(DataSet.BROOKS_PITCH_LOGS, pitch_logs_for_game_parsed)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath.name == "gid_2018_06_17_wasmlb_tormlb_1.json"
    pitch_logs_for_game_decoded = scraped_data.get_brooks_pitch_logs_for_game(BB_GAME_ID)
    assert isinstance(pitch_logs_for_game_decoded, BrooksPitchLogsForGame)
    result = verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def test_update_database_brooks_pitch_logs_for_game(db_session, scraped_data):
    pitch_logs_for_game = parse_brooks_pitch_logs_for_game_from_html(
        scraped_data, GAME_DATE, BBREF_GAME_ID
    )
    assert isinstance(pitch_logs_for_game, BrooksPitchLogsForGame)
    game_status = GameScrapeStatus.find_by_bb_game_id(db_session, BB_GAME_ID)
    assert game_status
    assert game_status.scraped_brooks_pitch_logs == 0
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, PITCH_APP_ID)
    assert not pitch_app_status
    result = update_status_brooks_pitch_logs_for_game(db_session, pitch_logs_for_game)
    assert result.success
    assert game_status.scraped_brooks_pitch_logs == 1
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, PITCH_APP_ID)
    assert pitch_app_status
    assert pitch_app_status.pitch_count_pitch_log == 25
    db_session.commit()


def verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game):
    pitch_logs = pitch_logs_for_game.pitch_logs
    assert len(pitch_logs) == 11
    assert pitch_logs[0].pitcher_name == "Tanner Roark"
    assert pitch_logs[0].pitcher_id_mlb == 543699
    assert pitch_logs[0].total_pitch_count == 96
    assert pitch_logs[0].pitch_count_by_inning == {"1": 23, "2": 16, "3": 37, "4": 20}
    assert pitch_logs[9].pitcher_name == "Tyler Clippard"
    assert pitch_logs[9].pitcher_id_mlb == 461325
    assert pitch_logs[9].total_pitch_count == 25
    assert pitch_logs[9].pitch_count_by_inning == {"8": 25}
    return Result.Ok()
