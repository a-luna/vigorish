import pytest

from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.database import PitchFx
from vigorish.tasks import AddToDatabaseTask
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask

TEST_ID = "NO_ERRORS"
GAME_DICT = COMBINED_DATA_GAME_DICT[TEST_ID]
GAME_DATE = GAME_DICT["game_date"]
BBREF_GAME_ID = GAME_DICT["bbref_game_id"]
BB_GAME_ID = GAME_DICT["bb_game_id"]
APPLY_PATCH_LIST = GAME_DICT["apply_patch_list"]


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in this module."""
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_boxscore(db_session, scraped_data, BBREF_GAME_ID)
    update_scraped_pitch_logs(db_session, scraped_data, GAME_DATE, BBREF_GAME_ID)
    update_scraped_pitchfx_logs(db_session, scraped_data, BB_GAME_ID)
    CombineScrapedDataTask(vig_app).execute(BBREF_GAME_ID, APPLY_PATCH_LIST)
    db_session.commit()
    return True


def test_add_data_to_database(vig_app):
    total_rows = vig_app.get_total_number_of_rows(PitchFx)
    assert total_rows == 0
    add_to_db = AddToDatabaseTask(vig_app)
    result = add_to_db.execute(2019)
    assert result.success
    total_rows = vig_app.get_total_number_of_rows(PitchFx)
    assert total_rows == 299
    result = add_to_db.execute()
    assert result.success
    total_rows = vig_app.get_total_number_of_rows(PitchFx)
    assert total_rows == 299
