import pytest

from tests.conftest import CSV_FOLDER, JSON_FOLDER, TESTS_FOLDER
from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.app import Vigorish
from vigorish.tasks import AddToDatabaseTask
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask

TEST_ID = "NO_ERRORS"
GAME_DATE = COMBINED_DATA_GAME_DICT[TEST_ID]["game_date"]
BBREF_GAME_ID = COMBINED_DATA_GAME_DICT[TEST_ID]["bbref_game_id"]
BB_GAME_ID = COMBINED_DATA_GAME_DICT[TEST_ID]["bb_game_id"]
APPPLY_PATCH_LIST = COMBINED_DATA_GAME_DICT[TEST_ID]["apply_patch_list"]


@pytest.fixture(scope="package")
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()

    def fin():
        app.db_session.close()
        for file in TESTS_FOLDER.glob("vig_*.db"):
            file.unlink()

    request.addfinalizer(fin)
    return app


@pytest.fixture(scope="package", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_with_single_game package."""
    vig_app.initialize_database(csv_folder=CSV_FOLDER, json_folder=JSON_FOLDER)
    assert vig_app.db_setup_complete
    update_scraped_bbref_games_for_date(vig_app, GAME_DATE)
    update_scraped_brooks_games_for_date(vig_app, GAME_DATE)
    update_scraped_boxscore(vig_app, BBREF_GAME_ID)
    update_scraped_pitch_logs(vig_app, GAME_DATE, BBREF_GAME_ID)
    update_scraped_pitchfx_logs(vig_app, BB_GAME_ID)
    combine_data_result_dict = CombineScrapedDataTask(vig_app).execute(BBREF_GAME_ID, APPPLY_PATCH_LIST)
    assert combine_data_result_dict["gather_scraped_data_success"]
    add_to_db_result = AddToDatabaseTask(vig_app).execute(2019)
    assert add_to_db_result.success
    return True
