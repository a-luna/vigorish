import os
import pytest

from tests.conftest import CSV_FOLDER, DOTENV_FILE, CONFIG_FILE, SQLITE_URL
from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.app import Vigorish
from vigorish.tasks import AddToDatabaseTask, CombineScrapedDataTask

TEST_ID = "NO_ERRORS"
GAME_DATE = COMBINED_DATA_GAME_DICT[TEST_ID]["game_date"]
BBREF_GAME_ID = COMBINED_DATA_GAME_DICT[TEST_ID]["bbref_game_id"]
BB_GAME_ID = COMBINED_DATA_GAME_DICT[TEST_ID]["bb_game_id"]
APPPLY_PATCH_LIST = COMBINED_DATA_GAME_DICT[TEST_ID]["apply_patch_list"]


@pytest.fixture(autouse=True)
def env_vars(request):
    """Sets environment variables to use .env and config.json files."""
    os.environ["ENV"] = "TEST"
    os.environ["DOTENV_FILE"] = str(DOTENV_FILE)
    os.environ["CONFIG_FILE"] = str(CONFIG_FILE)
    os.environ["DATABASE_URL"] = SQLITE_URL
    return True


@pytest.fixture(scope="package")
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()

    def fin():
        app.db_session.close()

    request.addfinalizer(fin)
    return app


@pytest.fixture(scope="package", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_with_single_game package."""
    vig_app.initialize_database(csv_folder=CSV_FOLDER)
    assert vig_app.db_setup_complete
    update_scraped_bbref_games_for_date(vig_app, GAME_DATE)
    update_scraped_brooks_games_for_date(vig_app, GAME_DATE)
    update_scraped_boxscore(vig_app, BBREF_GAME_ID)
    update_scraped_pitch_logs(vig_app, GAME_DATE, BBREF_GAME_ID)
    update_scraped_pitchfx_logs(vig_app, BB_GAME_ID)
    CombineScrapedDataTask(vig_app).execute(BBREF_GAME_ID, APPPLY_PATCH_LIST)
    result = AddToDatabaseTask(vig_app).execute(2019)
    assert result.success
    return True
