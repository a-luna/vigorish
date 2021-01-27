import pytest

from tests.conftest import CSV_FOLDER
from tests.test_task_backup_database_to_csv import remove_everything_in_backup_folder
from tests.util import (
    COMBINED_DATA_GAME_DICT,
    NO_ERRORS_PITCH_APP,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    PitchFx,
)
from vigorish.tasks import AddToDatabaseTask, BackupDatabaseTask, CombineScrapedDataTask, RestoreDatabaseTask

GAME_ID_DICT = COMBINED_DATA_GAME_DICT["NO_ERRORS"]
GAME_DATE = GAME_ID_DICT["game_date"]
BBREF_GAME_ID = GAME_ID_DICT["bbref_game_id"]
BB_GAME_ID = GAME_ID_DICT["bb_game_id"]
APPLY_PATCH_LIST = GAME_ID_DICT["apply_patch_list"]


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
    add_to_db = AddToDatabaseTask(vig_app)
    add_to_db.execute(2019)
    remove_everything_in_backup_folder()
    backup_db = BackupDatabaseTask(vig_app)
    result = backup_db.execute()
    assert result.success
    zip_file = result.value
    assert zip_file.exists()
    return True


def test_restore_database(vig_app):
    result = RestoreDatabaseTask(vig_app).execute(csv_folder=CSV_FOLDER)
    assert result.success
    status_date = DateScrapeStatus.find_by_date(vig_app.db_session, GAME_DATE)
    assert status_date.scraped_daily_dash_bbref
    assert status_date.scraped_daily_dash_brooks
    assert status_date.game_count_bbref
    assert status_date.game_count_brooks
    status_game = GameScrapeStatus.find_by_bbref_game_id(vig_app.db_session, BBREF_GAME_ID)
    assert status_game.scraped_bbref_boxscore == 1
    assert status_game.scraped_brooks_pitch_logs == 1
    assert status_game.combined_data_success == 1
    assert status_game.combined_data_fail == 0
    assert status_game.pitch_app_count_bbref == 12
    assert status_game.pitch_app_count_brooks == 12
    assert status_game.total_pitch_count_bbref == 298
    status_pitch_app = PitchAppScrapeStatus.find_by_pitch_app_id(vig_app.db_session, NO_ERRORS_PITCH_APP)
    assert status_pitch_app.scraped_pitchfx == 1
    assert status_pitch_app.no_pitchfx_data == 0
    assert status_pitch_app.combined_pitchfx_bbref_data == 1
    assert status_pitch_app.imported_pitchfx == 1
    row_count = vig_app.get_total_number_of_rows(PitchFx)
    assert row_count == 299
    vig_app.db_session.close()
