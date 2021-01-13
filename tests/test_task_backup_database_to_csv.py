import pytest

from tests.conftest import BACKUP_FOLDER
from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.tasks.add_to_database import AddToDatabaseTask
from vigorish.tasks.backup_database import BackupDatabaseTask
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask
from vigorish.util.sys_helpers import zip_file_report

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
    add_to_db = AddToDatabaseTask(vig_app)
    add_to_db.execute(vig_app.scraped_data.get_audit_report(), 2019)
    db_session.commit()
    return True


def test_backup_database_to_csv(vig_app):
    remove_everything_in_backup_folder()
    backup_db = BackupDatabaseTask(vig_app)
    result = backup_db.execute()
    assert result.success
    zip_file = result.value
    assert zip_file.exists()
    report = zip_file_report(zip_file)
    assert "Filename.......: scrape_status_date.csv" in report
    assert "Filename.......: scrape_status_game.csv" in report
    assert "Filename.......: scrape_status_pitch_app.csv" in report
    assert "Filename.......: pitchfx.csv" in report


def remove_everything_in_backup_folder():
    search_results = list(BACKUP_FOLDER.glob("*.zip"))
    if search_results:
        zip_file = search_results[0]
        zip_file.unlink()
    csv_folder = BACKUP_FOLDER.joinpath("__timestamp__")
    if not csv_folder.exists():
        return
    for csv_file in csv_folder.glob("*.csv"):
        csv_file.unlink()
    csv_folder.rmdir()
