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
from vigorish.tasks.add_pitchfx_to_database import AddPitchFxToDatabase
from vigorish.tasks.backup_database import BackupDatabaseTask


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_cli module."""
    db_session = vig_app["db_session"]
    scraped_data = vig_app["scraped_data"]
    game_id_dict = COMBINED_DATA_GAME_DICT["NO_ERRORS"]
    game_date = game_id_dict["game_date"]
    bbref_game_id = game_id_dict["bbref_game_id"]
    bb_game_id = game_id_dict["bb_game_id"]
    apply_patch_list = game_id_dict["apply_patch_list"]
    update_scraped_bbref_games_for_date(db_session, scraped_data, game_date)
    update_scraped_brooks_games_for_date(db_session, scraped_data, game_date)
    update_scraped_boxscore(db_session, scraped_data, bbref_game_id)
    update_scraped_pitch_logs(db_session, scraped_data, game_date, bbref_game_id)
    update_scraped_pitchfx_logs(db_session, scraped_data, bb_game_id)
    scraped_data.combine_boxscore_and_pfx_data(bbref_game_id, apply_patch_list)
    add_pfx_to_db = AddPitchFxToDatabase(vig_app)
    add_pfx_to_db.execute(vig_app["scraped_data"].get_audit_report(), 2019)
    db_session.commit()
    return True


def test_backup_database_to_csv(vig_app):
    remove_existing_zip_file()
    backup_db = BackupDatabaseTask(vig_app)
    result = backup_db.execute()
    assert result.success
    zip_file = result.value
    assert zip_file.exists()


def remove_existing_zip_file():
    search_results = [f for f in BACKUP_FOLDER.glob("*.zip")]
    if search_results:
        zip_file = search_results[0]
        zip_file.unlink()
