from tests.test_brooks_pitch_logs import BB_GAME_ID

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.data.scraped_data import ScrapedData
from tests.conftest import SQLITE_URL, DB_FILE, SQLITE_URL, CSV_FOLDER
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
from vigorish.config.database import (
    Base,
    DateScrapeStatus,
    GameScrapeStatus,
    get_total_number_of_rows,
    PitchAppScrapeStatus,
    PitchFx,
    prepare_database_for_restore,
)
from vigorish.data.scraped_data import ScrapedData
from vigorish.tasks.add_pitchfx_to_database import AddPitchFxToDatabase
from vigorish.tasks.backup_database import BackupDatabaseTask
from vigorish.tasks.restore_database import RestoreDatabaseTask

GAME_ID_DICT = COMBINED_DATA_GAME_DICT["NO_ERRORS"]
GAME_DATE = GAME_ID_DICT["game_date"]
BBREF_GAME_ID = GAME_ID_DICT["bbref_game_id"]
BB_GAME_ID = GAME_ID_DICT["bb_game_id"]
APPLY_PATCH_LIST = GAME_ID_DICT["apply_patch_list"]


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_cli module."""
    db_session = vig_app["db_session"]
    scraped_data = vig_app["scraped_data"]
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_boxscore(db_session, scraped_data, BBREF_GAME_ID)
    update_scraped_pitch_logs(db_session, scraped_data, GAME_DATE, BBREF_GAME_ID)
    update_scraped_pitchfx_logs(db_session, scraped_data, BB_GAME_ID)
    scraped_data.combine_boxscore_and_pfx_data(BBREF_GAME_ID, APPLY_PATCH_LIST)
    add_pfx_to_db = AddPitchFxToDatabase(vig_app)
    add_pfx_to_db.execute(vig_app["scraped_data"].get_audit_report(), 2019)
    remove_everything_in_backup_folder()
    backup_db = BackupDatabaseTask(vig_app)
    result = backup_db.execute()
    assert result.success
    zip_file = result.value
    assert zip_file.exists()
    db_session.commit()
    db_session.close()
    db_session = None
    DB_FILE.unlink()
    assert not DB_FILE.exists()
    return True


def test_restore_database(dotenv, config):
    db_engine = create_engine(SQLITE_URL)
    session_maker = sessionmaker(bind=db_engine)
    db_session = session_maker()
    scraped_data = ScrapedData(db_engine=db_engine, db_session=db_session, config=config)
    vig_app = {
        "dotenv": dotenv,
        "config": config,
        "db_engine": db_engine,
        "db_session": db_session,
        "scraped_data": scraped_data,
    }
    Base.metadata.drop_all(db_engine)
    Base.metadata.create_all(db_engine)
    result = prepare_database_for_restore(vig_app, CSV_FOLDER)
    assert result.success
    vig_app = result.value
    restore_db = RestoreDatabaseTask(vig_app)
    result = restore_db.execute()
    assert result.success
    db_session = vig_app["db_session"]
    status_date = DateScrapeStatus.find_by_date(db_session, GAME_DATE)
    assert status_date.scraped_daily_dash_bbref
    assert status_date.scraped_daily_dash_brooks
    assert status_date.game_count_bbref
    assert status_date.game_count_brooks
    status_game = GameScrapeStatus.find_by_bbref_game_id(db_session, BBREF_GAME_ID)
    assert status_game.scraped_bbref_boxscore == 1
    assert status_game.scraped_brooks_pitch_logs == 1
    assert status_game.combined_data_success == 1
    assert status_game.combined_data_fail == 0
    assert status_game.pitch_app_count_bbref == 12
    assert status_game.pitch_app_count_brooks == 12
    assert status_game.total_pitch_count_bbref == 298
    status_pitch_app = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, NO_ERRORS_PITCH_APP)
    assert status_pitch_app.scraped_pitchfx == 1
    assert status_pitch_app.no_pitchfx_data == 0
    assert status_pitch_app.combined_pitchfx_bbref_data == 1
    assert status_pitch_app.imported_pitchfx == 1
    row_count = get_total_number_of_rows(db_session, PitchFx)
    assert row_count == 299
    db_session.close()


def delete_all_rows(db_session, db_table):
    all_rows = db_session.query(db_table).all()
    for row in all_rows:
        db_session.delete(row)
    db_session.commit()
