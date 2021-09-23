import pytest

import vigorish.database as db
from tests.conftest import BACKUP_FOLDER, CSV_FOLDER, JSON_FOLDER, TESTS_FOLDER
from tests.util import (
    COMBINED_DATA_GAME_DICT,
    NO_ERRORS_PITCH_APP,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.app import Vigorish
from vigorish.cli.components.viewers.page_viewer import PageViewer
from vigorish.tasks import AddToDatabaseTask, BackupDatabaseTask, RestoreDatabaseTask
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask
from vigorish.util.sys_helpers import zip_file_report

TEST_ID = "NO_ERRORS"
GAME_DATE = COMBINED_DATA_GAME_DICT[TEST_ID]["game_date"]
BBREF_GAME_ID = COMBINED_DATA_GAME_DICT[TEST_ID]["bbref_game_id"]
BB_GAME_ID = COMBINED_DATA_GAME_DICT[TEST_ID]["bb_game_id"]
APPPLY_PATCH_LIST = COMBINED_DATA_GAME_DICT[TEST_ID]["apply_patch_list"]


@pytest.fixture()
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()
    app.initialize_database(csv_folder=CSV_FOLDER, json_folder=JSON_FOLDER)
    assert app.db_setup_complete
    update_scraped_bbref_games_for_date(app, GAME_DATE)
    update_scraped_brooks_games_for_date(app, GAME_DATE)
    update_scraped_boxscore(app, BBREF_GAME_ID)
    update_scraped_pitch_logs(app, GAME_DATE, BBREF_GAME_ID)
    update_scraped_pitchfx_logs(app, BB_GAME_ID)
    combine_data_result_dict = CombineScrapedDataTask(app).execute(BBREF_GAME_ID, APPPLY_PATCH_LIST)
    assert combine_data_result_dict["gather_scraped_data_success"]
    add_to_db_result = AddToDatabaseTask(app).execute(2019)
    assert add_to_db_result.success

    def fin():
        app.db_session.close()
        for file in TESTS_FOLDER.glob("vig_*.db"):
            file.unlink()

    request.addfinalizer(fin)
    return app


@pytest.mark.skip(reason="no way of currently testing this")
def test_restore_database(vig_app):
    total_rows = get_total_number_of_rows(vig_app, db.PitchFx)
    assert total_rows == 298
    result = AddToDatabaseTask(vig_app).execute()
    assert result.success
    total_rows = get_total_number_of_rows(vig_app, db.PitchFx)
    assert total_rows == 298

    remove_everything_in_backup_folder()
    result = BackupDatabaseTask(vig_app).execute()
    assert result.success
    zip_file = result.value
    assert zip_file.exists()
    report = zip_file_report(zip_file)
    assert isinstance(report, PageViewer)

    result = RestoreDatabaseTask(vig_app).execute(csv_folder=CSV_FOLDER)
    assert result.success
    status_date = db.DateScrapeStatus.find_by_date(vig_app.db_session, GAME_DATE)
    assert status_date.scraped_daily_dash_bbref
    assert status_date.scraped_daily_dash_brooks
    assert status_date.game_count_bbref
    assert status_date.game_count_brooks
    status_game = db.GameScrapeStatus.find_by_bbref_game_id(vig_app.db_session, BBREF_GAME_ID)
    assert status_game.scraped_bbref_boxscore == 1
    assert status_game.scraped_brooks_pitch_logs == 1
    assert status_game.combined_data_success == 1
    assert status_game.combined_data_fail == 0
    assert status_game.pitch_app_count_bbref == 12
    assert status_game.pitch_app_count_brooks == 12
    assert status_game.total_pitch_count_bbref == 298
    status_pitch_app = db.PitchAppScrapeStatus.find_by_pitch_app_id(vig_app.db_session, NO_ERRORS_PITCH_APP)
    assert status_pitch_app.scraped_pitchfx == 1
    assert status_pitch_app.no_pitchfx_data == 0
    assert status_pitch_app.combined_pitchfx_bbref_data == 1
    assert status_pitch_app.imported_pitchfx == 1
    row_count = get_total_number_of_rows(vig_app, db.PitchFx)
    assert row_count == 298
    vig_app.db_session.close()


def get_total_number_of_rows(vig_app, db_table):
    return vig_app.db_session.query(db_table).count()


def remove_everything_in_backup_folder():
    search_results = list(BACKUP_FOLDER.glob("__timestamp__.zip"))
    if search_results:
        zip_file = search_results[0]
        zip_file.unlink()
    csv_folder = BACKUP_FOLDER.joinpath("__timestamp__")
    if not csv_folder.exists():
        return
    for csv_file in csv_folder.glob("*.csv"):
        csv_file.unlink()
    csv_folder.rmdir()
