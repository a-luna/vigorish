from tests.conftest import BACKUP_FOLDER, CSV_FOLDER
from tests.util import COMBINED_DATA_GAME_DICT, NO_ERRORS_PITCH_APP
from vigorish.database import DateScrapeStatus, GameScrapeStatus, PitchAppScrapeStatus, PitchFx
from vigorish.tasks import AddToDatabaseTask, BackupDatabaseTask, RestoreDatabaseTask
from vigorish.util.sys_helpers import zip_file_report

GAME_ID_DICT = COMBINED_DATA_GAME_DICT["NO_ERRORS"]
GAME_DATE = GAME_ID_DICT["game_date"]
BBREF_GAME_ID = GAME_ID_DICT["bbref_game_id"]


def test_restore_database(vig_app):
    total_rows = vig_app.get_total_number_of_rows(PitchFx)
    assert total_rows == 299
    result = AddToDatabaseTask(vig_app).execute()
    assert result.success
    total_rows = vig_app.get_total_number_of_rows(PitchFx)
    assert total_rows == 299

    remove_everything_in_backup_folder()
    result = BackupDatabaseTask(vig_app).execute()
    assert result.success
    zip_file = result.value
    assert zip_file.exists()
    report = zip_file_report(zip_file)
    assert "Filename.......: scrape_status_date.csv" in report
    assert "Filename.......: scrape_status_game.csv" in report
    assert "Filename.......: scrape_status_pitch_app.csv" in report
    assert "Filename.......: pitchfx.csv" in report

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
