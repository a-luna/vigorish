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
from vigorish.config.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    PitchFx,
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
    remove_existing_csv_files()
    backup_db = BackupDatabaseTask(vig_app)
    result = backup_db.execute()
    assert result.success
    csv_map = result.value

    date_csv = csv_map[DateScrapeStatus]
    assert "scrape_status_date.csv" in str(date_csv)
    assert date_csv.exists()

    game_csv = csv_map[GameScrapeStatus]
    assert "scrape_status_game.csv" in str(game_csv)
    assert game_csv.exists()

    pitch_app_csv = csv_map[PitchAppScrapeStatus]
    assert "scrape_status_pitch_app.csv" in str(pitch_app_csv)
    assert pitch_app_csv.exists()

    pitchfx_csv = csv_map[PitchFx]
    assert "pitchfx.csv" in str(pitchfx_csv)
    assert pitchfx_csv.exists()
    raise AssertionError()


def remove_existing_csv_files():
    date_csv = BACKUP_FOLDER.joinpath("scrape_status_date.csv")
    if date_csv.exists():
        date_csv.unlink()
    game_csv = BACKUP_FOLDER.joinpath("scrape_status_game.csv")
    if game_csv.exists():
        game_csv.unlink()
    pitch_app_csv = BACKUP_FOLDER.joinpath("scrape_status_pitch_app.csv")
    if pitch_app_csv.exists():
        pitch_app_csv.unlink()
    pitchfx_csv = BACKUP_FOLDER.joinpath("pitchfx.csv")
    if pitchfx_csv.exists():
        pitchfx_csv.unlink()
