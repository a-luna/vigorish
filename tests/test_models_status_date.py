from datetime import datetime

import pytest

from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.database import DateScrapeStatus
from vigorish.tasks import AddToDatabaseTask
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask

TEST_ID = "NO_ERRORS"
GAME_DICT = COMBINED_DATA_GAME_DICT[TEST_ID]
GAME_DATE = GAME_DICT["game_date"]


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_all_game_data module."""
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    game_date = GAME_DICT["game_date"]
    bbref_game_id = GAME_DICT["bbref_game_id"]
    bb_game_id = GAME_DICT["bb_game_id"]
    apply_patch_list = GAME_DICT["apply_patch_list"]
    update_scraped_bbref_games_for_date(db_session, scraped_data, game_date)
    update_scraped_brooks_games_for_date(db_session, scraped_data, game_date)
    update_scraped_boxscore(db_session, scraped_data, bbref_game_id)
    update_scraped_pitch_logs(db_session, scraped_data, game_date, bbref_game_id)
    update_scraped_pitchfx_logs(db_session, scraped_data, bb_game_id)
    CombineScrapedDataTask(vig_app).execute(bbref_game_id, apply_patch_list)
    add_to_db = AddToDatabaseTask(vig_app)
    add_to_db.execute(2019)
    db_session.commit()
    return True


def test_status_date_status_report(db_session):
    status_date = DateScrapeStatus.find_by_date(db_session, GAME_DATE)
    assert status_date
    assert status_date.game_date == GAME_DATE
    report = status_date.status_report()
    assert report == [
        ("Overall Status For Date......................: Missing BBref boxscores and Brooks pitch logs"),
        "Scraped Daily Dashboard (BBRef/Brooks).......: YES/YES",
        "BBref Boxscores Scraped......................: NO 1/11",
        "Brooks Games Scraped.........................: NO 1/11",
        "PitchFx Logs Scraped.........................: NO 12/12 (100%)",
        "Combined BBRef/PitchFX Data (Success/Total)..: NO 1/1",
        "Pitch App Count (BBRef/Brooks)...............: 12/81",
        "Pitch App Count (PFx/data/no data)...........: 12/12/0",
        "PitchFX Data Errors (Valid AB/Invalid AB)....: NO 0/0",
        "Pitch Count (BBRef/Brooks/PFx)...............: 298/299/299",
        "Pitch Count Audited (BBRef/PFx/Removed)......: 298/298/1",
    ]


def test_status_date_as_dict(db_session):
    status_date = DateScrapeStatus.find_by_date(db_session, GAME_DATE)
    assert status_date
    assert status_date.game_date == GAME_DATE
    date_dict = status_date.as_dict()
    assert date_dict
    assert date_dict == {
        "combined_data_for_all_pitchfx_logs": False,
        "game_count_bbref": 11,
        "game_count_brooks": 11,
        "game_date": datetime(2019, 6, 17, 0, 0),
        "game_date_str": "06/17/2019",
        "id": 20190617,
        "percent_complete_bbref_boxscores_scraped": "0%",
        "percent_complete_brooks_pitch_logs": "0%",
        "percent_complete_pitchfx_logs_scraped": 1.0,
        "pitch_app_count_bbref": 12,
        "pitch_app_count_brooks": 81,
        "pitch_app_count_pitchfx": 12,
        "pitchfx_error_for_any_pitchfx_logs": False,
        "pitchfx_is_valid_for_all_pitchfx_logs": True,
        "scrape_status_description": "Missing BBref boxscores and Brooks pitch logs",
        "scraped_all_bbref_boxscores": False,
        "scraped_all_brooks_pitch_logs": False,
        "scraped_all_game_data": False,
        "scraped_all_pitchfx_logs": False,
        "scraped_daily_dash_bbref": 1,
        "scraped_daily_dash_brooks": 1,
        "scraped_no_data": False,
        "scraped_only_bbref_boxscores": False,
        "scraped_only_bbref_daily_dash": False,
        "scraped_only_both_bbref_boxscores_and_brooks_pitch_logs": False,
        "scraped_only_both_daily_dash": True,
        "scraped_only_brooks_daily_dash": False,
        "scraped_only_brooks_pitch_logs": False,
        "season_id": 11,
        "total_at_bats_extra_pitchfx": 0,
        "total_at_bats_extra_pitchfx_removed": 0,
        "total_at_bats_invalid_pitchfx": 0,
        "total_at_bats_missing_pitchfx": 0,
        "total_at_bats_pitchfx_complete": 82,
        "total_at_bats_pitchfx_error": 0,
        "total_batters_faced_bbref": 82,
        "total_batters_faced_pitchfx": 82,
        "total_bbref_boxscores_scraped": 1,
        "total_brooks_pitch_logs_scraped": 1,
        "total_duplicate_pitchfx_removed_count": 0,
        "total_extra_pitchfx_count": 0,
        "total_extra_pitchfx_removed_count": 1,
        "total_games": 11,
        "total_games_combined": 1,
        "total_games_combined_fail": 0,
        "total_games_combined_success": 1,
        "total_missing_pitchfx_count": 0,
        "total_pitch_apps_combined_data": 12,
        "total_pitch_apps_invalid_pitchfx": 0,
        "total_pitch_apps_no_pitchfx_data": 0,
        "total_pitch_apps_pitchfx_error": 0,
        "total_pitch_apps_pitchfx_is_valid": 12,
        "total_pitch_apps_scraped_pitchfx": 12,
        "total_pitch_apps_with_pitchfx_data": 12,
        "total_pitch_count_bbref": 298,
        "total_pitch_count_bbref_audited": 298,
        "total_pitch_count_pitch_logs": 299,
        "total_pitch_count_pitchfx": 299,
        "total_pitch_count_pitchfx_audited": 298,
    }
