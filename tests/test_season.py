import pytest

from tests.util import seed_database_with_2019_test_data
from vigorish.config.database import Season
from vigorish.enums import SeasonType

MLB_YEAR = 2019


@pytest.fixture(scope="module", autouse=True)
def create_test_data(db_session, scraped_data):
    """Initialize DB with data to verify test functions in test_cli module."""
    seed_database_with_2019_test_data(db_session, scraped_data)
    return True


def test_season_status_report(db_session):
    season = Season.find_by_year(db_session, MLB_YEAR)
    assert season
    assert season.year == MLB_YEAR
    assert season.season_type == SeasonType.REGULAR_SEASON
    report = season.status_report()
    assert "BBref Daily Dash Scraped.....................: 6/186 days (3%)" in report
    assert "Brooks Daily Dash Scraped....................: 6/186 days (3%)" in report
    assert "BBref Boxscores Scraped......................: NO 6/81" in report
    assert "Brooks Games Scraped.........................: NO 6/81" in report
    assert "PitchFx Logs Scraped.........................: NO 56/56 (100%)" in report
    assert "Combined BBRef/PitchFX Data (Success/Total)..: NO 6/6" in report
    assert "Pitch App Count (BBRef/Brooks)...............: 56/715" in report
    assert "Pitch App Count (PFx/data/no data)...........: 56/55/1" in report
    assert "PitchFX Data Errors (Valid AB/Invalid AB)....: NO 0/0" in report
    assert "Pitch Count (BBRef/Brooks/PFx)...............: 1899/1875/1875" in report
    assert "Pitch Count Audited (BBRef/PFx/Removed)......: 1899/1851/24" in report


def test_season_as_dict(db_session):
    season = Season.find_by_year(db_session, MLB_YEAR)
    assert season
    assert season.year == MLB_YEAR
    assert season.season_type == SeasonType.REGULAR_SEASON
    season_dict = season.as_dict()
    assert season_dict
    check_dict = {
        "name": "MLB 2019 Regular Season",
        "year": 2019,
        "start_date_str": "2019-03-28",
        "end_date_str": "2019-09-29",
        "asg_date_str": "2019-07-09",
        "total_days": 186,
        "total_days_scraped_bbref": 6,
        "percent_complete_bbref_games_for_date": 0.03225806451612903,
        "scraped_all_bbref_games_for_date": False,
        "total_days_scraped_brooks": 6,
        "percent_complete_brooks_games_for_date": 0.03225806451612903,
        "scraped_all_brooks_games_for_date": False,
        "total_games": 81,
        "total_games_combined_success": 6,
        "total_games_combined_fail": 0,
        "total_games_combined": 6,
        "total_bbref_boxscores_scraped": 6,
        "percent_complete_bbref_boxscores_scraped": "0%",
        "scraped_all_bbref_boxscores": False,
        "total_brooks_pitch_logs_scraped": 6,
        "percent_complete_brooks_pitch_logs": "0%",
        "scraped_all_brooks_pitch_logs": False,
        "pitch_app_count_bbref": 56,
        "pitch_app_count_brooks": 715,
        "total_pitch_count_bbref": 1899,
        "pitch_app_count_pitchfx": 56,
        "total_pitch_apps_scraped_pitchfx": 56,
        "total_pitch_apps_no_pitchfx_data": 1,
        "total_pitch_apps_with_pitchfx_data": 55,
        "total_pitch_apps_combined_data": 56,
        "total_pitch_apps_pitchfx_error": 0,
        "total_pitch_apps_invalid_pitchfx": 0,
        "total_pitch_apps_pitchfx_is_valid": 56,
        "total_pitch_count_pitch_logs": 1875,
        "total_pitch_count_bbref_audited": 1899,
        "total_pitch_count_pitchfx": 1875,
        "total_pitch_count_pitchfx_audited": 1851,
        "total_duplicate_pitchfx_removed_count": 22,
        "total_missing_pitchfx_count": 48,
        "total_extra_pitchfx_count": 0,
        "total_extra_pitchfx_removed_count": 2,
        "total_batters_faced_bbref": 484,
        "total_batters_faced_pitchfx": 480,
        "total_at_bats_missing_pitchfx": 12,
        "total_at_bats_extra_pitchfx": 0,
        "total_at_bats_extra_pitchfx_removed": 2,
        "total_at_bats_pitchfx_error": 0,
        "total_at_bats_invalid_pitchfx": 0,
        "scraped_all_pitchfx_logs": False,
        "combined_data_for_all_pitchfx_logs": False,
        "pitchfx_error_for_any_pitchfx_logs": False,
        "pitchfx_is_valid_for_all_pitchfx_logs": True,
        "percent_complete_pitchfx_logs_scraped": 1.0,
    }
    assert check_dict == season_dict
