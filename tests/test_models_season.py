from datetime import datetime, date
from unittest.mock import patch

import pytest

from tests.util import seed_database_with_2019_test_data
from vigorish.database import Season
from vigorish.enums import SeasonType

MLB_YEAR = 2019


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in this module."""
    seed_database_with_2019_test_data(vig_app)
    return True


def test_season_status_report(db_session):
    season = Season.find_by_year(db_session, MLB_YEAR)
    assert season
    assert season.year == MLB_YEAR
    assert season.season_type == SeasonType.REGULAR_SEASON
    report = season.status_report()
    assert "BBref Daily Dash Scraped.....................: 6/186 days (3%)" in report
    assert "Brooks Daily Dash Scraped....................: 6/186 days (3%)" in report
    assert "BBref Boxscores Scraped......................: NO 7/81" in report
    assert "Brooks Games Scraped.........................: NO 7/81" in report
    assert "PitchFx Logs Scraped.........................: NO 69/69 (100%)" in report
    assert "Combined BBRef/PitchFX Data (Success/Total)..: NO 7/7" in report
    assert "Pitch App Count (BBRef/Brooks)...............: 69/715" in report
    assert "Pitch App Count (PFx/data/no data)...........: 69/68/1" in report
    assert "PitchFX Data Errors (Valid AB/Invalid AB)....: NO 0/3" in report
    assert "Pitch Count (BBRef/Brooks/PFx)...............: 2,243/2,254/2,254" in report
    assert "Pitch Count Audited (BBRef/PFx/Removed)......: 2,243/2,173/62" in report


def test_season_as_dict(db_session):
    season = Season.find_by_year(db_session, MLB_YEAR)
    assert season
    assert season.year == MLB_YEAR
    assert season.season_type == SeasonType.REGULAR_SEASON
    season_dict = season.as_dict()
    assert season_dict
    check_dict = {
        "asg_date_str": "2019-07-09",
        "combined_data_for_all_pitchfx_logs": False,
        "end_date_str": "2019-09-29",
        "name": "MLB 2019 Regular Season",
        "percent_complete_bbref_boxscores_scraped": "0%",
        "percent_complete_bbref_games_for_date": 0.03225806451612903,
        "percent_complete_brooks_games_for_date": 0.03225806451612903,
        "percent_complete_brooks_pitch_logs": "0%",
        "percent_complete_pitchfx_logs_scraped": 1.0,
        "pitch_app_count_bbref": 69,
        "pitch_app_count_brooks": 715,
        "pitch_app_count_pitchfx": 69,
        "pitchfx_error_for_any_pitchfx_logs": False,
        "pitchfx_is_valid_for_all_pitchfx_logs": True,
        "scraped_all_bbref_boxscores": False,
        "scraped_all_bbref_games_for_date": False,
        "scraped_all_brooks_games_for_date": False,
        "scraped_all_brooks_pitch_logs": False,
        "scraped_all_pitchfx_logs": False,
        "start_date_str": "2019-03-28",
        "total_at_bats_extra_pitchfx": 0,
        "total_at_bats_extra_pitchfx_removed": 33,
        "total_at_bats_invalid_pitchfx": 6,
        "total_at_bats_missing_pitchfx": 18,
        "total_at_bats_pitchfx_error": 0,
        "total_batters_faced_bbref": 570,
        "total_batters_faced_pitchfx": 566,
        "total_bbref_boxscores_scraped": 7,
        "total_brooks_pitch_logs_scraped": 7,
        "total_days": 186,
        "total_days_scraped_bbref": 6,
        "total_days_scraped_brooks": 6,
        "total_duplicate_pitchfx_removed_count": 0,
        "total_extra_pitchfx_count": 0,
        "total_extra_pitchfx_removed_count": 62,
        "total_games": 81,
        "total_games_combined": 7,
        "total_games_combined_fail": 0,
        "total_games_combined_success": 7,
        "total_missing_pitchfx_count": 70,
        "total_pitch_apps_combined_data": 69,
        "total_pitch_apps_invalid_pitchfx": 3,
        "total_pitch_apps_no_pitchfx_data": 1,
        "total_pitch_apps_pitchfx_error": 0,
        "total_pitch_apps_pitchfx_is_valid": 66,
        "total_pitch_apps_scraped_pitchfx": 69,
        "total_pitch_apps_with_pitchfx_data": 68,
        "total_pitch_count_bbref": 2243,
        "total_pitch_count_bbref_audited": 2243,
        "total_pitch_count_pitch_logs": 2254,
        "total_pitch_count_pitchfx": 2254,
        "total_pitch_count_pitchfx_audited": 2173,
        "year": 2019,
    }

    assert check_dict == season_dict


def test_total_days_in_season(db_session, mocker):
    with patch("vigorish.models.season.date") as mock_date:
        mock_date.today.return_value = date(2019, 7, 10)
        s2019 = Season.find_by_year(db_session, 2019)
        assert s2019.total_days == 104


def test_total_days_not_in_season(db_session, mocker):
    with patch("vigorish.models.season.date") as mock_date:
        mock_date.today.return_value = date(2019, 12, 21)
        s2019 = Season.find_by_year(db_session, 2019)
        assert s2019.total_days == 186


def test_is_date_in_season(db_session):
    date_in_season = datetime(2019, 5, 25)
    result = Season.is_date_in_season(db_session, date_in_season)
    assert result.success
    season = result.value
    assert season.year == date_in_season.year

    date_not_in_season = datetime(2019, 1, 15)
    result = Season.is_date_in_season(db_session, date_not_in_season)
    assert result.failure
    assert "is not within the scope of the MLB 2019 Regular Season" in result.error

    invalid_year = datetime(1941, 12, 7)
    result = Season.is_date_in_season(db_session, invalid_year)
    assert result.failure
    assert "Database does not contain info for the MLB 1941 Regular Season" in result.error


def test_validate_date_range(db_session):
    start_date_2018 = datetime(2018, 6, 22)
    invalid_date_2019 = datetime(2019, 2, 24)
    start_date_2019 = datetime(2019, 5, 3)
    end_date_2019 = datetime(2019, 8, 11)

    result = Season.validate_date_range(db_session, start_date_2019, end_date_2019)
    assert result.success
    season = result.value
    assert season.year == 2019

    result = Season.validate_date_range(db_session, start_date_2018, end_date_2019)
    assert result.failure
    assert (
        "Start and end dates must both be in the same year and within the scope of that " "year's MLB Regular Season."
    ) in result.error

    result = Season.validate_date_range(db_session, end_date_2019, start_date_2019)
    assert result.failure
    assert '"start" must be a date before (or the same date as) "end":' in result.error

    result = Season.validate_date_range(db_session, invalid_date_2019, end_date_2019)
    assert result.failure
    assert "Start and end date must both be within the MLB 2019 Regular Season:" in result.error