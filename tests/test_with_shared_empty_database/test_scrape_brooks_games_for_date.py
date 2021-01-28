import pytest

from tests.util import GAME_DATE_BB_DAILY as GAME_DATE
from tests.util import (
    get_brooks_url_for_date,
    parse_brooks_games_for_date_from_html,
    update_scraped_bbref_games_for_date,
)
from vigorish.database import DateScrapeStatus, GameScrapeStatus, Season
from vigorish.enums import DataSet
from vigorish.scrape.brooks_games_for_date.models.game_info import BrooksGameInfo
from vigorish.scrape.brooks_games_for_date.models.games_for_date import BrooksGamesForDate
from vigorish.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_single_date,
)
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID
from vigorish.util.result import Result

GAME_ID = "gid_2018_04_17_lanmlb_sdnmlb_1"


@pytest.fixture(scope="module", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_brooks_games_for_date module."""
    update_scraped_bbref_games_for_date(vig_app, GAME_DATE)
    return True


def test_parse_brooks_games_for_date(vig_app):
    games_for_date = parse_brooks_games_for_date_from_html(vig_app, GAME_DATE)
    assert isinstance(games_for_date, BrooksGamesForDate)
    result = verify_brooks_games_for_date_apr_17_2018(games_for_date)
    assert result.success


def test_persist_brooks_games_for_date(vig_app):
    games_for_date_parsed = parse_brooks_games_for_date_from_html(vig_app, GAME_DATE)
    assert isinstance(games_for_date_parsed, BrooksGamesForDate)
    result = vig_app.scraped_data.save_json(DataSet.BROOKS_GAMES_FOR_DATE, games_for_date_parsed)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath.name == "brooks_games_for_date_2018-04-17.json"
    games_for_date_decoded = vig_app.scraped_data.get_brooks_games_for_date(GAME_DATE)
    assert isinstance(games_for_date_decoded, BrooksGamesForDate)
    result = verify_brooks_games_for_date_apr_17_2018(games_for_date_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def test_update_database_brooks_games_for_date(vig_app):
    games_for_date = parse_brooks_games_for_date_from_html(vig_app, GAME_DATE)
    assert isinstance(games_for_date, BrooksGamesForDate)
    date_status = vig_app.db_session.query(DateScrapeStatus).get(GAME_DATE.strftime(DATE_ONLY_TABLE_ID))
    assert date_status
    assert date_status.scraped_daily_dash_brooks == 0
    assert date_status.game_count_brooks == 0
    game_status = GameScrapeStatus.find_by_bb_game_id(vig_app.db_session, GAME_ID)
    assert not game_status
    result = Season.is_date_in_season(vig_app.db_session, GAME_DATE)
    assert result.success
    season = result.value
    result = update_brooks_games_for_date_single_date(vig_app.db_session, season, games_for_date)
    assert result.success
    assert date_status.scraped_daily_dash_brooks == 1
    assert date_status.game_count_brooks == 16
    game_status = GameScrapeStatus.find_by_bb_game_id(vig_app.db_session, GAME_ID)
    assert game_status
    assert game_status.game_time_hour == 22
    assert game_status.game_time_minute == 10
    assert game_status.game_time_zone == "America/New_York"
    assert game_status.pitch_app_count_brooks == 15
    vig_app.db_session.commit()


def verify_brooks_games_for_date_apr_17_2018(games_for_date):
    brooks_url = get_brooks_url_for_date(GAME_DATE)
    expected_game_count = 16
    assert isinstance(games_for_date, BrooksGamesForDate)
    assert games_for_date.dashboard_url == brooks_url
    assert games_for_date.game_date_str == GAME_DATE.strftime("%Y-%m-%d")
    assert games_for_date.game_count == expected_game_count
    assert len(games_for_date.games) == expected_game_count

    game0 = games_for_date.games[0]
    assert isinstance(game0, BrooksGameInfo)
    assert game0.game_date_year == 2018
    assert game0.game_date_month == 4
    assert game0.game_date_day == 17
    assert game0.game_time_hour == 15
    assert game0.game_time_minute == 7
    assert game0.time_zone_name == "America/New_York"
    assert game0.bb_game_id == "gid_2018_04_17_kcamlb_tormlb_1"
    assert game0.away_team_id_bb == "KCA"
    assert game0.home_team_id_bb == "TOR"
    assert game0.game_number_this_day == 1
    assert game0.pitcher_appearance_count == 7
    assert len(game0.pitcher_appearance_dict) == 7

    game1 = games_for_date.games[1]
    assert isinstance(game1, BrooksGameInfo)
    assert game1.game_date_year == 2018
    assert game1.game_date_month == 4
    assert game1.game_date_day == 17
    assert game1.game_time_hour == 0
    assert game1.game_time_minute == 0
    assert game1.time_zone_name == "America/New_York"
    assert game1.bb_game_id == "gid_2018_04_17_kcamlb_tormlb_2"
    assert game1.away_team_id_bb == "KCA"
    assert game1.home_team_id_bb == "TOR"
    assert game1.game_number_this_day == 2
    assert game1.pitcher_appearance_count == 12
    assert len(game1.pitcher_appearance_dict) == 12

    game8 = games_for_date.games[8]
    assert isinstance(game8, BrooksGameInfo)
    assert game8.game_date_year == 2018
    assert game8.game_date_month == 4
    assert game8.game_date_day == 17
    assert game8.game_time_hour == 19
    assert game8.game_time_minute == 35
    assert game8.time_zone_name == "America/New_York"
    assert game8.bb_game_id == "gid_2018_04_17_phimlb_atlmlb_1"
    assert game8.away_team_id_bb == "PHI"
    assert game8.home_team_id_bb == "ATL"
    assert game8.game_number_this_day == 1
    assert game8.pitcher_appearance_count == 13
    assert len(game8.pitcher_appearance_dict) == 13

    game15 = games_for_date.games[15]
    assert isinstance(game15, BrooksGameInfo)
    assert game15.game_date_year == 2018
    assert game15.game_date_month == 4
    assert game15.game_date_day == 17
    assert game15.game_time_hour == 22
    assert game15.game_time_minute == 10
    assert game15.time_zone_name == "America/New_York"
    assert game15.bb_game_id == "gid_2018_04_17_lanmlb_sdnmlb_1"
    assert game15.away_team_id_bb == "LAN"
    assert game15.home_team_id_bb == "SDN"
    assert game15.game_number_this_day == 1
    assert game15.pitcher_appearance_count == 15
    assert len(game15.pitcher_appearance_dict) == 15

    g5_pitch_dict = games_for_date.games[5].pitcher_appearance_dict
    assert "543606" in g5_pitch_dict
    assert g5_pitch_dict["543606"] == (
        "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0"
        "&year=2018&month=4&day=17&pitchSel=543606.xml&game=gid_2018_04_17_clemlb_minmlb_1/"
        "&prevGame=gid_2018_04_17_clemlb_minmlb_1/"
    )

    g6_pitch_dict = games_for_date.games[6].pitcher_appearance_dict
    assert "630023" in g6_pitch_dict
    assert g6_pitch_dict["630023"] == (
        "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0"
        "&year=2018&month=4&day=17&pitchSel=630023.xml&game=gid_2018_04_17_texmlb_tbamlb_1/"
        "&prevGame=gid_2018_04_17_texmlb_tbamlb_1/"
    )

    g5_pitch_dict = games_for_date.games[5].pitcher_appearance_dict
    assert "543606" in g5_pitch_dict
    assert g5_pitch_dict["543606"] == (
        "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0"
        "&year=2018&month=4&day=17&pitchSel=543606.xml&game=gid_2018_04_17_clemlb_minmlb_1/"
        "&prevGame=gid_2018_04_17_clemlb_minmlb_1/"
    )

    g5_pitch_dict = games_for_date.games[5].pitcher_appearance_dict
    assert "543606" in g5_pitch_dict
    assert g5_pitch_dict["543606"] == (
        "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0"
        "&year=2018&month=4&day=17&pitchSel=543606.xml&game=gid_2018_04_17_clemlb_minmlb_1/"
        "&prevGame=gid_2018_04_17_clemlb_minmlb_1/"
    )
    return Result.Ok()
