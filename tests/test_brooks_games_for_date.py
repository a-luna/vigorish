from datetime import datetime

from lxml import html

from vigorish.enums import DataSet
from vigorish.scrape.brooks_games_for_date.models.games_for_date import BrooksGamesForDate
from vigorish.scrape.brooks_games_for_date.models.game_info import BrooksGameInfo
from vigorish.scrape.brooks_games_for_date.parse_html import parse_brooks_dashboard_page
from vigorish.util.result import Result

DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE
GAME_DATE = datetime(2018, 4, 17)


def get_brooks_url_for_date(game_date):
    y = game_date.year
    m = game_date.month
    d = game_date.day
    return f"http://www.brooksbaseball.net/dashboard.php?dts={m}/{d}/{y}"


def parse_brooks_games_for_date_from_html(db_session, scraped_data):
    result = scraped_data.get_bbref_games_for_date(GAME_DATE)
    assert result.success
    bbref_games_for_date = result.value
    url = get_brooks_url_for_date(GAME_DATE)
    html_path = scraped_data.get_html(DATA_SET, GAME_DATE)
    html_text = html_path.read_text()
    page_content = html.fromstring(html_text, base_url=url)
    result = parse_brooks_dashboard_page(
        db_session, page_content, GAME_DATE, url, bbref_games_for_date
    )
    assert result.success
    brooks_games_for_date = result.value
    return brooks_games_for_date


def test_parse_brooks_games_for_date(db_session, scraped_data):
    games_for_date = parse_brooks_games_for_date_from_html(db_session, scraped_data)
    assert isinstance(games_for_date, BrooksGamesForDate)
    result = verify_brooks_games_for_date_apr_17_2018(games_for_date)
    assert result.success


def test_persist_brooks_games_for_date(db_session, scraped_data):
    games_for_date_parsed = parse_brooks_games_for_date_from_html(db_session, scraped_data)
    assert isinstance(games_for_date_parsed, BrooksGamesForDate)
    result = scraped_data.save_json(DATA_SET, games_for_date_parsed)
    assert result.success
    json_filepath = result.value
    assert json_filepath.name == "brooks_games_for_date_2018-04-17.json"
    result = scraped_data.get_brooks_games_for_date(GAME_DATE)
    assert result.success
    games_for_date_decoded = result.value
    assert isinstance(games_for_date_decoded, BrooksGamesForDate)
    result = verify_brooks_games_for_date_apr_17_2018(games_for_date_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


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
    assert game0.game_time_hour == 3
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
    assert game8.game_time_hour == 7
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
    assert game15.game_time_hour == 10
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
    assert (
        g5_pitch_dict["543606"]
        == "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=543606.xml&game=gid_2018_04_17_clemlb_minmlb_1/&prevGame=gid_2018_04_17_clemlb_minmlb_1/"
    )

    g6_pitch_dict = games_for_date.games[6].pitcher_appearance_dict
    assert "630023" in g6_pitch_dict
    assert (
        g6_pitch_dict["630023"]
        == "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=630023.xml&game=gid_2018_04_17_texmlb_tbamlb_1/&prevGame=gid_2018_04_17_texmlb_tbamlb_1/"
    )

    g5_pitch_dict = games_for_date.games[5].pitcher_appearance_dict
    assert "543606" in g5_pitch_dict
    assert (
        g5_pitch_dict["543606"]
        == "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=543606.xml&game=gid_2018_04_17_clemlb_minmlb_1/&prevGame=gid_2018_04_17_clemlb_minmlb_1/"
    )

    g5_pitch_dict = games_for_date.games[5].pitcher_appearance_dict
    assert "543606" in g5_pitch_dict
    assert (
        g5_pitch_dict["543606"]
        == "http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year=2018&month=4&day=17&pitchSel=543606.xml&game=gid_2018_04_17_clemlb_minmlb_1/&prevGame=gid_2018_04_17_clemlb_minmlb_1/"
    )
    return Result.Ok()
