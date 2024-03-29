from tests.util import GAME_DATE_BR_DAILY as GAME_DATE
from tests.util import get_bbref_url_for_date, parse_bbref_games_for_date_from_html
from vigorish.database import DateScrapeStatus
from vigorish.enums import DataSet
from vigorish.scrape.bbref_games_for_date.models.games_for_date import BBRefGamesForDate
from vigorish.status.update_status_bbref_games_for_date import (
    update_bbref_games_for_date_single_date,
)
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID
from vigorish.util.result import Result


def test_parse_bbref_games_for_date(vig_app):
    games_for_date = parse_bbref_games_for_date_from_html(vig_app, GAME_DATE)
    assert isinstance(games_for_date, BBRefGamesForDate)
    result = verify_bbref_games_for_date_jul_26_2018(games_for_date)
    assert result.success


def test_persist_bbref_games_for_date(vig_app):
    games_for_date_parsed = parse_bbref_games_for_date_from_html(vig_app, GAME_DATE)
    assert isinstance(games_for_date_parsed, BBRefGamesForDate)
    result = vig_app.scraped_data.save_json(DataSet.BBREF_GAMES_FOR_DATE, games_for_date_parsed)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath.name == "bbref_games_for_date_2018-07-26.json"
    games_for_date_decoded = vig_app.scraped_data.get_bbref_games_for_date(GAME_DATE)
    assert isinstance(games_for_date_decoded, BBRefGamesForDate)
    result = verify_bbref_games_for_date_jul_26_2018(games_for_date_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def test_update_database_bbref_games_for_date(vig_app):
    games_for_date = parse_bbref_games_for_date_from_html(vig_app, GAME_DATE)
    assert isinstance(games_for_date, BBRefGamesForDate)
    date_status = vig_app.db_session.query(DateScrapeStatus).get(GAME_DATE.strftime(DATE_ONLY_TABLE_ID))
    assert date_status
    assert date_status.scraped_daily_dash_bbref == 0
    assert date_status.game_count_bbref == 0
    result = update_bbref_games_for_date_single_date(vig_app.db_session, games_for_date)
    assert result.success
    assert date_status.scraped_daily_dash_bbref == 1
    assert date_status.game_count_bbref == 11
    vig_app.db_session.commit()


def verify_bbref_games_for_date_jul_26_2018(games_for_date):
    bbref_url = get_bbref_url_for_date(GAME_DATE)
    expected_game_count = 11
    assert isinstance(games_for_date, BBRefGamesForDate)
    assert games_for_date.dashboard_url == bbref_url
    assert games_for_date.game_date_str == GAME_DATE.strftime("%Y-%m-%d")
    assert games_for_date.game_count == expected_game_count
    assert len(games_for_date.games) == expected_game_count
    assert len(games_for_date.all_urls) == expected_game_count
    url0 = "https://www.baseball-reference.com/boxes/ANA/ANA201807260.shtml"
    url1 = "https://www.baseball-reference.com/boxes/ATL/ATL201807260.shtml"
    url2 = "https://www.baseball-reference.com/boxes/BAL/BAL201807260.shtml"
    url3 = "https://www.baseball-reference.com/boxes/BOS/BOS201807260.shtml"
    url4 = "https://www.baseball-reference.com/boxes/CHN/CHN201807260.shtml"
    url5 = "https://www.baseball-reference.com/boxes/CIN/CIN201807260.shtml"
    url6 = "https://www.baseball-reference.com/boxes/MIA/MIA201807260.shtml"
    url7 = "https://www.baseball-reference.com/boxes/NYA/NYA201807260.shtml"
    url8 = "https://www.baseball-reference.com/boxes/PIT/PIT201807260.shtml"
    url9 = "https://www.baseball-reference.com/boxes/SFN/SFN201807260.shtml"
    url10 = "https://www.baseball-reference.com/boxes/TEX/TEX201807260.shtml"
    assert games_for_date.games[0].url == url0
    assert games_for_date.games[1].url == url1
    assert games_for_date.games[2].url == url2
    assert games_for_date.games[3].url == url3
    assert games_for_date.games[4].url == url4
    assert games_for_date.games[5].url == url5
    assert games_for_date.games[6].url == url6
    assert games_for_date.games[7].url == url7
    assert games_for_date.games[8].url == url8
    assert games_for_date.games[9].url == url9
    assert games_for_date.games[10].url == url10
    return Result.Ok()
