from datetime import datetime

from lxml import html

from vigorish.enums import DataSet
from vigorish.scrape.bbref_games_for_date.models.games_for_date import BBRefGamesForDate
from vigorish.scrape.bbref_games_for_date.parse_html import parse_bbref_dashboard_page
from vigorish.util.result import Result

DATA_SET = DataSet.BBREF_GAMES_FOR_DATE
GAME_DATE = datetime(2018, 7, 26)


def get_bbref_url_for_date():
    y = GAME_DATE.year
    m = GAME_DATE.month
    d = GAME_DATE.day
    return f"https://www.baseball-reference.com/boxes/?month={m}&day={d}&year={y}"


def parse_bbref_games_for_date_from_html(scraped_data):
    bbref_url = get_bbref_url_for_date()
    html_path = scraped_data.get_html(DATA_SET, GAME_DATE)
    html_text = html_path.read_text()
    page_content = html.fromstring(html_text, base_url=bbref_url)
    result = parse_bbref_dashboard_page(page_content, GAME_DATE, bbref_url)
    assert result.success
    games_for_date = result.value
    return games_for_date


def test_parse_bbref_games_for_date(scraped_data):
    games_for_date = parse_bbref_games_for_date_from_html(scraped_data)
    assert isinstance(games_for_date, BBRefGamesForDate)
    result = verify_bbref_games_for_date_jul_26_2018(games_for_date)
    assert result.success


def test_persist_bbref_games_for_date(scraped_data):
    games_for_date_parsed = parse_bbref_games_for_date_from_html(scraped_data)
    assert isinstance(games_for_date_parsed, BBRefGamesForDate)
    result = scraped_data.save_json(DATA_SET, games_for_date_parsed)
    assert result.success
    json_filepath = result.value
    assert json_filepath.name == "bbref_games_for_date_2018-07-26.json"
    result = scraped_data.get_bbref_games_for_date(GAME_DATE)
    assert result.success
    games_for_date_decoded = result.value
    assert isinstance(games_for_date_decoded, BBRefGamesForDate)
    result = verify_bbref_games_for_date_jul_26_2018(games_for_date_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def verify_bbref_games_for_date_jul_26_2018(games_for_date):
    bbref_url = get_bbref_url_for_date()
    expected_game_count = 11
    assert isinstance(games_for_date, BBRefGamesForDate)
    assert games_for_date.dashboard_url == bbref_url
    assert games_for_date.game_date_str == GAME_DATE.strftime("%Y-%m-%d")
    assert games_for_date.game_count == expected_game_count
    assert len(games_for_date.boxscore_urls) == expected_game_count
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
    assert games_for_date.boxscore_urls[0] == url0
    assert games_for_date.boxscore_urls[1] == url1
    assert games_for_date.boxscore_urls[2] == url2
    assert games_for_date.boxscore_urls[3] == url3
    assert games_for_date.boxscore_urls[4] == url4
    assert games_for_date.boxscore_urls[5] == url5
    assert games_for_date.boxscore_urls[6] == url6
    assert games_for_date.boxscore_urls[7] == url7
    assert games_for_date.boxscore_urls[8] == url8
    assert games_for_date.boxscore_urls[9] == url9
    assert games_for_date.boxscore_urls[10] == url10
    return Result.Ok()
