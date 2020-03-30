from datetime import datetime

from lxml import html

from vigorish.enums import DataSet
from vigorish.scrape.bbref_games_for_date.models.games_for_date import BBRefGamesForDate
from vigorish.scrape.bbref_games_for_date.parse_html import parse_bbref_dashboard_page

DATA_SET = DataSet.BBREF_GAMES_FOR_DATE


def get_bbref_url_for_date(game_date):
    y = game_date.year
    m = game_date.month
    d = game_date.day
    return f"https://www.baseball-reference.com/boxes/?month={m}&day={d}&year={y}"


def scrape_bbref_games_for_date_from_html(html_path, game_date, url):
    html_text = html_path.read_text()
    page_content = html.fromstring(html_text, base_url=url)
    result = parse_bbref_dashboard_page(page_content, game_date, url)
    assert result.success
    games_for_date = result.value
    return games_for_date


def test_scrape_bbref_games_for_date(scraped_data):
    game_date = datetime(2018, 7, 26)
    bbref_url = get_bbref_url_for_date(game_date)
    expected_game_count = 11
    html_path = scraped_data.get_html(DATA_SET, game_date)
    games_for_date = scrape_bbref_games_for_date_from_html(html_path, game_date, bbref_url)
    assert isinstance(games_for_date, BBRefGamesForDate)
    assert games_for_date.dashboard_url == bbref_url
    assert games_for_date.game_date_str == game_date.strftime("%Y-%m-%d")
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


def test_persist_bbref_games_for_date(scraped_data):
    game_date = datetime(2018, 7, 26)
    bbref_url = get_bbref_url_for_date(game_date)
    expected_game_count = 11
    html_path = scraped_data.get_html(DATA_SET, game_date)
    games_for_date_parsed = scrape_bbref_games_for_date_from_html(html_path, game_date, bbref_url)
    assert isinstance(games_for_date_parsed, BBRefGamesForDate)
    result = scraped_data.save_json(DATA_SET, games_for_date_parsed)
    assert result.success
    json_filepath = result.value
    assert json_filepath.name == "bbref_games_for_date_2018-07-26.json"
    result = scraped_data.get_bbref_games_for_date(game_date)
    assert result.success
    games_for_date_decoded = result.value
    assert isinstance(games_for_date_decoded, BBRefGamesForDate)
    assert games_for_date_decoded.dashboard_url == bbref_url
    assert games_for_date_decoded.game_date_str == game_date.strftime("%Y-%m-%d")
    assert games_for_date_decoded.game_count == expected_game_count
    assert len(games_for_date_decoded.boxscore_urls) == 11
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
    assert games_for_date_decoded.boxscore_urls[0] == url0
    assert games_for_date_decoded.boxscore_urls[1] == url1
    assert games_for_date_decoded.boxscore_urls[2] == url2
    assert games_for_date_decoded.boxscore_urls[3] == url3
    assert games_for_date_decoded.boxscore_urls[4] == url4
    assert games_for_date_decoded.boxscore_urls[5] == url5
    assert games_for_date_decoded.boxscore_urls[6] == url6
    assert games_for_date_decoded.boxscore_urls[7] == url7
    assert games_for_date_decoded.boxscore_urls[8] == url8
    assert games_for_date_decoded.boxscore_urls[9] == url9
    assert games_for_date_decoded.boxscore_urls[10] == url10
