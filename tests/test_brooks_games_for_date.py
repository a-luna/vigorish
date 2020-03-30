from datetime import datetime

from lxml import html

from vigorish.enums import DataSet
from vigorish.scrape.brooks_games_for_date.models.games_for_date import BrooksGamesForDate
from vigorish.scrape.brooks_games_for_date.parse_html import parse_brooks_dashboard_page

DATA_SET = DataSet.BROOKS_GAMES_FOR_DATE


def get_brooks_url_for_date(game_date):
    y = game_date.year
    m = game_date.month
    d = game_date.day
    return f"http://www.brooksbaseball.net/dashboard.php?dts={m}/{d}/{y}"


def scrape_brooks_games_for_date_from_html(db_session, scraped_data, html_path, game_date, url):
    result = scraped_data.get_bbref_games_for_date(game_date)
    assert result.success
    bbref_games_for_date = result.value
    html_text = html_path.read_text()
    page_content = html.fromstring(html_text, base_url=url)
    result = parse_brooks_dashboard_page(
        db_session, page_content, game_date, url, bbref_games_for_date
    )
    assert result.success
    brooks_games_for_date = result.value
    return brooks_games_for_date


def test_scrape_brooks_games_for_date(db_session, scraped_data):
    game_date = datetime(2018, 4, 17)
    brooks_url = get_brooks_url_for_date(game_date)
    expected_game_count = 16
    html_path = scraped_data.get_html(DATA_SET, game_date)
    brooks_games_for_date = scrape_brooks_games_for_date_from_html(
        db_session, scraped_data, html_path, game_date, brooks_url
    )
    assert isinstance(brooks_games_for_date, BrooksGamesForDate)
    assert brooks_games_for_date.dashboard_url == brooks_url
    assert brooks_games_for_date.game_date_str == game_date.strftime("%Y-%m-%d")
    assert brooks_games_for_date.game_count == expected_game_count
    assert len(brooks_games_for_date.games) == expected_game_count
