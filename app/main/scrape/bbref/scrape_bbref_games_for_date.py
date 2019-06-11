from datetime import date
from pathlib import Path
from string import Template

from urllib.parse import urljoin

from app.main.constants import T_BBREF_DASH_URL
from app.main.scrape.bbref.models.games_for_date import BBRefGamesForDate
from app.main.util.decorators import RetryLimitExceededError
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_2
from app.main.util.result import Result
from app.main.util.scrape_functions import request_url
from app.main.util.string_functions import validate_bbref_game_id


DATA_SET = "bbref_games_for_date"

XPATH_BOXSCORE_URL_MAIN_CONTENT = (
    '//div[@id="content"]//div[contains(@class, "game_summaries")]'
    '//td[contains(@class, "gamelink")]/a/@href')

XPATH_BOXSCORE_URL_HEADER_NAV = (
    '//li[@id="header_scores"]//div[contains(@class, "game_summaries")]'
    '//td[contains(@class, "gamelink")]/a/@href')

def scrape_bbref_games_for_date(scrape_date):
    try:
        url = get_dashboard_url_for_date(scrape_date)
        response = request_url(url)
        return parse_bbref_dashboard_page(response, scrape_date, url)
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")

def get_dashboard_url_for_date(scrape_date):
    m = scrape_date.month
    d = scrape_date.day
    y = scrape_date.year
    return Template(T_BBREF_DASH_URL).substitute(m=m, d=d, y=y)

def parse_bbref_dashboard_page(response, scrape_date, url):
    games_for_date = BBRefGamesForDate()
    games_for_date.game_date = scrape_date
    games_for_date.game_date_str = scrape_date.strftime(DATE_ONLY)
    games_for_date.dashboard_url = url
    boxscore_urls = response.xpath(XPATH_BOXSCORE_URL_MAIN_CONTENT)
    if not boxscore_urls:
        games_for_date.game_count = 0
        return Result.Ok(games_for_date)
    result = verify_boxscore_urls(boxscore_urls, scrape_date, url)
    if result.failure:
        return result
    boxscore_urls = result.value
    games_for_date.boxscore_urls = []
    for rel_url in boxscore_urls:
        if 'allstar' in rel_url:
            continue
        games_for_date.boxscore_urls.append(urljoin(url, rel_url))
    games_for_date.game_count = len(games_for_date.boxscore_urls)
    return Result.Ok(games_for_date)

def verify_boxscore_urls(boxscore_urls, scrape_date, url):
    result = verify_boxscore_date(boxscore_urls, scrape_date, url)
    if result.success:
        return Result.Ok(boxscore_urls)
    if date.today() != scrape_date:
        return result
    boxscore_urls = response.xpath(XPATH_BOXSCORE_URL_HEADER_NAV)
    if not boxscore_urls:
        error = (
            "Unknown error occurred, failed to parse any boxscore URLs from BBref daily "
            f"dashboard page ({url})")
        return Result.Fail(error)
    result = verify_boxscore_date(boxscore_urls, scrape_date, url)
    if result.failure:
        return result
    return Result.Ok(boxscore_urls)

def verify_boxscore_date(boxscore_urls, scrape_date, url):
    box_url = urljoin(url, boxscore_urls[0])
    game_id = Path(box_url).stem
    result = validate_bbref_game_id(game_id)
    if result.failure:
        return result
    game_date = result.value['game_date']
    if not game_date == scrape_date.date():
        scrape_date_str = scrape_date.strftime(DATE_ONLY_2)
        error = (
            f"BBref daily dashboard URL for {scrape_date_str} redirected to game results "
            f"for the previous day. Please try again when boxscores for {scrape_date_str} are available."
        )
        return Result.Fail(error)
    return Result.Ok()
