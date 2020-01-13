from datetime import date
from pathlib import Path
from string import Template

from lxml import html
from urllib.parse import urljoin

from app.main.constants import T_BBREF_DASH_URL
from app.main.scrape.bbref.models.games_for_date import BBRefGamesForDate
from app.main.util.decorators import timeout, retry, RetryLimitExceededError
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_2
from app.main.util.result import Result
from app.main.util.s3_helper import download_html_bbref_games_for_date
from app.main.util.scrape_functions import render_url
from app.main.util.string_functions import validate_bbref_game_id


DATA_SET = "bbref_games_for_date"

XPATH_BOXSCORE_URL_MAIN_CONTENT = (
    '//div[@id="content"]//div[contains(@class, "game_summaries")]'
    '//td[contains(@class, "gamelink")]/a/@href')

XPATH_BOXSCORE_URL_HEADER_NAV = (
    '//li[@id="header_scores"]//div[contains(@class, "game_summaries")]'
    '//td[contains(@class, "gamelink")]/a/@href')

def scrape_bbref_games_for_date(scrape_date, driver):
    url = get_dashboard_url_for_date(scrape_date)
    result = get_bbref_games_for_date_html_from_s3(scrape_date)
    if result.failure:
        result = request_bbref_games_for_date_html(driver, url)
        if result.failure:
            return result
    response = result.value
    try:
        result = parse_bbref_dashboard_page(response, scrape_date, url)
        return result
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def get_dashboard_url_for_date(scrape_date):
    m = scrape_date.month
    d = scrape_date.day
    y = scrape_date.year
    return Template(T_BBREF_DASH_URL).substitute(m=m, d=d, y=y)


def get_bbref_games_for_date_html_from_s3(scrape_date):
    result = download_html_bbref_games_for_date(scrape_date)
    if result.failure:
        return result
    html_path = result.value
    contents = html_path.read_text()
    response = html.fromstring(contents)
    html_path.unlink()
    return Result.Ok(response)


def request_bbref_games_for_date_html(driver, url):
    try:
        response = render_url(driver, url)
        return Result.Ok(response)
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def parse_bbref_dashboard_page(response, scrape_date, url):
    games_for_date = BBRefGamesForDate()
    games_for_date.game_date = scrape_date
    games_for_date.game_date_str = scrape_date.strftime(DATE_ONLY)
    games_for_date.dashboard_url = url
    boxscore_urls = response.xpath(XPATH_BOXSCORE_URL_MAIN_CONTENT)
    if not boxscore_urls:
        games_for_date.game_count = 0
        return Result.Ok(games_for_date)
    result = verify_boxscore_date(boxscore_urls, scrape_date, url)
    if result.failure:
        return result
    games_for_date.boxscore_urls = []
    for rel_url in boxscore_urls:
        if 'allstar' in rel_url:
            continue
        games_for_date.boxscore_urls.append(urljoin(url, rel_url))
    games_for_date.game_count = len(games_for_date.boxscore_urls)
    return Result.Ok(games_for_date)


def verify_boxscore_date(boxscore_urls, scrape_date, url):
    rel_url = boxscore_urls[0]
    if 'allstar' in rel_url:
        return Result.Ok()
    box_url = urljoin(url, rel_url)
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
