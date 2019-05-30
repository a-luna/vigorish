from string import Template

from urllib.parse import urljoin

from app.main.constants import T_BBREF_DASH_URL
from app.main.scrape.bbref.models.games_for_date import BBRefGamesForDate
from app.main.util.decorators import RetryLimitExceededError
from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.result import Result
from app.main.util.scrape_functions import request_url


DATA_SET = "bbref_games_for_date"

XPATH_BOXSCORE_URL = (
    '//div[@id="content"]//div[@class="game_summaries"]'
    '//div[contains(@class, "game_summary")]//a[text()="Final"]/@href'
)

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

    boxscore_urls = response.xpath(XPATH_BOXSCORE_URL)
    if not boxscore_urls:
        games_for_date.game_count = 0
        return Result.Ok(games_for_date)

    games_for_date.boxscore_urls = []
    for rel_url in boxscore_urls:
        if 'allstar' in rel_url:
            continue
        box_url = urljoin(url, rel_url)
        games_for_date.boxscore_urls.append(box_url)
    games_for_date.game_count = len(games_for_date.boxscore_urls)
    return Result.Ok(games_for_date)
