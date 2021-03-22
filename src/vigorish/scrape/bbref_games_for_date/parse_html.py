from urllib.parse import urljoin

from lxml import html

from vigorish.scrape.bbref_games_for_date.models.game_info import BBRefGameInfo
from vigorish.scrape.bbref_games_for_date.models.games_for_date import BBRefGamesForDate
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_2
from vigorish.util.result import Result
from vigorish.util.string_helpers import get_bbref_game_id_from_url, validate_bbref_game_id

XPATH_BOXSCORE_URL_MAIN_CONTENT = (
    '//div[@id="content"]//div[contains(@class, "game_summaries")]' '//td[contains(@class, "gamelink")]/a/@href'
)

XPATH_BOXSCORE_URL_HEADER_NAV = (
    '//li[@id="header_scores"]//div[contains(@class, "game_summaries")]' '//td[contains(@class, "gamelink")]/a/@href'
)


def parse_bbref_dashboard_page(scraped_html, game_date, url):
    page_content = html.fromstring(scraped_html, base_url=url)
    games_for_date = BBRefGamesForDate()
    games_for_date.game_date = game_date
    games_for_date.game_date_str = game_date.strftime(DATE_ONLY)
    games_for_date.dashboard_url = url
    boxscore_urls = page_content.xpath(XPATH_BOXSCORE_URL_MAIN_CONTENT)
    if not boxscore_urls:
        games_for_date.game_count = 0
        return Result.Ok(games_for_date)
    result = verify_boxscore_date(boxscore_urls, game_date, url)
    if result.failure:
        return result
    games_for_date.games = []
    for rel_url in boxscore_urls:
        if "allstar" in rel_url:
            continue
        boxscore_url = urljoin(url, rel_url)
        game_id = get_bbref_game_id_from_url(boxscore_url)
        game_info = BBRefGameInfo(url=boxscore_url, bbref_game_id=game_id)
        games_for_date.games.append(game_info)
    games_for_date.game_count = len(games_for_date.games)
    return Result.Ok(games_for_date)


def verify_boxscore_date(boxscore_urls, game_date, url):
    rel_url = boxscore_urls[0]
    if "allstar" in rel_url:
        return Result.Ok()
    box_url = urljoin(url, rel_url)
    game_id = get_bbref_game_id_from_url(box_url)
    if not game_id:
        return Result.Fail(f"Failed to parse game id from url: {box_url}")
    result = validate_bbref_game_id(game_id)
    if result.failure:
        return result
    game_date_id = result.value["game_date"]
    if game_date_id != game_date.date():
        scrape_date_str = game_date.strftime(DATE_ONLY_2)
        error = (
            f"BBref daily dashboard URL for {scrape_date_str} redirected to game results for the "
            f"previous day. Please try again when boxscores for {scrape_date_str} are available."
        )
        return Result.Fail(error)
    return Result.Ok()
