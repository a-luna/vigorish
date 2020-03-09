"""Scrape brooksbaseball daily dashboard page."""
from datetime import datetime, date
from string import Template
from pathlib import Path

import requests
import w3lib.url
from lxml import html

from app.main.constants import T_BROOKS_DASH_URL, BROOKS_DASHBOARD_DATE_FORMAT
from app.main.scrape.brooks.models.games_for_date import BrooksGamesForDate
from app.main.scrape.brooks.models.game_info import BrooksGameInfo
from app.main.models.season import Season
from app.main.util.decorators import timeout, retry, RetryLimitExceededError
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from app.main.util.result import Result
from app.main.util.s3_helper import download_html_brooks_games_for_date
from app.main.util.scrape_functions import request_url
from app.main.util.string_functions import (
    parse_timestamp,
    validate_bbref_game_id_list,
    validate_bb_game_id,
)


DATA_SET = "brooks_games_for_date"

GAME_TABLE_XPATH = '//td[@class="dashcell"]/table'
GAME_INFO_XPATH = "./tbody//tr[1]//td[1]//text()"
PITCH_LOG_URL_XPATH = './tbody//a[text()="Game Log"]/@href'
KZONE_URL_XPATH = './tbody//a[text()="Strikezone Map"]/@href'


def scrape_brooks_games_for_date(session, driver, scrape_date, bbref_games_for_date):
    game_ids = [Path(url).stem for url in bbref_games_for_date.boxscore_urls]
    required_game_data = validate_bbref_game_id_list(game_ids)
    url = _get_dashboard_url_for_date(scrape_date)
    result = get_brooks_games_for_date_html_from_s3(scrape_date)
    if result.failure:
        result = request_brooks_games_for_date_html(driver, url)
        if result.failure:
            return result
    response = result.value
    return parse_daily_dash_page(session, response, scrape_date, url, required_game_data)


def get_brooks_games_for_date_html_from_s3(scrape_date):
    result = download_html_brooks_games_for_date(scrape_date)
    if result.failure:
        return result
    html_path = result.value
    response = html.fromstring(html_path.read_text())
    html_path.unlink()
    return Result.Ok(response)


def request_brooks_games_for_date_html(driver, url):
    try:
        response = render_webpage(driver, url)
        return Result.Ok(response)
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


@retry(max_attempts=5, delay=5, exceptions=(TimeoutError, Exception))
@timeout(seconds=15)
def render_webpage(driver, url):
    driver.get(url)
    return html.fromstring(driver.page_source, base_url=url)


def _get_dashboard_url_for_date(scrape_date):
    date_str = scrape_date.strftime(BROOKS_DASHBOARD_DATE_FORMAT)
    return Template(T_BROOKS_DASH_URL).substitute(date=date_str)


def parse_daily_dash_page(session, response, scrape_date, url, required_game_data):
    games_for_date = BrooksGamesForDate()
    games_for_date.game_date = scrape_date
    games_for_date.game_date_str = scrape_date.strftime(DATE_ONLY)
    games_for_date.dashboard_url = url
    games_for_date.game_count = 0
    games_for_date.games = []

    if Season.is_this_the_asg_date(session, scrape_date):
        return Result.Ok(games_for_date)
    game_tables = response.xpath(GAME_TABLE_XPATH)
    if not game_tables:
        error = f"Unable to parse any game data from {url}"
        return Result.Fail(error)
    for i, game in enumerate(game_tables):
        game_info_list = game.xpath(GAME_INFO_XPATH)
        if not game_info_list or len(game_info_list) != 2:
            error = f"Game info table #{i + 1} was not in the expected format"
            return Result.Fail(error)
        game_time = parse_timestamp(game_info_list[1])
        pitchlog_urls = game.xpath(PITCH_LOG_URL_XPATH)
        if not pitchlog_urls:
            result = _no_pitch_logs(game, i + 1, scrape_date, game_time, required_game_data)
            if result.failure:
                continue
            gameinfo = result.value
            games_for_date.games.append(gameinfo)
            continue
        result = _is_game_required(scrape_date, game_time, pitchlog_urls[0], required_game_data)
        if result.failure:
            continue
        gameinfo = result.value
        result = _parse_pitch_log_dict(gameinfo, pitchlog_urls)
        if result.failure:
            return result
        gameinfo = result.value
        games_for_date.games.append(gameinfo)

    games_for_date.game_count = len(games_for_date.games)
    _update_game_ids(games_for_date)
    return Result.Ok(games_for_date)


def _no_pitch_logs(game_table, game_number, scrape_date, game_time, required_game_data):
    k_zone_urls = game_table.xpath(KZONE_URL_XPATH)
    if not k_zone_urls:
        error = f"Unable to parse brooks_game_id for game table #{game_number}"
        return Result.Fail(error)
    result = _parse_gameinfo(scrape_date, game_time, k_zone_urls[0])
    if result.failure:
        return result
    gameinfo = result.value
    game_is_required = any(
        [gameinfo.home_team_id_bb == req["home_team_id"] for req in required_game_data]
    )
    if not game_is_required:
        return Result.Fail("Game not found in bbref parsed game list.")
    gameinfo.might_be_postponed = True
    gameinfo.pitcher_appearance_count = 0
    return Result.Ok(gameinfo)


def _is_game_required(scrape_date, game_time, url, required_game_data):
    result = _parse_gameinfo(scrape_date, game_time, url)
    if result.failure:
        return result
    gameinfo = result.value
    result = validate_bb_game_id(gameinfo.brooks_game_id)
    if result.failure:
        return result
    game_date = result.value["game_date"]
    if game_date != scrape_date:
        return Result.Fail("Game did not occur on the current date.")
    game_is_required = any(
        [gameinfo.home_team_id_bb == req["home_team_id"] for req in required_game_data]
    )
    if not game_is_required:
        return Result.Fail("Game not found in bbref parsed game list.")
    gameinfo.might_be_postponed = False
    return Result.Ok(gameinfo)


def _parse_gameinfo(scrape_date, game_time, url):
    gameinfo = BrooksGameInfo()
    gameinfo.game_date_year = scrape_date.year
    gameinfo.game_date_month = scrape_date.month
    gameinfo.game_date_day = scrape_date.day
    gameinfo.game_time_hour = game_time["hour"]
    gameinfo.game_time_minute = game_time["minute"]
    gameinfo.time_zone_name = "America/New_York"
    return _parse_gameinfo_from_url(gameinfo, url)


def _parse_gameinfo_from_url(gameinfo, url):
    gameid = _parse_gameid_from_url(url)
    if not gameid:
        error = (
            "URL not in expected format, unable to retrieve value of "
            f'query parameter "game":\n{url}'
        )
        return Result.Fail(error)
    gameinfo.brooks_game_id = gameid
    gameinfo.away_team_id_bb = _parse_away_team_from_gameid(gameid)
    gameinfo.home_team_id_bb = _parse_home_team_from_gameid(gameid)
    gameinfo.game_number_this_day = _parse_game_number_from_gameid(gameid)
    return Result.Ok(gameinfo)


def _parse_pitch_log_dict(gameinfo, pitchlog_url_list):
    gameinfo.pitcher_appearance_count = len(pitchlog_url_list)
    gameinfo.pitcher_appearance_dict = {}
    for url in pitchlog_url_list:
        pitcher_id = _parse_pitcherid_from_url(url)
        if pitcher_id is None:
            error = "Unable to parse pitcher_id from url query string:\n" f"url: {url}"
            return Result.Fail(error)
        gameinfo.pitcher_appearance_dict[pitcher_id] = url
    return Result.Ok(gameinfo)


def _parse_gameid_from_url(url):
    gameid = w3lib.url.url_query_parameter(url, "game")
    if not gameid:
        return None
    return gameid[:-1] if gameid.endswith(r"/") else gameid


def _parse_away_team_from_gameid(gameid):
    split = gameid.split("_")
    if len(split) != 7:
        return None
    teama = split[4]
    if len(teama) != 6:
        return None
    return teama[:3].upper()


def _parse_home_team_from_gameid(gameid):
    split = gameid.split("_")
    if len(split) != 7:
        return None
    teamh = split[5]
    if len(teamh) != 6:
        return None
    return teamh[:3].upper()


def _parse_game_number_from_gameid(gameid):
    split = gameid.split("_")
    if len(split) != 7:
        return None
    return split[6]


def _parse_pitcherid_from_url(url):
    pitcherid = w3lib.url.url_query_parameter(url, "pitchSel")
    if pitcherid is None:
        return None
    if not pitcherid.endswith(".xml"):
        return None
    return pitcherid[:-4]


def _update_game_ids(games_for_date):
    game_dict = {g.brooks_game_id: g for g in games_for_date.games}
    tracker = {g.brooks_game_id: False for g in games_for_date.games}
    for bb_gid in sorted(game_dict.keys(), reverse=True):
        if tracker[bb_gid]:
            continue
        g = game_dict[bb_gid]
        game_date = datetime(g.game_date_year, g.game_date_month, g.game_date_day)
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)

        if g.game_number_this_day == "2":
            g.bbref_game_id = f"{g.home_team_id_bb}{date_str}2"
            game_1_bb_id = f"{bb_gid[:-1]}{1}"
            game_1 = game_dict[game_1_bb_id]
            game_1.bbref_game_id = f"{g.home_team_id_bb}{date_str}1"
            tracker[bb_gid] = True
            tracker[game_1_bb_id] = True
        else:
            g.bbref_game_id = f"{g.home_team_id_bb}{date_str}0"
            tracker[bb_gid] = True
