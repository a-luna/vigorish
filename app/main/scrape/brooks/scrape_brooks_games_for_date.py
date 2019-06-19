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
from app.main.util.string_functions import parse_timestamp, validate_bbref_game_id_list


DATA_SET = "brooks_games_for_date"

_T_GAMEINFO_XPATH = '//table//tr[${r}]//td[@class="dashcell"][${g}]//table//tr[1]//td[1]//text()'
_T_PLOG_URLS_XPATH = '//table//tr[${r}]//td[@class="dashcell"][${g}]//a[text()="Game Log"]/@href'
_T_K_ZONE_URL_XPATH = '//table//tr[${r}]//td[@class="dashcell"][${g}]//a[text()="Strikezone Map"]/@href'


def scrape_brooks_games_for_date(session, scrape_date, bbref_games_for_date):
    try:
        game_ids = [Path(url).stem for url in bbref_games_for_date.boxscore_urls]
        required_game_data = validate_bbref_game_id_list(game_ids)
        url = _get_dashboard_url_for_date(scrape_date)
        response = request_url(url)
        return parse_daily_dash_page(session, response, scrape_date, url, required_game_data)
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def _get_dashboard_url_for_date(scrape_date):
    date_str = scrape_date.strftime(BROOKS_DASHBOARD_DATE_FORMAT)
    return Template(T_BROOKS_DASH_URL).substitute(date=date_str)

@retry(
    max_attempts=15, delay=5, exceptions=(TimeoutError, Exception))
@timeout(seconds=10)
def request_url(url):
    """Send a HTTP request for URL, return the response if successful."""
    page = requests.get(url)
    return html.fromstring(page.content, base_url=url)

def parse_daily_dash_page(session, response, scrape_date, url, required_game_data):
    games_for_date = BrooksGamesForDate()
    games_for_date.game_date = scrape_date
    games_for_date.game_date_str = scrape_date.strftime(DATE_ONLY)
    games_for_date.dashboard_url = url
    games_for_date.game_count = 0
    games_for_date.games = []

    if Season.is_this_the_asg_date(session, scrape_date):
        return Result.Ok(games_for_date)
    for row in range(2, 10):
        for game in range(1, 5):
            xpath_gameinfo = Template(_T_GAMEINFO_XPATH).substitute(r=row, g=game)
            gameinfo_list = response.xpath(xpath_gameinfo)
            if not gameinfo_list and not len(gameinfo_list) > 1:
                continue
            timestamp_str = gameinfo_list[1]
            pitchlog_urls_xpath = Template(_T_PLOG_URLS_XPATH).substitute(r=row, g=game)
            pitchlog_urls = response.xpath(pitchlog_urls_xpath)
            if not pitchlog_urls:
                result = _no_pitch_logs(
                    response,
                    row,
                    game,
                    scrape_date,
                    timestamp_str,
                    required_game_data
                )
                if result.failure:
                    continue
                gameinfo = result.value
                games_for_date.games.append(gameinfo)
                continue

            url = pitchlog_urls[0]
            result = _is_game_required(
                scrape_date,
                timestamp_str,
                url,
                required_game_data
            )
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


def _no_pitch_logs(response, row, game, scrape_date, timestamp_str, required_game_data):
    xpath_k_zone_urls = Template(_T_K_ZONE_URL_XPATH).substitute(r=row, g=game)
    k_zone_urls = response.xpath(xpath_k_zone_urls)
    result = _parse_gameinfo(scrape_date, timestamp_str, k_zone_urls[0])
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


def _is_game_required(scrape_date, timestamp_str, url, required_game_data):
    result = _parse_gameinfo(scrape_date, timestamp_str, url)
    if result.failure:
        return result
    gameinfo = result.value
    game_is_required = any(
        [gameinfo.home_team_id_bb == req["home_team_id"] for req in required_game_data]
    )
    if not game_is_required:
        return Result.Fail("Game not found in bbref parsed game list.")
    gameinfo.might_be_postponed = False
    return Result.Ok(gameinfo)


def _parse_gameinfo(scrape_date, timestamp_str, url):
    gameinfo = BrooksGameInfo()
    game_time = parse_timestamp(timestamp_str)
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
    gameinfo.bb_game_id = gameid
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
    game_dict = {g.bb_game_id: g for g in games_for_date.games}
    tracker = {g.bb_game_id: False for g in games_for_date.games}
    ids_ordered = sorted(game_dict.keys(), reverse=True)
    for bb_gid in ids_ordered:
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
