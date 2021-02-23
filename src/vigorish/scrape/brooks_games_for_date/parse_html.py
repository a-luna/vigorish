"""Scrape brooksbaseball daily dashboard page."""
from datetime import datetime

import w3lib.url
from lxml import html

import vigorish.database as db
from vigorish.constants import BB_BR_TEAM_ID_MAP, PACIFIC_TZ_TEAMS
from vigorish.scrape.brooks_games_for_date.models.game_info import BrooksGameInfo
from vigorish.scrape.brooks_games_for_date.models.games_for_date import BrooksGamesForDate
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    parse_timestamp,
    validate_bbref_game_id_list,
    validate_brooks_game_id,
)

GAME_TABLE_XPATH = '//td[@class="dashcell"]/table'
GAME_INFO_XPATH = "./tbody//tr[1]//td[1]//text()"
PITCH_LOG_URL_XPATH = './tbody//a[text()="Game Log"]/@href'
KZONE_URL_XPATH = './tbody//a[text()="Strikezone Map"]/@href'


def parse_brooks_dashboard_page(db_session, scraped_html, game_date, url, bbref_games_for_date):
    page_content = html.fromstring(scraped_html, base_url=url)
    games_for_date = BrooksGamesForDate()
    games_for_date.game_date = game_date
    games_for_date.game_date_str = game_date.strftime(DATE_ONLY)
    games_for_date.dashboard_url = url
    games_for_date.game_count = 0
    games = []
    if db.Season.is_this_the_asg_date(db_session, game_date):
        return Result.Ok(games_for_date)
    game_tables = page_content.xpath(GAME_TABLE_XPATH)
    if not game_tables:
        return Result.Ok(games_for_date)
    for i, game in enumerate(game_tables):
        required_game_data = validate_bbref_game_id_list(bbref_games_for_date.all_bbref_game_ids)
        game_info_list = game.xpath(GAME_INFO_XPATH)
        if not game_info_list or len(game_info_list) != 2:
            error = f"Game info table #{i + 1} was not in the expected format"
            return Result.Fail(error)
        game_time = parse_timestamp(game_info_list[1])
        pitchlog_urls = game.xpath(PITCH_LOG_URL_XPATH)
        if not pitchlog_urls:
            result = _no_pitch_logs(game, i + 1, game_date, game_time, required_game_data)
            if result.failure:
                continue
            gameinfo = result.value
            games.append(gameinfo)
            continue
        result = _is_game_required(game_date, game_time, pitchlog_urls[0], required_game_data)
        if result.failure:
            continue
        gameinfo = result.value
        result = _parse_pitch_log_dict(gameinfo, pitchlog_urls)
        if result.failure:
            return result
        gameinfo = result.value
        if all(gameinfo != g for g in games):
            games.append(gameinfo)

    games_for_date.games = games
    games_for_date.game_count = len(games_for_date.games)
    _update_game_ids(games_for_date)
    return Result.Ok(games_for_date)


def _no_pitch_logs(game_table, game_number, game_date, game_time, required_game_data):
    k_zone_urls = game_table.xpath(KZONE_URL_XPATH)
    if not k_zone_urls:
        error = f"Unable to parse bb_game_id for game table #{game_number}"
        return Result.Fail(error)
    result = _parse_gameinfo(game_date, game_time, k_zone_urls[0])
    if result.failure:
        return result
    gameinfo = result.value
    game_is_required = any(gameinfo.home_team_id_bb == req["home_team_id"] for req in required_game_data)
    if not game_is_required:
        return Result.Fail("Game not found in bbref parsed game list.")
    gameinfo.might_be_postponed = True
    gameinfo.pitcher_appearance_count = 0
    return Result.Ok(gameinfo)


def _is_game_required(game_date, game_time, url, required_game_data):
    result = _parse_gameinfo(game_date, game_time, url)
    if result.failure:
        return result
    gameinfo = result.value
    result = validate_brooks_game_id(gameinfo.bb_game_id)
    if result.failure:
        return result
    game_date_check = result.value["game_date"]
    if game_date != game_date_check:
        return Result.Fail("Game did not occur on the current date.")
    game_is_required = any(gameinfo.home_team_id_bb == req["home_team_id"] for req in required_game_data)
    if not game_is_required:
        return Result.Fail("Game not found in bbref parsed game list.")
    gameinfo.might_be_postponed = False
    return Result.Ok(gameinfo)


def _parse_gameinfo(game_date, game_time, url):
    result = _parse_gameinfo_from_url(url)
    if result.failure:
        return result
    gameinfo = result.value
    home_team_id_br = BB_BR_TEAM_ID_MAP.get(gameinfo.home_team_id_bb, gameinfo.home_team_id_bb)
    game_time_hour = (
        game_time["hour"]
        if (game_time["hour"] in [11, 12] or (game_time["hour"] == 10 and home_team_id_br not in PACIFIC_TZ_TEAMS))
        else game_time["hour"] + 12
        if game_time["hour"] in range(1, 11)
        else 0
    )
    gameinfo.game_date_year = game_date.year
    gameinfo.game_date_month = game_date.month
    gameinfo.game_date_day = game_date.day
    gameinfo.game_time_hour = game_time_hour
    gameinfo.game_time_minute = game_time["minute"]
    gameinfo.time_zone_name = "America/New_York"
    return Result.Ok(gameinfo)


def _parse_gameinfo_from_url(url):
    gameid = _parse_gameid_from_url(url)
    if not gameid:
        error = "URL not in expected format, unable to retrieve value of " f'query parameter "game":\n{url}'
        return Result.Fail(error)
    gameinfo = BrooksGameInfo()
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
    return int(split[6])


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
    for bb_gid in sorted(game_dict.keys(), reverse=True):
        if tracker[bb_gid]:
            continue
        g = game_dict[bb_gid]
        game_date = datetime(g.game_date_year, g.game_date_month, g.game_date_day)
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)

        if g.game_number_this_day == 2:
            g.bbref_game_id = f"{g.home_team_id_bb}{date_str}2"
            game_1_bb_id = f"{bb_gid[:-1]}{1}"
            game_1 = game_dict[game_1_bb_id]
            game_1.bbref_game_id = f"{g.home_team_id_bb}{date_str}1"
            tracker[bb_gid] = True
            tracker[game_1_bb_id] = True
        else:
            g.bbref_game_id = f"{g.home_team_id_bb}{date_str}0"
            tracker[bb_gid] = True
