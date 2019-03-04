"""Scrape brooksbaseball daily dashboard page."""
from datetime import datetime, date
from string import Template

import requests
import w3lib.url
from lxml import html

from app.main.constants import (
    T_BROOKS_DASH_URL, BROOKS_DASHBOARD_DATE_FORMAT
)
from app.main.scrape.brooks.models.games_for_date import BrooksGamesForDate
from app.main.scrape.brooks.models.game_info import BrooksGameInfo
from app.main.models.season import Season
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from app.main.util.result import Result
from app.main.util.scrape_functions import request_url
from app.main.util.string_functions import parse_timestamp

TEMPL_XPATH_GAMEINFO = (
    '//table//tr[${r}]//td[@class="dashcell"][${g}]'
    '//table//tr[1]//td[1]//text()'
)
TEMPL_XPATH_PITCHLOG_URLS = (
    '//table//tr[${r}]//td[@class="dashcell"][${g}]'
    '//a[text()="Game Log"]/@href'
)
TEMPL_XPATH_K_ZONE_URL = (
    '//table//tr[${r}]//td[@class="dashcell"][${g}]'
    '//a[text()="Strikezone Map"]/@href'
)

def scrape_brooks_games_for_date(scrape_dict):
    driver = scrape_dict['driver']
    scrape_date = scrape_dict['date']
    session = scrape_dict['session']
    if not scrape_date:
        scrape_date = date.now()
    result = Season.is_date_in_season(session, scrape_date)
    if result.failure:
        return result

    url = __get_dashboard_url_for_date(scrape_date)
    driver.get(url)
    page = driver.page_source
    response = html.fromstring(page, base_url=url)
    return __parse_daily_dash_page(response, scrape_date, url)

def __get_dashboard_url_for_date(scrape_date):
    date_str = scrape_date.strftime(BROOKS_DASHBOARD_DATE_FORMAT)
    return Template(T_BROOKS_DASH_URL).substitute(date=date_str)

def __parse_daily_dash_page(response, scrape_date, url):
    games_for_date = BrooksGamesForDate()
    games_for_date.game_date_str = scrape_date.strftime(DATE_ONLY)
    games_for_date.dashboard_url = url
    games_for_date.games = []

    for row in range(2,10):
        for game in range(1,5):
            xpath_gameinfo = Template(TEMPL_XPATH_GAMEINFO)\
                .substitute(r=row, g=game)
            gameinfo_list = response.xpath(xpath_gameinfo)
            if not gameinfo_list and not len(gameinfo_list) > 1:
                continue

            timestamp_str = gameinfo_list[1]
            result = __init_gameinfo(timestamp_str, scrape_date)
            if result.failure:
                return result
            g = result.value

            xpath_pitchlog_urls = Template(TEMPL_XPATH_PITCHLOG_URLS)\
                .substitute(r=row, g=game)
            pitchlog_urls = response.xpath(xpath_pitchlog_urls)
            if not pitchlog_urls:
                result = __game_with_no_pitch_logs(response, g, row, game)
                g.might_be_postponed = True
                g.pitcher_appearance_count = 0
                games_for_date.games.append(g)
                continue

            result = __parse_pitch_log_dict(g, pitchlog_urls)
            if result.failure:
                return result
            g = result.value
            games_for_date.games.append(g)

    games_for_date.game_count = len(games_for_date.games)
    __update_game_ids(games_for_date)

    return Result.Ok(games_for_date)

def __init_gameinfo(timestamp_str, scrape_date):
    gameinfo = BrooksGameInfo()
    gameinfo.might_be_postponed = False
    gameinfo.game_date_year = scrape_date.year
    gameinfo.game_date_month = scrape_date.month
    gameinfo.game_date_day = scrape_date.day
    gameinfo.time_zone_name = 'America/New_York'

    game_time = parse_timestamp(timestamp_str)
    if game_time:
        gameinfo.game_time_hour = game_time['hour']
        gameinfo.game_time_minute = game_time['minute']
    else:
        gameinfo.game_time_hour = 0
        gameinfo.game_time_minute = 0
    return Result.Ok(gameinfo)

def __game_with_no_pitch_logs(response, gameinfo, row , game):
    xpath_k_zone_urls = Template(TEMPL_XPATH_K_ZONE_URL)\
        .substitute(r=row, g=game)
    k_zone_urls = response.xpath(xpath_k_zone_urls)
    return __parse_gameinfo_from_url(gameinfo, k_zone_urls[0])

def __parse_pitch_log_dict(gameinfo, pitchlog_url_list):
    gameinfo.pitcher_appearance_count = len(pitchlog_url_list)
    if len(pitchlog_url_list) == 2:
        gameinfo.might_be_postponed = True
    url = pitchlog_url_list[0]
    result = __parse_gameinfo_from_url(gameinfo, url)
    if result.failure:
        return result
    gameinfo = result.value

    gameinfo.pitcher_appearance_dict = {}
    for url in pitchlog_url_list:
        pitcher_id = __parse_pitcherid_from_url(url)
        if pitcher_id is None:
            error = (
                'Unable to parse pitcher_id from url query string:\n'
                f'url: {url}'
            )
            return Result.Fail(error)
        gameinfo.pitcher_appearance_dict[pitcher_id] = url
    return Result.Ok(gameinfo)

def __parse_gameinfo_from_url(gameinfo, url):
    gameid = __parse_gameid_from_url(url)
    if not gameid:
        error = (
            'URL not in expected format, unable to retrieve value of '
            f'query parameter "game":\n{url}'
        )
        return Result.Fail(error)
    gameinfo.bb_game_id = gameid
    gameinfo.away_team_id_bb = __parse_away_team_from_gameid(gameid)
    gameinfo.home_team_id_bb = __parse_home_team_from_gameid(gameid)
    gameinfo.game_number_this_day = __parse_game_number_from_gameid(gameid)
    return Result.Ok(gameinfo)

def __parse_gameid_from_url(url):
    gameid = w3lib.url.url_query_parameter(url, "game")
    if not gameid:
        return None
    return gameid[:-1] if gameid.endswith(r'/') else gameid

def __parse_away_team_from_gameid(gameid):
    split = gameid.split('_')
    if len(split) != 7:
        return None
    teama = split[4]
    if len(teama) != 6:
        return None
    return teama[:3].upper()

def __parse_home_team_from_gameid(gameid):
    split = gameid.split('_')
    if len(split) != 7:
        return None
    teamh = split[5]
    if len(teamh) != 6:
        return None
    return teamh[:3].upper()

def __parse_game_number_from_gameid(gameid):
    split = gameid.split('_')
    if len(split) != 7:
        return None
    return split[6]

def __parse_pitcherid_from_url(url):
    pitcherid = w3lib.url.url_query_parameter(url, "pitchSel")
    if pitcherid is None:
        return None
    if not pitcherid.endswith(".xml"):
        return None
    return pitcherid[:-4]

def __update_game_ids(games_for_date):
    game_dict = {g.bb_game_id:g for g in games_for_date.games}
    tracker = {g.bb_game_id:False for g in games_for_date.games}
    ids_ordered = sorted(game_dict.keys(), reverse=True)
    for bb_gid in ids_ordered:
        if tracker[bb_gid]:
            continue
        g = game_dict[bb_gid]
        game_date = datetime(g.game_date_year, g.game_date_month, g.game_date_day)
        date_str = game_date.strftime(DATE_ONLY_TABLE_ID)

        if g.game_number_this_day == '2':
            g.bbref_game_id = f'{g.home_team_id_bb}{date_str}2'
            game_1_bb_id = f'{bb_gid[:-1]}{1}'
            game_1 = game_dict[game_1_bb_id]
            game_1.bbref_game_id = f'{g.home_team_id_bb}{date_str}1'
            tracker[bb_gid] = True
            tracker[game_1_bb_id] = True
        else:
            g.bbref_game_id = f'{g.home_team_id_bb}{date_str}0'
            tracker[bb_gid] = True