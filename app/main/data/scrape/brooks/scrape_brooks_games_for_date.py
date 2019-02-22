"""Scrape brooksbaseball daily dashboard page."""
from datetime import datetime, date
from string import Template

import requests
import w3lib.url
from lxml import html

from app.main.constants import (
    T_BROOKS_DASH_URL, BROOKS_DASHBOARD_DATE_FORMAT
)
from app.main.data.scrape.brooks.models.games_for_date import BrooksGamesForDate
from app.main.data.scrape.brooks.models.game_info import BrooksGameInfo
from app.main.models.season import Season
from app.main.util.dt_format_strings import DATE_ONLY
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

def scrape_brooks_games_for_date(scrape_dict):
    scrape_date = scrape_dict['date']
    session = scrape_dict['session']
    if not scrape_date:
        scrape_date = date.now()
    result = Season.is_date_in_season(session, scrape_date)
    if not result['success']:
        return result

    url = __get_dashboard_url_for_date(scrape_date)
    result = request_url(url)
    if not result['success']:
        return result
    response = result['response']
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
            init_result = __init_gameinfo(timestamp_str, scrape_date)
            if not init_result['success']:
                return init_result
            g = init_result['result']

            xpath_pitchlog_urls = Template(TEMPL_XPATH_PITCHLOG_URLS)\
                .substitute(r=row, g=game)
            pitchlog_urls = response.xpath(xpath_pitchlog_urls)
            if not pitchlog_urls:
                continue

            parse_result = __parse_pitch_log_dict(g, pitchlog_urls)
            if not parse_result['success']:
                return parse_result
            g = parse_result['result']
            games_for_date.games.append(g)

    games_for_date.game_count = len(games_for_date.games)
    return dict(success=True, result=games_for_date)

def __init_gameinfo(timestamp_str, scrape_date):
    gameinfo = BrooksGameInfo()
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
    return dict(success=True, result=gameinfo)

def __parse_pitch_log_dict(gameinfo, pitchlog_url_list):
    gameinfo.pitcher_appearance_count = len(pitchlog_url_list)
    url = pitchlog_url_list[0]
    parse_gameinfo_result = __parse_gameinfo_from_url(gameinfo, url)
    if not parse_gameinfo_result['success']:
        return parse_gameinfo_result
    gameinfo = parse_gameinfo_result['result']

    gameinfo.pitcher_appearance_dict = {}
    for url in pitchlog_url_list:
        pitcher_id = __parse_pitcherid_from_url(url)
        if pitcher_id is None:
            error = (
                'Unable to parse pitcher_id from url query string:\n'
                f'url: {url}'
            )
            return dict(success=False, message=error)
        gameinfo.pitcher_appearance_dict[pitcher_id] = url
    return dict(success=True, result=gameinfo)

def __parse_gameinfo_from_url(gameinfo, url):
    gameid = __parse_gameid_from_url(url)
    if not gameid:
        error = (
            'URL not in expected format, unable to retrieve value of '
            f'query parameter "game":\n{url}'
        )
        return dict(success=False, message=error)
    gameinfo.bb_game_id = gameid
    gameinfo.away_team_id_bb = __parse_away_team_from_gameid(gameid)
    gameinfo.home_team_id_bb = __parse_home_team_from_gameid(gameid)
    gameinfo.game_number_this_day = __parse_game_number_from_gameid(gameid)
    return dict(success=True, result=gameinfo)

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