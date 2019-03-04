import datetime
import json
import random
import re
import time
from pathlib import Path
from string import Template

from lxml import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from tqdm import tqdm

from app.main.constants import DEFENSE_POSITIONS
from app.main.util.dt_format_strings import DATE_ONLY_UNDERSCORE
from app.main.util.list_functions import display_dict
from app.main.util.numeric_functions import is_even
from app.main.util.result import Result
from app.main.util.scrape_functions import request_url, get_chromedriver
from app.main.util.string_functions import fuzzy_match, normalize

from app.main.scrape.bbref.models.bat_stats import BBRefBatStats
from app.main.scrape.bbref.models.bat_stats_detail import BBRefBatStatsDetail
from app.main.scrape.bbref.models.boxscore import BBRefBoxscore
from app.main.scrape.bbref.models.boxscore_game_meta import BBRefBoxscoreMeta
from app.main.scrape.bbref.models.half_inning import BBRefHalfInning
from app.main.scrape.bbref.models.boxscore_team_data import BBRefBoxscoreTeamData
from app.main.scrape.bbref.models.pbp_event import BBRefPlayByPlayEvent
from app.main.scrape.bbref.models.pbp_substitution import BBRefInGameSubstitution
from app.main.scrape.bbref.models.pitch_stats import BBRefPitchStats
from app.main.scrape.bbref.models.starting_lineup_slot import BBRefStartingLineupSlot
from app.main.scrape.bbref.models.team_linescore_totals import BBRefTeamLinescoreTotals
from app.main.scrape.bbref.models.umpire import BBRefUmpire

_TEAM_ID_XPATH = '//a[@itemprop="name"]/@href'
_TEAM_RUNS_XPATH = '//div[@class="score"]/text()'
_AWAY_TEAM_RECORD_XPATH = '//div[@class="scorebox"]/div[1]/div[3]/text()'
_HOME_TEAM_RECORD_XPATH = '//div[@class="scorebox"]/div[2]/div[3]/text()'
_SCOREBOX_META_XPATH = '//div[@class="scorebox_meta"]//div/text()'
_LINESCORE_KEYS_XPATH = '//table[contains(@class, "linescore")]//th/text()'
_LINESCORE_AWAY_VALS_XPATH = '//table[contains(@class, "linescore")]//tbody/tr[1]//td/text()'
_LINESCORE_HOME_VALS_XPATH = '//table[contains(@class, "linescore")]//tbody/tr[2]//td/text()'

_BATTING_STATS_TABLE = '//div[contains(@class, "overthrow")]//table[contains(@id, "batting")]'
_BATTER_IDS_XPATH = './tbody//td[@data-stat="batting_avg"]/../th[@data-stat="player"]/@data-append-csv'
_BATTER_NAMES_XPATH = './tbody//td[@data-stat="batting_avg"]/../th[@data-stat="player"]//a/text()'
_BATTER_STATS_ROW = './tbody//th[@data-append-csv="{pid}"]/..'

_BATTER_AB_XPATH = './td[@data-stat="AB"]/text()'
_BATTER_R_XPATH = './td[@data-stat="R"]/text()'
_BATTER_H_XPATH = './td[@data-stat="H"]/text()'
_BATTER_RBI_XPATH = './td[@data-stat="RBI"]/text()'
_BATTER_BB_XPATH = './td[@data-stat="BB"]/text()'
_BATTER_SO_XPATH = './td[@data-stat="SO"]/text()'
_BATTER_PA_XPATH = './td[@data-stat="PA"]/text()'

_BATTER_AVG_TO_DATE_XPATH = './td[@data-stat="batting_avg"]/text()'
_BATTER_OBP_TO_DATE_XPATH = './td[@data-stat="onbase_perc"]/text()'
_BATTER_SLG_TO_DATE_XPATH = './td[@data-stat="slugging_perc"]/text()'
_BATTER_OPS_TO_DATE_XPATH = './td[@data-stat="onbase_plus_slugging"]/text()'
_BATTER_TOTAL_PITCHES_XPATH = './td[@data-stat="pitches"]/text()'
_BATTER_TOTAL_STRIKES_XPATH = './td[@data-stat="strikes_total"]/text()'

_BATTER_WPA_XPATH = './td[@data-stat="wpa_bat"]/text()'
_BATTER_ALI_XPATH = './td[@data-stat="leverage_index_avg"]/text()'
_BATTER_WPA_POS_XPATH = './td[@data-stat="wpa_bat_pos"]/text()'
_BATTER_WPA_NEG_XPATH = './td[@data-stat="wpa_bat_neg"]/text()'
_BATTER_RE24_XPATH = './td[@data-stat="re24_bat"]/text()'
_BATTER_DETAILS_XPATH = './td[@data-stat="details"]/text()'

_PITCHING_STATS_TABLE = '//div[contains(@class, "overthrow")]//table[contains(@id, "pitching")]'
_PITCHER_IDS_XPATH = './tbody//td[@data-stat="earned_run_avg"]/../th[@data-stat="player"]/@data-append-csv'
_PITCHER_NAMES_XPATH = './tbody//td[@data-stat="earned_run_avg"]/../th[@data-stat="player"]//a/text()'
_PITCHER_STATS_ROW = './tbody//th[@data-append-csv="{pid}"]/..'

_PITCHER_IP_XPATH = './td[@data-stat="IP"]/text()'
_PITCHER_H_XPATH = './td[@data-stat="H"]/text()'
_PITCHER_R_XPATH = './td[@data-stat="R"]/text()'
_PITCHER_ER_XPATH = './td[@data-stat="ER"]/text()'
_PITCHER_BB_XPATH = './td[@data-stat="BB"]/text()'
_PITCHER_SO_XPATH = './td[@data-stat="SO"]/text()'
_PITCHER_HR_XPATH = './td[@data-stat="HR"]/text()'
_PITCHER_BATTERS_FACED_XPATH = './td[@data-stat="batters_faced"]/text()'
_PITCHER_PITCH_COUNT_XPATH = './td[@data-stat="pitches"]/text()'
_PITCHER_STRIKES_TOTAL_XPATH = './td[@data-stat="strikes_total"]/text()'
_PITCHER_STRIKES_CONTACT_XPATH = './td[@data-stat="strikes_contact"]/text()'
_PITCHER_STRIKES_SWINGING_XPATH = './td[@data-stat="strikes_swinging"]/text()'
_PITCHER_STRIKES_LOOKING_XPATH = './td[@data-stat="strikes_looking"]/text()'

_PITCHER_GB_XPATH = './td[@data-stat="inplay_gb_total"]/text()'
_PITCHER_FB_XPATH = './td[@data-stat="inplay_fb_total"]/text()'
_PITCHER_LD_XPATH = './td[@data-stat="inplay_ld"]/text()'
_PITCHER_UNK_XPATH = './td[@data-stat="inplay_unk"]/text()'

_PITCHER_GSC_XPATH = './td[@data-stat="game_score"]/text()'
_PITCHER_IR_XPATH = './td[@data-stat="inherited_runners"]/text()'
_PITCHER_IS_XPATH = './td[@data-stat="inherited_score"]/text()'
_PITCHER_WPA_XPATH = './td[@data-stat="wpa_def"]/text()'
_PITCHER_ALI_XPATH = './td[@data-stat="leverage_index_avg"]/text()'
_PITCHER_RE24_XPATH = './td[@data-stat="re24_def"]/text()'

_UMPIRES_XPATH = '//div[@id="content"]/div[9]/div[3]/div[1]/text()'
_FIRST_PITCH_WEATHER_XPATH = '//div[@id="content"]/div[9]/div[3]/div[4]/text()'
_AWAY_LINEUP_ORDER_XPATH = '//div[@id="lineups_1"]//table//tbody//tr//td[1]/text()'
_AWAY_LINEUP_PLAYER_XPATH = '//div[@id="lineups_1"]//table//tbody//a/@href'
_AWAY_LINEUP_DEF_POS_XPATH = '//div[@id="lineups_1"]//table//tbody//tr//td[3]/text()'
_HOME_LINEUP_ORDER_XPATH = '//div[@id="lineups_2"]//table//tbody//tr//td[1]/text()'
_HOME_LINEUP_PLAYER_XPATH = '//div[@id="lineups_2"]//table//tbody//a/@href'
_HOME_LINEUP_DEF_POS_XPATH = '//div[@id="lineups_2"]//table//tbody//tr//td[3]/text()'
_PLAY_BY_PLAY_TABLE = '//div[contains(@class, "overthrow")]//table[contains(@id, "play_by_play")]'
_PBP_INNING_SUMMARY_TOP_XPATH = './tbody//th[@data-stat="inning_summary_12"]/text()'
_PBP_INNING_SUMMARY_TOP_ROW_NUM_XPATH = './tbody//tr[@class="pbp_summary_top"]/@data-row'
_PBP_INNING_SUMMARY_BOTTOM_XPATH = './tbody//tr[@class="pbp_summary_bottom"]//td[last()]/text()'
_PBP_INNING_SUMMARY_BOTTOM_ROW_NUM_XPATH = './tbody//tr[@class="pbp_summary_bottom"]/@data-row'
_PBP_INNING_SUMMARY_BOTTOM_LAST_XPATH = './tbody//tr[@class="pbp_summary_bottom"]//span[@class="half_inning_summary"]/text()'
_PBP_IN_GAME_SUBSTITUTION_ROW_NUM_XPATH = './tbody//tr[@class="ingame_substitution"]/@data-row'
_T_PBP_IN_GAME_SUBSTITUTION_XPATH = './tbody//tr[@class="ingame_substitution"][@data-row="${row}"]//td[@data-stat="inning_summary_3"]//div/text()'
_PBP_INNING_XPATH = './tbody//th[@data-stat="inning"]/text()'
_PBP_SCORE_XPATH = './tbody//td[@data-stat="score_batting_team"]/text()'
_PBP_OUTS_XPATH = './tbody//td[@data-stat="outs"]/text()'
_PBP_RUNNERS_ON_BASE_XPATH = './tbody//td[@data-stat="runners_on_bases_pbp"]/text()'
_PBP_PITCH_SEQUENCE_XPATH = './tbody//span[@class="pitch_sequence"]/text()'
_PBP_TEAM_BATTING_XPATH = './tbody//td[@data-stat="batting_team_id"]/text()'
_PBP_BATTER_NAME_XPATH = './tbody//td[@data-stat="batter"]/text()'
_PBP_PITCHER_NAME_XPATH = './tbody//td[@data-stat="pitcher"]/text()'
_PBP_PLAY_DESC_XPATH = './tbody//td[@data-stat="play_desc"]/text()'
_PBP_RUNS_OUTS_XPATH = './tbody//tr[@id="event_{n}"]/td[@data-stat="runs_outs_result"]/text()'
_T_PBP_ROW_NUMBER_XPATH = './tbody//tr[@id="event_${n}"]/@data-row'

_GAME_ID_PATTERN = r'[A-Z][A-Z][A-Z]\d{9,9}'
_TEAM_ID_PATTERN = r'[A-Z][A-Z][A-Z]'
_ATTENDANCE_PATTERN = r'\d{1,2},\d{3,3}'
_GAME_DATE_PATTERN = r'[1-3]?[0-9], 20[0-1][0-9]'
_GAME_DURATION_PATTERN = r'[1-9]:[0-5][0-9]'
_INNING_TOTALS_PATTERN = (
    r'(?P<runs>\d{1,2})\s\b\w+\b,\s'
    r'(?P<hits>\d{1,2})\s\b\w+\b,\s'
    r'(?P<errors>\d{1,2})\s\b\w+\b,\s'
    r'(?P<left_on_base>\d{1,2})\s\b\w+\b.\s(\b\w+\b){1,1}(\s\b\w+\b){0,1}\s'
    r'(?P<away_team_runs>\d{1,2}),\s(\b\w+\b){1,1}(\s\b\w+\b){0,1}\s'
    r'(?P<home_team_runs>\d{1,2})'
)
INNING_TOTALS_REGEX = re.compile(_INNING_TOTALS_PATTERN)
CHANGE_POS_PATTERN = r'from\s\b(?P<old_pos>\w+)\b\sto\s\b(?P<new_pos>\w+)\b'
CHANGE_POS_REGEX = re.compile(CHANGE_POS_PATTERN)
POS_REGEX = re.compile(r'\([BCDFHLPRS123]{1,2}\)')
NUM_REGEX = re.compile(r'[1-9]{1}')


def scrape_bbref_boxscores_for_date(scrape_dict):
    driver = scrape_dict['driver']
    scrape_date = scrape_dict['date']
    games_for_date = scrape_dict['input_data']
    boxscore_urls = games_for_date.boxscore_urls

    scraped_boxscores = []
    player_name_match_logs = []
    with tqdm(
        total=len(boxscore_urls),
        ncols=100,
        unit='boxscore',
        mininterval=0.12,
        maxinterval=10,
        leave=True,
        position=1
    ) as pbar:
        for url in boxscore_urls:
            max_attempts = 10
            attempts = 1
            parsing_boxscore = True
            while(parsing_boxscore):
                try:
                    uri = Path(url)
                    pbar.set_description(f'Processing {uri.stem}..')
                    driver.get(url)

                    WebDriverWait(driver, 6000).until(
                        ec.presence_of_element_located((By.XPATH, _BATTING_STATS_TABLE))
                    )
                    WebDriverWait(driver, 6000).until(
                        ec.presence_of_element_located((By.XPATH, _PITCHING_STATS_TABLE))
                    )
                    WebDriverWait(driver, 6000).until(
                        ec.presence_of_element_located((By.XPATH, _PLAY_BY_PLAY_TABLE))
                    )

                    page = driver.page_source
                    response = html.fromstring(page, base_url=url)
                    result = __parse_bbref_boxscore(response, url)
                    if result.failure:
                        return result
                    bbref_boxscore = result.value
                    scraped_boxscores.append(bbref_boxscore)
                    player_match_log = bbref_boxscore.player_id_match_log
                    if player_match_log:
                        player_name_match_logs.extend(player_match_log)

                    time.sleep(random.uniform(2.5, 3.0))
                    parsing_boxscore = False
                    pbar.update()
                except Exception:
                    attempts += 1
                    if (attempts < max_attempts):
                        pbar.set_description('Page failed to load, retrying')
                    else:
                        error = 'Unable to retrive URL content after {m} failed attempts, aborting task\n'.format(m=max_attempts)
                        return Result.Fail(error)

    if player_name_match_logs:
        date_str = scrape_date.strftime(DATE_ONLY_UNDERSCORE)
        with open(f'player_match_log_{date_str}.json', 'w') as f:
            matches = ''
            for log in player_name_match_logs:
                matches += str(log) + '\n'
            f.write(matches)
    return Result.Ok(scraped_boxscores)

def __parse_bbref_boxscore(response, url, silent=False):
    """Parse boxscore data from the page source."""
    boxscore = BBRefBoxscore()
    boxscore.boxscore_url = url

    if not silent:
        pbar = tqdm(total=4, ncols=100, unit='chunk', leave=False, position=2)
        pbar.set_description(f'Parsing game info........')

    game_id = _parse_bbref_gameid_from_url(url)
    if not game_id:
        error = 'Failed to parse game ID'
        return Result.Fail(error)
    boxscore.bbref_game_id = game_id

    #print('\nBBRef Game ID..........: {gid}'.format(gid=game_id))

    boxscore.away_team_data = BBRefBoxscoreTeamData()
    boxscore.home_team_data = BBRefBoxscoreTeamData()

    away_team_id = _parse_away_team_id(response)
    if not away_team_id:
        error = 'Failed to parse away team ID'
        return Result.Fail(error)
    boxscore.away_team_data.team_id_br = away_team_id

    #print('Away Team ID...........: {at}'.format(at=away_team_id))

    home_team_id = _parse_home_team_id(response)
    if not home_team_id:
        error = 'Failed to parse home team ID'
        return Result.Fail(error)
    boxscore.home_team_data.team_id_br = home_team_id

    #print('Home Team ID...........: {ht}'.format(ht=home_team_id))

    away_team_runs = _parse_away_team_runs(response)
    if not away_team_runs:
        error = 'Failed to parse away team runs scored'
        return Result.Fail(error)
    boxscore.away_team_data.total_runs_scored_by_team = away_team_runs
    boxscore.home_team_data.total_runs_scored_by_opponent = away_team_runs

    #print('Away Team Runs Scored..: {ar}'.format(ar=away_team_runs))

    home_team_runs = _parse_home_team_runs(response)
    if not home_team_runs:
        error = 'Failed to parse home team runs scored'
        return Result.Fail(error)
    boxscore.home_team_data.total_runs_scored_by_team = home_team_runs
    boxscore.away_team_data.total_runs_scored_by_opponent = home_team_runs

    #print('Home Team Runs Scored..: {hr}'.format(hr=home_team_runs))

    away_team_record = _parse_away_team_record(response)
    if not away_team_record:
        error = 'Failed to parse away team record'
        return Result.Fail(error)
    boxscore.away_team_data.total_wins_before_game = away_team_record[0]
    boxscore.away_team_data.total_losses_before_game = away_team_record[1]

    #away_team_record_before_game = '{w}-{l}'.format(w=away_team_record[0], l=away_team_record[1])
    #print('Away Team Record.......: {at}'.format(at=away_team_record_before_game))

    home_team_record = _parse_home_team_record(response)
    if not home_team_record:
        error = 'Failed to parse home team record'
        return Result.Fail(error)
    boxscore.home_team_data.total_wins_before_game = home_team_record[0]
    boxscore.home_team_data.total_losses_before_game = home_team_record[1]

    #home_team_record_before_game = '{w}-{l}'.format(w=home_team_record[0], l=home_team_record[1])
    #print('Home Team Record.......: {ht}'.format(ht=home_team_record_before_game))

    boxscore.game_meta_info = BBRefBoxscoreMeta()
    scorebox_meta_strings = response.xpath(_SCOREBOX_META_XPATH)

    attendance_matches = _parse_attendance_from_strings(scorebox_meta_strings)
    if attendance_matches is not None:
        boxscore.game_meta_info.attendance = attendance_matches['match']
        attendance_index = attendance_matches['index']
        del scorebox_meta_strings[attendance_index]
        #print('Attendance.............: {a}'.format(a=item.attendance))
    else:
        boxscore.game_meta_info.attendance = "0"
        #print('Attendance.............: Was not found on page')

    venue_matches = _parse_venue_from_strings(scorebox_meta_strings)
    if venue_matches is None:
        error = 'Failed to parse park name'
        return Result.Fail(error)

    boxscore.game_meta_info.park_name = venue_matches['match']
    venue_index = venue_matches['index']
    del scorebox_meta_strings[venue_index]

    #print('Park Name..............: {p}'.format(p=item.park_name))

    game_duration_matches = _parse_game_duration_from_strings(scorebox_meta_strings)
    if game_duration_matches is None:
        error = 'Failed to parse game duration'
        return Result.Fail(error)

    boxscore.game_meta_info.game_duration = game_duration_matches['match']
    game_duration_index = game_duration_matches['index']
    del scorebox_meta_strings[game_duration_index]

    #print('Game Duration..........: {d}'.format(d=item.game_duration))

    day_night_field = _parse_day_night_field_type_from_strings(scorebox_meta_strings)
    if day_night_field is None:
        error = 'Failed to parse game time/field type'
        return Result.Fail(error)

    split = day_night_field['match'].split(',')
    day_night_field_index = day_night_field['index']
    del scorebox_meta_strings[day_night_field_index]

    if len(split) < 2:
        error = 'Failed to parse game time/field type'
        return Result.Fail(error)

    boxscore.game_meta_info.day_night = split[0].strip()
    boxscore.game_meta_info.field_type = split[1].strip().title()

    #print('Day/Night..............: {f}'.format(f=item.day_night))
    #print('Field Type.............: {f}'.format(f=item.field_type))

    first_pitch_weather = response.xpath(_FIRST_PITCH_WEATHER_XPATH)
    if not first_pitch_weather:
        error = 'Failed to parse first pitch weather info'
        return Result.Fail(error)
    split2 = first_pitch_weather[0].split(',')
    if len(split2) < 3:
        error = 'Failed to parse first pitch weather info'
        return Result.Fail(error)

    boxscore.game_meta_info.first_pitch_temperature = split2[0].strip()[:2]
    boxscore.game_meta_info.first_pitch_wind = split2[1].strip()
    boxscore.game_meta_info.first_pitch_clouds = split2[2].strip().strip('.')

    #print('Temperature............: {f}'.format(f=item.first_pitch_temperature))
    #print('Wind Speed.............: {f}'.format(f=item.first_pitch_wind))
    #print('Cloud Cover............: {f}'.format(f=item.first_pitch_clouds))

    if len(split2) > 3:
        boxscore.game_meta_info.first_pitch_precipitation = split2[3].strip().strip('.')
        #print('Precipitation..........: {f}'.format(f=item.first_pitch_precipitation))

    away_team_linescore_totals = _parse_linescore_totals(response, away_team_id, False)
    if not away_team_linescore_totals:
        error = 'Failed to parse away team linescore totals'
        return Result.Fail(error)
    boxscore.away_team_data.total_hits_by_team = away_team_linescore_totals.total_hits
    boxscore.away_team_data.total_errors_by_team = away_team_linescore_totals.total_errors
    boxscore.home_team_data.total_hits_by_opponent = away_team_linescore_totals.total_hits
    boxscore.home_team_data.total_errors_by_opponent = away_team_linescore_totals.total_errors

    home_team_linescore_totals = _parse_linescore_totals(response, home_team_id, True)
    if not home_team_linescore_totals:
        error = 'Failed to parse home team linescore totals'
        return Result.Fail(error)

    boxscore.home_team_data.total_hits_by_team = home_team_linescore_totals.total_hits
    boxscore.home_team_data.total_errors_by_team = home_team_linescore_totals.total_errors
    boxscore.away_team_data.total_hits_by_opponent = home_team_linescore_totals.total_hits
    boxscore.away_team_data.total_errors_by_opponent = home_team_linescore_totals.total_errors

    if not silent:
        pbar.update()
        pbar.set_description(f'Parsing bat stats........')

    result = response.xpath(_BATTING_STATS_TABLE)
    if not result or len(result) != 2:
        error = 'Failed to parse batting stats table'
        return Result.Fail(error)
    away_team_bat_table = result[0]
    home_team_bat_table = result[1]

    away_team_batter_name_dict = _parse_batter_name_dict(away_team_bat_table)
    if not away_team_batter_name_dict:
        error = 'Failed to parse away team batter name dictionary'
        return Result.Fail(error)

    away_team_batting_stats = _parse_batting_stats(away_team_bat_table, away_team_id, home_team_id)
    if not away_team_batting_stats:
        error = 'Failed to parse away team batting stats'
        return Result.Fail(error)
    boxscore.away_team_data.batting_stats = away_team_batting_stats

    home_team_batter_name_dict = _parse_batter_name_dict(home_team_bat_table)
    if not home_team_batter_name_dict:
        error = 'Failed to parse home team batter name dictionary'
        return Result.Fail(error)

    home_team_batting_stats = _parse_batting_stats(home_team_bat_table, home_team_id, away_team_id)
    if not home_team_batting_stats:
        error = 'Failed to parse home team batting stats'
        return Result.Fail(error)
    boxscore.home_team_data.batting_stats = home_team_batting_stats

    #print('Successfully parsed batting stats from BBRef boxscore page ({n} players total)'.format(n=len(batting_stats)))
    if not silent:
        pbar.update()
        pbar.set_description(f'Parsing pitch stats......')

    result = response.xpath(_PITCHING_STATS_TABLE)
    if not result or len(result) != 2:
        error = 'Failed to parse pitching stats table'
        return Result.Fail(error)
    away_team_pitch_table = result[0]
    home_team_pitch_table = result[1]

    away_team_pitcher_name_dict = _parse_pitcher_name_dict(away_team_pitch_table)
    if not away_team_pitcher_name_dict:
        error = 'Failed to parse away team pitcher name dictionary'
        return Result.Fail(error)

    away_team_pitching_stats =  _parse_pitching_stats(away_team_pitch_table, away_team_id, home_team_id)
    if not away_team_pitching_stats:
        error = 'Failed to parse away team pitching stats'
        return Result.Fail(error)
    boxscore.away_team_data.pitching_stats = away_team_pitching_stats

    home_team_pitcher_name_dict = _parse_pitcher_name_dict(home_team_pitch_table)
    if not home_team_pitcher_name_dict:
        error = 'Failed to parse home team pitcher name dictionary'
        return Result.Fail(error)
    batter_name_dict = {**away_team_batter_name_dict, **home_team_batter_name_dict}
    pitcher_name_dict = {**away_team_pitcher_name_dict, **home_team_pitcher_name_dict}
    player_name_dict = {**batter_name_dict, **pitcher_name_dict}

    home_team_pitching_stats =  _parse_pitching_stats(home_team_pitch_table, home_team_id, away_team_id)
    if not home_team_pitching_stats:
        error = 'Failed to parse home team pitching stats'
        return Result.Fail(error)
    boxscore.home_team_data.pitching_stats = home_team_pitching_stats

    #print('Successfully parsed pitching stats from BBRef boxscore page ({n} players total)'.format(n=len(pitching_stats)))
    if not silent:
        pbar.update()
        pbar.set_description(f'Parsing play-by-play.....')

    umpires = _parse_umpires(response)
    if not umpires:
        error = 'Failed to parse umpire info'
        return Result.Fail(error)
    boxscore.umpires = umpires

    #_print_umpires(umpires)

    away_team_lineup = _parse_away_team_lineup(response)
    if not away_team_lineup:
        error = 'Failed to parse away team starting lineup'
        return Result.Fail(error)
    boxscore.away_team_data.starting_lineup = away_team_lineup

    #print('\n{at} Starting Lineup:'.format(at=away_team_id))
    #_print_lineup(away_team_lineup)

    home_team_lineup = _parse_home_team_lineup(response)
    if not home_team_lineup:
        error = 'Failed to parse home team starting lineup'
        return Result.Fail(error)
    boxscore.home_team_data.starting_lineup = home_team_lineup

    #print('\n{ht} Starting Lineup:'.format(ht=home_team_id))
    #_print_lineup(home_team_lineup)

    result = response.xpath(_PLAY_BY_PLAY_TABLE)
    if not result:
        error = 'Failed to parse play by play table'
        return Result.Fail(error)
    play_by_play_table = result[0]

    inning_summaries_top = _parse_inning_summary_top(play_by_play_table)
    if not inning_summaries_top:
        error = 'Failed to parse inning start summaries'
        return Result.Fail(error)

    inning_summaries_bottom = _parse_inning_summary_bottom(play_by_play_table)
    if not inning_summaries_bottom:
        error = 'Failed to parse inning end summaries'
        return Result.Fail(error)

    boxscore.player_id_match_log = []
    result = _parse_in_game_substitutions(play_by_play_table, game_id, player_name_dict)
    if result.failure:
        error = f'Failed to parse in game substitutions:\n{result.error}'
        return Result.Fail(error)
    result_dict = result.value
    in_game_substitutions = result_dict['sub_list']
    boxscore.player_id_match_log.extend(result_dict['player_id_match_log'])

    result = _parse_play_by_play(
        play_by_play_table,
        game_id,
        batter_name_dict,
        pitcher_name_dict,
        away_team_id,
        home_team_id
    )
    if result.failure:
        error = f'rescrape_did_not_parse_play_by_play:\n{result.error}'
        return Result.Fail(error)
    result_dict = result.value
    play_by_play_events = result_dict['play_by_play']
    boxscore.player_id_match_log.extend(result_dict['player_id_match_log'])

    result = _create_innings_list(
        game_id,
        inning_summaries_top,
        inning_summaries_bottom,
        play_by_play_events,
        in_game_substitutions
    )
    if result.failure:
        error = f'Failed to parse innnings list:\n{result.error}'
        return Result.Fail(error)
    boxscore.innings_list = result.value

    #print('Sucessfully parsed play-by-play data from BBRef boxscore page ({n} events total)'.format(n=len(play_by_play)))
    if not silent:
        pbar.update()
        pbar.set_description(f'Finished parsing.........')

    player_team_dict = _create_player_team_dict(play_by_play_events)
    if not player_team_dict:
        error = 'Player name was unmatched in play by play events'
        return Result.Fail(error)

    player_team_dict = _verify_all_players_in_dict(response, player_team_dict)
    if not player_team_dict:
        error = 'Player id was not found in play by play events'
        return Result.Fail(error)
    boxscore.player_team_dict = player_team_dict
    boxscore.player_name_dict = player_name_dict

    if not silent:
        pbar.close()
    return Result.Ok(boxscore)


def _parse_attendance_from_strings(strings):
    attendance = []
    for i in range(0, len(strings)):
        matches = re.findall(_ATTENDANCE_PATTERN, strings[i])
        if matches:
            for m in matches:
                d = {"match": m.replace(',', ''), "index": i}
                attendance.append(d)
    if len(attendance) == 1:
        return attendance[0]
    return None


def _parse_venue_from_strings(strings):
    venue_terms = ['stadium', 'park', 'field', 'coliseum', 'centre', 'estadio']
    for i in range(0, len(strings)):
        for t in venue_terms:
            if t in strings[i].lower():
                d = {"match": strings[i][2:], "index": i}
                return d
    return None


def _parse_game_duration_from_strings(strings):
    duration = []
    for i in range(0, len(strings)):
        if 'start time' in strings[i].lower():
            continue
        matches = re.findall(_GAME_DURATION_PATTERN, strings[i])
        if matches:
            for m in matches:
                d = {"match": m, "index": i}
                duration.append(d)
    if len(duration) == 1:
        return duration[0]
    return None


def _parse_day_night_field_type_from_strings(strings):
    matches = []
    for i in range(0, len(strings)):
        if 'game, on' in strings[i].lower():
            d = {"match": strings[i], "index": i}
            matches.append(d)
    if len(matches) == 1:
        return matches[0]
    return None


def _parse_batting_stats(team_batting_table, player_team_id, opponent_team_id):
    batting_stats = []
    for player_id in team_batting_table.xpath(_BATTER_IDS_XPATH):
        stats_row = team_batting_table.xpath(_BATTER_STATS_ROW.format(pid=player_id))
        if not stats_row:
            return None
        player_stats = stats_row[0]

        at_bats = ""
        result = player_stats.xpath(_BATTER_AB_XPATH)
        if result is not None and len(result) > 0:
            at_bats = result[0]
        else:
            at_bats = "0"

        runs_scored = ""
        result = player_stats.xpath(_BATTER_R_XPATH)
        if result is not None and len(result) > 0:
            runs_scored = result[0]
        else:
            runs_scored = "0"

        hits = ""
        result = player_stats.xpath(_BATTER_H_XPATH)
        if result is not None and len(result) > 0:
            hits = result[0]
        else:
            hits = "0"

        rbis = ""
        result = player_stats.xpath(_BATTER_RBI_XPATH)
        if result is not None and len(result) > 0:
            rbis = result[0]
        else:
            rbis = "0"

        bases_on_balls = ""
        result = player_stats.xpath(_BATTER_BB_XPATH)
        if result is not None and len(result) > 0:
            bases_on_balls = result[0]
        else:
            bases_on_balls = "0"

        strikeouts = ""
        result = player_stats.xpath(_BATTER_SO_XPATH)
        if result is not None and len(result) > 0:
            strikeouts = result[0]
        else:
            strikeouts = "0"

        plate_appearances = ""
        result = player_stats.xpath(_BATTER_PA_XPATH)
        if result is not None and len(result) > 0:
            plate_appearances = result[0]
        else:
            plate_appearances = "0"

        avg_to_date = ""
        result = player_stats.xpath(_BATTER_AVG_TO_DATE_XPATH)
        if result is not None and len(result) > 0:
            avg_to_date = result[0]
        else:
            avg_to_date = "0"

        obp_to_date = ""
        result = player_stats.xpath(_BATTER_OBP_TO_DATE_XPATH)
        if result is not None and len(result) > 0:
            obp_to_date = result[0]
        else:
            obp_to_date = "0"

        slg_to_date = ""
        result = player_stats.xpath(_BATTER_SLG_TO_DATE_XPATH)
        if result is not None and len(result) > 0:
            slg_to_date = result[0]
        else:
            slg_to_date = "0"

        ops_to_date = ""
        result = player_stats.xpath(_BATTER_OPS_TO_DATE_XPATH)
        if result is not None and len(result) > 0:
            ops_to_date = result[0]
        else:
            ops_to_date = "0"

        total_pitches = ""
        result = player_stats.xpath(_BATTER_TOTAL_PITCHES_XPATH)
        if result is not None and len(result) > 0:
            total_pitches = result[0]
        else:
            total_pitches = "0"

        total_strikes = ""
        result = player_stats.xpath(_BATTER_TOTAL_STRIKES_XPATH)
        if result is not None and len(result) > 0:
            total_strikes = result[0]
        else:
            total_strikes = "0"

        wpa_bat = ""
        result = player_stats.xpath(_BATTER_WPA_XPATH)
        if result is not None and len(result) > 0:
            wpa_bat = result[0]
        else:
            wpa_bat = "0"

        avg_lvg_index = ""
        result = player_stats.xpath(_BATTER_ALI_XPATH)
        if result is not None and len(result) > 0:
            avg_lvg_index = result[0]
        else:
            avg_lvg_index = "0"

        wpa_bat_pos = ""
        result = player_stats.xpath(_BATTER_WPA_POS_XPATH)
        if result is not None and len(result) > 0:
            wpa_bat_pos = result[0]
        else:
            wpa_bat_pos = "0"

        wpa_bat_neg = ""
        result = player_stats.xpath(_BATTER_WPA_NEG_XPATH)
        if result is not None and len(result) > 0:
            wpa_bat_neg = result[0]
        else:
            wpa_bat_neg = "0"

        re24_bat = ""
        result = player_stats.xpath(_BATTER_RE24_XPATH)
        if result is not None and len(result) > 0:
            re24_bat = result[0]
        else:
            re24_bat = "0"

        details = []
        result = player_stats.xpath(_BATTER_DETAILS_XPATH)
        if result is not None and len(result) > 0:
            for s in result[0].split(','):
                d = BBRefBatStatsDetail()
                t = s.split('\u00b7')
                if len(t) == 1:
                    d.count = "1"
                    d.stat = t[0].strip('\n')
                    details.append(d)
                if len(t) == 2:
                    d.count = t[0]
                    d.stat = t[1].strip('\n')
                    details.append(d)

        player_batting_stats = BBRefBatStats()
        player_batting_stats.player_id_br = player_id
        player_batting_stats.player_team_id_br = player_team_id
        player_batting_stats.opponent_team_id_br = opponent_team_id
        player_batting_stats.at_bats = at_bats
        player_batting_stats.runs_scored = runs_scored
        player_batting_stats.hits = hits
        player_batting_stats.rbis = rbis
        player_batting_stats.bases_on_balls = bases_on_balls
        player_batting_stats.strikeouts = strikeouts
        player_batting_stats.plate_appearances = plate_appearances
        player_batting_stats.avg_to_date = avg_to_date
        player_batting_stats.obp_to_date = obp_to_date
        player_batting_stats.slg_to_date = slg_to_date
        player_batting_stats.ops_to_date = ops_to_date
        player_batting_stats.total_pitches = total_pitches
        player_batting_stats.total_strikes = total_strikes
        player_batting_stats.wpa_bat = wpa_bat
        player_batting_stats.avg_lvg_index = avg_lvg_index
        player_batting_stats.wpa_bat_pos = wpa_bat_pos
        player_batting_stats.wpa_bat_neg = wpa_bat_neg
        player_batting_stats.re24_bat = re24_bat
        player_batting_stats.details = details
        batting_stats.append(player_batting_stats)
    return batting_stats


def _parse_pitching_stats(team_pitching_table, player_team_id, opponent_team_id):
    pitch_appearances = []
    for player_id in team_pitching_table.xpath(_PITCHER_IDS_XPATH):
        stats_row = team_pitching_table.xpath(_PITCHER_STATS_ROW.format(pid=player_id))
        if not stats_row:
            return None
        player_stats = stats_row[0]

        innings_pitched = ""
        result = player_stats.xpath(_PITCHER_IP_XPATH)
        if result is not None and len(result) > 0:
            innings_pitched = result[0]
        else:
            innings_pitched = "0"

        hits = ""
        result = player_stats.xpath(_PITCHER_H_XPATH)
        if result is not None and len(result) > 0:
            hits = result[0]
        else:
            hits = "0"

        runs =  ""
        result = player_stats.xpath(_PITCHER_R_XPATH)
        if result is not None and len(result) > 0:
            runs = result[0]
        else:
            runs = "0"

        earned_runs = ""
        result = player_stats.xpath(_PITCHER_ER_XPATH)
        if result is not None and len(result) > 0:
            earned_runs = result[0]
        else:
            earned_runs = "0"

        bases_on_balls = ""
        result = player_stats.xpath(_PITCHER_BB_XPATH)
        if result is not None and len(result) > 0:
            bases_on_balls = result[0]
        else:
            bases_on_balls = "0"

        strikeouts = ""
        result = player_stats.xpath(_PITCHER_SO_XPATH)
        if result is not None and len(result) > 0:
            strikeouts = result[0]
        else:
            strikeouts = "0"

        homeruns = ""
        result = player_stats.xpath(_PITCHER_HR_XPATH)
        if result is not None and len(result) > 0:
            homeruns = result[0]
        else:
            homeruns = "0"

        batters_faced = ""
        result = player_stats.xpath(_PITCHER_BATTERS_FACED_XPATH)
        if result is not None and len(result) > 0:
            batters_faced = result[0]
        else:
            batters_faced = "0"

        pitch_count = ""
        result = player_stats.xpath(_PITCHER_PITCH_COUNT_XPATH)
        if result is not None and len(result) > 0:
            pitch_count = result[0]
        else:
            pitch_count = "0"

        strikes = ""
        result = player_stats.xpath(_PITCHER_STRIKES_TOTAL_XPATH)
        if result is not None and len(result) > 0:
            strikes = result[0]
        else:
            strikes = "0"

        strikes_contact = ""
        result = player_stats.xpath(_PITCHER_STRIKES_CONTACT_XPATH)
        if result is not None and len(result) > 0:
            strikes_contact = result[0]
        else:
            strikes_contact = "0"

        strikes_swinging = ""
        result = player_stats.xpath(_PITCHER_STRIKES_SWINGING_XPATH)
        if result is not None and len(result) > 0:
            strikes_swinging = result[0]
        else:
            strikes_swinging = "0"

        strikes_looking = ""
        result = player_stats.xpath(_PITCHER_STRIKES_LOOKING_XPATH)
        if result is not None and len(result) > 0:
            strikes_looking = result[0]
        else:
            strikes_looking = "0"

        ground_balls = ""
        result = player_stats.xpath(_PITCHER_GB_XPATH)
        if result is not None and len(result) > 0:
            ground_balls = result[0]
        else:
            ground_balls = "0"

        fly_balls = ""
        result = player_stats.xpath(_PITCHER_FB_XPATH)
        if result is not None and len(result) > 0:
            fly_balls = result[0]
        else:
            fly_balls = "0"

        line_drives = ""
        result = player_stats.xpath(_PITCHER_LD_XPATH)
        if result is not None and len(result) > 0:
            line_drives = result[0]
        else:
            line_drives = "0"

        unknown_type = ""
        result = player_stats.xpath(_PITCHER_UNK_XPATH)
        if result is not None and len(result) > 0:
            unknown_type = result[0]
        else:
            unknown_type = "0"

        game_score = ""
        result = player_stats.xpath(_PITCHER_GSC_XPATH)
        if result is not None and len(result) > 0:
            game_score = result[0]
        else:
            game_score = "0"

        inherited_runners = ""
        result = player_stats.xpath(_PITCHER_IR_XPATH)
        if result is not None and len(result) > 0:
            inherited_runners = result[0]
        else:
            inherited_runners = "0"

        inherited_scored = ""
        result = player_stats.xpath(_PITCHER_IS_XPATH)
        if result is not None and len(result) > 0:
            inherited_scored = result[0]
        else:
            inherited_scored = "0"

        wpa_pitch = ""
        result = player_stats.xpath(_PITCHER_WPA_XPATH)
        if result is not None and len(result) > 0:
            wpa_pitch = result[0]
        else:
            wpa_pitch = "0"

        avg_lvg_index = ""
        result = player_stats.xpath(_PITCHER_ALI_XPATH)
        if result is not None and len(result) > 0:
            avg_lvg_index = result[0]
        else:
            avg_lvg_index = "0"

        re24_pitch = ""
        result = player_stats.xpath(_PITCHER_RE24_XPATH)
        if result is not None and len(result) > 0:
            re24_pitch = result[0]
        else:
            re24_pitch  = "0"

        pitch_app = BBRefPitchStats()
        pitch_app.player_id_br = player_id
        pitch_app.player_team_id_br = player_team_id
        pitch_app.opponent_team_id_br = opponent_team_id
        pitch_app.innings_pitched = innings_pitched
        pitch_app.hits = hits
        pitch_app.runs = runs
        pitch_app.earned_runs = earned_runs
        pitch_app.bases_on_balls = bases_on_balls
        pitch_app.strikeouts = strikeouts
        pitch_app.homeruns = homeruns
        pitch_app.batters_faced = batters_faced
        pitch_app.pitch_count = pitch_count
        pitch_app.strikes = strikes
        pitch_app.strikes_contact = strikes_contact
        pitch_app.strikes_swinging = strikes_swinging
        pitch_app.strikes_looking = strikes_looking
        pitch_app.ground_balls = ground_balls
        pitch_app.fly_balls = fly_balls
        pitch_app.line_drives = line_drives
        pitch_app.unknown_type = unknown_type
        pitch_app.game_score = game_score
        pitch_app.inherited_runners = inherited_runners
        pitch_app.inherited_scored = inherited_scored
        pitch_app.wpa_pitch = wpa_pitch
        pitch_app.avg_lvg_index = avg_lvg_index
        pitch_app.re24_pitch = re24_pitch
        pitch_appearances.append(pitch_app)
    return pitch_appearances

def _parse_play_by_play(response, game_id, batter_id_dict, pitcher_id_dict, away_team_id, home_team_id):
    pbp_inning_list = response.xpath(_PBP_INNING_XPATH)
    pbp_score_list = response.xpath(_PBP_SCORE_XPATH)
    pbp_outs_before_play_list = response.xpath(_PBP_OUTS_XPATH)
    pbp_runners_on_base_list = response.xpath(_PBP_RUNNERS_ON_BASE_XPATH)
    pbp_pitch_sequence = response.xpath(_PBP_PITCH_SEQUENCE_XPATH)
    pbp_team_batting = response.xpath(_PBP_TEAM_BATTING_XPATH)
    pbp_batter_name = response.xpath(_PBP_BATTER_NAME_XPATH)
    pbp_pitcher_name = response.xpath(_PBP_PITCHER_NAME_XPATH)
    pbp_play_description = response.xpath(_PBP_PLAY_DESC_XPATH)

    pbp_runs_outs_result = []
    pbp_row_numbers = []
    for i in range(0, len(pbp_inning_list)):
        play_num = i + 1
        runs_outs_query = _PBP_RUNS_OUTS_XPATH.format(n=play_num)
        row_number_query = Template(_T_PBP_ROW_NUMBER_XPATH).substitute(n=play_num)

        runs_outs = ""
        result = response.xpath(runs_outs_query)
        if result is not None and len(result) > 0:
            runs_outs = result[0]
        pbp_runs_outs_result.append(runs_outs)

        row_num = 0
        result = response.xpath(row_number_query)
        if result:
            row_num = int(result[0])
        pbp_row_numbers.append(row_num)

    play_by_play = []
    player_id_match_log = []
    for i in range(0, len(pbp_inning_list)):
        try:
            play = BBRefPlayByPlayEvent()
            play.scrape_success = True
            play.pbp_table_row_number = pbp_row_numbers[i]
            play.inning_label = pbp_inning_list[i]
            play.score = pbp_score_list[i]
            play.outs_before_play = pbp_outs_before_play_list[i]
            play.runners_on_base = pbp_runners_on_base_list[i]
            play.pitch_sequence = pbp_pitch_sequence[i]
            play.runs_outs_result = pbp_runs_outs_result[i]
            play.team_batting_id_br = pbp_team_batting[i]
            play.play_description = pbp_play_description[i]

            if pbp_team_batting[i] == away_team_id:
                play.team_pitching_id_br = home_team_id
            else:
                play.team_pitching_id_br = away_team_id
        except IndexError as e:
            error = f'Error: {repr(e)}'
            return Result.Fail(error)

        try:
            pitcher_name = pbp_pitcher_name[i].replace(u'\xa0', u' ')
            result = _match_player_name_to_player_id(pitcher_name, pitcher_id_dict)
            if result.failure:
                return result
            match_dict = result.value
            if match_dict['match_type'] != 'Exact match':
                player_id_match_log.append(match_dict['match_details'])
            play.pitcher_id_br = match_dict['player_id']
        except Exception as e:
            error = f"""
            Exception occurred trying to match '{pitcher_name}' with a player_id:
            Error: {repr(e)}
            """
            return Result.Fail(error)

        try:
            batter_name = pbp_batter_name[i].replace(u'\xa0', u' ')
            result = _match_player_name_to_player_id(batter_name, batter_id_dict)
            if result.failure:
                return result
            match_dict = result.value
            if match_dict['match_type'] != 'Exact match':
                player_id_match_log.append(match_dict['match_details'])
            play.batter_id_br = match_dict['player_id']
        except Exception as e:
            error = f"""
            Exception occurred trying to match '{batter_name}' with a player_id:
            Error: {repr(e)}
            """
            return Result.Fail(error)

        play_by_play.append(play)

    result_dict = dict(
        play_by_play=play_by_play,
        player_id_match_log=player_id_match_log
    )
    return Result.Ok(result_dict)

def _match_player_name_to_player_id(name, id_dict):
    try:
        result = _match_player_id(name, id_dict)
        if result.failure:
            return result
        match_dict = result.value
        player_id = match_dict['player_id']
        match_type = match_dict['match_type']
        match_details = None
        if match_type != 'Exact match':
            dict_swapped = {v:k for k,v in id_dict.items()}
            matched_name = dict_swapped[player_id]
            match_details = dict(
                player_name=name,
                match_type=match_type,
                matched_id=player_id,
                matched_name=matched_name
            )
        result = dict(
            player_id=player_id,
            match_type=match_type,
            match_details=match_details
        )
        return Result.Ok(result)
    except Exception as e:
        error = f"""
        Exception occurred trying to match '{name}' with a player_id:
        Error: {repr(e)}
        """
        return Result.Fail(error)

def _create_player_team_dict(play_by_play):
    ids = []
    teams = []
    for pbp in play_by_play:
        ids.append(pbp.batter_id_br)
        ids.append(pbp.pitcher_id_br)
        teams.append(pbp.team_batting_id_br)
        teams.append(pbp.team_pitching_id_br)

    player_dict = dict(zip(ids, teams))
    for id in player_dict.keys():
        if len(id) == 0:
            return None
    return player_dict

def _verify_all_players_in_dict(response, player_dict):
    player_ids = response.xpath(_BATTER_IDS_XPATH)
    pitcher_ids = response.xpath(_PITCHER_IDS_XPATH)
    player_ids.extend(pitcher_ids)

    missing_ids = set(player_ids) - set(player_dict.keys())
    if not missing_ids:
        return player_dict

    for missing in missing_ids:
        i = player_ids.index(missing)
        if i == 0:
            return None
        searching_for_team = True
        while(searching_for_team):
            i -= 1
            prev_player = player_ids[i]
            if prev_player in missing_ids:
                continue
            prev_player_team = player_dict[prev_player]
            player_dict[missing] = prev_player_team
            searching_for_team = False
    return player_dict

def _parse_bbref_gameid_from_url(url):
    matches = re.findall(_GAME_ID_PATTERN, url)
    return matches[0] if matches else None

def _parse_away_team_id(response):
    name_urls = response.xpath(_TEAM_ID_XPATH)
    if not name_urls:
        return None
    matches = re.findall(_TEAM_ID_PATTERN, name_urls[0])
    return matches[0] if matches else None

def _parse_home_team_id(response):
    name_urls = response.xpath(_TEAM_ID_XPATH)
    if not name_urls:
        return None
    matches = re.findall(_TEAM_ID_PATTERN, name_urls[1])
    return matches[0] if matches else None

def _parse_away_team_runs(response):
    runs_scored = response.xpath(_TEAM_RUNS_XPATH)
    if not runs_scored:
        return None
    return runs_scored[0]

def _parse_home_team_runs(response):
    runs_scored = response.xpath(_TEAM_RUNS_XPATH)
    if not runs_scored or len(runs_scored) < 2:
        return None
    return runs_scored[1]

def _parse_away_team_record(response):
    team_record_dirty = response.xpath(_AWAY_TEAM_RECORD_XPATH)
    if not team_record_dirty:
        return None
    team_record = team_record_dirty[0].split('-')
    if len(team_record) != 2:
        return None
    return team_record

def _parse_home_team_record(response):
    team_record_dirty = response.xpath(_HOME_TEAM_RECORD_XPATH)
    if not team_record_dirty:
        return None
    team_record = team_record_dirty[0].split('-')
    if len(team_record) != 2:
        return None
    return team_record

def _parse_linescore_totals(response, team_id, is_home_team):
    if is_home_team:
        query = _LINESCORE_HOME_VALS_XPATH
    else:
        query = _LINESCORE_AWAY_VALS_XPATH

    keys = response.xpath(_LINESCORE_KEYS_XPATH)
    vals = response.xpath(query)
    if not keys or not vals:
        return None
    if len(keys) < 2 or len(vals) < 2:
        return None
    del keys[0:2]
    del vals[0:2]
    if len(keys) < 3 or len(vals) < 3:
        return None
    del keys[:-3]
    del vals[:-3]
    if len(keys) != len(vals):
        return None
    parsed_totals = dict(zip(keys, vals))
    linescore_totals = BBRefTeamLinescoreTotals()
    linescore_totals.team_id_br = team_id
    linescore_totals.total_runs = parsed_totals['R']
    linescore_totals.total_hits = parsed_totals['H']
    linescore_totals.total_errors = parsed_totals['E']
    return linescore_totals

def _parse_batter_name_dict(team_batting_table):
    batter_ids = team_batting_table.xpath(_BATTER_IDS_XPATH)
    batter_names = team_batting_table.xpath(_BATTER_NAMES_XPATH)
    if not batter_ids or not batter_names:
        return None
    if len(batter_ids) != len(batter_names):
        return None
    return dict(zip(batter_names, batter_ids))

def _parse_pitcher_name_dict(team_pitching_table):
    pitcher_ids = team_pitching_table.xpath(_PITCHER_IDS_XPATH)
    pitcher_names = team_pitching_table.xpath(_PITCHER_NAMES_XPATH)
    if not pitcher_ids or not pitcher_names:
        return None
    if len(pitcher_ids) != len(pitcher_names):
        return None
    return dict(zip(pitcher_names, pitcher_ids))

def _parse_umpires(response):
    umpires = response.xpath(_UMPIRES_XPATH)[0]
    if umpires is None:
        return None
    split = umpires.split(',')
    umps = []
    for s in split:
        u = BBRefUmpire()
        split2 = s.strip('.').strip().split('-')
        if len(split2) != 2:
            return None
        u.field_location = split2[0].strip()
        u.umpire_name = split2[1].strip()
        umps.append(u)
    return umps

def _parse_away_team_lineup(response):
    bat_orders = response.xpath(_AWAY_LINEUP_ORDER_XPATH)
    player_links = response.xpath(_AWAY_LINEUP_PLAYER_XPATH)
    def_positions = response.xpath(_AWAY_LINEUP_DEF_POS_XPATH)
    if bat_orders is None or player_links is None or def_positions is None:
        return None

    lineup = []
    for i in range(0, len(bat_orders)):
        bat = BBRefStartingLineupSlot()
        split = player_links[i].split('/')
        if len(split) != 4:
            return None
        bat.player_id_br = split[3][:-6]
        bat.bat_order = bat_orders[i]
        bat.def_position = def_positions[i]
        lineup.append(bat)
    return lineup

def _parse_home_team_lineup(response):
    bat_orders = response.xpath(_HOME_LINEUP_ORDER_XPATH)
    player_links = response.xpath(_HOME_LINEUP_PLAYER_XPATH)
    def_positions = response.xpath(_HOME_LINEUP_DEF_POS_XPATH)
    if bat_orders is None or player_links is None or def_positions is None:
        return None

    lineup = []
    for i in range(0, len(bat_orders)):
        bat = BBRefStartingLineupSlot()
        split = player_links[i].split('/')
        if len(split) != 4:
            return None
        bat.player_id_br = split[3][:-6]
        bat.bat_order = bat_orders[i]
        bat.def_position = def_positions[i]
        lineup.append(bat)
    return lineup

def _match_player_id(name, id_dict):
    if name in id_dict:
        result = dict(
            match_type='Exact match',
            player_id=id_dict[name]
        )
    else:
        (best_match, _) = fuzzy_match(name, id_dict.keys())
        result = dict(
            match_type='Fuzzy match',
            player_id=id_dict[best_match]
        )

    return Result.Ok(result)

def _parse_inning_summary_top(response):
    summaries = response.xpath(_PBP_INNING_SUMMARY_TOP_XPATH)
    if summaries is None:
        return None
    inning_summaries = []
    for s in summaries:
        inning_summaries.append(s.strip().replace(u'\xa0', u' '))
    row_numbers = response.xpath(_PBP_INNING_SUMMARY_TOP_ROW_NUM_XPATH)
    if row_numbers is None:
        return None
    summary_row_numbers = []
    for s in row_numbers:
        summary_row_numbers.append(int(s))
    if len(inning_summaries) != len(summary_row_numbers):
        return None
    return dict(zip(summary_row_numbers, inning_summaries))

def _parse_inning_summary_bottom(response):
    summaries = response.xpath(_PBP_INNING_SUMMARY_BOTTOM_XPATH)
    if summaries is None:
        return None
    inning_summaries = []
    for s in summaries:
        inning_summaries.append(s.strip())
    last_summary = response.xpath(_PBP_INNING_SUMMARY_BOTTOM_LAST_XPATH)
    for s in last_summary:
        inning_summaries.append(s.strip())
    row_numbers = response.xpath(_PBP_INNING_SUMMARY_BOTTOM_ROW_NUM_XPATH)
    if row_numbers is None:
        return None
    summary_row_numbers = []
    for s in row_numbers:
        summary_row_numbers.append(int(s))
    if len(inning_summaries) != len(summary_row_numbers):
        return None
    return dict(zip(summary_row_numbers, inning_summaries))

def _parse_in_game_substitutions(response, game_id, player_name_dict):
    row_numbers = response.xpath(_PBP_IN_GAME_SUBSTITUTION_ROW_NUM_XPATH)
    if row_numbers is None:
        error = 'No in game substitutions found in play-by-play table'
        return Result.Fail(error)

    sub_list = []
    player_id_match_log = []
    for n in row_numbers:
        row_num = int(n)
        subs_xpath = Template(_T_PBP_IN_GAME_SUBSTITUTION_XPATH).substitute(row=row_num)
        sub_descriptions = response.xpath(subs_xpath)
        if sub_descriptions is None:
            continue

        for i in range(0, len(sub_descriptions)):
            s = sub_descriptions[i].strip().replace(u'\xa0', u' ')
            result = _parse_substitution_description(s)
            if result.failure:
                return result
            sub_dict = result.value
            substitution = BBRefInGameSubstitution()
            substitution.pbp_table_row_number = row_num
            substitution.sub_description = sub_dict['description']
            substitution.incoming_player_pos = sub_dict['incoming_player_pos']
            substitution.outgoing_player_pos = sub_dict['outgoing_player_pos']
            substitution.lineup_slot = sub_dict['lineup_slot']

            result = _get_sub_player_ids(
                sub_dict['incoming_player_name'],
                sub_dict['outgoing_player_name'],
                player_name_dict
            )
            if result.failure:
                return result
            id_dict = result.value
            if id_dict['player_id_match_log']:
                player_id_match_log.extend(id_dict['player_id_match_log'])

            substitution.outgoing_player_id_br = id_dict['outgoing_player_id_br']
            substitution.incoming_player_id_br = id_dict['incoming_player_id_br']
            sub_list.append(substitution)

    result = dict(
        sub_list=sub_list,
        player_id_match_log=player_id_match_log
    )
    return Result.Ok(result)

def _parse_substitution_description(sub_description):
    parsed_sub = {}
    parsed_sub['description'] = sub_description
    split = None
    pre_split = [s.strip() for s in sub_description.split('(change occurred mid-batter)')]
    sub_description = pre_split[0]
    if 'replaces' in sub_description:
        split = [s.strip() for s in sub_description.split('replaces')]
    elif 'pinch hit for' and 'and is now' in sub_description:
        split = [s.strip() for s in sub_description.split('pinch hit for')]
        parsed_sub['incoming_player_name'] = split[0]
        remaining_description = split[1]
        split2 = [s.strip() for s in remaining_description.split('and is now')]
        parsed_sub['incoming_player_pos'] = split[0]
        parsed_sub['outgoing_player_pos'] = split[0]
        parsed_sub['outgoing_player_name'] = 'N/A'
        parsed_sub['lineup_slot'] = 0
        return Result.Ok(parsed_sub)
    elif 'pinch hits for' in sub_description:
        parsed_sub['incoming_player_pos'] = 'PH'
        split = [s.strip() for s in sub_description.split('pinch hits for')]
    elif 'pinch runs for' in sub_description:
        parsed_sub['incoming_player_pos'] = 'PR'
        split = [s.strip() for s in sub_description.split('pinch runs for')]
    elif 'moves' in sub_description:
        split = [s.strip() for s in sub_description.split('moves')]
    else:
        error = 'Substitution description was in an unrecognized format. (Before first split)'
        return Result.Fail(error)

    if not split or len(split) != 2:
        error = 'First split operation did not produce a list with length=2.'
        return Result.Fail(error)
    parsed_sub['incoming_player_name'] = split[0]
    remaining_description = split[1]

    if 'batting' in remaining_description:
        split2 = [s.strip() for s in remaining_description.split('batting')]
    elif 'from' in remaining_description:
        match = CHANGE_POS_REGEX.search(remaining_description)
        if match:
            match_dict = match.groupdict()
            parsed_sub['outgoing_player_name'] = parsed_sub['incoming_player_name']
            parsed_sub['outgoing_player_pos'] = match_dict['old_pos']
            parsed_sub['incoming_player_pos'] = match_dict['new_pos']
            parsed_sub['lineup_slot'] = 0
            return Result.Ok(parsed_sub)
    elif remaining_description.endswith('pitching'):
        split2 = [s.strip() for s in remaining_description.split('pitching')]
        parsed_sub['outgoing_player_name'] = split2[0]
        parsed_sub['outgoing_player_pos'] = 'P'
        parsed_sub['incoming_player_pos'] = 'P'
        parsed_sub['lineup_slot'] = 0
        return Result.Ok(parsed_sub)
    else:
        error = 'Substitution description was in an unrecognized format. (After first split)'
        return Result.Fail(error)

    if not split2 or len(split2) != 2:
        error = 'Second split operation did not produce a list with length=2.'
        return Result.Fail(error)
    remaining_description = split2[0]
    lineup_slot_str = split2[1]

    match = NUM_REGEX.search(lineup_slot_str)
    if not match:
        error = 'Failed to parse batting order number.'
        return Result.Fail(error)
    parsed_sub['lineup_slot'] = int(match.group())

    split3 = None
    match = POS_REGEX.search(remaining_description)
    if match:
        parsed_sub['outgoing_player_pos'] = match.group()[1:-1]
        split3 = [s.strip() for s in remaining_description.split(match.group())]
        if not split3 or len(split3) != 2:
            error = 'Third split operation did not produce a list with length=2.'
            return Result.Fail(error)
        parsed_sub['outgoing_player_name'] = split3[0]
        remaining_description = split3[1]
    if not remaining_description:
        return Result.Ok(parsed_sub)

    split4 = None
    if 'pitching' in remaining_description:
        parsed_sub['incoming_player_pos'] = 'P'
        split4 = [s.strip() for s in remaining_description.split('pitching')]
        if not split4 or len(split4) != 2:
            error = 'Fourth split operation did not produce a list with length=2.'
            return Result.Fail(error)
        if 'outgoing_player_name' not in parsed_sub:
            parsed_sub['outgoing_player_name'] = split4[0]
        remaining_description = split4[1]

    elif 'playing' in remaining_description:
        split4 = [s.strip() for s in remaining_description.split('playing')]
        if not split4 or len(split4) != 2:
            error = 'Fourth split operation did not produce a list with length=2.'
            return Result.Fail(error)
        if 'outgoing_player_name' not in parsed_sub:
            parsed_sub['outgoing_player_name'] = split4[0]
        remaining_description = split4[1]

        found_incoming_player_pos = False
        for pos in DEFENSE_POSITIONS:
            if pos in remaining_description:
                parsed_sub['incoming_player_pos'] = pos
                found_incoming_player_pos = True
                break
        if not found_incoming_player_pos:
            error = (
                'Unable to find incoming player position in remaining description:\n'
                f'remaining_description: {remaining_description}\n'
            )
            return Result.Fail(error)
    elif 'pinch hit' in sub_description:
        parsed_sub['outgoing_player_name'] = remaining_description
        parsed_sub['outgoing_player_pos'] = 'PH'

    if 'outgoing_player_pos' not in parsed_sub:
        parsed_sub['outgoing_player_pos'] = parsed_sub['incoming_player_pos']
    return Result.Ok(parsed_sub)

def _get_sub_player_ids(incoming_player_name, outgoing_player_name, player_name_dict):
    player_id_match_log = []
    if outgoing_player_name != 'N/A':
        try:
            result = _match_player_name_to_player_id(
                outgoing_player_name,
                player_name_dict
            )
            if result.failure:
                return result
            match_dict = result.value
            if match_dict['match_type'] != 'Exact match':
                player_id_match_log.append(match_dict['match_details'])
            outgoing_player_id_br = match_dict['player_id']
        except Exception as e:
            error = f"""
            Exception occurred trying to match '{outgoing_player_name}' with a player_id:
            Error: {repr(e)}
            """
            return Result.Fail(error)

    if incoming_player_name != 'N/A':
        try:
            result = _match_player_name_to_player_id(
                incoming_player_name,
                player_name_dict
            )
            if result.failure:
                return result
            match_dict = result.value
            if match_dict['match_type'] != 'Exact match':
                player_id_match_log.append(match_dict['match_details'])
            incoming_player_id_br = match_dict['player_id']
        except Exception as e:
            error = f"""
            Exception occurred trying to match '{incoming_player_name}' with a player_id:
            Error: {repr(e)}
            """
            return Result.Fail(error)

    if incoming_player_name == 'N/A':
        incoming_player_id_br = 'N/A'
    if outgoing_player_name == 'N/A':
        outgoing_player_id_br = 'N/A'

    result = dict(
        incoming_player_id_br=incoming_player_id_br,
        outgoing_player_id_br=outgoing_player_id_br,
        player_id_match_log=player_id_match_log
    )
    return Result.Ok(result)

def _create_innings_list(
    game_id,
    summaries_begin,
    summaries_end,
    game_events,
    substitutions
):
    if len(summaries_begin) != len(summaries_end):
        error = 'Begin inning and end inning summary list lengths do not match.'
        return Result.Fail(error)

    start_boundaries = sorted(summaries_begin.keys())
    end_boundaries = sorted(summaries_end.keys())
    inning_number = 0
    innings_list = []
    for i in range(0, len(summaries_begin)):
        if is_even(i):
            inning_number += 1
            inning_label = f't{inning_number}'
            inning_id = f'{game_id}_INN_TOP{inning_number:02d}'
        else:
            inning_label = f'b{inning_number}'
            inning_id = f'{game_id}_INN_BOT{inning_number:02d}'

        start_row = start_boundaries[i]
        end_row = end_boundaries[i]
        inning_events = [
            g for g in game_events
            if g.pbp_table_row_number > start_row and
            g.pbp_table_row_number < end_row
        ]
        inning_substitutions = [
            sub for sub in substitutions
            if sub.pbp_table_row_number > start_row and
            sub.pbp_table_row_number < end_row
        ]

        inning = BBRefHalfInning()
        inning.inning_id = inning_id
        inning.inning_label = inning_label
        inning.begin_inning_summary = summaries_begin[start_row]
        inning.end_inning_summary = summaries_end[end_row]
        inning.game_events = inning_events
        inning.substitutions = inning_substitutions

        for event in inning_events:
            event.inning_id = inning_id

        for sub in inning_substitutions:
            sub.inning_id = inning_id
            sub.inning_label = inning_label

        match = INNING_TOTALS_REGEX.search(summaries_end[end_row])
        if match:
            inning_totals = match.groupdict()
            inning.inning_total_runs = inning_totals['runs']
            inning.inning_total_hits = inning_totals['hits']
            inning.inning_total_errors = inning_totals['errors']
            inning.inning_total_left_on_base = inning_totals['left_on_base']
            inning.away_team_runs_after_inning = inning_totals['away_team_runs']
            inning.home_team_runs_after_inning = inning_totals['home_team_runs']

        innings_list.append(inning)
    return Result.Ok(innings_list)

def _print_lineup(lineup):
    for bat in lineup:
        print('{o}: {b} ({p})'.format(
            o=bat.bat_order, b=bat.player_id_br, p=bat.def_position)
        )

def _print_umpires(umpires):
    print('\nUmpires:')
    for i in range(0, len(umpires)):
        print('{pos}: {ump}'.format(pos=umpires[i].field_location, ump=umpires[i].umpire_name))

