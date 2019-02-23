"""Decode json dicts of scraped data to custom objects."""
from datetime import datetime, timezone
from dateutil import parser, tz

from app.main.data.scrape.bbref.models.bat_stats import BBRefBatStats
from app.main.data.scrape.bbref.models.bat_stats_detail import BBRefBatStatsDetail
from app.main.data.scrape.bbref.models.boxscore import BBRefBoxScore
from app.main.data.scrape.bbref.models.games_for_date import BBRefGamesForDate
from app.main.data.scrape.bbref.models.inning_runs_scored import BBRefInningRunsScored
from app.main.data.scrape.bbref.models.pbp_event import BBRefPlayByPlayEvent
from app.main.data.scrape.bbref.models.pitch_stats import BBRefPitchStats
from app.main.data.scrape.bbref.models.starting_lineup_slot import BBRefStartingLineupSlot
from app.main.data.scrape.bbref.models.team_linescore_totals import BBRefTeamLinescoreTotals
from app.main.data.scrape.bbref.models.umpire import BBRefUmpire
from app.main.data.scrape.brooks.models.games_for_date import BrooksGamesForDate
from app.main.data.scrape.brooks.models.game_info import BrooksGameInfo


def decode_bbref_games_for_date(json_dict):
    """Convert json dictionary to BbrefGamesForDate"""
    if "__bbref_games_for_date__" in json_dict:
        games_for_date = BBRefGamesForDate()
        games_for_date.dashboard_url = json_dict['dashboard_url']
        games_for_date.game_date_str = json_dict['game_date_str']
        games_for_date.game_count = json_dict['game_count']
        games_for_date.boxscore_urls = json_dict['boxscore_urls']

        try:
            games_for_date.game_date = parser.parse(json_dict['game_date_str'])
            return dict(success=True, result=games_for_date)
        except Exception as e:
            error = f'Error: {repr(e)}'
            return dict(success=False, message=error)

def decode_brooks_game_info(json_dict):
    """Convert json dictionary to BrooksGameInfo object."""
    if '__brooks_game_info__' in json_dict:
        gameinfo = BrooksGameInfo()
        gameinfo.game_date_year = json_dict['game_date_year']
        gameinfo.game_date_month = json_dict['game_date_month']
        gameinfo.game_date_day = json_dict['game_date_day']
        gameinfo.game_time_hour = json_dict['game_time_hour']
        gameinfo.game_time_minute = json_dict['game_time_minute']
        gameinfo.time_zone_name = json_dict['time_zone_name']
        gameinfo.bb_game_id = json_dict['bb_game_id']
        gameinfo.away_team_id_bb = json_dict['away_team_id_bb']
        gameinfo.home_team_id_bb = json_dict['home_team_id_bb']
        gameinfo.game_number_this_day = json_dict['game_number_this_day']
        gameinfo.pitcher_appearance_count = json_dict['pitcher_appearance_count']
        gameinfo.pitcher_appearance_dict = json_dict['pitcher_appearance_dict']

        try:
            gameinfo.game_start_time = datetime(
                year=json_dict['game_date_year'],
                month=json_dict['game_date_month'],
                day=json_dict['game_date_day'],
                hour=json_dict['game_time_hour'],
                minute=json_dict['game_time_minute'],
                tzinfo=tz.gettz(json_dict['time_zone_name'])
            )
            return gameinfo
        except Exception as e:
            error = f'Error: {repr(e)}'
            return dict(success=False, message=error)

def decode_brooks_games_for_date(json_dict):
    """Convert json dictionary to BrooksGamesForDate object."""
    if '__brooks_games_for_date__' in json_dict:
        games_for_date = BrooksGamesForDate()
        games_for_date.dashboard_url = json_dict['dashboard_url']
        games_for_date.game_date_str = json_dict['game_date_str']
        games_for_date.game_count = json_dict['game_count']

        try:
            games_for_date.game_date = parser.parse(json_dict['game_date_str'])
            games_for_date.games = [
                decode_brooks_game_info(g)
                for g
                in json_dict['games']
            ]
            return dict(success=True, result=games_for_date)
        except Exception as e:
            error = f'Error: {repr(e)}'
            return dict(success=False, message=error)

def decode_bbref_boxscore(json_dict):
    """Convert json dictionary to BBRefBoxScore object."""
    if "__bbref_boxscore__" in json_dict:
        boxscore = BBRefBoxScore()
        boxscore.scrape_success = bool(json_dict['scrape_success'])
        boxscore.scrape_error = json_dict['scrape_error']
        boxscore.boxscore_url = json_dict['boxscore_url']
        boxscore.bbref_game_id = json_dict['bbref_game_id']
        boxscore.away_team_id_br = json_dict['away_team_id_br']
        boxscore.home_team_id_br = json_dict['home_team_id_br']
        boxscore.away_team_runs = int(json_dict['away_team_runs'])
        boxscore.home_team_runs = int(json_dict['home_team_runs'])
        boxscore.away_team_wins_before_game = int(json_dict['away_team_wins_before_game'])
        boxscore.away_team_losses_before_game = int(json_dict['away_team_losses_before_game'])
        boxscore.home_team_wins_before_game = int(json_dict['home_team_wins_before_game'])
        boxscore.home_team_losses_before_game = int(json_dict['home_team_losses_before_game'])
        boxscore.attendance = int(json_dict['attendance'])
        boxscore.park_name = json_dict['park_name']
        boxscore.game_duration = json_dict['game_duration']
        boxscore.day_night = json_dict['day_night']
        boxscore.field_type = json_dict['field_type']
        boxscore.first_pitch_temperature = int(json_dict['first_pitch_temperature'])
        boxscore.first_pitch_wind = json_dict['first_pitch_wind']
        boxscore.first_pitch_clouds = json_dict['first_pitch_clouds']
        boxscore.first_pitch_precipitation = json_dict['first_pitch_precipitation']
        boxscore.away_team_linescore_innings = [
            decode_bbref_inning_runs_scored(r)
            for r
            in json_dict['away_team_linescore_innings']
        ]
        boxscore.away_team_linescore_totals = decode_bbref_team_linescore_totals(
            json_dict['away_team_linescore_totals']
        )
        boxscore.home_team_linescore_innings = [
            decode_bbref_inning_runs_scored(r)
            for r
            in json_dict['home_team_linescore_innings']
        ]
        boxscore.home_team_linescore_totals = decode_bbref_team_linescore_totals(
            json_dict['home_team_linescore_totals']
        )
        boxscore.batting_stats = [
            decode_bbref_batting_stats(b)
            for b
            in json_dict['batting_stats']
        ]
        boxscore.pitching_stats = [
            decode_bbref_pitching_stats(p)
            for p
            in json_dict['pitching_stats']
        ]
        boxscore.umpires = [
            decode_bbref_umpire(u)
            for u
            in json_dict['umpires']
        ]
        boxscore.away_starting_lineup = [
            decode_bbref_lineup_slot(s)
            for s
            in json_dict['away_starting_lineup']
        ]
        boxscore.home_starting_lineup = [
            decode_bbref_lineup_slot(s)
            for s
            in json_dict['home_starting_lineup']
        ]
        boxscore.play_by_play = [
            decode_bbref_playbyplay_event(e)
            for e
            in json_dict['play_by_play']
        ]
        boxscore.inning_summaries = json_dict['inning_summaries']
        boxscore.player_dict = json_dict['player_dict']
        return boxscore

def decode_bbref_inning_runs_scored(json_dict):
    inning_runs = BBRefInningRunsScored()
    inning_runs.team_id_br = json_dict['team_id_br']
    inning_runs.inning = int(json_dict['inning'])
    inning_runs.runs = json_dict['runs']
    return inning_runs

def decode_bbref_team_linescore_totals(json_dict):
    team_totals = BBRefTeamLinescoreTotals()
    team_totals.team_id_br = json_dict['team_id_br']
    team_totals.total_runs = int(json_dict['total_runs'])
    team_totals.total_hits = int(json_dict['total_hits'])
    team_totals.total_errors = int(json_dict['total_errors'])
    return team_totals

def decode_bbref_batting_stats(json_dict):
    bat_stats = BBRefBatStats()
    bat_stats.player_id_br = json_dict['player_id_br']
    bat_stats.at_bats = int(json_dict['at_bats'])
    bat_stats.runs_scored = int(json_dict['runs_scored'])
    bat_stats.hits = int(json_dict['hits'])
    bat_stats.rbis = int(json_dict['rbis'])
    bat_stats.bases_on_balls = int(json_dict['bases_on_balls'])
    bat_stats.strikeouts = int(json_dict['strikeouts'])
    bat_stats.plate_appearances = int(json_dict['plate_appearances'])
    bat_stats.avg_to_date = float(json_dict['avg_to_date'])
    bat_stats.obp_to_date = float(json_dict['obp_to_date'])
    bat_stats.slg_to_date = float(json_dict['slg_to_date'])
    bat_stats.ops_to_date = float(json_dict['ops_to_date'])
    bat_stats.total_pitches = int(json_dict['total_pitches'])
    bat_stats.total_strikes = int(json_dict['total_strikes'])
    bat_stats.wpa_bat = float(json_dict['wpa_bat'])
    bat_stats.avg_lvg_index = float(json_dict['avg_lvg_index'])
    bat_stats.wpa_bat_pos = float(json_dict['wpa_bat_pos'])
    bat_stats.wpa_bat_neg = float(json_dict['wpa_bat_neg'])
    bat_stats.re24_bat = float(json_dict['re24_bat'])
    bat_stats.details = [
        decode_bbref_bat_stats_detail(d)
        for d
        in json_dict['details']
    ]
    return bat_stats

def decode_bbref_bat_stats_detail(json_dict):
    detail = BBRefBatStatsDetail()
    detail.count = int(json_dict['count'])
    detail.stat = json_dict['stat']
    return detail

def decode_bbref_pitching_stats(json_dict):
    pitch_stats = BBRefPitchStats()
    pitch_stats.player_id_br = json_dict['player_id_br']
    pitch_stats.innings_pitched = float(json_dict['innings_pitched'])
    pitch_stats.hits = int(json_dict['hits'])
    pitch_stats.runs = int(json_dict['runs'])
    pitch_stats.earned_runs = int(json_dict['earned_runs'])
    pitch_stats.bases_on_balls = int(json_dict['bases_on_balls'])
    pitch_stats.strikeouts = int(json_dict['strikeouts'])
    pitch_stats.homeruns = int(json_dict['homeruns'])
    pitch_stats.batters_faced = int(json_dict['batters_faced'])
    pitch_stats.pitch_count = int(json_dict['pitch_count'])
    pitch_stats.strikes = int(json_dict['strikes'])
    pitch_stats.strikes_contact = int(json_dict['strikes_contact'])
    pitch_stats.strikes_swinging = int(json_dict['strikes_swinging'])
    pitch_stats.strikes_looking = int(json_dict['strikes_looking'])
    pitch_stats.ground_balls = int(json_dict['ground_balls'])
    pitch_stats.fly_balls = int(json_dict['fly_balls'])
    pitch_stats.line_drives = int(json_dict['line_drives'])
    pitch_stats.unknown_type = int(json_dict['unknown_type'])
    pitch_stats.game_score = int(json_dict['game_score'])
    pitch_stats.inherited_runners = int(json_dict['inherited_runners'])
    pitch_stats.inherited_scored = int(json_dict['inherited_scored'])
    pitch_stats.wpa_pitch = float(json_dict['wpa_pitch'])
    pitch_stats.avg_lvg_index = float(json_dict['avg_lvg_index'])
    pitch_stats.re24_pitch = float(json_dict['re24_pitch'])
    return pitch_stats

def decode_bbref_umpire(json_dict):
    umpire = BBRefUmpire()
    umpire.field_location = json_dict['field_location']
    umpire.umpire_name = json_dict['umpire_name']
    return umpire

def decode_bbref_lineup_slot(json_dict):
    lineup_slot = BBRefStartingLineupSlot()
    lineup_slot.player_id_br = json_dict['player_id_br']
    lineup_slot.bat_order = int(json_dict['bat_order'])
    lineup_slot.def_position = json_dict['def_position']
    return lineup_slot

def decode_bbref_playbyplay_event(json_dict):
    pbp_event = BBRefPlayByPlayEvent()
    pbp_event.scrape_success = bool(json_dict['scrape_success'])
    pbp_event.scrape_error = json_dict['scrape_error']
    pbp_event.inning = json_dict['inning']
    pbp_event.score = json_dict['score']
    pbp_event.outs_before_play = int(json_dict['outs_before_play'])
    pbp_event.runners_on_base = json_dict['runners_on_base']
    pbp_event.pitch_sequence = json_dict['pitch_sequence']
    pbp_event.runs_outs_result = json_dict['runs_outs_result']
    pbp_event.team_batting_id_br = json_dict['team_batting_id_br']
    pbp_event.team_pitching_id_br = json_dict['team_pitching_id_br']
    pbp_event.play_description = json_dict['play_description']
    pbp_event.pitcher_id_br = json_dict['pitcher_id_br']
    pbp_event.batter_id_br = json_dict['batter_id_br']
    return pbp_event