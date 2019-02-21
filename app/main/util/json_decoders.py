"""Decode json dicts of scraped data to custom objects."""
from datetime import datetime, timezone
from dateutil import parser, tz

from app.main.data.scrape.brooks.models.games_for_date import BrooksGamesForDate
from app.main.data.scrape.brooks.models.game_info import BrooksGameInfo

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
