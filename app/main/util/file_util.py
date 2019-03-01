"""Functions for reading and writing files."""
import json
import os
from pathlib import Path
from string import Template

from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.json_decoders import (
    decode_brooks_games_for_date, decode_bbref_games_for_date,
    decode_bbref_boxscore
)
from app.main.util.result import Result

T_BROOKS_GAMESFORDATE_FILENAME = 'brooks_games_for_date_${date}.json'
T_BBREF_GAMESFORDATE_FILENAME = 'bbref_games_for_date_${date}.json'
T_BBREF_BOXSCORE_FILENAME = '${gid}.json'

def write_brooks_games_for_date_to_file(games_for_date, folderpath=None):
    date_str = games_for_date.game_date_str
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME).substitute(date=date_str)
    json_dict = games_for_date.as_json()
    return write_json_dict_to_file(json_dict, filename, folderpath)

def write_bbref_games_for_date_to_file(games_for_date, folderpath=None):
    date_str = games_for_date.game_date_str
    filename = Template(T_BBREF_GAMESFORDATE_FILENAME).substitute(date=date_str)
    json_dict = games_for_date.as_json()
    return write_json_dict_to_file(json_dict, filename, folderpath)

def write_bbref_boxscore_to_file(boxscore, folderpath=None):
    game_id = boxscore.bbref_game_id
    filename = Template(T_BBREF_BOXSCORE_FILENAME).substitute(gid=game_id)
    json_dict = boxscore.as_json()
    return write_json_dict_to_file(json_dict, filename, folderpath)

def write_json_dict_to_file(json_dict, filename, folderpath=None):
    """Write BrooksGamesForDate object to a file in json format."""
    folderpath = folderpath if folderpath else Path.cwd()
    filepath = folderpath / filename
    try:
        filepath.write_text(json_dict)
        return Result.Ok(filepath)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)

def read_brooks_games_for_date_from_file(game_date, folderpath=None, delete_file=False):
    """Decode BrooksGamesForDate object from json file."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = game_date.strftime(DATE_ONLY)
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME).substitute(date=date_str)
    filepath = folderpath / filename
    try:
        contents = filepath.read_text()
        if delete_file:
            filepath.unlink()
        return decode_brooks_games_for_date(json.loads(contents))
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)

def read_bbref_games_for_date_from_file(game_date, folderpath=None, delete_file=False):
    """Decode BBRefGamesForDate object from json file."""
    folderpath = folderpath if folderpath else Path.cwd()
    date_str = game_date.strftime(DATE_ONLY)
    filename = Template(T_BBREF_GAMESFORDATE_FILENAME).substitute(date=date_str)
    filepath = folderpath / filename
    try:
        contents = filepath.read_text()
        if delete_file:
            filepath.unlink()
        return decode_bbref_games_for_date(json.loads(contents))
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)

def read_bbref_boxscore_from_file(bbref_game_id, folderpath=None, delete_file=False):
    """Decode BBRefBoxscore object from file."""
    folderpath = folderpath if folderpath else Path.cwd()
    filename = Template(T_BBREF_BOXSCORE_FILENAME).substitute(gid=bbref_game_id)
    filepath = folderpath / filename
    try:
        contents = filepath.read_text()
        if delete_file:
            filepath.unlink()
        return decode_bbref_boxscore(json.loads(contents))
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)