"""Functions for reading and writing files."""
import json
import os
from pathlib import Path
from string import Template

from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.json_decoders import decode_brooks_games_for_date

T_BROOKS_GAMESFORDATE_FILENAME = 'brooks_games_for_date_${date}.json'

def write_brooks_games_for_date_to_file(games_for_date, folderpath=None):
    """Write BrooksGamesForDate object to a file in json format."""
    if not folderpath:
        folderpath = Path.cwd()
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME)\
        .substitute(date=games_for_date.game_date_str)
    filepath = folderpath / filename
    try:
        with open(filepath, 'w') as f:
            f.write(games_for_date.as_json())
        return dict(success=True, filepath=filepath)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return dict(success=False, message=error)

def read_brooks_games_for_date_from_file(game_date, folderpath=None, delete_file=False):
    """Decode BrooksGamesForDate object from json file."""
    if not folderpath:
        folderpath = Path.cwd()
    date_str = game_date.strftime(DATE_ONLY)
    filename = Template(T_BROOKS_GAMESFORDATE_FILENAME)\
        .substitute(date=date_str)
    filepath = folderpath / filename
    try:
        contents = filepath.read_text()
        if delete_file:
            filepath.unlink()
        return decode_brooks_games_for_date(json.loads(contents))
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return dict(success=False, message=error)